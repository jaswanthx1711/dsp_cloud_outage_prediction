"""
ingestion_dag.py — Data Ingestion DAG (Airflow 3.x)

Runs every 1 minute. Pipeline:
1. read_data        — pick a random CSV from raw_data/, delete it, push via XCom
2. validate_data    — Great Expectations v1.x Checkpoint + row-level checks
3a. save_statistics — PostgresHook → ingestion_stats table
3b. send_alerts     — Teams webhook (medium/high criticality only)
3c. split_and_save_data — route rows to good_data/ or bad_data/

Tasks 3a, 3b, 3c run in parallel after 2.
"""

from __future__ import annotations

import json
import os
import glob
import random
from datetime import datetime, timezone

import pandas as pd
import requests as http_requests

# Airflow 3: imports consolidated under airflow.sdk
from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RAW_DATA_DIR = "/opt/airflow/data/raw_data"
GOOD_DATA_DIR = "/opt/airflow/data/good_data"
BAD_DATA_DIR = "/opt/airflow/data/bad_data"
GX_ROOT_DIR = "/opt/airflow/gx"

TEAMS_WEBHOOK_ENV = "TEAMS_WEBHOOK_URL"

VALID_CLOUD_PROVIDERS = {"AWS", "GCP", "Azure", "IBM", "Oracle", "Alibaba"}
REQUIRED_COLUMNS = [
    "cloud_provider", "service", "severity",
    "system_load_before_outage", "number_of_customers_affected",
    "ticket_count", "backup_system_triggered",
]


def compute_criticality(total_rows: int, invalid_rows: int, has_schema_error: bool) -> str:
    """Return 'high', 'medium', 'low', or 'none'."""
    if has_schema_error:
        return "high"
    if total_rows == 0:
        return "low"
    pct = invalid_rows / total_rows
    if pct > 0.50:
        return "high"
    if pct >= 0.10:
        return "medium"
    if pct > 0:
        return "low"
    return "none"


# ---------------------------------------------------------------------------
# DAG definition (Airflow 3 decorator syntax)
# ---------------------------------------------------------------------------

@dag(
    dag_id="ingestion_dag",
    schedule="* * * * *",           # every minute
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["ingestion", "data-quality"],
)
def ingestion_dag():

    # -----------------------------------------------------------------------
    # Task 1 — read_data
    # -----------------------------------------------------------------------
    @task()
    def read_data() -> dict:
        """Pick a random CSV from raw_data/, read it, delete it. Return via XCom."""
        files = glob.glob(os.path.join(RAW_DATA_DIR, "*.csv"))
        if not files:
            raise AirflowSkipException("No files in raw_data/ — skipping run.")

        chosen = random.choice(files)
        df = pd.read_csv(chosen)
        os.remove(chosen)
        print(f"[read_data] Read and deleted: {chosen} ({len(df)} rows)")

        return {
            "file_path": chosen,
            "filename": os.path.basename(chosen),
            "data_json": df.to_json(orient="records"),
            "columns": list(df.columns),
        }

    # -----------------------------------------------------------------------
    # Task 2 — validate_data
    # -----------------------------------------------------------------------
    @task()
    def validate_data(file_info: dict) -> dict:
        """
        Run a GX v1.x Checkpoint and derive row-level validity directly from its
        result (Checkpoint -> ValidationDefinition -> ExpectationSuite result),
        instead of re-implementing the same checks by hand.

        NOTE: `partial_unexpected_index_list` (GX's per-expectation list of failing
        row indices) is capped at 20 entries by default. Since raw_data files are
        always exactly 10 rows (see split_dataset.py), this cap never truncates —
        every failing row in a batch is captured.

        Only "wrong_type" (system_load_before_outage must parse as numeric) stays
        as a manual row-level pass: GX's type expectations key off the pandas
        column dtype, which collapses to `object` once a single bad string lands
        in an otherwise-numeric column, so GX can't attribute the failure to a
        specific row for mixed-type JSON-sourced columns.
        """
        import great_expectations as gx
        from great_expectations.expectations import (
            ExpectColumnToExist,
            ExpectColumnValuesToBeBetween,
            ExpectColumnValuesToBeInSet,
            ExpectColumnValuesToNotBeNull,
        )

        data_json = file_info["data_json"]
        columns = file_info["columns"]
        filename = file_info["filename"]

        df = pd.read_json(data_json, orient="records")
        total_rows = len(df)

        # ── Schema check (file-level) ─────────────────────────────────────────
        missing_cols = [c for c in REQUIRED_COLUMNS if c not in columns]
        has_schema_error = bool(missing_cols)

        # Expectation type + column -> our error taxonomy label
        EXPECTATION_LABELS = {
            ("expect_column_values_to_not_be_null", None): "null_value",
            ("expect_column_values_to_be_between", "ticket_count"): "out_of_range",
            ("expect_column_values_to_be_between", "duration_minutes"): "statistical_outlier",
            ("expect_column_values_to_be_in_set", "cloud_provider"): "invalid_categorical",
        }

        row_errors: dict[int, list[str]] = {}
        error_type_counts: dict[str, int] = {}

        def add_error(idx: int, label: str) -> None:
            row_errors.setdefault(int(idx), [])
            if label not in row_errors[int(idx)]:
                row_errors[int(idx)].append(label)
            error_type_counts[label] = error_type_counts.get(label, 0) + 1

        # ── Manual check: wrong_type (see docstring — GX can't do this per-row) ─
        if "system_load_before_outage" in df.columns:
            for idx, val in df["system_load_before_outage"].items():
                if val is not None and not (isinstance(val, float) and pd.isna(val)):
                    try:
                        float(val)
                    except (TypeError, ValueError):
                        add_error(idx, "wrong_type")

        # ── GX v1.x Checkpoint — source of truth for the other 4 categories ────
        report_url = ""
        gx_ran_successfully = False
        try:
            context = gx.get_context(mode="file", project_root_dir=GX_ROOT_DIR)

            datasource = context.data_sources.add_or_update_pandas(name="ingestion_pandas")
            asset = datasource.add_dataframe_asset(name=f"batch_{filename}")
            batch_def = asset.add_batch_definition_whole_dataframe(name="whole")

            expectations = [ExpectColumnToExist(column=col) for col in REQUIRED_COLUMNS]
            for col in REQUIRED_COLUMNS:
                if col in columns:
                    expectations.append(
                        ExpectColumnValuesToNotBeNull(column=col, result_format="COMPLETE")
                    )
            if "ticket_count" in columns:
                expectations.append(
                    ExpectColumnValuesToBeBetween(
                        column="ticket_count", min_value=0, result_format="COMPLETE"
                    )
                )
            if "cloud_provider" in columns:
                expectations.append(
                    ExpectColumnValuesToBeInSet(
                        column="cloud_provider",
                        value_set=list(VALID_CLOUD_PROVIDERS),
                        result_format="COMPLETE",
                    )
                )
            if "duration_minutes" in columns:
                expectations.append(
                    ExpectColumnValuesToBeBetween(
                        column="duration_minutes", max_value=100_000, result_format="COMPLETE"
                    )
                )

            suite = gx.ExpectationSuite(name="ingestion_suite")
            suite.expectations = expectations
            suite = context.suites.add_or_update(suite)

            vd_name = f"vd_{filename.replace('.', '_').replace('-', '_')}"
            try:
                vd = context.validation_definitions.get(vd_name)
                vd.suite = suite
                vd.data = batch_def
                context.validation_definitions.update(vd)
            except Exception:
                vd = context.validation_definitions.add(
                    gx.ValidationDefinition(name=vd_name, data=batch_def, suite=suite)
                )

            cp_name = f"cp_{filename.replace('.', '_').replace('-', '_')}"
            try:
                checkpoint = context.checkpoints.get(cp_name)
            except Exception:
                checkpoint = context.checkpoints.add(
                    gx.Checkpoint(name=cp_name, validation_definitions=[vd])
                )

            checkpoint_result = checkpoint.run(batch_parameters={"dataframe": df})
            gx_ran_successfully = True

            # ── Extract row-level failures straight from the Checkpoint result ──
            for validation_result in checkpoint_result.run_results.values():
                for exp_result in validation_result.results:
                    if exp_result.success:
                        continue
                    exp_type = exp_result.expectation_config.type
                    if exp_type == "expect_column_to_exist":
                        continue  # already covered by has_schema_error
                    column = exp_result.expectation_config.kwargs.get("column")
                    label = EXPECTATION_LABELS.get((exp_type, column)) or EXPECTATION_LABELS.get(
                        (exp_type, None)
                    )
                    if not label:
                        continue
                    bad_idx = exp_result.result.get("partial_unexpected_index_list") or []
                    for idx in bad_idx:
                        add_error(idx, label)

            context.build_data_docs()

            try:
                sites = context.get_docs_sites_urls()
                if sites:
                    report_url = sites[0].get("site_url", "")
            except Exception:
                pass

        except Exception as gx_err:
            print(f"[validate_data] GX error (non-fatal): {gx_err}")

        if not gx_ran_successfully:
            # Fallback so a GX outage doesn't stop the pipeline: apply the same
            # 4 checks by hand so ingestion can still route good/bad rows.
            print("[validate_data] GX checkpoint failed — falling back to manual row checks.")
            for idx, row in df.iterrows():
                for col in REQUIRED_COLUMNS:
                    if col in df.columns and pd.isna(row.get(col)):
                        add_error(idx, "null_value")
                        break
                if "ticket_count" in df.columns:
                    try:
                        if float(row.get("ticket_count")) < 0:
                            add_error(idx, "out_of_range")
                    except (TypeError, ValueError):
                        pass
                if "cloud_provider" in df.columns:
                    cp = row.get("cloud_provider")
                    if isinstance(cp, str) and cp not in VALID_CLOUD_PROVIDERS:
                        add_error(idx, "invalid_categorical")
                if "duration_minutes" in df.columns:
                    try:
                        if float(row.get("duration_minutes")) > 100_000:
                            add_error(idx, "statistical_outlier")
                    except (TypeError, ValueError):
                        pass

        if has_schema_error:
            error_type_counts["missing_column"] = len(missing_cols)

        invalid_rows = len(row_errors)
        valid_rows = total_rows - invalid_rows
        criticality = compute_criticality(total_rows, invalid_rows, has_schema_error)

        print(
            f"[validate_data] {filename}: total={total_rows}, "
            f"invalid={invalid_rows}, criticality={criticality}, "
            f"gx_driven={gx_ran_successfully}"
        )

        return {
            "filename": filename,
            "data_json": data_json,
            "total_rows": total_rows,
            "valid_rows": valid_rows,
            "invalid_rows": invalid_rows,
            "criticality": criticality,
            "error_types": error_type_counts,
            "row_errors_json": json.dumps({str(k): v for k, v in row_errors.items()}),
            "has_schema_error": has_schema_error,
            "report_url": report_url,
        }

    # -----------------------------------------------------------------------
    # Task 3a — save_statistics
    # -----------------------------------------------------------------------
    @task()
    def save_statistics(validation_result: dict) -> None:
        """Write ingestion summary to ingestion_stats table via PostgresHook."""
        pg = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg.get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO ingestion_stats
                (filename, total_rows, valid_rows, invalid_rows, criticality, error_types)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                validation_result["filename"],
                validation_result["total_rows"],
                validation_result["valid_rows"],
                validation_result["invalid_rows"],
                validation_result["criticality"],
                json.dumps(validation_result["error_types"]),
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"[save_statistics] Saved stats for {validation_result['filename']}")

    # -----------------------------------------------------------------------
    # Task 3b — send_alerts
    # -----------------------------------------------------------------------
    @task()
    def send_alerts(validation_result: dict) -> None:
        """Send Teams webhook for medium/high criticality. Skip silently for low/none."""
        criticality = validation_result["criticality"]
        if criticality not in ("medium", "high"):
            print(f"[send_alerts] criticality={criticality} — no alert needed.")
            return

        webhook_url = os.getenv(TEAMS_WEBHOOK_ENV, "")
        if not webhook_url:
            print("[send_alerts] TEAMS_WEBHOOK_URL not set — skipping.")
            return

        filename = validation_result["filename"]
        total = validation_result["total_rows"]
        invalid = validation_result["invalid_rows"]
        error_types = validation_result["error_types"]
        report_url = validation_result["report_url"]
        pct = round(invalid / total * 100, 1) if total else 0
        error_summary = ", ".join(f"{k}: {v}" for k, v in error_types.items()) or "none"

        emoji = "🔴" if criticality == "high" else "🟡"
        message = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "FF0000" if criticality == "high" else "FFA500",
            "summary": f"Data Quality Alert — {criticality.upper()}",
            "sections": [{
                "activityTitle": f"{emoji} Data Quality Alert — {criticality.upper()}",
                "activitySubtitle": f"File: `{filename}`",
                "facts": [
                    {"name": "Total Rows", "value": str(total)},
                    {"name": "Invalid Rows", "value": f"{invalid} ({pct}%)"},
                    {"name": "Error Types", "value": error_summary},
                    {"name": "Report", "value": report_url or "N/A"},
                ],
                "markdown": True,
            }],
        }

        try:
            resp = http_requests.post(webhook_url, json=message, timeout=10)
            resp.raise_for_status()
            print(f"[send_alerts] Teams alert sent — {criticality}")
        except Exception as e:
            print(f"[send_alerts] Failed: {e}")

    # -----------------------------------------------------------------------
    # Task 3c — split_and_save_data
    # -----------------------------------------------------------------------
    @task()
    def split_and_save_data(validation_result: dict) -> None:
        """Route rows: all-good → good_data, all-bad → bad_data, mixed → both."""
        os.makedirs(GOOD_DATA_DIR, exist_ok=True)
        os.makedirs(BAD_DATA_DIR, exist_ok=True)

        df = pd.read_json(validation_result["data_json"], orient="records")
        row_errors: dict[int, list] = {
            int(k): v
            for k, v in json.loads(validation_result["row_errors_json"]).items()
        }
        filename = validation_result["filename"]
        total_rows = validation_result["total_rows"]
        invalid_rows = validation_result["invalid_rows"]

        bad_idx = set(row_errors.keys())
        good_df = df[~df.index.isin(bad_idx)]
        bad_df = df[df.index.isin(bad_idx)]

        if invalid_rows == 0:
            df.to_csv(os.path.join(GOOD_DATA_DIR, filename), index=False)
            print(f"[split] All good → good_data/{filename}")
        elif invalid_rows == total_rows:
            df.to_csv(os.path.join(BAD_DATA_DIR, filename), index=False)
            print(f"[split] All bad → bad_data/{filename}")
        else:
            base, ext = os.path.splitext(filename)
            good_df.to_csv(os.path.join(GOOD_DATA_DIR, f"{base}_good{ext}"), index=False)
            bad_df.to_csv(os.path.join(BAD_DATA_DIR, f"{base}_bad{ext}"), index=False)
            print(f"[split] Mixed: {len(good_df)} good, {len(bad_df)} bad")

    # ── Wiring ───────────────────────────────────────────────────────────────
    file_info = read_data()
    validation_result = validate_data(file_info)

    # Parallel downstream tasks
    save_statistics(validation_result)
    send_alerts(validation_result)
    split_and_save_data(validation_result)


ingestion_dag()
