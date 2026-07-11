"""
training_dag.py — Model Training & Promotion DAG (Airflow 3.x, Defense 2)

Runs every 5 minutes. Pipeline:

    load_data -> train_model -> save_training_stats -> evaluate_candidate
        -> (branch) -> promote_to_champion -> notify_api_reload
                                            -> archive_data
                     -> alert_promotion_failed

1. load_data           — count rows accumulated in good_data/ since the last
                          training run; skip the DAG run if below threshold.
                          Combine the static base dataset + accumulated
                          good_data into one training CSV (written to disk —
                          not passed through XCom, see docstring below).
2. train_model          — train a candidate pipeline (model_service/app/model.py,
                          mounted read-only into this container so serving and
                          training share the exact same feature pipeline),
                          log params/metrics/model to MLflow, register a new
                          model version.
3. save_training_stats  — persist per-feature training statistics + run
                          metrics to Postgres (training_runs / training_stats)
                          so the drift dashboard has a baseline to compare
                          against.
4. evaluate_candidate   — compare candidate vs. current @champion on RMSE
                          (candidate must be <= champion) and inference
                          latency (must be < threshold). No champion yet ->
                          auto-promote (subject to the latency check).
5a. promote_to_champion — set the MLflow @champion alias, flip is_champion
                          in Postgres.
5b. notify_api_reload   — POST /reload-model so the API picks up the new
                          champion without a restart.
5c. archive_data        — move the good_data files consumed by this run into
                          archived_data/ (only reached after a successful
                          promotion; runs in parallel with notify_api_reload).
5d. alert_promotion_failed — Teams "ML Alerts" notification with the reason,
                          taken instead of 5a-5c when the candidate doesn't
                          beat the champion.
"""

from __future__ import annotations

import glob
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests as http_requests

from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

# model_service is mounted read-only at /opt/airflow/model_service so
# the training DAG reuses the exact same feature pipeline the API serves with
# (see docker-compose.yml). Import it directly rather than duplicating the
# sklearn Pipeline definition here.
sys.path.insert(0, "/opt/airflow/model_service")
from app.model import train_and_evaluate  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_DATASET_PATH = "/opt/airflow/data/cloud_outages_dataset.csv"
GOOD_DATA_DIR = "/opt/airflow/data/good_data"
ARCHIVED_DATA_DIR = "/opt/airflow/data/archived_data"
TRAINING_TMP_DIR = "/opt/airflow/data/.training_tmp"
TRACKER_FILE = "/opt/airflow/data/.last_training_timestamp"

API_URL = os.getenv("API_URL", "http://api:8000")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_NAME = os.getenv("MODEL_NAME", "cloud_outage_duration")
EXPERIMENT_NAME = "cloud_outage_duration_training_v2"

MIN_NEW_ROWS_FOR_TRAINING = int(os.getenv("MIN_NEW_ROWS_FOR_TRAINING", "50"))
INFERENCE_MS_THRESHOLD = float(os.getenv("INFERENCE_MS_THRESHOLD", "50"))
RMSE_PROMOTION_THRESHOLD = os.getenv("RMSE_PROMOTION_THRESHOLD", "").strip()

NUMERIC_STATS_FEATURES = ["system_load_before_outage", "number_of_customers_affected", "ticket_count"]
CATEGORICAL_STATS_FEATURES = ["cloud_provider", "service", "severity", "backup_system_triggered"]

ML_ALERTS_WEBHOOK_ENV = "ML_ALERTS_TEAMS_WEBHOOK_URL"


def _read_last_ts() -> float:
    try:
        if os.path.exists(TRACKER_FILE):
            return float(Path(TRACKER_FILE).read_text().strip())
    except Exception:
        pass
    return 0.0


def _write_last_ts(ts: float) -> None:
    Path(TRACKER_FILE).write_text(str(ts))


@dag(
    dag_id="training_dag",
    schedule="*/5 * * * *",          # every 5 minutes
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["training", "mlflow"],
)
def training_dag():

    # -----------------------------------------------------------------------
    # Task 1 — load_data
    # -----------------------------------------------------------------------
    @task()
    def load_data() -> dict:
        """
        Check how many rows have accumulated in good_data/ since the last
        training run. Skip the DAG run if under MIN_NEW_ROWS_FOR_TRAINING.

        Otherwise, combine the static base dataset with all currently
        accumulated good_data/ files into one training set. This combined
        set is written to a temp CSV rather than passed through XCom — the
        base dataset alone is 50k+ rows, well past what should go through
        the XCom metadata-DB backend.
        """
        last_ts = _read_last_ts()
        good_files = glob.glob(os.path.join(GOOD_DATA_DIR, "*.csv"))
        new_files = [f for f in good_files if os.path.getmtime(f) > last_ts]

        new_row_count = 0
        new_frames = []
        for f in new_files:
            try:
                fdf = pd.read_csv(f)
                new_row_count += len(fdf)
                new_frames.append(fdf)
            except Exception as e:
                print(f"[load_data] Could not read {f}: {e}")

        print(f"[load_data] {new_row_count} new row(s) across {len(new_files)} file(s) since last training run.")

        if new_row_count < MIN_NEW_ROWS_FOR_TRAINING:
            raise AirflowSkipException(
                f"Only {new_row_count} new rows accumulated (< {MIN_NEW_ROWS_FOR_TRAINING}) — skipping training run."
            )

        base_df = pd.read_csv(BASE_DATASET_PATH)
        combined = pd.concat([base_df] + new_frames, ignore_index=True)

        os.makedirs(TRAINING_TMP_DIR, exist_ok=True)
        run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        training_csv_path = os.path.join(TRAINING_TMP_DIR, f"training_{run_stamp}.csv")
        combined.to_csv(training_csv_path, index=False)

        max_ts = max((os.path.getmtime(f) for f in new_files), default=last_ts)

        return {
            "training_csv_path": training_csv_path,
            "consumed_files": new_files,
            "new_row_count": new_row_count,
            "base_row_count": len(base_df),
            "max_ts": max_ts,
        }

    # -----------------------------------------------------------------------
    # Task 2 — train_model
    # -----------------------------------------------------------------------
    @task()
    def train_model(load_result: dict) -> dict:
        """Train a candidate pipeline and log it to MLflow's model registry."""
        import mlflow

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment(EXPERIMENT_NAME)

        pipeline, metrics = train_and_evaluate(load_result["training_csv_path"])
        print(f"[train_model] candidate metrics: {metrics}")

        with mlflow.start_run() as run:
            mlflow.log_params({
                "n_estimators": 100,
                "random_state": 42,
                "n_train": metrics["n_train"],
                "n_test": metrics["n_test"],
            })
            mlflow.log_metrics({
                "rmse": metrics["rmse"],
                "mae": metrics["mae"],
                "r2": metrics["r2"],
                "inference_ms_per_row": metrics["inference_ms_per_row"],
            })
            model_info = mlflow.sklearn.log_model(
                pipeline, artifact_path="model", registered_model_name=MODEL_NAME
            )
            run_id = run.info.run_id
            model_version = str(model_info.registered_model_version)

        print(f"[train_model] Registered {MODEL_NAME} version {model_version} (run_id={run_id})")

        return {
            **load_result,
            "run_id": run_id,
            "model_version": model_version,
            "metrics": metrics,
        }

    # -----------------------------------------------------------------------
    # Task 3 — save_training_stats
    # -----------------------------------------------------------------------
    @task()
    def save_training_stats(train_result: dict) -> dict:
        """
        Persist per-feature training statistics + run metrics to Postgres.
        is_champion starts False; promote_to_champion flips it after a
        successful promotion. This gives the drift dashboard a baseline to
        compare live serving data against.
        """
        df = pd.read_csv(train_result["training_csv_path"])
        metrics = train_result["metrics"]

        pg = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg.get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO training_runs
                (model_name, model_version, is_champion, rmse, mae, r2,
                 inference_ms_per_row, n_train, n_test)
            VALUES (%s, %s, FALSE, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                MODEL_NAME,
                train_result["model_version"],
                metrics["rmse"],
                metrics["mae"],
                metrics["r2"],
                metrics["inference_ms_per_row"],
                metrics["n_train"],
                metrics["n_test"],
            ),
        )
        training_run_id = cur.fetchone()[0]

        for col in NUMERIC_STATS_FEATURES:
            if col not in df.columns:
                continue
            series = pd.to_numeric(df[col], errors="coerce").dropna()
            cur.execute(
                """
                INSERT INTO training_stats
                    (training_run_id, feature_name, feature_type, stat_mean, stat_std, stat_min, stat_max)
                VALUES (%s, %s, 'numeric', %s, %s, %s, %s)
                """,
                (training_run_id, col, float(series.mean()), float(series.std()),
                 float(series.min()), float(series.max())),
            )

        for col in CATEGORICAL_STATS_FEATURES:
            if col not in df.columns:
                continue
            dist = df[col].value_counts(normalize=True).to_dict()
            cur.execute(
                """
                INSERT INTO training_stats
                    (training_run_id, feature_name, feature_type, category_distribution)
                VALUES (%s, %s, 'categorical', %s::jsonb)
                """,
                (training_run_id, col, json.dumps(dist)),
            )

        conn.commit()
        cur.close()
        conn.close()
        print(f"[save_training_stats] training_run_id={training_run_id}")

        return {**train_result, "training_run_id": training_run_id}

    # -----------------------------------------------------------------------
    # Task 4 — evaluate_candidate
    # -----------------------------------------------------------------------
    @task()
    def evaluate_candidate(stats_result: dict) -> dict:
        """
        Compare the candidate against the current champion. Promotion
        criteria (both must hold):
          - candidate RMSE <= champion RMSE (no champion yet -> auto-pass)
          - candidate inference_ms_per_row < INFERENCE_MS_THRESHOLD
        """
        metrics = stats_result["metrics"]
        candidate_rmse = metrics["rmse"]
        candidate_latency = metrics["inference_ms_per_row"]

        pg = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg.get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT rmse FROM training_runs WHERE is_champion = TRUE ORDER BY run_timestamp DESC LIMIT 1"
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        champion_rmse = row[0] if row else -1.0

        reasons = []
        latency_ok = candidate_latency < INFERENCE_MS_THRESHOLD
        if not latency_ok:
            reasons.append(
                f"inference_ms_per_row={candidate_latency} >= threshold={INFERENCE_MS_THRESHOLD}"
            )

        if champion_rmse < 0:
            performance_ok = True
            reasons.append("no existing champion — first training run")
        else:
            performance_ok = candidate_rmse <= champion_rmse
            if not performance_ok:
                reasons.append(f"candidate_rmse={candidate_rmse} > champion_rmse={champion_rmse}")
            if RMSE_PROMOTION_THRESHOLD:
                abs_threshold = float(RMSE_PROMOTION_THRESHOLD)
                if candidate_rmse > abs_threshold:
                    performance_ok = False
                    reasons.append(f"candidate_rmse={candidate_rmse} > absolute threshold={abs_threshold}")

        promote = performance_ok and latency_ok
        reason = "; ".join(reasons) if reasons else "candidate met all promotion criteria"

        print(f"[evaluate_candidate] promote={promote} reason={reason}")

        return {
            **stats_result,
            "promote": promote,
            "reason": reason,
            "champion_rmse": champion_rmse,
            "candidate_rmse": candidate_rmse,
        }

    # -----------------------------------------------------------------------
    # Branch — decide_promotion
    # -----------------------------------------------------------------------
    @task.branch()
    def decide_promotion(evaluation: dict) -> str:
        return "promote_to_champion" if evaluation["promote"] else "alert_promotion_failed"

    # -----------------------------------------------------------------------
    # Task 5a — promote_to_champion
    # -----------------------------------------------------------------------
    @task()
    def promote_to_champion(evaluation: dict) -> dict:
        """Set the @champion alias in MLflow and flip is_champion in Postgres."""
        import mlflow
        from mlflow import MlflowClient

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        client = MlflowClient()
        client.set_registered_model_alias(MODEL_NAME, "champion", evaluation["model_version"])
        print(f"[promote_to_champion] {MODEL_NAME} version {evaluation['model_version']} is now @champion")

        pg = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg.get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE training_runs SET is_champion = FALSE WHERE is_champion = TRUE")
        cur.execute(
            "UPDATE training_runs SET is_champion = TRUE, promoted_at = NOW() WHERE id = %s",
            (evaluation["training_run_id"],),
        )
        conn.commit()
        cur.close()
        conn.close()

        return evaluation

    # -----------------------------------------------------------------------
    # Task 5b — notify_api_reload
    # -----------------------------------------------------------------------
    @task()
    def notify_api_reload(promote_result: dict) -> None:
        try:
            resp = http_requests.post(f"{API_URL}/reload-model", timeout=30)
            resp.raise_for_status()
            print(f"[notify_api_reload] API reloaded: {resp.json()}")
        except Exception as e:
            print(f"[notify_api_reload] Failed to reload API model: {e}")

    # -----------------------------------------------------------------------
    # Task 5c — archive_data
    # -----------------------------------------------------------------------
    @task()
    def archive_data(promote_result: dict) -> None:
        """Move the good_data files consumed by this training run into archived_data/."""
        os.makedirs(ARCHIVED_DATA_DIR, exist_ok=True)
        for fpath in promote_result["consumed_files"]:
            try:
                dest = os.path.join(ARCHIVED_DATA_DIR, os.path.basename(fpath))
                os.replace(fpath, dest)
            except FileNotFoundError:
                print(f"[archive_data] {fpath} already moved/removed — skipping.")
        _write_last_ts(promote_result["max_ts"])
        print(f"[archive_data] Archived {len(promote_result['consumed_files'])} file(s).")

    # -----------------------------------------------------------------------
    # Task 5d — alert_promotion_failed
    # -----------------------------------------------------------------------
    @task()
    def alert_promotion_failed(evaluation: dict) -> None:
        webhook_url = os.getenv(ML_ALERTS_WEBHOOK_ENV, "")
        champion_rmse_val = evaluation['champion_rmse']
        champion_rmse_str = f"{champion_rmse_val:.4f}" if champion_rmse_val >= 0 else "None"
        message_text = (
            f"Candidate v{evaluation['model_version']} NOT promoted. "
            f"candidate_rmse={evaluation['candidate_rmse']:.4f}, "
            f"champion_rmse={champion_rmse_str}. Reason: {evaluation['reason']}"
        )
        print(f"[alert_promotion_failed] {message_text}")

        if not webhook_url:
            print(f"[alert_promotion_failed] {ML_ALERTS_WEBHOOK_ENV} not set — skipping Teams alert.")
            return

        payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "FFA500",
            "summary": "ML Alert — Model Promotion Rejected",
            "sections": [{
                "activityTitle": "🟡 ML Alert — Candidate Model Not Promoted",
                "text": message_text,
                "markdown": True,
            }],
        }
        try:
            resp = http_requests.post(webhook_url, json=payload, timeout=10)
            resp.raise_for_status()
        except Exception as e:
            print(f"[alert_promotion_failed] Failed to send Teams alert: {e}")

    # ── Wiring ───────────────────────────────────────────────────────────────
    load_result = load_data()
    train_result = train_model(load_result)
    stats_result = save_training_stats(train_result)
    evaluation = evaluate_candidate(stats_result)

    branch = decide_promotion(evaluation)

    promote_result = promote_to_champion(evaluation)
    fail_alert = alert_promotion_failed(evaluation)

    branch >> [promote_result, fail_alert]

    notify_api_reload(promote_result)
    archive_data(promote_result)


training_dag()
