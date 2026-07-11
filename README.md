# Cloud Outage Prediction System

This is our end-to-end Machine Learning production system. We built it to predict **how long a cloud outage will last (in hours)** based on incident data like severity, system load, and affected customers. This project was developed for the **Data Science in Production** course (Defense 1 + Defense 2).

---

## Architecture Overview

The system is fully containerized and consists of six layers:

1.  **User Interface:** A Streamlit WebApp for manual predictions and viewing historical data.
2.  **Serving Layer:** A FastAPI Model API that loads the current `@champion` model from the MLflow registry and saves every prediction to PostgreSQL via SQLAlchemy.
3.  **Data Orchestration:** Apache Airflow 3 pipelines for automated ingestion, scheduled predictions, and scheduled retraining.
4.  **Data Quality:** Great Expectations (GX v1.x) integrated into the ingestion DAG to stop bad data before it hits the model — the Checkpoint result itself drives the good/bad row split, not a duplicate hand-rolled check.
5.  **Model Lifecycle:** MLflow tracking server + model registry. The training DAG retrains, evaluates, and promotes candidate models using `@champion`/`@candidate` aliases.
6.  **Monitoring:** Grafana dashboards reading directly from PostgreSQL for data quality and model drift/prediction monitoring, with alerts routed to Teams.

```mermaid
graph TD
    User((User)) --> WebApp[Streamlit WebApp]
    WebApp -->|HTTP| API[FastAPI Model API]
    API -->|ORM| DB[(PostgreSQL DB)]
    API -->|models:/name@champion| MLflow[(MLflow Registry)]

    subgraph Airflow_3_Orchestration
        IngestDAG[Ingestion DAG] -->|Every 1m| GX[GX Validation]
        GX -->|Success| GoodData[data/good_data]
        GX -->|Fail| BadData[data/bad_data]

        PredictDAG[Prediction DAG] -->|Every 2m| GoodData
        PredictDAG -->|Single batch call| API

        TrainDAG[Training DAG] -->|Every 5m| GoodData
        TrainDAG -->|log run + model| MLflow
        TrainDAG -->|promote @champion| MLflow
        TrainDAG -->|/reload-model| API
        TrainDAG -->|archive consumed files| ArchivedData[data/archived_data]
    end

    DB --> Grafana[Grafana Dashboards]
    Grafana -->|alerts| Teams[Teams Channels]
```

---

## Project Structure

*   `airflow/`: Docker configuration and initialization for Airflow 3.
*   `dags/`: Automated workflows — `ingestion_dag.py`, `prediction_dag.py`, `training_dag.py`.
*   `data/`: Data storage (`raw_data`, `good_data`, `bad_data`, `archived_data`).
*   `gx/`: Great Expectations v1.x config.
*   `model_service/`: FastAPI backend and ML pipeline code (`app/model.py` is also mounted read-only into the Airflow containers so the training DAG reuses the exact same feature pipeline the API serves with).
*   `webapp/`: Streamlit frontend code.
*   `grafana/`: Datasource, dashboard, and alerting provisioning (auto-loaded on container start).
*   `db/`: `init.sql` — schema for `predictions`, `ingestion_stats`, `training_runs`, `training_stats`.
*   `tests/`: Automated unit tests.
*   `.github/`: CI (lint + test on PRs to `develop`/`main`) and CD (GitHub Release on merge to `main`) workflows.

---

## Quick Setup & Demo Guide

### 1. Prerequisites
Make sure you have **Docker Desktop** installed and running.

### 2. Environment Configuration
Copy the template and generate your secret keys:
```bash
cp .env.example .env
```
> **Tip:** Open `.env` and fill in `AIRFLOW_FERNET_KEY` and `AIRFLOW_SECRET_KEY` using the generation commands in the file's comments. Also set `TEAMS_WEBHOOK_URL` (Data Quality Alerts channel) and `ML_ALERTS_TEAMS_WEBHOOK_URL` (ML Alerts channel) — Grafana alerts and DAG Teams notifications are silent without them.

### 3. Prepare the Demo Data
```bash
# Split the main dataset into files of exactly 10 rows each inside /data/raw_data
python split_dataset.py --input data/cloud_outages_dataset.csv --output data/raw_data --num-files 30

# (Optional) Inject errors to test the Great Expectations validation
python generate_data_issues.py --input data/cloud_outages_dataset.csv --output data/raw_data/corrupted.csv --probability 0.4
```

### 4. Launch the System
```bash
docker compose up --build -d
```

> **If you're upgrading an existing environment** (a `postgres_data` volume from before Defense 2): Postgres only runs `db/init.sql` on an empty volume, so the new `training_runs`/`training_stats` tables won't appear automatically. Apply them once with:
> ```bash
> docker compose exec -T db psql -U <POSTGRES_USER> -d <POSTGRES_DB> < db/init.sql
> ```
> (Safe to re-run — every statement is `IF NOT EXISTS`.)

### 5. Before the defense
- Empty `data/good_data/` and `data/bad_data/` so the pipelines demo from a clean state.
- Let the training DAG run at least once beforehand (every 5 minutes, needs `MIN_NEW_ROWS_FOR_TRAINING` new rows in `good_data`) so a `@champion` model and drift baseline already exist — the Grafana drift dashboard needs `training_stats` populated to show anything meaningful.
- Generate a few days of demo traffic beforehand so both dashboards have real trend lines, not empty panels.

---

## Service Dashboard

Once the containers are healthy, you can access everything here:

| Service | Address | Login (if req.) |
| :--- | :--- | :--- |
| **Streamlit Webapp** | [http://localhost:8501](http://localhost:8501) | — |
| **Airflow 3 UI** | [http://localhost:8080](http://localhost:8080) | `admin` / `admin` |
| **FastAPI Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | — |
| **GX Data Docs** | [http://localhost:8090](http://localhost:8090) | — |
| **MLflow UI** | [http://localhost:5001](http://localhost:5001) | — |
| **Grafana** | [http://localhost:3000](http://localhost:3000) | `admin` / value of `GRAFANA_ADMIN_PASSWORD` (default `admin`) |

---

## Model Lifecycle (Defense 2)

*   **Model:** Random Forest Regressor (Sklearn), pipeline defined once in `model_service/app/model.py` and shared by both the API and the training DAG.
*   **Registry:** MLflow model registry, model name from `MODEL_NAME` env var (default `cloud_outage_duration`). The API loads `models:/<MODEL_NAME>@champion` at startup and via `POST /reload-model`.
*   **Bootstrap fallback:** If no `@champion` exists yet (first ever startup, before the training DAG has run), the API trains a one-off local model from the static dataset so it's usable immediately.
*   **Training DAG (`training_dag.py`, every 5 min):** `load_data` (skips if `good_data` hasn't accumulated `MIN_NEW_ROWS_FOR_TRAINING` new rows) → `train_model` (logs params/metrics/model to MLflow) → `save_training_stats` (per-feature baseline for the drift dashboard) → `evaluate_candidate` → branch: `promote_to_champion` (sets the MLflow alias, flips `is_champion` in Postgres) → `notify_api_reload` + `archive_data` in parallel, or `alert_promotion_failed` (Teams "ML Alerts").
*   **Promotion criteria:** candidate RMSE `<=` champion RMSE (no champion yet → auto-promote) **and** `inference_ms_per_row < INFERENCE_MS_THRESHOLD`. No champion case handled explicitly.
*   **Anomaly Detection:** Any outage predicted to last **> 5 hours** is flagged as a critical anomaly.

---

## Team & Responsibilities

We divided the work according to specialized domains to ensure the highest code quality:

*   **Member 1:** (Me) - Infrastructure, Docker Orchestration, Airflow 3 Migration, and CI/CD setup.
*   **Member 2:** FastAPI Backend Developer & Great Expectations Validation lead.
*   **Member 3:** Frontend Specialist (Streamlit) and UI/API Integration.
*   **Member 4:** Data Engineer (Split & Error scripts) and Quality Assurance (Pytest).

---

## CI/CD and Git Workflow

We use a professional **Feature Branch** workflow:
1.  Develop on `feature/*` branches, branched from `develop`.
2.  Push to trigger **CI** (`.github/workflows/ci.yml`): Flake8 lint and pytest, as two separate steps, on PRs to `develop`/`main`.
3.  Merge to `develop` via Pull Requests only after CI passes.
4.  Merge `develop` into `main` before each defense — this triggers **CD** (`.github/workflows/cd.yml`), which auto-bumps a semantic version tag from conventional commit prefixes and publishes a GitHub Release with auto-generated notes.
