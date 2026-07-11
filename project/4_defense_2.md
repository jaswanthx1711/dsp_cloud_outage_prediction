# Defense 2 — ML Lifecycle & Monitoring

> For Docker, CI/CD, database, and Git workflow requirements, refer to the [project overview](0_project_overview.md).

For the second defense, you need to deliver the following components:

- **MLflow** server with model registry
- **Training DAG** (retrain, evaluate, promote)
- **API model reloading** via `/reload-model` endpoint (loads model from MLflow registry)
- **nginx** container to serve Great Expectations Data Docs HTML reports
- **Grafana** — Data quality monitoring dashboard (4+ panels)
- **Grafana** — Drift and prediction monitoring dashboard (4+ panels)
- **Grafana alerts** routed to the appropriate Teams channels
- **CD pipeline** — automated GitHub Release on merge to `main`

⚠️ **Defense 1 features must still be working.** Broken defense 1 components will be penalized again.

# Training job

To retrain your model with new data and promote it to production, you will develop a training pipeline using Airflow and MLflow.

## MLflow Integration

- Add an **MLflow server container** to your docker-compose
- Store artifacts locally using a **docker volume** (e.g., `./mlflow-artifacts:/mlflow/artifacts`)
- Use the **model registry** with **aliases** (not stages — stages are deprecated)
- Set `@champion` alias on the production model
- API loads model via: `models:/ModelName@champion`

## Training DAG (scheduled)

This DAG runs on a schedule (you choose the frequency).

**Bonus**: Trigger retraining when your drift monitoring detects significant drift AND enough new data has accumulated (both conditions are required — drift alone isn't enough, the model needs sufficient data to learn from).

```
load_data → train_model → save_training_stats → evaluate_candidate → (conditional) promote_to_champion ─┬→ notify_api_reload
                                                                                                        └→ archive_data
```

| Task | Description |
|------|-------------|
| load_data | Check if enough new data has accumulated in `good_data` since the last training run. If so, load the training dataset. If not, skip the DAG run. |
| train_model | Train model, log to MLflow (metrics + model) |
| save_training_stats | Save feature statistics to the database so your drift dashboard has a training baseline to compare against. Only the promoted model's training stats should serve as the drift baseline. You decide which statistics to compute and store |
| evaluate_candidate | Compare candidate vs champion accuracy on test set + check inference time < threshold |
| promote_to_champion | Set `@champion` alias in MLflow (only if evaluation passes) |
| notify_api_reload | Call API `/reload-model` endpoint to load new champion |
| archive_data | Move files from `good_data` to `archived_data` (only after successful promotion, runs in parallel with notify_api_reload) |

> Think about where your test set comes from and whether it allows a fair comparison between candidate and champion.

> Store the model version with each prediction so your dashboards can distinguish behavior before and after retraining.

## Promotion Criteria

The candidate model is promoted to champion only if:
- **Performance**: Candidate >= Champion on your chosen primary metric (e.g., accuracy, F1, RMSE). Matching or exceeding is sufficient — the new data better reflects current production conditions
- **Inference time**: Candidate inference time < threshold (you define the threshold based on your use case)

> Handle the case where no champion model exists yet (first training run).

## If Promotion Fails

If the candidate model does not meet the promotion criteria:
- Log the comparison results to MLflow
- Send an alert to `ML Alerts` Teams channel with the reason

---

# nginx

Use the nginx container to serve the Great Expectations Data Docs HTML reports. This allows clickable links in your Teams alert notifications (e.g., `http://localhost:8080/report_2026-01-15_14-30.html`).

---

# Monitoring dashboards

To monitor the quality of the ingested data and detect possible model issues, you will develop **2 dashboards** using `Grafana`.

Both dashboards should:
- Query data directly from the PostgreSQL database
- Update in near real-time (during the defense, graphs should change after each ingestion and prediction cycle)
- Use thresholds so that graph colors convey meaning (green ✅, orange ⚠️, red ❌)
- Use temporal queries — show trends over time windows, not all-time aggregates
- Have at least **4 graphs each**, all monitoring different aspects

## Ingested data monitoring dashboard

This dashboard is for the **data operations team**. It should help them monitor the quality of ingested data, spot emerging problems, and decide when to take action.

The data powering this dashboard comes from the error statistics you save to the database during the ingestion job. You decide what to store and how to structure it — your schema should support the kind of queries your panels need.

**Example insights this dashboard could surface:**
- Are certain error categories trending up or down over the last hour?
- What percentage of rows are invalid per ingestion, and is it getting worse?
- Which error types are most frequent in recent ingestions?

**What to avoid:**
- All-time aggregates with no temporal context (e.g., "total errors ever")
- Single stat panels that don't show when problems occurred

Your panels should cover the 5 error categories from your validation pipeline: completeness, validity, consistency, schema, type.

## Data drift and prediction issues dashboard

This dashboard is for **ML engineers and data scientists**. It should help them detect model issues (modeling, not technical) while the model operates in production.

The data powering this dashboard comes from:
- The **predictions table** (features + predictions saved by the API)
- **Training feature statistics** that you save after each training run (so you have a baseline to compare against)

You need to monitor at least:
- **1 numerical feature**: compare its serving distribution to the training baseline over time
- **1 categorical feature**: compare its serving distribution to the training baseline over time

**Requirements:**
- At least 2 graphs on **input features** (drift detection)
- At least 2 graphs on **predictions** (model behavior over time)

**Example insights this dashboard could surface:**
- A feature's mean has been drifting away from the training baseline over the last hour
- The model's prediction distribution has shifted (e.g., suddenly predicting one class much more often, or predicting unrealistic values)

**What to avoid:**

| Bad panel | Why |
|-----------|-----|
| Average feature value (all time) | No temporal context |
| Single "current drift %" stat | Doesn't show trend |
| Predictions per hour | Infrastructure metric, not model behavior |
| API latency/response time | Operational metric, not ML monitoring |

## Grafana Alerts

Configure at least **2 Grafana alerts per dashboard** for critical situations (e.g., all ingested data has errors, model outputting constant predictions, significant drift detected).

Alerts must route to the appropriate Teams channel:

| Channel | Alert types |
|---------|-------------|
| `Data Quality Alerts` | Ingestion errors, validation failures |
| `ML Alerts` | Drift detected, prediction anomalies |

---

## Defense Format, Rules & Deliverables

Refer to the [Defense Instructions](5_defense_instructions.md) for the full defense format, rules, preparation checklist, and deliverables.

Good luck 🤞 🍀
