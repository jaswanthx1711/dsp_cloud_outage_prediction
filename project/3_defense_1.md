# Defense 1 — Data Pipeline & Serving

For the first defense, you need to deliver the following components:

- **Docker compose** with all Defense 1 services containerized
- **Git workflow + CI** pipeline (linting + tests)
- **Database** with predictions and ingestion stats tables
- **API** serving predictions (model loaded from file, no MLflow yet)
- **Streamlit UI** with prediction and past predictions pages
- **Data error generation script**
- **Data splitting script**
- **Ingestion DAG** (complete with Great Expectations validation, stats, and alerts)
- **Prediction DAG**

> For Docker, CI, database, and Git workflow requirements, refer to the [project overview](0_project_overview.md).

<p align="center">
    <img
        src="../images/project-defense-1.png"
        alt="Project architecture"
    />
</p>

# API — FastAPI Implementation Requirements

These are the FastAPI features you must use when building the model service API:

| Feature | Why | Used For |
|---------|-----|----------|
| Request body with Pydantic | Validate and document input data automatically | Prediction input - ensures correct types and required fields |
| Response models | Consistent, documented output format | Return predictions with proper structure |
| Status codes | Proper HTTP semantics (client vs server errors) | 400 for bad input, 404 for not found, 422 for validation errors |
| Model loading at startup | Load model once when API starts (not on every request!) | Defense 1: load from file. Hint: look up `lifespan` in FastAPI docs |
| Runtime model reloading (Defense 2) | Swap model without restarting the container | `/reload-model` endpoint — load new champion from MLflow registry |
| Error handling (`HTTPException`) | Proper error responses with meaningful messages | Bad input, model not found, prediction failures |
| Dependency injection (`Depends()`) | Reusable components, clean code | Database session management |

> ⚠️ Be careful mixing `async def` and synchronous code (e.g., blocking database calls). If unsure, use regular `def` for endpoints that do synchronous I/O — FastAPI handles them correctly in a thread pool.

# Database — SQLAlchemy Implementation Details

## SQLAlchemy Requirements

- Use **SQLAlchemy ORM** to define tables as Python classes
- Use **environment variables** for database connection string (not hardcoded):

```python
# ❌ Bad - hardcoded
DATABASE_URL = "postgresql://user:password123@localhost/dsp"

# ✅ Good - from environment
import os
DATABASE_URL = os.getenv("DATABASE_URL")
```

---

# Data ingestion job

To simulate having a continuous flow of data, you will be developing a data ingestion job for:

- Ingesting new data every minute
- Validating the data quality of the ingested data
- Raising alerts if any data quality issue
- Saving data issues in the database for monitoring

## Script to generate data issues

Your clean dataset won't have enough errors to test your validation pipeline. Create a Python script that:

1. Takes your clean dataset as input
2. Randomly injects errors into rows with **configurable probability per error type**
3. Outputs a corrupted dataset

**Why random injection?** Each generated file will have a different mix of errors, creating realistic variation in your Grafana dashboards (e.g., "missing values trending up", "10-30% invalid rows per batch").

### Required error types (implement all 5):

| Category | Error Type | Level | Example |
|----------|-----------|-------|---------|
| Completeness | Null values in required column | Row | `customer_id` is empty |
| Validity | Value outside valid range | Row | `age = -5` or `age = 200` |
| Consistency | Invalid categorical value | Row | `country = "XYZ"` not in allowed list |
| Schema | Missing required column | File | `price` column doesn't exist |
| Type | Wrong data type | Row | `"abc"` in numeric column |

### Additional error types (choose 2 or more based on your dataset):

| Error Type | Level | Example |
|------------|-------|---------|
| Duplicate primary keys | Row | Same `order_id` appears twice |
| Format mismatch (regex) | Row | Invalid email: `user@` |
| Cross-field inconsistency | Row | `end_date < start_date` |
| Impossible values | Row | Birth date in the future |
| Statistical outliers | Row | `price = 0.001` or `quantity = 999999` |

**Total: minimum 7 error types**

During the follow-up session, you will demo the different error types being detected.

## Script to split the dataset

To ingest new data, you first need to split your main dataset in multiple files and store them in a folder named *raw_data* that will feed your ingestion job as shown below. To do that, you need to create a python script that takes your dataset path, the path of *raw_data* folder and **the number of files to generate** (**not** the number of rows per file)

> ⚠️ Each generated file must contain exactly **10 rows**. Not respecting this will result in a penalty.

**Tip**: Generate enough files to cover your demo duration. If ingestion runs every 1 minute and your demo is 20 minutes, generate at least 25-30 files.

<p align="center">
    <img
        src="../images/project-data-preparation-for-ingestion-job.png"
        alt="Data preparation for ingestion job"
    />
</p>

## Data ingestion DAG

In the data ingestion job, at each DAG execution, you will:

1. **`read_data`**: Read **randomly** one file from the *raw_data* folder (delete the file afterward to avoid re-processing), return its content along with the file path

2. **`validate_data`**: Validate data quality using *Great Expectations*, determine error criticality

3. **`save_statistics`**: Save statistics about detected data problems to the database. Store summary statistics (not raw rows) — what you store and how you structure it should support the queries your Grafana data quality dashboard needs.

4. **`send_alerts`**: Generate validation report using **Great Expectations Data Docs** (not manual HTML!) and send Teams notification to `Data Quality Alerts` channel with:
   - Criticality of the data problem:
     - **High**: missing column OR >50% invalid rows
     - **Medium**: 10-50% invalid rows  
     - **Low**: >0% and <10% invalid rows
   - Summary of the errors
   - Report file name (clickable link via nginx is added in Defense 2)
   
   **Note**: No issues detected → no alert. Only send Teams notifications for **medium or high** criticality to avoid alert fatigue.

5. **`split_and_save_data`**: Split the ingested file if needed:
   - No data quality issues → move to *good_data*
   - All rows have problems → move to *bad_data*
   - Some rows have problems → split into 2 files (one for each folder)
    <p align="center">
        <img
            src="../images/project-data-quality-validation.png"
            alt="Split good and bad data"
        />
    </p>

Tasks `save_statistics`, `send_alerts`, and `split_and_save_data` run in parallel after `validate_data`.

<p align="center">
    <img
        src="../images/project-ingestion-dag.png"
        alt="Ingestion dag"
    />
</p>

### Airflow Requirements

These features apply to both the ingestion and prediction DAGs:

| Feature | Why | Used For |
|---------|-----|----------|
| XComs | Pass data between tasks (not local files) | Share file paths, validation results |
| Airflow Connections | Secure credential management (not hardcoded) | Store database connection details |
| PostgresHook | Database access the Airflow way | Save ingestion stats to database |

### Great Expectations Required Features

> Use Great Expectations **v1.x** (GX Core) — do not follow v0.x tutorials, the API is completely different.

| Feature | Why | Used For |
|---------|-----|----------|
| Expectation Suite | Group expectations together | Define all validation rules for your dataset |
| Validation Definition | Pair data with expectations | Associates a Batch Definition with an Expectation Suite |
| Checkpoint | Orchestrate validation runs | Trigger validation + actions in one call |
| Data Docs | Auto-generated HTML reports | **Required** - no manual HTML! |

#### Common Mistakes to Avoid

| Mistake | Why it's wrong |
|---------|----------------|
| Hardcode expectations in Python | Should use Expectation Suite (reusable, versionable) |
| Manual HTML reports | Must use Data Docs |
| No Checkpoint | Run validation manually instead of using orchestration |
| Ignore validation results | Don't extract stats for DB/dashboard |

---

# Prediction job

Now that you have your continuous flow of data, you can put in place a prediction job to make scheduled predictions on the ingested data. Unlike the webapp where the user can make on-demand predictions, this job will be executed every 2 minutes automatically.

It should be composed of 2 tasks:

1. `check_for_new_data`: in this task, you will check if there are any new ingested files in the *good_data* folder. If so, you need to pass the list of these files to the next task, otherwise mark the dag run status as `skipped`([Dag run status](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html#dag-run-status)). **The dag run should be marked as skipped and not only the `make_predictions` task**
2. `make_predictions`: in this task, you will read the files passed by the `check_for_new_data` task and make a **single API call** to the model service to make predictions (batch prediction, not one call per row or per file)

**Note**: The prediction job should not delete or move files from `good_data`. Files are moved to `archived_data` only by the training DAG after a successful model promotion.

**Hint**: Track the last processed file or timestamp.

- Prediction dag tasks

<p align="center">
    <img
        src="../images/project-prediction-dag.png"
        alt="Prediction dag"
    />
</p>

- Prediction dag skipped when there is no newly ingested data

<p align="center">
    <img
        src="../images/project-prediction-dag-skipped.png"
        alt="Prediction dag when skipped"
    />
</p>

---

## Defense Format, Rules & Deliverables

Refer to the [Defense Instructions](5_defense_instructions.md) for the full defense format, rules, preparation checklist, and deliverables.

Good luck 🤞 🍀
