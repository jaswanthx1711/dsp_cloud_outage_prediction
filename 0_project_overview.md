# Data Science in production — Project Overview

For the data science in production course, you will be developing an ML powered application. This project will be developed in a group of 4 to 5 people and will represent 60% of your final grade.

The project is composed of **2 parts**:
- **Part 1** — developed during **DSP 1** (2nd semester)
- **Part 2** — developed during **DSP 2** (3rd semester)

Project defenses take place during the last session of each semester (DSP 1 and DSP 2).

You are free to choose the use case you want, but it must be a **tabular dataset** with a simple ML problem. Your first working model should take **no more than 1 hour of development time** to build (data exploration + training + basic evaluation).

**Why keep it simple?** The goal of this project is to learn MLOps - Docker, Airflow, MLflow, Great Expectations, Grafana, etc. A complex use case will distract you from these learning objectives and eat up time you need for infrastructure work.

## General Tips

### Planning & Teamwork
- **Start early** - Don't underestimate Docker, Airflow, and Great Expectations learning curves.
- **Split the work** - Divide tasks among team members, but integrate early and often. Don't wait until the last week to merge everyone's work - integration issues take longer to fix than you expect.

### Development
- **Incremental development** - Get one component working before adding the next. Don't try to build everything at once.
- **Test locally first** - Make sure each service works standalone before integrating.
- **Document as you go** - Add README and comments — your teammates need to understand your code too.

### Using GenAI
- **Understand before you copy** — You must explain every line during the defense.
- **Verify against docs** — GenAI may hallucinate functions or use deprecated APIs. Always cross-check.
- **Specify versions** — Tell it "I'm using Airflow 3.x, Python 3.12" to get relevant code.
- **Small prompts, iterate** — Ask for one thing at a time. If the answer is wrong, give feedback.

## Components

Your final application will be composed of the following components (technologies to use in parentheses):

- A user interface where the user can make on-demand predictions and view past predictions ([streamlit](https://streamlit.io/))
- An API for exposing the ML model ([FastAPI](https://fastapi.tiangolo.com/)) and saving the predictions to the database
- An SQL database for saving data (predictions along with the used features, ...) ([PostgreSQL](https://www.postgresql.org/), [SQLAlchemy](https://www.sqlalchemy.org/))
- A prediction job to make scheduled predictions ([Airflow](https://airflow.apache.org/))
- An ingestion job to ingest and validate the data quality ([Great Expectations](https://greatexpectations.io/))
- A training job to retrain the model and promote it to production ([MLflow](https://mlflow.org/))
- A monitoring dashboard to monitor data quality problems of the ingested data and the drift of training and serving data ([Grafana](https://grafana.com/))


These components will interact as shown in this architecture:

<p align="center">
    <img
        src="../images/project-architecture.png"
        alt="Project architecture"
    />
</p>

## Technical Requirements

- **Python**: 3.12
- **Airflow**: 3.x
- **Other libraries**: use latest versions compatible with Python 3.12

---

## User interface

To make predictions and visualize past predictions, you will develop a webapp that is composed of 2 webpages:

1. **Prediction page**: for making single and batch predictions. It should contain:
- A **form** to fill in the features for a single sample prediction.
- An **upload button** to upload a csv file for making batch predictions: the file should contain the inference data in the correct format without labels (just saying :wink: )
- A **predict button** to make on-demand predictions by calling the model API service

**Note**:

- When predictions are returned by the model service (API), they need to be displayed as a dataframe along with the features used for making the prediction
- For single and batch predictions, you should not save the data in a csv file and send the file path to the API, you should read the data and send it instead

2. **Past predictions page** for visualizing past predictions. It should contain:
- A **date selection component** to select the start and end date
- A **prediction source drop list** to select the source of the predictions to retrieve
  - `webapp`: to show only predictions made using the webapp
  - `scheduled`: to show predictions made using the prediction job
  - `all`: to show predictions made from the webapp or the prediction job

### Streamlit Key Points

- Use **multipage apps** to separate Prediction and Past Predictions pages
- Use `requests` library to call your FastAPI endpoints
- Display results as dataframes

> Handle error states gracefully — API unreachable, invalid input, empty results.

⚠️ **Important**: The webapp should NOT import the model or connect to the database directly. All data must go through the API.

## Model service (API)

To make predictions, the model will not be stored in the webapp as it will be used by multiple services (webapp and prediction job). It will instead be exposed as an API that the webapp and the prediction job can call to request predictions.

This API will be used for:

- serving the model (making predictions)
- saving the model predictions and used features in the database
- returning past predictions and used features to the webapp

### API Endpoints

| Endpoint | Method | Description | Example |
|----------|--------|-------------|---------|
| `/health` | GET | Health check for Docker container readiness | Returns `{"status": "healthy"}` |
| `/predict` | POST | Make predictions (single and batch) | `{"features": [{"feature1": 10, "feature2": "A"}, ...]}` |
| `/past-predictions` | GET | Retrieve past predictions | `GET /past-predictions?limit=100` |
| `/reload-model` | POST | Reload model from MLflow registry *(Defense 2)* | `POST /reload-model` |

> Your API should support filtering past predictions by relevant criteria. Think about what filters would be useful for the Streamlit UI.

> Design your response models carefully — the prediction response should contain enough information for the UI to display results and for the database to store them.

## Database (PostgreSQL + SQLAlchemy)

To store past predictions and data quality issues, you will set up a `PostgreSQL` database that will be used by:

1. The model service to save the model predictions and query them (write + read)
2. The data ingestion job to save data quality problems (write)
3. The dashboard to display data quality problems and data drift (read)

> **📝 Architecture Note**: You'll notice two different patterns for database access:
> - The **API** uses SQLAlchemy ORM for predictions (standard web application pattern)
> - **Airflow** uses PostgresHook for ingestion stats (native Airflow pattern)
>
> This is intentional - in production, you'll encounter both approaches. The API centralizes business logic, while Airflow's hooks integrate with its connection management and logging. Understanding when to use each pattern is part of the learning objectives.

Create at minimum 3 tables:
- **Predictions**: store predictions with model version, and enough context to support the UI filters and Grafana dashboards
- **Ingestion stats**: store ingestion error statistics to power your data quality monitoring dashboard
- **Training stats** *(Defense 2)*: store training feature statistics to power your drift monitoring dashboard

> Design your schema to support the queries your Grafana dashboards will need — think about what columns you'll filter and aggregate on.

Use **environment variables** for database connection string (not hardcoded).

> Your schema will evolve as you build — plan for this from the start.

> **Note**: Use a **single PostgreSQL instance** with separate databases: one for your application data (predictions, stats) and one for Airflow metadata. Both can run in the same container.

## Folder structure

```
project/
├── dags/                # Airflow DAGs
├── data/
│   ├── raw_data/        # Input files for ingestion
│   ├── good_data/       # Valid data for predictions & training
│   ├── bad_data/        # Invalid data (for review)
│   └── archived_data/   # Used training data (after successful retraining)
├── model_service/       # FastAPI app
├── webapp/              # Streamlit app
├── docker-compose.yml
├── .env
└── ...
```

## Git Requirements

Use Git to practice collaborative development workflows.

**Branch structure**:
- `main` - stable code for defenses only
- `develop` - integration branch, one commit per feature
- `feature/*` - feature branches (e.g., `feature/ingestion-dag`, `feature/api-predict`)

**Workflow**:
- No direct commits to `main` or `develop`
- Create feature branches from `develop` — name them by feature (e.g., `feature/ingestion-dag`), not by team member (e.g., ~~`feature/alice`~~)
- Use pull requests to merge code into `develop`
- Squash commits when merging (one clean commit per feature)
- Delete feature branches after merge
- Merge `develop` into `main` only before defenses

**Practice what you learned**: merging, rebasing, resolving conflicts, code reviews.

## CI/CD Requirements

### CI — Continuous Integration (Defense 1)

Set up a GitHub Actions pipeline that runs on pull requests to `develop`.

**Pipeline must include**:
- Linting with `flake8`
- Unit tests with `pytest`

> ⚠️ Linting and testing must be **separate steps** in your pipeline, not combined in a single step.

**Example tests**:
- Criticality calculation (e.g., >50% invalid → high)
- File splitting logic (valid/invalid row separation)
- Stats computation (e.g., correct error counts from validation results)
- Pydantic request/response validation

### CD — Continuous Delivery (Defense 2)

Set up a GitHub Actions workflow that runs when `develop` is merged into `main`.

**Pipeline must**:
- Automatically create a **GitHub Release** with a version tag
- Use [semantic versioning](https://semver.org/) (e.g., `v1.0.0`, `v1.1.0`)
- Include a brief description of what changed (can be auto-generated from commit messages)

> **Note**: In a real production environment, CD would deploy to a server or push Docker images to a registry. Since this project runs locally with `docker compose`, we use automated releases as a simplified CD step to practice the concept of automated delivery.

> **Tip**: You can test your CD workflow in a separate repository before integrating it into your project.

## Docker Requirements

All services must run as Docker containers, orchestrated with a single `docker-compose.yml`.

**Services to containerize**: Streamlit, FastAPI, PostgreSQL, Airflow, MLflow, Grafana, nginx (to serve Data Docs HTML reports)

**Important**: Use `docker compose` (space), not `docker-compose` (hyphen). The hyphenated command is deprecated.

**Compose file tips**:
- No `version` field needed (deprecated)
- Use official images where available (e.g., `postgres`, `grafana/grafana`, `mlflow/mlflow`)
- Use `build: ./path` for custom images (Streamlit, FastAPI)
- Use volumes to persist data (database, MLflow artifacts)
- Use `.env` file for shared environment variables
- Add `.env` to your `.gitignore` — never commit secrets to Git. Use a `.env.example` with placeholder values for your teammates.
- Services communicate via Docker network using service names (e.g., `http://api:8000`)
- Only expose ports for services accessed externally (Streamlit, Grafana, nginx)

**Service startup**:
- Use `depends_on` with `condition: service_healthy` to wait for dependencies
- Add health checks to critical services (database, API) so dependent services wait until they're actually ready
- Use `restart: unless-stopped` for resilience

Health checks let Docker verify a service is responding, not just that the container started. Docker runs the check command periodically inside the container — if it fails, the container is marked unhealthy and dependent services won't start.

```yaml
services:
  db:
    image: postgres
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $POSTGRES_USER"]
      interval: 5s
      timeout: 3s
      retries: 5

  api:
    build: ./api
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 10s
      timeout: 3s
      retries: 3
    depends_on:
      db:
        condition: service_healthy
```

> **Note**: `pg_isready` is included in the PostgreSQL image. `curl` is **not** included in Python base images — add `RUN apt-get update && apt-get install -y curl` in your Dockerfile.

**Dockerfile tips** (needed for Streamlit/FastAPI):
- Use `requirements.txt` for Python dependencies
- Set `WORKDIR` to organize container filesystem
- Add `.dockerignore` to exclude unnecessary files from builds (e.g., `.git`, `__pycache__`, `.venv`)

**Airflow-specific**:
- Airflow needs multiple containers (webserver, scheduler)
- Mount DAGs folder so changes reflect without rebuild

**Common pitfalls**:
- Database data lost on restart? → Use named volumes, not anonymous
- Container can't connect to another? → Use service name, not `localhost`

**Development**:
- `docker compose watch` - auto-reload during development
- `docker compose up --build` - rebuild images after Dockerfile changes

**Debugging**:
- `docker compose ps` - check container status
- `docker compose logs -f <service>` - view logs in real-time
- `docker compose exec <service> sh` - shell into a container
- `docker compose down -v` - full reset including volumes (careful!)
