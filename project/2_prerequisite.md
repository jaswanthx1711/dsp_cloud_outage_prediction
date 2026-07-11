# DSP — Pre-requisite Assignment

Before the next session, each group must have the following components developed and running in Docker:

1. **FastAPI** — all endpoints except reload-model
2. **Streamlit webapp**
3. **PostgreSQL database** — predictions table, API reads/writes via SQLAlchemy
4. **Docker Compose** — all 3 services running together with `docker compose up`
5. **Data splitting script** — takes a dataset and splits it into N files in `raw_data/`
6. **Data error injection script** — takes a clean dataset and injects errors with configurable probability (minimum 7 error types)

For full details on each component, refer to the [project overview](0_project_overview.md) and [1st defense specifications](3_defense_1.md).

## Submission

- ⚠️ Only **one person per group** should submit
- Submit your **GitHub repository link** via the assignment form
- The repository must be **private**
- Add me as a collaborator: `alaabakhti`
- All code must be in the `main` branch

⚠️ **This is a prerequisite for the follow-up session.** Groups that don't have these components working will not be able to follow along.
