# Defense — Preparation & Rules

## Before the Defense

### Code Preparation

- Merge all your code to `main` using a **Pull Request from `develop` to `main`** before the defense
- The demo **must** be done from the `main` branch
- Delete all branches except `main` and `develop` before the defense. It is okay to keep branches for work in progress (unfinished features)

### Slides & Repository Submission

Submit the following in the Teams assignment tab before the defense (one submission per team):

- **2 slides as a single PDF file**:
  1. **Slide 1** — List of features that are not working or not implemented
  2. **Slide 2** — Who did what (task distribution among team members). Only list tasks for the **current defense** — do not include features from previous defenses. Each team member must be able to answer questions about the features they worked on
- **GitHub repository link**

> ⚠️ **Missing slides or GitHub link = -2 penalty on the final grade.**

> ⚠️ **Be honest about what is not working.** List all non-working features in slide 1 and do not attempt to demo them. If we discover during the demo that a feature listed as working is actually broken, a **-2 penalty** will be applied.

### Environment Setup

- Have your environment **fully ready on 2 computers** before the defense (Docker containers running, data loaded, demo scenarios prepared)
- No extra time will be given for setup — everything must be running when your slot starts
- **Join 15 minutes before your time slot**

### Dashboard Preparation (Defense 2)

- Make sure to **generate data in the weeks before the defense** so that your Grafana dashboards have meaningful graphs to display
- Dashboards with empty or insufficient data will not be considered complete

### Data Preparation

- Empty the `good_data` and `bad_data` folders before the defense so the ingestion and prediction pipelines can be demonstrated from a clean state
- Prepare **5 demo files** somewhere on your computer (not in `raw_data`). During the defense, copy them into `raw_data` to demo the ingestion and prediction jobs:
  1. **1 file** with only errors (to demo high criticality alert + all rows to `bad_data`)
  2. **3 files** with only valid data (to demo clean ingestion + prediction pipeline)
  3. **1 file** with a mix of good and bad rows (to demo data splitting)

### Tips

- **Data Docs**: You must be able to open the Great Expectations Data Docs report in a browser for a given ingestion run. Showing raw HTML code is not acceptable — the report must render in the browser

---

## During the Defense

### Format

Each group has **15 minutes** (~1 min slides, ~10 min demo, ~4 min Q&A). **Only features that are demoed during the defense time will be considered as done.** Any non-demoed feature is considered as not done — no exceptions.

### Demo Rules

- **Single computer only** — the entire demo must run from one computer
- In case of an **unexpected technical problem**, you may switch to your backup computer **once**. Switching computers results in a **-2 penalty on the final grade**
- Demo must be done from the `main` branch
- **Do not demo features that are not working** — list them in slide 1 instead. Demoing broken features wastes your limited time
- **Time is strict.** Groups that go over may be cut off

### Online Defense Rules

- **Camera must be open for the entire defense** — anyone with their camera off will be considered absent and receives a grade of **0**

### Absence

- If a group does not show up for their time slot, all members receive a grade of **0**
- If a team member is absent, they receive an individual grade of **0**. The rest of the group is graded normally

### Grading Rules

- **Only fully functional features are considered done.** Partially working features count as not done
- **Features from previous defenses** (webapp, API, database, etc.) are graded at the macro level: if any part of the component is broken, the entire component is considered not done
- **New features** (e.g., data validation, send alerts, split data, save statistics in the ingestion DAG) are graded in detail
- **Airflow 3.x is required** — using Airflow 2.x results in a **-2 penalty**
- **Penalties are cumulative** — multiple infractions result in multiple deductions

### Q&A Rules

- **Airflow and Docker**: every group member must be able to answer questions, as these topics were covered in class
- **Great Expectations**: only the team member who implemented it is responsible for answering questions about it
- Each team member must be able to answer questions about the features they worked on (as listed in slide 2)

### README

Your repository must include a **documented README** with:
- How to set up and run the project (`docker compose up`, environment variables, etc.)
- Any prerequisites or setup steps needed before running

This is required for grading — if the instructor cannot run your project from the README instructions, it will impact your grade.

---

## Checklist

- [ ] Code merged to `main` via PR from `develop`
- [ ] All finished feature branches deleted (only `main`, `develop`, and WIP branches remain)
- [ ] 2 slides (PDF) + GitHub link submitted in Teams
- [ ] Environment ready on 2 computers (containers running, data loaded)
- [ ] Dashboards populated with data (generated over the past weeks)
- [ ] `good_data` and `bad_data` folders emptied
- [ ] 5 demo files prepared (1 all errors, 3 clean, 1 mixed) — ready to copy into `raw_data`
- [ ] `.env` file configured
- [ ] All features tested end-to-end from `main` branch
- [ ] Data Docs reports render in browser
- [ ] README with setup and run instructions
- [ ] Camera ready (for online defenses)
- [ ] Join 15 minutes before your time slot
