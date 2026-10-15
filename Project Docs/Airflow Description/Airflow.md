# What Airflow Does to an ETL Pipeline

## 1. The Problem Airflow Solves

Without Airflow, our pipeline is a script you start by hand. If the API call fails at 3 AM, nobody retries it. If transform fails, nobody knows. If you want to rerun last week, you edit code.

Airflow is a **workflow orchestrator**. It does not extract, transform, or load data itself. It **runs, schedules, orders, retries, and monitors** the Python code that does.

> Airflow answers: *when* does each step run, *in what order*, *what if it fails*, and *what happened*.

---

## 2. Core Concepts

| Concept | Meaning | In our project |
|---|---|---|
| **DAG** | Directed Acyclic Graph: a workflow where tasks have a defined order and no loops | The whole daily pipeline |
| **Task** | One unit of work in the DAG | `extract`, `validate`, `save_raw`, `transform`, `load`, `quality_checks`, `refresh_views` |
| **Operator** | A template for a type of task | `PythonOperator` (run a Python function), `BashOperator`, `SQLExecuteQueryOperator` |
| **Dependency** | "B runs only after A succeeds" | `extract >> validate >> save_raw >> ...` |
| **DAG Run** | One execution of the DAG for a given date | The run for 2026-10-12 |
| **Task Instance** | One task inside one DAG run | `transform` on 2026-10-12 |
| **Schedule** | When DAG runs are created | Daily, after market close |

**Design pattern:** the DAG is a **Pipes and Filters** workflow (each task is a filter, the dependencies are the pipes) controlled by an **Orchestrator**.

---

## 3. Airflow's Components

| Component | Role |
|---|---|
| **Scheduler** | Reads DAG files, creates DAG runs on schedule, decides which tasks are ready |
| **Executor / Workers** | Actually run the tasks |
| **Metadata database** | Stores every run's state, task state, retries, and history (PostgreSQL) |
| **Web UI** | Shows the DAG graph, run history, logs, and buttons to trigger or retry |
| **REST API** | Lets other systems trigger and monitor runs (our Phase 2 API uses this) |

Flow: **Scheduler** creates a run → **Executor** sends tasks to **Workers** → state saved in the **Metadata DB** → visible in the **UI** and **API**.

---

## 4. What Airflow Does for Each ETL Need

### 4.1 Scheduling (FR-8.2)
Runs the pipeline automatically (e.g. once per trading day after market close) using a cron-style schedule. No manual start.

### 4.2 Ordering (FR-8.1)
Guarantees the sequence:

```
extract → validate → save raw → transform → load → quality checks → refresh views
```

If `validate` fails, `transform` never runs on bad data.

### 4.3 Retries (FR-8.3)
Each task defines `retries` and `retry_delay` (with optional exponential backoff). A task that fails because of a temporary API or network problem is retried automatically, and only that task reruns, not the whole pipeline.

### 4.4 Failure handling
- Downstream tasks are **skipped** when an upstream task fails.
- Failed tasks are marked clearly in the UI.
- Callbacks or email alerts can be added on failure.

### 4.5 Backfills (FR-8.4)
Airflow gives every run a **logical date**. To load history for missing days, you ask it to run the DAG for a past date range. This works well with our incremental loading and upserts, since reruns are safe.

### 4.6 Parallelism (FR-8.5)
**Dynamic task mapping** creates one task per symbol batch at runtime. Batches run in parallel (within worker limits), and a failure in one batch retries only that batch.

### 4.7 Logging and history (FR-9.1)
Every task keeps its own log, start and end time, duration, and state. Nothing is lost between runs.

### 4.8 Passing information between tasks
Tasks pass **small values** (counts, run IDs, file paths) with **XCom**. Large data never goes through XCom: tasks hand data over through the database (raw and analytics tables).

### 4.9 Monitoring and control
The UI and REST API show run state and let you trigger, clear, or retry runs. Phase 2's FastAPI service calls the Airflow REST API to trigger the DAG and read its status (FR-12.1 and FR-12.2).

---

## 5. How It Looks in Our Project

```
[Schedule: daily after market close]
            │
            ▼
   ┌─────────────────┐
   │ extract         │  call Twelve Data in batches (8 per minute)
   └────────┬────────┘
            ▼
   ┌─────────────────┐
   │ validate        │  check status and payload
   └────────┬────────┘
            ▼
   ┌─────────────────┐
   │ save_raw        │  JSONB into raw schema
   └────────┬────────┘
            ▼
   ┌─────────────────┐
   │ transform       │  Pandas: clean, returns, moving averages
   └────────┬────────┘
            ▼
   ┌─────────────────┐
   │ load            │  upsert into analytics schema
   └────────┬────────┘
            ▼
   ┌─────────────────┐
   │ quality_checks  │  nulls, duplicates, gaps, row counts
   └────────┬────────┘
            ▼
   ┌─────────────────┐
   │ refresh_views   │  REFRESH MATERIALIZED VIEW
   └─────────────────┘
```

---

## 6. Minimal DAG Skeleton

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="financial_market_pipeline",
    schedule="@daily",          # configurable
    start_date=datetime(2026, 10, 1),
    catchup=False,              # set True to enable automatic backfill
    default_args=default_args,
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=run_extract)
    validate = PythonOperator(task_id="validate", python_callable=run_validate)
    save_raw = PythonOperator(task_id="save_raw", python_callable=run_save_raw)
    transform = PythonOperator(task_id="transform", python_callable=run_transform)
    load = PythonOperator(task_id="load", python_callable=run_load)
    quality = PythonOperator(task_id="quality_checks", python_callable=run_quality)
    refresh = PythonOperator(task_id="refresh_views", python_callable=run_refresh)

    extract >> validate >> save_raw >> transform >> load >> quality >> refresh
```

The functions (`run_extract` and so on) are our own Python modules. Airflow only calls them, which is why the Phase 2 refactor into modules matters: the DAG stays thin and the logic stays testable without Airflow.

---

## 7. Rules for Good Airflow Tasks

| Rule | Why |
|---|---|
| **Idempotent** | Rerunning a task gives the same result (our upserts guarantee this) |
| **Atomic** | A task either fully succeeds or leaves nothing half-done (transactional loads) |
| **Small data in XCom only** | XCom lives in the metadata DB |
| **No secrets in DAG code** | Use environment variables or Airflow Connections |
| **Thin DAGs** | Keep business logic in modules, not in the DAG file |

---

## 8. What Airflow Is Not

- Not a data processing engine (Pandas and SQL do the work)
- Not a database (it only stores metadata about runs)
- Not a streaming tool (it is built for scheduled batch workflows, which matches our daily pipeline)

---

## 9. Summary

| Without Airflow | With Airflow |
|---|---|
| Manual run | Automatic schedule |
| One failure kills everything | Task-level retries and skipped downstream tasks |
| No history | Full run and task history with logs |
| Hard to rerun the past | Backfills by date range |
| No visibility | UI and REST API |
| Cannot be triggered by other apps | Triggered through the REST API in Phase 2 |