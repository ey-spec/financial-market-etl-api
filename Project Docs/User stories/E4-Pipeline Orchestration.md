# E4: Pipeline Orchestration

**Phase:** 1 | **Covers:** FR-8, FR-9.1, FR-9.2 (+ NFR-2.2, NFR-3.1, NFR-3.3, NFR-3.4, NFR-3.5, NFR-5.1, NFR-5.2, NFR-6.1, NFR-6.3, NFR-6.5, NFR-7.2, NFR-7.4, NFR-8.1, NFR-8.2, NFR-8.3, NFR-9.4)

## Overview
This epic automates the whole Phase 1 workflow so it runs on a schedule, retries what can be retried, and records exactly what happened in every run. Airflow only runs, orders, retries, and monitors our Python code. It does not extract, transform, or load anything itself, so the DAG stays thin and the logic lives in the testable modules of E1, E2, and E3.

## What this epic covers
- **Environment:** pinned, reproducible Airflow with a metadata database separate from the project data
- **DAG:** `start_run → [per batch: extract → validate → save_raw → transform → load] → quality_checks → refresh_views → finalize_run`
- **Scheduling:** once per trading day after market close, configurable
- **Retries and timeouts:** task-level, with non-retryable failures failing fast
- **Parallelism:** one mapped task group per symbol batch, API calls serialized by pools
- **Backfills and reprocessing:** past date ranges, and rebuilds from raw with no API call
- **Run logging:** `pipeline_runs` (one row per run) and `pipeline_run_symbols` (one row per symbol per run)
- **Run outcome:** one place (`finalize_run`) decides success or failure, and the quality gate decides whether views refresh
- **End-to-end test:** proves Phase 1 works as a whole

## Depends on
- **E1:** extraction, validation, raw-save, credit, and config functions
- **E2:** transform and quality-check functions
- **E3:** all DDL, including `pipeline_runs` and `pipeline_run_symbols` (E3-US11), staging and landing tables, the load function (E3-US6), and the refresh function (E3-US8)

**Completing this epic finishes Phase 1.**
Build order: E3 Part A (tables) → E1 → E2 → E3 Parts B and C → this epic's DAG wiring.

## Decisions

### Rules

| Topic | Decision |
|---|---|
| DAG id | `financial_market_pipeline` |
| Thin DAG | The DAG file only wires tasks. Every task calls one function from an importable ETL module. No business logic and no secrets in DAG code. |
| Metadata DB | Airflow's own database is separate from the project data database (the same Postgres server is fine). |
| Task chain | `start_run` → per-batch group (`extract → validate → save_raw → transform → load`) → `quality_checks` → `refresh_views` → `finalize_run`. This is the FR-8.1 chain plus two bookend tasks. |
| Data hand-over | Through the database only. XCom carries only small values (run ID, batch lists, counts). |
| API response hand-over | `extract` writes to `analytics.stg_api_responses`. `validate` marks rows valid or invalid and sends invalid ones to `raw.quarantine`. `save_raw` copies valid ones to `raw.raw_responses`. A retry of `validate` or `save_raw` never re-calls the API and never duplicates rows (`landing_id`, E3-US2). |
| Run ID | One UUID per run. Taken from `dag_run.conf.run_id` if present (Phase 2 API), otherwise `uuid5(namespace, airflow_run_id)`. It is deterministic, so a retried `start_run` gets the same ID. |
| Run date | Logic uses watermarks and the actual run date. The logical date is a label only, which avoids the off-by-one trap where a run's logical date is the previous interval. |
| Overlap | `max_active_runs = 1`, so two runs never race on watermarks or credits. |
| Schedule default | `0 22 * * 1-5` (22:00 UTC, Monday to Friday), after the US and EGX closes. Configurable. `catchup = False`. |
| EGX note | EGX trades Sunday to Thursday, so Sunday's bar is fetched on Monday's run. Nothing is lost because extraction is watermark-driven. |
| Retries (defaults) | `extract`: 1 retry. All other tasks: 3 retries. Delay 2 min, exponential backoff, capped at 30 min. |
| Why extract retries less | E1-US4 already retries each HTTP call up to 5 times, and every attempt costs a credit. Stacking 3 more task retries could mean up to 20 calls per symbol. |
| Non-retryable | Config errors, auth errors (401/403), and schema or validation bugs raise `AirflowFailException`. |
| Timeouts | `execution_timeout` per task (extract 30 min, others 15 min) and `dagrun_timeout` of 6 hours, all configurable. |
| Batches | Planned by `start_run` as provider-pure symbol batches (size per provider from config, 8 for Twelve Data). Inside `extract`, E1's credit logic paces the real calls so no minute window exceeds the limit, including date-range chunks and retries (E1-US3). |
| Pools | `twelvedata_api` and `eodhd_api`, 1 slot each. API calls run one batch at a time, while later stages of finished batches run in parallel. |
| Pause between batches | Applied inside `extract` before the pool slot is released (60 s for Twelve Data). |
| Daily credit budget | `analytics.api_usage`, reserved atomically before each call (E1-US3, E3-US4). |
| Deferral order | Deferred symbols first, then symbols with no watermark, then the oldest watermarks. "Deferred" means the symbol's status in the most recent earlier run that included it. |
| Batch isolation | A failing batch fails alone. Other batches continue. |
| "Loaded" | A run has "loaded" data when at least one symbol finished with status `success` (a committed load transaction with rows). |
| Single writer for totals | Each stage writes only its own columns of its own symbols' rows in `pipeline_run_symbols`. Only `finalize_run` writes totals into `pipeline_runs`, by summing. |
| Run status | `queued` (only when the Phase 2 API pre-creates the row), `running`, `success`, `failed`. Quality outcome is the separate `quality_status` column. |
| What makes a run `failed` | Any symbol with status `failed`, any failed task, `quality_status = fail`, a failed `refresh_views`, or a DAG-level failure or timeout. |
| What does not fail a run | Deferred symbols, `no_new_data` symbols, quarantined symbols (they are reported, see open questions), and `quality_status = warn`. |
| Quality gate | `quality_checks` runs if at least one symbol loaded, otherwise `quality_status = skipped`. `refresh_views` runs only if quality is `pass` or `warn` and at least one symbol loaded. |
| Row accounting | `rows_extracted = rows_loaded + rows_rejected + duplicates_removed`, where `rows_loaded = rows_inserted + rows_updated + rows_unchanged`. `rows_recalculated` and lookback rows are outside the balance (same rule as E2 and E3). |
| Counters | Each stage sets (never increments) its columns, so a retry gives the same result. |
| Retention | `pipeline_runs` and `pipeline_run_symbols` are never deleted (NFR-8.3). |

### Who writes which `pipeline_run_symbols` column

| Stage | Writes |
|---|---|
| `start_run` | Creates the row (`provider`, `batch_no`) for every planned symbol |
| `extract` | `deferred`, `failed`, or `no_new_data` status where it applies, plus error fields |
| `validate` | `quarantined` or `no_new_data` status, plus error fields |
| `transform` | `rows_extracted`, `rows_rejected`, `duplicates_removed` (E2-US1, E2-US2) |
| `load` | `rows_inserted`, `rows_updated`, `rows_unchanged`, `rows_recalculated`, and status `success`. On rollback, a separate transaction sets `failed` (E3-US6). |
| Task failure callback | `failed` plus sanitized error for the batch's symbols, for any stage that dies without reporting |

### Table definitions (created by E3-US11, specified here)

`analytics.pipeline_runs`

| Column | Notes |
|---|---|
| run_id | UUID, PK |
| airflow_dag_id, airflow_run_id | `airflow_run_id` is unique (links to Airflow, FR-12.3) |
| trigger_type | scheduled / manual / api / backfill |
| mode | incremental / backfill / reprocess |
| status | queued / running / success / failed |
| quality_status | pass / warn / fail / skipped (null while running) |
| params | JSONB: symbols, start_date, end_date, source_run_id |
| started_at, ended_at | `TIMESTAMPTZ` UTC. Duration is computed from them. |
| symbols_requested, symbols_succeeded, symbols_no_new_data, symbols_failed, symbols_quarantined, symbols_deferred | counters |
| rows_extracted, rows_rejected, duplicates_removed, rows_inserted, rows_updated, rows_unchanged, rows_loaded, rows_recalculated | counters |
| error_type, error_message | sanitized and truncated, no secrets |
| created_at | |

`analytics.pipeline_run_symbols`: PK (run_id, symbol), FK to `pipeline_runs` with `ON DELETE RESTRICT`. Columns: `provider`, `batch_no`, `status` (success / no_new_data / failed / quarantined / deferred), the same row counters per symbol, `error_type`, `error_message`, `updated_at`.

---

## User Stories

### E4-US1: Airflow Environment Setup
- **As a** Orchestration Engineer,
- **I want to** run Airflow locally with a pinned version, started by one documented command,
- **So that** every teammate gets the same working orchestrator and setup risk stays low.
- **Priority:** High
- **Covers:** NFR-6.1, NFR-6.5, NFR-5.1, NFR-5.2

**Acceptance Criteria:**
- Given a clean machine with Docker, When the documented command runs, Then the scheduler, API server/web UI, and metadata database start and the UI is reachable.
- Given the setup, When it is inspected, Then the Airflow version, Python version, and provider packages are pinned.
- Given the executor, When it is configured, Then it is `LocalExecutor`.
- Given the database setup, When it runs, Then Airflow's metadata database is separate from the project data database.
- Given the project code, When Airflow starts, Then the ETL modules (E1, E2, E3) are importable by the DAG (mounted or installed).
- Given secrets (database URL, API keys, Airflow admin password), When they are supplied, Then they come from environment variables or Airflow Connections, and `.env.example` has placeholders only.
- Given the pools, When Airflow starts, Then `twelvedata_api` and `eodhd_api` (1 slot each) exist, created by a setup script that can run repeatedly without error.
- Given a restart, When the stack comes back, Then DAG state and run history persist.
- Given the Airflow connection to the data database, When it is configured, Then it uses the `etl_writer` role (E3-US1), never a superuser.

**Notes:** Phase 1 runs Airflow on its own. E6 later folds it into the full Docker Compose stack. Drop the `eodhd_api` pool if E1-US8 is dropped.

---

### E4-US2: DAG Definition and Task Ordering
- **As a** Orchestration Engineer,
- **I want to** define the DAG with the full chain and safety settings,
- **So that** steps always run in the right order and bad data never moves forward.
- **Priority:** High
- **Covers:** FR-8.1, NFR-5.1, NFR-3.3

**Acceptance Criteria:**
- Given the DAG file, When it is parsed, Then it has no import errors and contains `start_run`, then per batch `extract → validate → save_raw → transform → load`, then `quality_checks`, `refresh_views`, and `finalize_run`.
- Given `validate` fails for a batch, When the run continues, Then `save_raw`, `transform`, and `load` do not run for that batch.
- Given the DAG settings, When inspected, Then `max_active_runs = 1`, `catchup = False`, and `dagrun_timeout` are set.
- Given the DAG file, When it is inspected, Then each task only calls one function from the ETL modules, and no secret or business logic is in it.
- Given data between tasks, When tasks hand over work, Then only small values pass through XCom. Responses and rows pass through the database.
- Given a run already active, When a second run is triggered, Then it waits and does not run in parallel.
- Given the trigger rules, When some batches fail, Then `quality_checks`, `refresh_views`, and `finalize_run` still start (`all_done`) and decide for themselves from the database whether to run or skip (E4-US8). `finalize_run` always runs.
- Given a task that skips itself (for example `extract` in `reprocess` mode), When downstream tasks are evaluated, Then they still run, because skipped does not count as failed for them.
- Given each task, When it is rerun for the same run ID, Then it gives the same result: `start_run` upserts the run row, `extract` replaces its own landing rows, `validate` and `save_raw` are idempotent via `landing_id`, `transform` clears its own staging rows first (E2-US7), and `load` is an upsert (E3-US6).
- Given a DAG-level failure callback, When a run fails or times out, Then the run is marked `failed` in `pipeline_runs` (see E4-US7).

**Notes:** Use the TaskFlow API (`@task`, `@task_group`) with dynamic task mapping for the per-batch groups.

---

### E4-US3: Scheduling
- **As a** Orchestration Engineer,
- **I want to** run the pipeline automatically once per trading day after market close,
- **So that** data stays current with no manual start.
- **Priority:** High
- **Covers:** FR-8.2, NFR-6.3

**Acceptance Criteria:**
- Given the default configuration, When Airflow is running, Then a run is created at 22:00 UTC Monday to Friday.
- Given the cron expression, When it is changed in configuration, Then the new schedule applies after a DAG reload, with no code edit.
- Given a scheduled run, When symbols are chosen, Then they come from the symbol list in configuration (E1-US7), ordered as in E4-US5.
- Given a scheduled run, When extraction starts, Then it uses the run date and the watermarks, not the logical date, to decide what to request (E1-US2).
- Given a missed schedule, When Airflow comes back, Then it does not create a flood of past runs (`catchup = False`). The next run fetches everything after each watermark, so no data is lost.
- Given a scheduled run, When the row is created, Then `trigger_type = scheduled` and `mode = incremental`.
- Given the schedule, When the DAG is viewed, Then the next run time is visible in the UI.

---

### E4-US4: Retries, Timeouts, and Failure Handling
- **As a** Orchestration Engineer,
- **I want to** retry temporary failures at task level and fail fast on permanent ones,
- **So that** a short outage heals itself and a real bug is not retried pointlessly.
- **Priority:** High
- **Covers:** FR-8.3, NFR-2.2, NFR-3.1, NFR-3.4

**Acceptance Criteria:**
- Given the default retry settings, When a task fails with a temporary error, Then it is retried with exponential backoff (2 min base, capped at 30 min), and only that task reruns.
- Given `extract`, When it fails, Then it has 1 retry only, because HTTP-level retries already happened (E1-US4) and credits are limited.
- Given a configuration error, an authentication failure (401/403), or a validation bug, When it occurs, Then the task fails immediately with `AirflowFailException` and no retries.
- Given retries are exhausted, When the task fails, Then downstream tasks of that batch are skipped, the batch's symbols are marked `failed`, and the run ends `failed`.
- Given a retried `extract`, When it reruns, Then it replaces only its own landing rows and reserves new credits like any other call (E1-US3). It never double-counts rows.
- Given a task, When it runs, Then it has an `execution_timeout` so it can never hang (values from configuration).
- Given any failure, When it is recorded, Then `error_type` and a sanitized `error_message` are saved on the batch's symbols and in the run row, and the full traceback stays in the Airflow task log.
- Given error messages, When they are written, Then they never contain API keys, passwords, or full connection strings.
- Given retry and timeout values, When they are changed in configuration, Then they apply with no code edit.
- Given a batch failed after retries, When the other batches continue, Then they are unaffected.

---

### E4-US5: Parallel Per-Batch Execution with API Rate Control
- **As a** Orchestration Engineer,
- **I want to** run one task group per symbol batch while keeping API calls inside the limits,
- **So that** a failure retries only its own batch and parallel work never breaks the rate limit.
- **Priority:** Medium
- **Covers:** FR-8.5, FR-1.4, NFR-7.2, NFR-7.4

**Acceptance Criteria:**
- Given the symbol list, When `start_run` plans the run, Then it orders symbols as: deferred symbols first, then symbols with no watermark, then the oldest watermarks, and splits them into provider-pure batches (batches never mix providers, E1-US8). It returns the batch list as a small XCom value and records `batch_no` per symbol.
- Given the batch list, When the DAG runs, Then it creates one mapped task group per batch (dynamic task mapping).
- Given several batches of one provider, When their `extract` tasks run, Then the provider pool (1 slot) makes them run one at a time. The later stages (`validate` to `load`) of finished batches run in parallel with the next batch's `extract`.
- Given a batch finished extracting, When it releases the pool slot, Then it has first waited the configured pause (60 s for Twelve Data).
- Given the daily budget in `analytics.api_usage`, When a request would exceed the provider's daily limit, Then no call is made, the remaining symbols are marked `deferred`, the reason is logged, and the run does not fail because of it.
- Given EODHD symbols, When batches are planned, Then they use the `eodhd_api` pool and the EODHD daily limit (E1-US8).
- Given one batch fails, When the run continues, Then the other batches are still extracted and loaded.
- Given an empty symbol list, When `start_run` plans the run, Then no batches are created, `quality_status = skipped`, and the run finishes `success` with zero symbols.

**Notes:** Parallel batches would break the 8 credits/minute limit without the pool. That is why the pool exists. Drop the EODHD parts if E1-US8 is dropped.

---

### E4-US6: Backfills and Reprocessing
- **As a** Data Engineer,
- **I want to** run the DAG for a past date range, or rebuild a range from stored raw data,
- **So that** history can be loaded or repaired safely, without extra API calls when the raw data already exists.
- **Priority:** Medium
- **Covers:** FR-8.4, FR-1.2, NFR-3.3, NFR-3.5

**Acceptance Criteria:**
- Given a manual trigger with `mode = backfill`, `start_date`, `end_date`, and optional `symbols`, When the run starts, Then one run covers the whole range (not one run per day). The explicit dates override the watermark (E1-US2) and long ranges are split as in E1-US1.
- Given trigger parameters, When they are submitted, Then they are validated (start not after end, no future end date, symbols in the configured list, `mode` known) and an invalid trigger fails at `start_run` with a clear message, before any API call.
- Given a backfill of old dates, When it loads, Then the watermark never moves backward (E3-US6) and affected later metrics are recalculated (E2-US7).
- Given `mode = reprocess` with a date range or a `source_run_id`, When the run starts, Then `extract`, `validate`, and `save_raw` skip themselves, no API request is made, no credit is reserved, and `transform → load → quality_checks → refresh_views` rebuild the range from `raw.raw_responses` (`purpose = 'primary'` only, E2).
- Given a reprocess run, When it writes staging and fact rows, Then they carry the new run ID, while the raw rows keep the run ID that fetched them.
- Given a backfill or reprocess is run twice for the same range, When both finish, Then the data is identical and has no duplicates.
- Given Airflow's own backfill command, When it is used, Then it is safe (watermark-driven, zero-row runs), but the parameter method above is the documented one because it saves credits.
- Given the run row, When a backfill or reprocess run is stored, Then `mode`, `trigger_type`, and `params` (including `source_run_id` when given) record what was requested.

**Notes:** After changing the configured start date or window sizes, reprocess the affected symbols so cumulative returns and metrics are rebuilt (E2-US3).

---

### E4-US7: Run Logging and the `pipeline_runs` Lifecycle
- **As a** Data Engineer,
- **I want to** record every run's times, status, symbols, row counts, and errors,
- **So that** any run can be audited, and the Phase 2 API and monitoring can read its history.
- **Priority:** High
- **Covers:** FR-9.1, FR-9.2, NFR-8.1, NFR-8.2, NFR-8.3

**Acceptance Criteria:**
- Given a run begins, When `start_run` executes, Then it upserts the `pipeline_runs` row (`status = running`, `started_at`, run ID, Airflow run ID, trigger type, mode, params, `symbols_requested`) and creates one `pipeline_run_symbols` row per planned symbol. A repeat of `start_run` changes nothing. If the Phase 2 API pre-created the row as `queued`, it is moved to `running`.
- Given a stage finishes, When it reports, Then it sets (not increments) only its own columns, as listed in "Who writes which column".
- Given a symbol's final status, When it is recorded, Then it is one of `success`, `no_new_data`, `failed`, `quarantined`, or `deferred`.
- Given the run ends, When `finalize_run` executes, Then it writes the totals into `pipeline_runs` by summing `pipeline_run_symbols`, sets `ended_at`, and sets `status` (E4-US8).
- Given the counters, When `finalize_run` runs, Then it checks `rows_extracted = rows_loaded + rows_rejected + duplicates_removed` (recalculated rows excluded) and logs any mismatch as an error. The per-symbol check that fails the run is E2-US4.
- Given a failed run, When it is recorded, Then `error_type`, a sanitized `error_message`, and the failed symbols are stored.
- Given the DAG is killed or times out, When the failure callback fires, Then the run is marked `failed` and does not stay `running` forever. `finalize_run` also creates the row if `start_run` never got to.
- Given any log line from our code, When it is written, Then it includes timestamp, level, run ID, task ID, and symbol where relevant, and never contains API keys or full connection strings.
- Given a run ID, When searched, Then it finds the run row, its symbol rows, the raw and quarantine rows, and the quality results that share it (indexes from E3-US11).
- Given the history tables, When history is queried, Then no job deletes or overwrites past runs (`ON DELETE RESTRICT`, `etl_writer` has no `DELETE`).

---

### E4-US8: Quality Gate, View Refresh, and Run Outcome
- **As a** Data Engineer,
- **I want to** one place that checks quality, decides whether views refresh, and sets the final run status,
- **So that** bad data never reaches the views and every run ends in a clear, correct state.
- **Priority:** High
- **Covers:** FR-8.1, FR-9.2, FR-7.3, FR-6.4, NFR-8.2

**Acceptance Criteria:**
- Given at least one symbol loaded, When `quality_checks` starts, Then it calls E2's checks for the symbols loaded by this run, stores results (E2-US5), and sets `quality_status` to `pass`, `warn`, or `fail` (E2-US6).
- Given no symbol loaded (all deferred, `no_new_data`, or failed), When `quality_checks` starts, Then it skips itself, sets `quality_status = skipped`, and stores no results.
- Given `quality_status` is `pass` or `warn` and at least one symbol loaded, When `refresh_views` starts, Then it calls the `SECURITY DEFINER` refresh function (E3-US8) and reports success per view.
- Given `quality_status = fail`, or nothing loaded, When `refresh_views` starts, Then it skips itself and the views keep their last good data. The committed data is never rolled back.
- Given `refresh_views` fails, When the error occurs, Then the previous view contents stay readable, the task fails, and the run ends `failed`.
- Given `finalize_run` (trigger rule `all_done`), When it executes, Then it sums the symbol rows into the `pipeline_runs` totals, sets `ended_at`, and sets `status` by these rules:
  - `failed` if any symbol is `failed`, any task failed, `quality_status = fail`, or `refresh_views` failed
  - `success` otherwise, including runs with deferred, `no_new_data`, or quarantined symbols, and runs with `quality_status = warn` or `skipped`
- Given a failed run, When it is finalized, Then `error_type`, a sanitized `error_message`, and the failed symbols are stored, and the good batches' data is kept.
- Given `finalize_run` ran before and runs again for the same run ID, When it finishes, Then the totals and status are identical (idempotent).
- Given scratch data older than the configured number of days, When `finalize_run` ends, Then it calls the staging and landing cleanup (E3-US4) as a best-effort last step. A cleanup failure is logged and never changes the run status.
- Given a run ends, When anyone queries it, Then the final state reads clearly from `status`, `quality_status`, and the symbol counters, for example "success, warn, 2 deferred".

**Notes:** E2 only sets `quality_status`. This story is the single owner of the final `status`.

---

### E4-US9: End-to-End Pipeline Test
- **As a** Data Engineer,
- **I want to** run the full DAG against a test database with a mocked API,
- **So that** Phase 1 is proven to work as a whole before Phase 2 starts.
- **Priority:** High
- **Covers:** NFR-9.4, NFR-3.3, NFR-3.5

**Acceptance Criteria:**
- Given the test setup, When it runs, Then it uses a test database built by the real migrations (E3-US1), a mocked Twelve Data API serving fixtures (no network, no real key), and the real ETL modules.
- Given a DAG parse test, When it runs, Then the DAG loads with no import errors and has the expected tasks and settings.
- Given a clean test database, When the DAG runs in `incremental` mode for the test symbols, Then raw, fact, dimension, watermark, quality, and run-history tables are filled, `pipeline_runs.status = success`, and the row counters balance.
- Given the same run is repeated, When it finishes, Then no duplicates exist and the fact table is unchanged (idempotency).
- Given one symbol returns an invalid payload, When the DAG runs, Then that symbol is `quarantined`, the others load normally, and nothing invalid reaches raw or fact.
- Given the mocked API exhausts the daily budget, When the DAG runs, Then the remaining symbols are `deferred`, the run is `success`, and the next run processes them first.
- Given a failure injected mid-load, When the DAG runs, Then that batch rolls back, its symbols are `failed`, the watermark is unchanged, other batches are kept, and the run ends `failed`.
- Given a `reprocess` run over an existing range, When it runs, Then the mock API receives zero calls and the fact data is identical to before.
- Given a failing quality check, When the DAG runs, Then `quality_status = fail`, the views are not refreshed, the run is `failed`, and the committed data is kept.
- Given a push or pull request, When CI runs (E6), Then this test runs and must pass.

---

## Remaining open questions
- Should quarantined symbols fail the run, or only be reported (the current decision)?
- Which Airflow major version is pinned (3.x uses an API server, which the setup assumes)?
- How does the Phase 2 API (E5) trigger the DAG: REST call with `dag_run.conf.run_id`, and does it pre-create the `queued` row?
- How many days of staging and landing data are kept before cleanup (config value, E3-US4)?
- Is a Monday to Friday 22:00 UTC schedule acceptable for both US and EGX symbols?

---

## Summary Table

| ID | Title | Priority | Covers |
| :--- | :--- | :--- | :--- |
| E4-US1 | Airflow Environment Setup | High | NFR-6.1, NFR-6.5, NFR-5.1, NFR-5.2 |
| E4-US2 | DAG Definition and Task Ordering | High | FR-8.1, NFR-5.1, NFR-3.3 |
| E4-US3 | Scheduling | High | FR-8.2, NFR-6.3 |
| E4-US4 | Retries, Timeouts, and Failure Handling | High | FR-8.3, NFR-2.2, NFR-3.1, NFR-3.4 |
| E4-US5 | Parallel Per-Batch Execution with API Rate Control | Medium | FR-8.5, FR-1.4, NFR-7.2, NFR-7.4 |
| E4-US6 | Backfills and Reprocessing | Medium | FR-8.4, FR-1.2, NFR-3.3, NFR-3.5 |
| E4-US7 | Run Logging and the `pipeline_runs` Lifecycle | High | FR-9.1, FR-9.2, NFR-8.1 to 8.3 |
| E4-US8 | Quality Gate, View Refresh, and Run Outcome | High | FR-8.1, FR-9.2, FR-7.3, FR-6.4, NFR-8.2 |
| E4-US9 | End-to-End Pipeline Test | High | NFR-9.4, NFR-3.3, NFR-3.5 |