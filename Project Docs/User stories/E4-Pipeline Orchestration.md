# E4: Pipeline Orchestration

**Phase:** 1 | **Covers:** FR-8, FR-9.1, FR-9.2 (+ NFR-2.2, NFR-3.3, NFR-3.4, NFR-3.5, NFR-5.1, NFR-6.3, NFR-7.4, NFR-8.1, NFR-8.2, NFR-8.3, NFR-9.4)

## Overview
This epic automates the whole Phase 1 workflow so it runs on a schedule without manual work, retries what can be retried, and records exactly what happened in every run. Airflow only runs, orders, retries, and monitors our Python code. It does not extract, transform, or load anything itself, so the DAG stays thin and the logic lives in testable modules.

## What this epic covers
- **Environment:** a pinned, reproducible Airflow setup with a metadata database separate from our data
- **DAG:** `start_run → [per-batch: extract → validate → save_raw → transform → load] → quality_checks → refresh_views → finalize_run`
- **Scheduling:** once per trading day after market close, configurable
- **Retries and timeouts:** task-level, with non-retryable failures failing fast
- **Parallelism:** one task group per symbol batch, with API calls serialized by pools
- **Backfills and reprocessing:** past date ranges, and rebuilds from raw with no API call
- **Run logging:** a `pipeline_runs` table plus a per-symbol `pipeline_run_symbols` table
- **Run outcome:** one place that decides success, failure, and whether views refresh
- **End-to-end test:** proves Phase 1 works as a whole

## Depends on
E1, E2, E3 (the DAG calls their code and writes to their tables). **Completing this epic finishes Phase 1.**
Build order: E3 Part A (tables, including `pipeline_runs`) → E1 → E2 → E3 Parts B and C → this epic's DAG wiring.

## Decisions

### Rules

| Topic | Decision |
|---|---|
| DAG id | `financial_market_pipeline` |
| Thin DAG | The DAG file only wires tasks. Every task calls a function from an importable ETL module. No business logic and no secrets in DAG code. |
| Metadata DB | Airflow's own database is separate from the project data database (same Postgres server is fine) |
| Task chain | `start_run` → per-batch group (`extract → validate → save_raw → transform → load`) → `quality_checks` → `refresh_views` → `finalize_run`. This is the FR-8.1 chain plus two bookend tasks. |
| Data hand-over | Through the database only. XCom carries only small values (run ID, batch lists, counts). |
| API response hand-over | `extract` writes responses to the scratch table `analytics.stg_api_responses`. `validate` marks them valid or invalid and writes invalid ones to `raw.quarantine`. `save_raw` copies valid ones to `raw.raw_responses`. A failure in `validate` or `save_raw` never re-calls the API, so no credits are wasted. |
| Run ID | One UUID per run. Taken from `dag_run.conf.run_id` if present (Phase 2 API), otherwise `uuid5(namespace, airflow_run_id)`. Because it is deterministic, a retried `start_run` gets the same ID. The Airflow run ID is stored next to it. |
| Run date | Logic uses watermarks and the actual run date. The logical date is a label only, which avoids the off-by-one trap where a run's logical date is the previous interval. |
| Overlap | `max_active_runs = 1`, so two runs never race on watermarks |
| Schedule default | `0 22 * * 1-5` (22:00 UTC, Monday to Friday), after the US and EGX closes. Configurable. `catchup = False`. |
| EGX note | EGX trades Sunday to Thursday, so Sunday's bar is fetched on Monday's run. Nothing is lost, because extraction is watermark-driven. |
| Retries (defaults) | `extract`: 1 retry. All other tasks: 3 retries. Delay 2 min, exponential backoff, capped at 30 min. |
| Why extract retries less | E1 already retries each HTTP call up to 5 times. Stacking 3 more task retries on top could mean up to 20 calls per symbol and burn the daily credit budget. |
| Non-retryable | Config errors, auth errors (401/403), and schema or validation bugs raise `AirflowFailException` (fail now, no retries) |
| Timeouts | `execution_timeout` per task (extract 30 min, others 15 min) and `dagrun_timeout` of 6 hours, all configurable |
| Parallelism | Batches are provider-pure (never mix providers). Pools `twelvedata_api` and `eodhd_api` have 1 slot each, so API calls run one batch at a time while later stages of earlier batches run in parallel. |
| Pause between batches | Applied inside `extract` before it releases the pool slot (60 s for Twelve Data), so the next batch cannot start early |
| Daily credit budget | Table `analytics.api_usage` (provider, usage_date UTC, credits_used). Credits are reserved atomically before each call. Deferred symbols are processed first next run (no watermark first, then oldest watermark). |
| Batch isolation | A failing batch fails alone. Other batches continue. |
| Partial failure | If any batch fails after retries, the run is `failed`, and `failed_symbols` lists them. Data loaded by good batches is kept. |
| Single writer for totals | Each batch writes only its own rows in `pipeline_run_symbols`. Only `finalize_run` writes totals into `pipeline_runs` (by summing). No concurrent updates to one row. |
| Run status values | `running`, `success`, `failed` (plus `queued` when the Phase 2 API pre-creates the row). Quality warnings use the separate `quality_status` column. |
| Row accounting | `rows_extracted = rows_inserted + rows_updated + rows_unchanged + rows_rejected + duplicates_removed`. `rows_loaded = rows_inserted + rows_updated + rows_unchanged`. `rows_recalculated` is reported separately and is not part of the balance (same rule as E2 and E3). |
| Quality gate | `quality_checks` runs if at least one batch loaded. `refresh_views` runs only if quality is not `fail` and at least one batch loaded. |
| Retention | `pipeline_runs` and `pipeline_run_symbols` are never deleted (NFR-8.3). |

### Table definitions (created by E3 migrations, specified here)

`analytics.pipeline_runs`

| Column | Notes |
|---|---|
| run_id | UUID, PK |
| airflow_dag_id, airflow_run_id | `airflow_run_id` is unique (this is the FR-12.3 link) |
| trigger_type | scheduled / manual / api / backfill |
| mode | incremental / backfill / reprocess |
| status | queued / running / success / failed |
| quality_status | pass / warn / fail / skipped (null while running) |
| params | JSONB: symbols, start_date, end_date |
| started_at, ended_at | `TIMESTAMPTZ` UTC. Duration is computed from them. |
| symbols_requested, symbols_succeeded, symbols_no_new_data, symbols_failed, symbols_quarantined, symbols_deferred | counters |
| rows_extracted, rows_rejected, duplicates_removed, rows_inserted, rows_updated, rows_unchanged, rows_loaded, rows_recalculated | counters |
| error_type, error_message | sanitized and truncated, no secrets |
| created_at | |

`analytics.pipeline_run_symbols`: PK (run_id, symbol), FK to `pipeline_runs`; columns `provider`, `batch_no`, `status` (success / no_new_data / failed / quarantined / deferred), the same row counters per symbol, `error_type`, `error_message`, `updated_at`.

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
- Given the executor, When it is configured, Then it is `LocalExecutor` (enough for the project, no Celery).
- Given the database setup, When it runs, Then Airflow's metadata database is separate from the project data database.
- Given the project code, When Airflow starts, Then the ETL modules are importable by the DAG (mounted or installed).
- Given secrets (database URL, API keys, Airflow admin password), When they are supplied, Then they come from environment variables or Airflow Connections, and `.env.example` has placeholders only.
- Given a restart, When the stack comes back, Then DAG state and run history persist in the metadata database.
- Given the pools, When Airflow starts, Then `twelvedata_api` and `eodhd_api` (1 slot each) exist, created by a setup script.

**Notes:** Phase 1 runs Airflow on its own. E6 later folds it into the full Docker Compose stack.

---

### E4-US2: DAG Definition and Task Ordering
- **As a** Orchestration Engineer,
- **I want to** define the DAG with the full chain and safety settings,
- **So that** steps always run in the right order and bad data never moves forward.
- **Priority:** High
- **Covers:** FR-8.1, NFR-5.1

**Acceptance Criteria:**
- Given the DAG file, When it is parsed, Then it has no import errors and contains `start_run`, then per batch `extract → validate → save_raw → transform → load`, then `quality_checks`, `refresh_views`, and `finalize_run`.
- Given `validate` fails for a batch, When the run continues, Then `save_raw`, `transform`, and `load` do not run for that batch.
- Given the DAG settings, When inspected, Then `max_active_runs = 1`, `catchup = False`, and `dagrun_timeout` are set.
- Given the DAG file, When it is inspected, Then each task only calls a function from the ETL modules, and no secret or business logic is in it.
- Given data between tasks, When tasks hand over work, Then only small values pass through XCom (run ID, batch lists, counts). Responses and rows pass through the database.
- Given a run already active, When a second run is triggered, Then it waits and does not run in parallel with the first.
- Given each task, When it is rerun for the same run ID, Then it gives the same result (idempotent): `start_run` upserts the run row, `extract` replaces its landing rows, `load` is an upsert (E3-US6).
- Given a DAG-level failure callback, When a run fails or times out, Then it marks the run `failed` in `pipeline_runs` (see E4-US7).

**Notes:** `finalize_run` uses trigger rule `all_done`, so it runs no matter what failed. Use the TaskFlow API (`@task`) for the mapped groups.

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
- Given a scheduled run, When symbols are chosen, Then they come from the symbol list in configuration, with deferred symbols first (see E4-US5).
- Given a scheduled run, When extraction starts, Then it uses the run date and the watermarks, not the logical date, to decide what to request.
- Given a missed schedule (for example the machine was off), When Airflow comes back, Then it does not create a flood of past runs (`catchup = False`). The next run fetches everything after each watermark, so no data is lost.
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
- Given `extract`, When it fails, Then it has 1 retry only, because HTTP-level retries already happened inside E1-US4 and credits are limited.
- Given a configuration error, an authentication failure (401/403), or a validation bug, When it occurs, Then the task fails immediately with no retries.
- Given a task fails, When retries are exhausted, Then the task is marked failed, downstream tasks of that batch are skipped, and the run ends `failed`.
- Given a task, When it runs, Then it has an `execution_timeout`, so it can never hang (values from configuration).
- Given any failure, When it is recorded, Then `error_type` and a sanitized `error_message` are saved on the batch's symbols and in the run row, and the full traceback stays in the Airflow task log.
- Given retry and timeout values, When they are changed in configuration, Then they apply with no code edit.
- Given an exhausted retry on a batch, When the other batches finish, Then they are unaffected.

---

### E4-US5: Parallel Per-Batch Execution with API Rate Control
- **As a** Orchestration Engineer,
- **I want to** run one task group per symbol batch while keeping API calls inside the limits,
- **So that** a failure retries only its own batch and parallel work never breaks the rate limit.
- **Priority:** Medium
- **Covers:** FR-8.5, FR-1.4, NFR-7.2, NFR-7.4

**Acceptance Criteria:**
- Given the symbol list, When `start_run` plans the run, Then it creates provider-pure batches (size from configuration, 8 for Twelve Data) and returns the batch list as a small XCom value.
- Given the batch list, When the DAG runs, Then it creates one mapped task group per batch (dynamic task mapping).
- Given several batches, When their `extract` tasks run, Then the provider pool (1 slot) makes them run one at a time. The later stages (`validate` to `load`) of finished batches run in parallel with the next batch's `extract`.
- Given a batch finished extracting, When it releases the pool slot, Then it has first waited the configured pause (60 s for Twelve Data).
- Given the daily budget in `analytics.api_usage`, When a batch would exceed the provider's daily limit, Then its symbols are marked `deferred`, no call is made, and the run does not fail because of it.
- Given deferred symbols from an earlier run, When the next run plans batches, Then they come first, then symbols with no watermark, then the oldest watermarks.
- Given EODHD symbols, When batches are planned, Then they use the `eodhd_api` pool and the EODHD daily limit.
- Given one batch fails, When the run continues, Then the other batches are still extracted and loaded.
- Given an empty symbol list, When `start_run` plans the run, Then no batches are created and the run finishes `success` with zero symbols.

**Notes:** Parallel batches would break the 8 credits/minute limit without the pool. This is why the pool exists. Drop the EODHD parts if E1-US8 is dropped.

---

### E4-US6: Backfills and Reprocessing
- **As a** Data Engineer,
- **I want to** run the DAG for a past date range, or rebuild a range from stored raw data,
- **So that** history can be loaded or repaired safely, without extra API calls when the raw data already exists.
- **Priority:** Medium
- **Covers:** FR-8.4, FR-1.2, NFR-3.3, NFR-3.5

**Acceptance Criteria:**
- Given a manual trigger with `mode = backfill`, `start_date`, `end_date`, and optional `symbols`, When the run starts, Then one run covers the whole range (not one run per day), splitting requests as E1-US1 describes.
- Given parameters, When they are submitted, Then they are validated (start not after end, no future end date, symbols in the configured list) and an invalid trigger fails at `start_run` with a clear message.
- Given a backfill of old dates, When it loads, Then the watermark never moves backward (E3-US6) and affected later metrics are recalculated (E2-US7).
- Given `mode = reprocess` with a range or a source run ID, When the run starts, Then `extract`, `validate`, and `save_raw` are skipped, and `transform → load → quality_checks → refresh_views` rebuild the range from raw with no API call.
- Given a backfill or reprocess is run twice for the same range, When both finish, Then the data is identical and has no duplicates.
- Given Airflow's own backfill command for a date range, When it is used, Then it is also safe (zero-row runs), but the parameter method is the documented one because it saves credits.
- Given the run row, When a backfill or reprocess run is stored, Then `mode`, `trigger_type`, and `params` record what was requested.

---

### E4-US7: Run Logging and the `pipeline_runs` Lifecycle
- **As a** Data Engineer,
- **I want to** record every run's times, status, symbols, row counts, and errors,
- **So that** any run can be audited, and the Phase 2 API and monitoring can read its history.
- **Priority:** High
- **Covers:** FR-9.1, FR-9.2, NFR-8.1, NFR-8.2, NFR-8.3

**Acceptance Criteria:**
- Given a run begins, When `start_run` executes, Then it upserts a row in `pipeline_runs` (`status = running`, `started_at`, run ID, Airflow run ID, trigger type, mode, params, `symbols_requested`) and creates one `pipeline_run_symbols` row per planned symbol. A repeat of `start_run` changes nothing.
- Given a batch stage finishes, When it reports, Then it sets (not increments) that symbol's counters in `pipeline_run_symbols`: extracted, rejected, duplicates removed, inserted, updated, unchanged, recalculated, plus status and error.
- Given the run ends, When `finalize_run` executes, Then it writes the totals into `pipeline_runs` by summing `pipeline_run_symbols`, sets `ended_at`, and sets `status`.
- Given a symbol, When its final status is recorded, Then it is one of `success`, `no_new_data`, `failed`, `quarantined`, or `deferred`.
- Given the row counters, When `finalize_run` runs, Then `rows_extracted` equals inserted + updated + unchanged + rejected + duplicates removed, and any mismatch is logged as an error.
- Given a failed run, When it is recorded, Then `error_type`, a sanitized `error_message`, and the failed symbols are stored.
- Given the DAG is killed or times out, When the failure callback fires, Then the run is marked `failed` and does not stay `running` forever. `finalize_run` also creates the row if `start_run` never got to.
- Given any log line from our code, When it is written, Then it includes timestamp, level, run ID, task ID, and symbol where relevant, and it never contains API keys or full connection strings.
- Given a run ID, When searched, Then it finds the run row, its symbol rows, the raw and quarantine rows, and the quality results that share it.
- Given the tables, When history is queried, Then no job deletes or