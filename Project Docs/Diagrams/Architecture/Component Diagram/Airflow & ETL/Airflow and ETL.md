# Component Diagram: ETL Pipeline (Airflow Container)

**Covers:** E1, E2, E3 (load and repositories), E4 | Phase 1

## 1. Purpose

The container diagram showed Airflow as one box. This diagram opens that box and shows the main building blocks of code inside it and how they depend on each other.

The Airflow container holds two things:
- the **DAG**, which is only a schedule and a list of tasks
- the **ETL package**, which holds all the real logic (the same package the API image installs in Phase 2)

**Main idea:** Airflow does not do ETL. Our components do. The DAG only says "run this function, then that one". This keeps the logic testable without Airflow (NFR-9.1).


## 3. Elements outside the container

| Element | What it is | Who talks to it |
|---|---|---|
| Twelve Data / EODHD | The external market data APIs | Provider Clients only |
| Project DB | PostgreSQL with the `raw` and `analytics` schemas | Repositories only |
| Metadata DB | Airflow's own memory (run and task state) | Airflow itself |

Only one component touches each outside element. If the provider or the database changes, only that one place changes.

## 4. Components

### 4.1 DAG (`financial_market_pipeline`)

| Item | Detail |
|---|---|
| Responsibility | Schedule (22:00 UTC, Monday to Friday), task order, retries, timeouts. It wires tasks together and nothing else. |
| Rules | Each task calls exactly one function from the ETL package. No business logic and no secrets in DAG code. |
| Pattern | Orchestrator. The workflow is Pipes and Filters: each task is a filter and each dependency is a pipe. |
| Stories | E4-US2, E4-US3, E4-US4, E4-US5 |

### 4.2 Config

| Item | Detail |
|---|---|
| Responsibility | Loads every changeable value from environment variables or config files: API keys, base URLs, symbols, batch size, pauses, retry limits, window sizes, quality thresholds, schedule. |
| Behavior | Fails fast with a clear message naming the missing setting, before any API call. |
| Why no arrows | Every component reads it. Drawing all arrows would hide the real structure. |
| Pattern | One shared configuration source, so Airflow and the API stay consistent. |
| Stories | E1-US7 |

### 4.3 Extraction group

**Provider Clients**

| Item | Detail |
|---|---|
| Responsibility | Build the request, call the provider, and return the raw response. One client for Twelve Data and one for EODHD. |
| Pattern | Adapter and Strategy. Both clients follow one interface, so the pipeline does not care which provider a symbol uses. The provider is chosen by the symbol's `primary_provider` in config. |
| Also handles | Splitting long backfills into date ranges (5000-point limit), and the incremental start date (watermark + 1 day). |
| Stories | E1-US1, E1-US2, E1-US8 |

**Rate Limiter and Credit Budget**

| Item | Detail |
|---|---|
| Responsibility | Protect the limits: 8 credits per minute and 800 per day for Twelve Data, 20 per day for EODHD. |
| Batches | Groups requests into batches that fit the per-minute limit. |
| Pauses | Waits between batches (60 seconds for Twelve Data). |
| Credits | Reserves one credit before every call, retries included. |
| Retries | Retries timeouts, 429, and 5xx with exponential backoff plus jitter. Never retries 400 or 404. Fails immediately on 401 or 403 (bad key). |
| Daily budget | When the budget is spent, remaining symbols are marked `deferred` and the run does not fail because of it. |
| Why separate | The same limit logic applies to any provider, and it can be tested without calling anything. |
| Stories | E1-US3, E1-US4 |

### 4.4 Processing stages

The stages are drawn left to right in the same order as the DAG tasks.

**Validator**

| Item | Detail |
|---|---|
| Responsibility | Inspect each response saved by `extract`: HTTP status, payload shape (`status == "ok"`, `meta`, `values[]`, types), and that the symbol matches. Mark each response valid or invalid. |
| Detail | A 200 response with an error body is still invalid, except the documented "no data" case, which counts as zero rows. |
| Rule | It never calls the API again. It works from the landing table. |
| Stories | E1-US5 |

**Raw Writer**

| Item | Detail |
|---|---|
| Responsibility | Copy valid responses to `raw.raw_responses` and invalid ones to `raw.quarantine`, unchanged, with metadata. |
| Retry safety | `landing_id` is unique, so a retried task never creates a duplicate row. |
| Stories | E1-US5, E1-US6 |

**Transformer**

| Item | Detail |
|---|---|
| Responsibility | Read only from raw (`purpose = 'primary'`), parse JSON, cast types, standardize timestamps, remove duplicates, reject invalid rows, flag missing values. Then calculate the metrics: daily return, moving averages, rolling volume averages, volatility, cumulative return, rolling high and low. |
| Lookback | Reads up to 31 earlier rows from the fact table so rolling windows stay correct on incremental runs. |
| Output | Writes to the staging table (`stg_daily_prices`). |
| Design choice | Mostly pure functions on DataFrames, so it is testable with no API and no database. It does not need to be class-heavy. |
| Stories | E2-US1, E2-US2, E2-US3, E2-US7 |

**Loader**

| Item | Detail |
|---|---|
| Responsibility | Move staged rows into the star schema. |
| One transaction per symbol batch | Creates missing dimension rows, upserts facts (set-based `INSERT ... ON CONFLICT DO UPDATE`), and moves the watermark. |
| Failure | If anything fails, the whole batch rolls back, the old watermark stays, and other batches are unaffected. |
| Pattern | Unit of Work. |
| Stories | E3-US5, E3-US6 |

**Quality Checker**

| Item | Detail |
|---|---|
| Responsibility | Read-only checks after loading: nulls, duplicates, invalid values, date gaps, and row-count balance. |
| Thresholds | Compares each result to warn and fail thresholds from config, stores results per run, and sets the run's quality status. |
| Pattern | Each check is a small, similar unit (Strategy or Template Method), which can be modeled as classes in the class diagram. |
| Stories | E2-US4, E2-US5, E2-US6 |

### 4.5 Run Tracker

| Item | Detail |
|---|---|
| Responsibility | The audit trail of every run. |
| `start_run` | Creates the run row and per-symbol rows, validates trigger parameters, and plans the batches. |
| During the run | Each stage updates its own counters. |
| `finalize_run` | Sums the counters, decides success or failed, and cleans up old scratch data. |
| Key rule | Only `finalize_run` sets the final run status. |
| Stories | E4-US5, E4-US7, E4-US8 |

### 4.6 Repositories

| Item | Detail |
|---|---|
| Responsibility | The only place with SQL: raw tables, staging, dimensions, facts, watermark, API usage, quality results, run history. |
| Pattern | Repository. Components ask for data in business terms ("get the watermark for symbol X"), not in SQL. |
| Benefits | SQL lives in one place, tests can use fake repositories, and the `etl_writer` role permissions are enforced in one layer. |
| Stories | E3 as a whole |

## 5. Arrows

| Arrow | Meaning |
|---|---|
| DAG → Extraction | The `extract` task triggers extraction. |
| DAG → Processing stages | One task per stage calls one function. |
| DAG → Run Tracker | `start_run` and `finalize_run` bookend the run. |
| DAG ⇢ Metadata DB | Airflow records task state. Dashed because Airflow does it, not our code. |
| Provider Clients → External APIs | The only HTTP calls out. |
| Provider Clients → Rate Limiter | Ask for a credit and a wait before every call. |
| Extraction, Stages, Run Tracker → Repositories | All database access goes through Repositories. |
| Repositories → Project DB | The only SQL connection. |

**Not drawn on purpose:** the stages do not call each other. Each reads the previous stage's output from the database, so a failed stage can be retried without redoing earlier ones.

## 6. One daily run through the diagram

1. The schedule fires. The DAG calls the Run Tracker's `start_run`, which creates the run row and plans symbol batches.
2. For each batch, `extract` uses Provider Clients, asking the Rate Limiter before each call. Responses are saved to the landing table.
3. The Validator marks responses valid or invalid. The Raw Writer stores them in raw or quarantine.
4. The Transformer reads raw, cleans it, and calculates metrics into staging.
5. The Loader upserts into the star schema and moves the watermark, in one transaction.
6. After all batches, the Quality Checker runs. If quality does not fail, the views refresh.
7. The Run Tracker's `finalize_run` writes the totals and the final status.

## 7. Design decisions

| Decision | Reason |
|---|---|
| Thin DAG, fat package | Logic is testable, and the same code runs in Airflow and in the API image. |
| One component per outside dependency | Only Provider Clients know the APIs and only Repositories know SQL. |
| Hand-over through the database | Retries are safe and no large data goes through XCom. |
| Separate stages | A failure retries only its own stage. |

## 8. Simplifications and open points

- Batch planning (deferred symbols first, then no watermark, then oldest watermark) is drawn inside Run Tracker. It could become its own Batch Planner component.
- Dimension population is drawn inside Loader. It could be separate.
- The view refresh step (`refresh_views`) is not drawn. It is a small call to a database function and could be added under Loader or Quality.
- Component names are a proposal from E1 to E4 and can be renamed or merged by the team.