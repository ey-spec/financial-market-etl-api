# E3: Database & Analytics

**Phase:** 1 | **Covers:** FR-5, FR-6, FR-9.3, FR-9.4 (+ the storage side of FR-1.3, FR-2.3, FR-3, FR-4.1, FR-7.2, FR-9.2; NFR-3.2, NFR-3.3, NFR-4.1, NFR-4.2, NFR-4.3, NFR-4.4, NFR-5.1, NFR-6.1, NFR-7.3, NFR-8.3, NFR-11.3, NFR-12.1)

## Overview
This epic designs the PostgreSQL database, loads transformed data into it with full history, and builds the analytics on top. It owns every table in the project: the raw layer, the staging and state tables, the star schema, and the quality results. E1 and E2 write into these tables, so the schema is built first.

## What this epic covers
- **Setup:** `raw` and `analytics` schemas, versioned migrations, least-privilege roles
- **Raw layer:** append-only response storage and the quarantine table
- **Star schema:** `dim_symbol`, `dim_date`, `fact_daily_prices` with all metric columns, keys, constraints, indexes
- **Operational tables:** landing, staging, watermark, API credit usage, quality results
- **Dimension population:** symbols from provider metadata or config, dates generated
- **Loading:** set-based upserts, one transaction per symbol batch, history never deleted, watermark updated in the same transaction
- **Analytical SQL:** window functions and CTEs (top gainers/losers, monthly volume, rolling returns, best/worst days)
- **Views:** materialized views and plain views, refreshed after quality checks pass
- **Visualization:** price chart with moving averages, reading only the analytics layer
- **ERD:** documented and kept in sync with the migrations
- **Run history tables:** `pipeline_runs` and `pipeline_run_symbols` (columns specified in E4, created here)


## Depends on
This epic is split in time, which removes the circular dependency with E1, E2, and E4:
- **Part A (US1 to US5, US10, US11) has no dependencies.** It is built first and includes the run history tables, so every `run_id` foreign key can exist from day one.
- **Part B (US6 loading)** needs E2's staging output.
- **Part C (US7 to US9)** needs loaded data.
- **E4** specifies the columns of `pipeline_runs` and `pipeline_run_symbols` and writes to them. E3 only creates them.

Build order: E3 Part A → E1 → E2 → E3 Parts B and C → E4 DAG wiring.

## Decisions

### Rules

| Topic | Decision |
|---|---|
| Layers | `raw` (original responses, quarantine) and `analytics` (everything else) |
| Symbol identity | `dim_symbol.symbol` is the configured identifier (for example `AAPL`, `COMI.EGX`) and is unique |
| One provider per symbol | A symbol has one `primary_provider` (from configuration), so the fact key never collides. Cross-check fetches (`purpose = 'cross_check'`) are kept in raw only and never reach staging or the fact table. |

| Fact key | `(symbol_id, trade_date)`, matching Resources.md and NFR-4.1 |
| Types | Prices and rolling high/low `NUMERIC(18,6)`, volume `BIGINT`, returns, volatility and cumulative return `NUMERIC(12,8)`, rolling volume averages `NUMERIC(20,2)`, timestamps `TIMESTAMPTZ` in UTC (same as E2) |

| Raw payload | `raw.raw_responses.payload` is `JSONB`. "Unchanged" means the same JSON content, though JSONB does not keep key order or whitespace. |
| Quarantine body | `TEXT`, because a bad response may not be valid JSON |
| Never delete | No `DELETE` or `TRUNCATE` on raw, fact, dimension, quality, run history, or API usage tables. FKs use `ON DELETE RESTRICT`. The only deletes allowed in the whole database are staging and landing cleanup (E3-US4). |

| Dimension changes | SCD Type 1 (overwrite) |
| Load unit | One transaction per symbol batch, matching the DAG's per-batch tasks. A failed batch rolls back alone. |
| Watermark | Updated in the same transaction as the fact upsert, so it can never get ahead of the data |
| Row origin | Staging rows are `new` (from this run's raw rows) or `recalculated` (older rows whose metrics changed). `new` rows are counted as inserted, updated, or unchanged. `recalculated` rows are counted separately and stay outside the row-count balance (E2, E4). |

| Views | Heavy aggregations are materialized views, row-level logic is a plain view. Refresh runs only after quality checks do not fail and at least one batch loaded (E4-US8). |

| Volume summary | `mv_symbol_volume_stats` is a view over the fact table, not a table filled by E2 (resolves E2's open question) |

| Metric column names | Named by role, not by number: `ma_short`, `ma_long`, `vol_avg_short`, `vol_avg_long`, `volatility_long`, `rolling_high_long`, `rolling_low_long`. Window sizes (7 and 30 by default) come from configuration, so changing them needs no migration (matches E2). |


| Run ID links | `run_id` is a foreign key to `pipeline_runs` on `raw.raw_responses`, `raw.quarantine`, `analytics.quality_results`, and `analytics.pipeline_run_symbols`. Scratch tables have none. |

| Idempotent hand-over | `landing_id` is carried from `stg_api_responses` into `raw.raw_responses` and `raw.quarantine` (unique, inserted with `ON CONFLICT DO NOTHING`), so a retried `validate` or `save_raw` never creates duplicates. |


| Credit reservation | One atomic statement per call: `INSERT ... ON CONFLICT (provider, usage_date) DO UPDATE SET credits_used = credits_used + 1 WHERE credits_used + 1 <= daily_limit RETURNING ...`. No row returned means the budget is spent. |

### Table catalog

| Table | Key columns |
|---|---|
| `raw.raw_responses` | raw_id (PK), landing_id (unique), run_id (FK), provider, purpose (primary / cross_check), symbol, source_endpoint, request_params (JSONB, no secrets), http_status, fetched_at, has_preliminary_bar, payload (JSONB) |
| `raw.quarantine` | quarantine_id (PK), landing_id (unique), run_id (FK), provider, symbol, source_endpoint, request_params, http_status, fetched_at, failure_reason (NOT NULL), response_body (TEXT) |
| `analytics.dim_symbol` | symbol_id (PK), symbol (unique), name, exchange, mic_code, currency, exchange_timezone, instrument_type, primary_provider, is_active, created_at, updated_at |
| `analytics.dim_date` | full_date (PK), year, quarter, month, month_name, day, day_of_week, day_name, week_of_year, is_weekend |
| `analytics.fact_daily_prices` | PK (symbol_id, trade_date); open, high, low, close, adjusted_close, volume; daily_return, ma_short, ma_long, vol_avg_short, vol_avg_long, volatility_long, cumulative_return, rolling_high_long, rolling_low_long; has_missing_values, is_preliminary; run_id, created_at, updated_at |
| `analytics.stg_daily_prices` | PK (run_id, symbol, trade_date); same columns as the fact (symbol as text instead of symbol_id) plus row_origin. No foreign keys, it is scratch space. |
| `analytics.stg_api_responses` | landing_id (PK), run_id, batch_no, provider, purpose, symbol, source_endpoint, request_params, http_status, fetched_at, has_preliminary_bar, body (TEXT), validation_status (pending / valid / invalid), failure_reason, created_at. Scratch space, no foreign keys. |
| `analytics.symbol_watermark` | symbol_id (PK, FK), last_loaded_date, run_id, updated_at |
| `analytics.api_usage` | PK (provider, usage_date UTC), credits_used, updated_at |
| `analytics.quality_results` | result_id (PK), run_id (FK), check_name, symbol_id (nullable FK), measured_value, warn_threshold, fail_threshold, status (pass / warn / fail), checked_at. Unique on run_id + check_name + symbol_id, treating NULL as one value. |
| `analytics.pipeline_runs` | Columns specified in E4 (including `quality_status` and the row counters). `airflow_run_id` is unique. |
| `analytics.pipeline_run_symbols` | PK (run_id, symbol), FK to `pipeline_runs`. Columns specified in E4. |

---

## User Stories

### E3-US1: Database Setup, Migrations, and Access Roles
- **As a** Database Engineer,
- **I want to** create the schemas and all tables through versioned migrations with least-privilege roles,
- **So that** any teammate can rebuild the database from scratch and no component has more access than it needs.
- **Priority:** High
- **Covers:** FR-5.1, NFR-5.1, NFR-5.2, NFR-6.1

**Acceptance Criteria:**
- Given an empty PostgreSQL database, When the migrations run, Then the `raw` and `analytics` schemas and every table in the catalog exist with their keys and indexes.
- Given the migrations run a second time, When they finish, Then there is no error and no change.
- Given an existing database that already holds data, When a new migration runs, Then existing rows are preserved. Migrations never drop tables that hold data.
- Given the repository, When it is inspected, Then migrations are numbered SQL files in Git, applied by one documented command.
- Given the roles, When permissions are checked, Then `etl_writer` can insert into raw and quarantine, insert/update the analytics tables, and delete only from `stg_daily_prices` and `stg_api_responses`. It cannot delete or truncate any other table. `analytics_reader` can only select from the analytics schema and has no access to `raw`.

- Given role passwords and connection details, When they are configured, Then they come from environment variables and never appear in migration files.

- Given the materialized views, When they are refreshed, Then refresh runs through a `SECURITY DEFINER` function owned by the view owner and executable by `etl_writer`, because only a view's owner can refresh it.

---

### E3-US2: Raw Layer Tables (Append-Only)
- **As a** Data Engineer,
- **I want to** have tables that keep every valid response and every quarantined response untouched,
- **So that** we have a permanent audit trail and can reprocess without the API.
- **Priority:** High
- **Covers:** FR-3.1, FR-3.2, FR-3.3, FR-2.3, NFR-4.4, NFR-12.1

**Acceptance Criteria:**
- Given a valid response, When it is stored in `raw.raw_responses`, Then it carries `landing_id`, symbol, fetch timestamp (UTC), run ID, provider, `purpose`, source endpoint, `request_params`, HTTP status, `has_preliminary_bar`, and the JSONB payload.

- Given `source_endpoint` and `request_params`, When they are stored, Then they never contain the API key or token.
- Given an invalid response, When it is stored in `raw.quarantine`, Then the body is saved as text with a mandatory failure reason and the same metadata.
- Given any row in a raw table, When an `UPDATE`, `DELETE`, or `TRUNCATE` is attempted, Then a trigger rejects it and the privileges are also revoked (two layers of protection).
- Given the same symbol and dates are fetched twice, When both responses are stored, Then both are kept. De-duplication happens in the transform (E2), not in raw.
- Given the raw tables, When queried by run ID or by symbol and fetch time, Then indexes on `(run_id)` and `(symbol, fetched_at DESC)` support it.
- Given the retention policy, When the system runs, Then no job ever purges raw or quarantine data.
- Given the same `landing_id` is inserted twice (a retried `save_raw` or `validate`), When the insert runs with `ON CONFLICT DO NOTHING`, Then no duplicate row is created in `raw.raw_responses` or `raw.quarantine`.
- Given a row in either raw table, When it is inserted, Then its `run_id` must exist in `pipeline_runs`.
---

### E3-US3: Star Schema Tables (Dimensions and Fact)
- **As a** Database Engineer,
- **I want to** define `dim_symbol`, `dim_date`, and `fact_daily_prices` with keys, constraints, and indexes,
- **So that** analytics are fast, consistent, and protected from bad or duplicate data at the database level.
- **Priority:** High (FR-5.4 is a Should, but E2 and Resources.md already depend on these tables, so consider promoting it to a Must)
- **Covers:** FR-5.4, FR-5.5, NFR-4.1, NFR-4.2, NFR-4.3, NFR-7.3

**Acceptance Criteria:**
- Given the fact table, When it is created, Then its primary key is `(symbol_id, trade_date)` with foreign keys to `dim_symbol` and `dim_date`, both `ON DELETE RESTRICT`.
- Given a duplicate `(symbol_id, trade_date)` insert, When it is attempted without an upsert, Then the database rejects it.
- Given the columns, When types are checked, Then they match the Rules table: exact `NUMERIC` for prices, `BIGINT` for volume, and `TIMESTAMPTZ` in UTC for timestamps.
- Given constraints, When bad data is inserted, Then the database rejects `close` that is null or ≤ 0, any non-null open, high, low, or adjusted_close ≤ 0, `volume` < 0, and `high` < `low`. Nullable columns that are null pass.

- Given the fact table, When it holds metric columns, Then it includes every E2 metric under the role-based names in the Decisions table (returns, short and long moving averages, rolling volume averages, volatility, cumulative return, rolling high/low) plus `has_missing_values`, `is_preliminary`, and the last-writing `run_id`.
- Given the fact table, When it holds metric columns, Then it includes every metric from E2 (returns, MAs, rolling volume averages, volatility, cumulative return, rolling high/low) plus `has_missing_values`, `is_preliminary`, and the last-writing `run_id`.

**Notes:** The CHECK constraints are a second line of defense. E2 already rejects invalid rows, so a constraint failure means an upstream bug and aborts that batch.

---

### E3-US4: Landing, Staging, Watermark, API Usage, and Quality Tables
- **As a** Data Engineer,
- **I want to** have the state and hand-over tables that E1 and E2 write to,
- **So that** tasks pass data through the database (never XCom), progress is tracked safely, API credits are controlled, and quality results are stored per run.
- **Priority:** High
- **Covers:** FR-1.3, FR-1.4, FR-4.1, FR-7.2, NFR-4.5, NFR-7.2

**Acceptance Criteria:**
- Given `extract` finishes a request, When it saves the response, Then a row is written to `analytics.stg_api_responses` with a unique `landing_id`, and `validate` marks it `valid` or `invalid` without calling the API.
- Given the transform finishes, When it writes to `analytics.stg_daily_prices`, Then every row carries its run ID and a `row_origin` of `new` or `recalculated`.
- Given a staging table, When duplicate `(run_id, symbol, trade_date)` rows are inserted, Then the primary key rejects them.
- Given a batch's transform starts, When staging is prepared, Then only that run ID's rows for that batch's symbols are cleared. Other batches' rows are untouched. Given a batch's `extract` starts, Then it replaces only its own landing rows the same way.
- Given staging or landing rows older than a configurable number of days, When cleanup runs, Then they are purged. These two tables are the only place deletes are allowed.
- Given `analytics.symbol_watermark`, When it is queried, Then it returns one row per symbol with the last loaded date, the run that set it, and the update time.
- Given `analytics.api_usage`, When a credit is reserved, Then the atomic statement from the Decisions table is used, so parallel tasks can never exceed the daily limit.
- Given `analytics.quality_results`, When the same run ID, check, and symbol are saved twice, Then the unique rule makes the second save replace the first.
- Given quality results, When joined to `pipeline_runs` by run ID, Then they match that run (enforced by the foreign key).

---

### E3-US5: Dimension Population
- **As a** Data Engineer,
- **I want to** fill `dim_symbol` and `dim_date` automatically,
- **So that** fact rows always have valid parents and the calendar is ready for date-based analysis.
- **Priority:** High
- **Covers:** FR-5.4

**Acceptance Criteria:**
- Given a Twelve Data response, When dimensions are populated, Then `dim_symbol` is filled from `meta` (symbol, exchange, MIC code, currency, timezone, instrument type).
- Given an EODHD symbol, When dimensions are populated, Then name, exchange, and currency come from configuration, since the response has none.
- Given a symbol already exists, When its details changed, Then the row is updated in place (SCD Type 1) and no duplicate is created.
- Given a batch about to be loaded, When a symbol has no `dim_symbol` row, Then the row is created first, inside the same transaction, so the foreign key never fails.
- Given the setup, When `dim_date` is seeded, Then it holds every date from the configured start date to the end of next year, with year, quarter, month, month name, day, ISO weekday, day name, week of year, and `is_weekend`.
- Given a fact date outside the seeded range, When it is loaded, Then the missing `dim_date` rows are added automatically.
- Given `is_weekend`, When used, Then it is a plain Saturday/Sunday flag only. Trading-day logic uses the exchange calendars from config (EGX trades Sunday to Thursday).

---

### E3-US6: Idempotent, Transactional Upsert Loading
- **As a** Data Engineer,
- **I want to** upsert staged rows into the fact table in one transaction per symbol batch,
- **So that** reruns never create duplicates, failed loads leave existing data intact, and history is never lost.
- **Priority:** High
- **Covers:** FR-5.2, FR-5.3, FR-5.7, NFR-3.1, NFR-3.2, NFR-3.3, NFR-4.1

**Acceptance Criteria:**
- Given staged rows, When the load runs, Then it uses one set-based `INSERT ... SELECT ... ON CONFLICT (symbol_id, trade_date) DO UPDATE` per batch, not row-by-row inserts.
- Given a conflicting row whose values are identical, When the upsert runs, Then the row is skipped (updated only `WHERE` a value differs), so `updated_at` does not change for nothing.
- Given a revised row (for example a preliminary bar replaced by the final bar), When it is loaded, Then the existing row is updated and its `is_preliminary` becomes false.
- Given `recalculated` staging rows, When the load runs, Then their metric columns are updated on existing fact rows and they are reported separately from `new` rows.
- Given a batch, When the load commits, Then the dimension upserts, the fact upsert, and the watermark update all commit together.
- Given any error during a batch, When it occurs, Then the transaction rolls back, existing data and the old watermark are untouched, and the error is logged with the run ID and the batch's symbols.
- Given a batch fails and rolls back, When the error is recorded, Then a separate transaction sets that batch's symbols to `failed` with a sanitized `error_type` and `error_message` in `pipeline_run_symbols`.
- Given the watermark, When it is updated, Then it moves to the latest finalized (`is_preliminary = false`) trade date loaded for each symbol and never decreases. If a symbol loaded only a preliminary bar, its watermark is left unchanged.

- Given the same staging data is loaded twice, When both runs finish, Then the fact table is identical after each (no duplicates).
- Given a load finishes, When it reports, Then it sets (not increments) each symbol's `rows_inserted`, `rows_updated`, `rows_unchanged`, and `rows_recalculated` in `pipeline_run_symbols`. `rows_loaded` is inserted + updated + unchanged.

- Given the load code, When it is inspected, Then it contains no `DELETE` or `TRUNCATE` against raw, fact, or dimension tables.
- Given a test database, When integration tests run, Then they cover a repeat load (idempotency) and a failure injected mid-batch (rollback), and CI can run them.

**Notes:** E2-US6 failing a quality check does not roll back the load, since the data is already committed. History is kept, and `refresh_views` is skipped (see E3-US8).

---

### E3-US7: Analytical SQL Queries
- **As a** Data Analyst,
- **I want to** have documented SQL queries using window functions and CTEs,
- **So that** we can answer the core performance questions straight from the analytics layer.
- **Priority:** High
- **Covers:** FR-6.1, FR-6.2

**Acceptance Criteria:**
- Given the query set, When it is inspected, Then each query is its own `.sql` file with a header giving its purpose, parameters, and output columns.
- Given top gainers and losers, When the query runs for a date range (default: last 30 calendar days), Then it returns the top N and bottom N symbols by period return `(last close / first close − 1)`, ranked with `RANK()`, using a CTE for the first and last close. Symbols with fewer than 2 rows in the period are excluded.
- Given monthly average volume, When the query runs, Then it returns the average volume per symbol per month, plus the change versus the previous month using `LAG`.
- Given rolling returns, When the query runs, Then it returns the 7- and 30-trading-row returns using `close / LAG(close, n) − 1`, null until `n` prior rows exist.
- Given best and worst days, When the query runs, Then it returns each symbol's top 5 and bottom 5 daily returns using `RANK() OVER (PARTITION BY symbol ...)`, with ties sharing a rank.
- Given the stored `ma_short`, When the cross-check query runs, Then it recomputes the average with `AVG() OVER (... ROWS BETWEEN n-1 PRECEDING AND CURRENT ROW)` using the configured short window `n` (7 by default) and reports any row that differs by more than the rounding scale.
- Given any query, When it runs, Then it excludes preliminary bars and null values, and reads only the `analytics` schema with the `analytics_reader` role.
- Given a small seeded test dataset, When each query runs, Then the results match hand-calculated expected values.

**Notes:** Volumes are not comparable across symbols or currencies, so only same-symbol volume comparisons are used. Returns are fractions and are comparable across symbols.

---

### E3-US8: Views and Materialized Views
- **As a** Data Analyst,
- **I want to** have the common aggregations saved as views and refreshed after each good run,
- **So that** consumers (the chart, Grafana, the API) read ready-made results.
- **Priority:** Medium
- **Covers:** FR-6.3, FR-6.4

**Acceptance Criteria:**
- Given the `analytics` schema, When it is inspected, Then it contains `mv_symbol_volume_stats` (average, min, max volume per symbol), `mv_top_movers_30d`, `mv_monthly_volume`, `mv_best_worst_days`, and a plain view `v_rolling_returns`.
- Given each materialized view, When it is created, Then it is created with data and has a unique index so it can be refreshed `CONCURRENTLY`: `symbol_id` for the stats and top movers views, `(symbol_id, month)` for monthly volume, and `(symbol_id, kind, trade_date)` for best/worst days. The unique key never uses the rank, because tied days share a rank.
- Given `refresh_views` runs, When it executes, Then one `SECURITY DEFINER` function refreshes all materialized views in a defined order and reports per-view success.
- Given quality status `pass` or `warn` and at least one batch loaded, When the DAG reaches `refresh_views`, Then the views refresh. If quality failed or nothing loaded, they are skipped and keep the last good data (E4-US8).
- Given a refresh fails, When the error occurs, Then the previous contents stay readable, the task fails, the run is marked `failed`, and the error is logged with the run ID.
- Given the views, When `analytics_reader` queries them, Then it can select but cannot modify or refresh them.
- Given the views, When they are defined, Then they exclude preliminary bars and apply the same definitions as E3-US7.

**Notes:** FR-6.4 says "after each successful load". We refresh after a successful load and passing quality checks, since that is the DAG order. Update FR-6.4 to say so.
---

### E3-US9: Time-Series Visualization
- **As a** Data Analyst,
- **I want to** generate a price chart with moving averages for a chosen symbol,
- **So that** trends are visible at a glance.
- **Priority:** High
- **Covers:** FR-9.3, FR-9.4

**Acceptance Criteria:**
- Given a symbol and a date range, When the chart script runs, Then it plots the close price with the short and long moving averages (7 and 30 days by default, from configuration), titled with the symbol and currency, with labeled axes and a legend.
- Given the data source, When the script reads data, Then it queries only the analytics layer (the fact table or views) through the `analytics_reader` role. The role has no access to `raw`, so FR-9.4 is enforced by permissions.
- Given a moving average that is null for the first rows, When plotted, Then those lines start later and no zeros are drawn.
- Given a preliminary bar, When plotted, Then it is excluded or visibly marked.
- Given the option is enabled, When the chart runs, Then volume is drawn as a second panel.
- Given a symbol with no data in the range, When the script runs, Then it prints a clear message and exits without crashing.
- Given the output, When the script finishes, Then the chart is saved to a file (PNG or HTML) at a configurable path and the connection string comes from configuration.

**Notes:** Matplotlib or Plotly, team choice. Plotly gives an interactive HTML file.

---

### E3-US10: ERD and Schema Documentation
- **As a** Database Engineer,
- **I want to** have an ERD of the whole database,
- **So that** the design is documented and the team and graders can understand it quickly.
- **Priority:** High
- **Covers:** FR-5.6, NFR-11.3

**Acceptance Criteria:**
- Given the ERD, When it is viewed, Then it shows every table in the catalog (raw, star schema, operational) with primary keys, foreign keys, and cardinalities, and groups tables by schema.
- Given the diagram, When it is exported, Then both the editable source (dbdiagram.io) and an image are stored in the repository.
- Given a migration changes the schema, When it is merged, Then the ERD is updated in the same pull request.
- Given the ERD, When compared with the migrations, Then table and column names match exactly.

---

### E3-US11: Run History Tables (new)
- **As a** Database Engineer,
- **I want to** create `pipeline_runs` and `pipeline_run_symbols` with the columns E4 specifies,
- **So that** every run is auditable and every other table can reference its run ID.
- **Priority:** High
- **Covers:** FR-9.2, NFR-8.1, NFR-8.3

**Acceptance Criteria:**
- Given the migrations run, When they finish, Then `analytics.pipeline_runs` exists with `run_id` (UUID, PK), a unique `airflow_run_id`, `status` limited to `queued`, `running`, `success`, `failed`, and `quality_status` limited to `pass`, `warn`, `fail`, `skipped` (null while running), plus the counters and error columns from E4.
- Given `analytics.pipeline_run_symbols`, When it is created, Then its primary key is `(run_id, symbol)` with a foreign key to `pipeline_runs`, and `status` is limited to `success`, `no_new_data`, `failed`, `quarantined`, `deferred`.
- Given the foreign keys, When a run is deleted, Then the database blocks it (`ON DELETE RESTRICT`), and no job deletes or overwrites past runs.
- Given a run ID, When searched, Then indexes find the run, its symbol rows, the raw and quarantine rows, and the quality results that share it.
- Given the run history tables, When permissions are checked, Then `etl_writer` can insert and update them and cannot delete from them.

**Notes:** Part A story, built before E1. E4-US7 owns how these tables are written.

---

## Remaining open questions
- Is the PostgreSQL version 15 or newer? The quality-results unique rule relies on `NULLS NOT DISTINCT`. On older versions, use a unique index on `COALESCE(symbol_id, 0)`.
- What backfill start date seeds `dim_date`?
- Is the 30-day window right for `mv_top_movers_30d`?
- Where do EODHD cross-check comparison results go? Raw stores the fetch, but no table stores the comparison yet.
- Matplotlib or Plotly?

---

## Summary Table

| ID | Title | Priority | Covers |
| :--- | :--- | :--- | :--- |
| E3-US1 | Database Setup, Migrations, and Access Roles | High | FR-5.1, NFR-5.1, NFR-6.1 |
| E3-US2 | Raw Layer Tables (Append-Only) | High | FR-3.1 to 3.3, FR-2.3 |
| E3-US3 | Star Schema Tables (Dimensions and Fact) | High | FR-5.4, FR-5.5 |
| E3-US4 | Landing, Staging, Watermark, API Usage, and Quality Tables | High | FR-1.3, FR-1.4, FR-4.1, FR-7.2 |
| E3-US5 | Dimension Population | High | FR-5.4 |
| E3-US6 | Idempotent, Transactional Upsert Loading | High | FR-5.2, 5.3, 5.7 |
| E3-US7 | Analytical SQL Queries | High | FR-6.1, FR-6.2 |
| E3-US8 | Views and Materialized Views | Medium | FR-6.3, FR-6.4 |
| E3-US9 | Time-Series Visualization | High | FR-9.3, FR-9.4 |
| E3-US10 | ERD and Schema Documentation | High | FR-5.6 |
| E3-US11 | Run History Tables | High | FR-9.2 |