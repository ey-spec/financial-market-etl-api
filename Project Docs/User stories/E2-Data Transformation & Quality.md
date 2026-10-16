# E2: Data Transformation & Quality

**Phase:** 1 | **Covers:** FR-4, FR-7 (+ NFR-3.3, NFR-3.5, NFR-4.1, NFR-4.2, NFR-4.3, NFR-4.5, NFR-4.6, NFR-6.3, NFR-9.1, NFR-9.2)

## Overview
This epic turns raw API JSON into clean, typed, analysis-ready rows with calculated metrics, and then checks that the loaded result can be trusted. Transformation reads only from the raw layer and writes to a staging table. E3 loads from staging into the analytics tables. Quality checks then run on the loaded data and store their results per run.

## What this epic covers
- **Parsing and typing:** raw JSON to tables with exact types, for both providers
- **Cleaning:** deduplicate, standardize timestamps (UTC), reject invalid rows, handle missing values with documented rules
- **Metrics:** daily returns, moving averages, volume statistics, volatility, cumulative return, rolling high/low
- **Incremental safety:** lookback rows for rolling windows, recalculation when a past row is revised, safe reruns and reprocessing from raw
- **Data-quality checks:** nulls, duplicates, invalid values, date gaps (against a trading calendar), row-count reconciliation
- **Quality report:** results stored per run, with configurable warn/fail thresholds
- **Testability:** transformation logic runs and is tested without the API or the database

## Depends on
- **E1:** `raw.raw_responses` (valid responses with run ID, provider, symbol, fetch timestamp, `has_preliminary_bar`, and `purpose`). E2 reads only `purpose = 'primary'` rows. Cross-check rows and quarantined responses are never read.
- **E3:** DDL for `analytics.stg_daily_prices`, the metric columns of `fact_daily_prices`, `dim_symbol`, `analytics.quality_results`, and the view `mv_symbol_volume_stats`. E3 also does the upsert load, so E2 never writes to the fact table directly.
- **E4:** the run ID, the DAG order (per batch `transform → load`, then `quality_checks`), and `pipeline_run_symbols`, where E2 sets each symbol's counters. Until the DAG exists, E2 code takes the run ID as a parameter.

## Decisions (resolved from the project docs)

| Topic | Decision |
|---|---|
| Input | Raw rows (`purpose = 'primary'`) of the current run ID and the current batch's symbols (or a given run or date range when reprocessing). Never in-memory API responses. |
| Handoff | Transform writes to `analytics.stg_daily_prices` tagged with run ID and `row_origin`. Load upserts from staging. Each batch handles only its own symbols. No large data passes through XCom. |
| Duplicate key | (symbol, trade date). Keep the row with the latest fetch timestamp. |
| Dates and time | Trade date stays a `DATE` in exchange-local terms and is never converted. Fetch and ingestion timestamps are stored in UTC (ISO 8601). |
| Types | Prices `NUMERIC(18,6)`, volume `BIGINT`, returns, volatility and cumulative return `NUMERIC(12,8)`, rolling volume averages `NUMERIC(20,2)` (same as E3) |
| Precision | Metrics are computed in float64, then rounded to the stored scale on write. |
| Price used for metrics | `close`. `adjusted_close` is stored when the provider supplies it (null for Twelve Data). |
| Windows | 7 and 30 trading rows (configurable) |
| Lookback | 31 prior rows per symbol (largest window + 1), read from `fact_daily_prices` |
| Invalid rows (rejected) | `close` missing, any price ≤ 0, volume < 0, high < low |
| Missing values (kept, flagged) | Missing open, high, low, or volume stays null with `has_missing_values = true` |
| Preliminary bars | When a raw response has `has_preliminary_bar = true`, the row with the latest trade date in that response gets `is_preliminary = true`. It is overwritten by the next run's upsert. |
| Missing trading days | Never forward-filled. Windows count trading rows present. A return after a gap spans the gap, and the gap check flags it. |
| Quality scope | Rows loaded by this run. The date-gap check covers each affected symbol's full loaded history. |
| Run status | E2 never sets the final run status. `quality_checks` sets `quality_status` (`pass`, `warn`, `fail`) and `finalize_run` (E4-US8) turns `fail` into `status = failed`. A warning keeps `status = success`. |
| Row accounting | `rows_extracted = rows_loaded + rows_rejected + duplicates_removed`, where `rows_loaded = inserted + updated + unchanged`. `recalculated` rows and lookback rows are outside the balance (same rule as E3 and E4). |
| Counters | Each stage sets (not increments) its symbol's counters in `pipeline_run_symbols`. |


### Metric definitions (NFR-4.6)

| Metric | Definition |
|---|---|
| Ordering | Per symbol, oldest → newest before any calculation |
| Window rule | A window of N means the last N trading rows **including the current row**. Missing calendar days are not filled. If any value needed inside the window is null, the result is null. |
| Daily return | `(close_t − close_t-1) / close_t-1`, where `t-1` is the previous trading row. Stored as a fraction (0.0123 = 1.23%), `NUMERIC(12,8)`. Null if there is no previous row or the previous close is 0 or null. |
| Moving average (short = 7, long = 30) | Mean of the last N closes. Null until N rows exist. `NUMERIC(18,6)`. |
| Rolling volume average (7, 30) | Mean of the last N volumes. Null until N rows exist. `NUMERIC(20,2)`. |
| Volume summary per symbol | Average, min, max volume over the symbol's history are provided by the view `mv_symbol_volume_stats` (E3-US8). E2 does not compute or store them. |
| Rolling volatility (30) | Sample standard deviation (n−1) of the last 30 daily returns, daily and not annualized. Null until 30 returns exist (so at least 31 rows). `NUMERIC(12,8)`. |
| Cumulative return | `close_t / baseline_close − 1`. `baseline_close` is the close of the symbol's first trading day on or after the configured start date, read from `fact_daily_prices` (or from the current batch if not loaded yet). Null if no baseline exists. `NUMERIC(12,8)`. |
| Rolling high / low (30) | Max of `high` and min of `low` over the last 30 rows. Null until 30 rows exist. `NUMERIC(18,6)`. |
| Configurable values | The window sizes (7, 30, 30) and the start date come from configuration (E1-US7). The 30 above stands for the configured long window. |
| Rounding | Metrics are computed in float64 and rounded to the stored scale only when written. |

---

## User Stories

### E2-US1: JSON Parsing and Data Type Mapping
- **As a** Data Engineer,
- **I want to** parse raw JSON from the raw layer into typed tables,
- **So that** the data can be queried and calculated on accurately.
- **Priority:** High
- **Covers:** FR-4.1, FR-4.2, NFR-4.3

**Acceptance Criteria:**
- Given valid raw rows with `purpose = 'primary'`, When parsing runs, Then it reads from the `raw` schema (not in-memory responses) and uses the recorded provider to pick the parser: one output row per `values[]` entry (Twelve Data) or per array element (EODHD).
- Given parsed rows, When types are applied, Then trade date → `DATE`, open/high/low/close/adjusted_close → `NUMERIC`, volume → `BIGINT`. Twelve Data's numeric strings and EODHD's JSON numbers are both converted.
- Given rows that carry no symbol, When parsing runs, Then the symbol comes from `meta` (Twelve Data) or from the raw row's request metadata (EODHD).
- Given a provider with no `adjusted_close`, When parsing runs, Then that column is null.
- Given a record with a malformed structure or an unparsable value, When parsing runs, Then that record is rejected, logged with run ID, symbol and reason, and counted as rejected. The other records continue.
- Given a valid response with an empty `values[]`, When parsing runs, Then it outputs an empty table with the correct columns and no error.
- Given quarantined responses exist, When parsing runs, Then they are never read.
- Given parsing finishes, When the step ends, Then it sets each symbol's `rows_extracted` (entries parsed) and `rows_rejected` in `pipeline_run_symbols`.

**Notes:** If EODHD is dropped in E1-US8, remove the EODHD parts of this story.

---

### E2-US2: Data Cleaning and Standardization
- **As a** Data Engineer,
- **I want to** deduplicate, standardize timestamps, and apply documented missing-value and validity rules,
- **So that** the dataset is clean, consistent, and safe for calculations.
- **Priority:** High
- **Covers:** FR-4.3, FR-4.4, FR-4.5, NFR-4.1, NFR-4.2

**Acceptance Criteria:**
- Given parsed data, When cleaning runs, Then the trade date stays a `DATE` and all fetch/ingestion timestamps are stored in UTC (ISO 8601).
- Given duplicate rows with the same (symbol, trade date), When deduplication runs, Then one row remains, the one with the latest fetch timestamp, and the removed count is recorded.
- Given a row is missing `close`, When cleaning runs, Then the row is rejected and counted.
- Given a row has any price ≤ 0, a volume < 0, or high < low, When cleaning runs, Then the row is rejected, logged with the reason, and counted.
- Given a row is missing open, high, low, or volume, When cleaning runs, Then the value stays null and the row is flagged `has_missing_values = true`.
- Given a non-trading day or a gap in the data, When cleaning runs, Then prices are not forward-filled and no rows are invented.
- Given a raw response with `has_preliminary_bar = true`, When cleaning runs, Then the row with the latest trade date in that response is kept with `is_preliminary = true`.
- Given already-clean data, When cleaning runs, Then the output equals the input.
- Given cleaning finishes, When the step ends, Then it sets each symbol's `rejected` and `duplicates_removed` counters in `pipeline_run_symbols`.

**Notes:** Rejected rows are logged and counted, and the original data stays untouched in raw, so nothing is lost.

---

### E2-US3: Financial and Time-Series Metrics
- **As a** Data Engineer,
- **I want to** calculate returns, moving averages, rolling volume averages, and time-series metrics from the formulas in the Decisions section,
- **So that** analytics-ready metrics are consistent and reproducible.
- **Priority:** High
- **Covers:** FR-4.6, FR-4.7, FR-4.8 (rolling part; the per-symbol summary is in E3-US8), FR-4.9, NFR-4.6

**Acceptance Criteria:**
- Given cleaned data, When metrics run, Then each symbol is sorted oldest → newest first.
- Given sorted closes, When returns are calculated, Then `daily_return = (close_t − close_t-1) / close_t-1`. The first row, a missing previous close, or a previous close of 0 gives null (no division error).
- Given fewer rows than the window, When moving averages run, Then the 7-day and 30-day averages are null until the window is full.
- Given volume data, When statistics run, Then rolling 7- and 30-day average volume are produced per row. Zero volume is valid, and null volume gives null averages.
- Given daily returns, When time-series metrics run, Then rolling 30-day volatility (sample standard deviation) and rolling 30-day high/low are produced, each null until its window is full.
- Given a symbol, When cumulative return is calculated, Then it uses the baseline close defined in the Decisions section and is null if no baseline exists.
- Given a missing trading day, When metrics run, Then the day is not filled and windows count the trading rows that exist.
- Given metrics are computed, When results are written, Then every metric uses `close` and values are rounded to the column scales in the Decisions section.
- Given window sizes or the start date are changed in config, When the next run executes, Then the new values apply with no code change.

**Notes:** Per-symbol average, min, and max volume come from `mv_symbol_volume_stats` (E3-US8). If the configured start date changes, reprocess the affected symbols (E2-US7) so cumulative returns use the new baseline.

---

### E2-US4: Automated Data-Quality Checks
- **As a** Data Engineer,
- **I want to** run automated checks on the loaded data after each load,
- **So that** anomalies are detected before the data is used.
- **Priority:** High
- **Covers:** FR-7.1, NFR-4.5

**Acceptance Criteria:**
- Given at least one batch loaded, When checks run, Then they read the loaded data for the symbols loaded by this run, are read-only, and never modify data.
- Given loaded rows, When the null check runs, Then it counts nulls in required columns (symbol, trade date, close) and reports null counts for optional columns.
- Given loaded rows, When the duplicate check runs, Then it counts duplicate (symbol, trade date) keys (a second line of defense, since the primary key already blocks them) and reports the duplicates removed during cleaning.
- Given loaded rows, When the validity check runs, Then it counts prices ≤ 0, volumes < 0, and high < low. Negative returns are valid.
- Given a symbol's loaded history, When the gap check runs, Then it lists expected trading days between the symbol's first loaded date and its last finalized date (from its exchange trading calendar in configuration) that have no row. EGX, for example, trades Sunday to Thursday.
- Given the per-symbol counters in `pipeline_run_symbols`, When the row-count check runs, Then for each symbol it verifies `rows_extracted = rows_loaded + rows_rejected + duplicates_removed`, ignoring recalculated rows. Any mismatch is reported.
- Given no anomalies, When checks run, Then every check reports `pass` with zero errors.
- Given each check, When it finishes, Then it produces a measured value that feeds E2-US6.

**Notes:** Checks run after the loads because the DAG order is `load → quality_checks` and FR-7.1 requires it. They read the per-symbol counters (not `pipeline_runs` totals), because `finalize_run` writes the totals later.

---

### E2-US5: Quality Report Storage
- **As a** Data Engineer,
- **I want to** store the results of every quality check per run,
- **So that** each run's data quality is auditable and repeatable.
- **Priority:** High
- **Covers:** FR-7.2, NFR-4.5

**Acceptance Criteria:**
- Given checks finish, When the results are saved, Then one row per check (and per symbol where relevant) is stored in `analytics.quality_results` with run ID, check name, symbol (nullable reference to `dim_symbol`), measured value, warn threshold, fail threshold, status (pass / warn / fail), and check time.
- Given a run ID, When results are queried, Then they join to that run in `pipeline_runs`.
- Given a run, When the report is requested, Then it shows pass/warn/fail counts and lists every non-pass check.
- Given the quality step is rerun for the same run ID, When results are saved, Then rows are replaced, not duplicated (unique on run ID + check + symbol).
- Given results and logs, When they are written, Then they contain no secrets.
- Given no batch loaded anything, When the run reaches `quality_checks`, Then the step is skipped and no rows are stored (see E2-US6).

---

### E2-US6: Quality Thresholds and Run Outcome
- **As a** Data Engineer,
- **I want to** configure thresholds that warn or fail a run,
- **So that** degraded data is caught without code changes.
- **Priority:** Medium
- **Covers:** FR-7.3, NFR-6.3

**Acceptance Criteria:**
- Given a metric breaches a warning threshold, When evaluated, Then `quality_status = warn` is set on the run and `status` stays `success`.
- Given a metric breaches a failure threshold, When evaluated, Then `quality_status = fail` is set with the error type and message, and `finalize_run` (E4-US8) marks the run `failed`.
- Given a failed quality check, When the run ends, Then the committed data is kept (history is never deleted) and the DAG skips `refresh_views` (E4-US8), so the views never show unchecked bad data.
- Given all metrics are within limits, When evaluated, Then `quality_status = pass`.
- Given no batch loaded anything, When the run ends, Then `quality_status = skipped` and no thresholds are evaluated (E4-US8).
- Given thresholds are changed in config, When the next run executes, Then the new values apply with no code change.

**Starting thresholds (tune in config):**

| Check | Warn | Fail |
|---|---|---|
| Invalid + duplicate rate (rejected + duplicates removed) / extracted | > 0.5% | > 1% (matches the KPI) |
| Null in a required column | none | any |
| Duplicate keys in analytics | none | any |
| Row-count mismatch (per symbol) | none | any |
| Missing trading days (share of expected days) | any gap | > 5% |

**Notes:** The watermark (E1-US2) is not rolled back when quality fails. Fix the cause, then reprocess from raw (E2-US7). E2 sets only `quality_status`. The final `status` is set by `finalize_run`.
---

### E2-US7: Incremental, Idempotent, and Reprocessable Transformation
- **As a** Data Engineer,
- **I want to** transform only new data while keeping rolling metrics correct, and rerun or reprocess safely,
- **So that** daily runs are fast, results are identical on rerun, and no API call is needed to fix mistakes.
- **Priority:** High
- **Covers:** FR-4.1, NFR-3.3, NFR-3.5

**Acceptance Criteria:**
- Given new rows for a symbol, When metrics run, Then up to 31 prior rows are read from `fact_daily_prices` as lookback context. Lookback rows are not written to staging and are not counted as extracted or loaded.
- Given staging rows, When they are written, Then each is tagged `row_origin = 'new'` (from this run's raw rows) or `'recalculated'`.
- Given a past row is revised (for example a preliminary bar replaced by the final bar), When it is loaded, Then the following rows within the largest window (up to 30) are recalculated and written to staging as `recalculated`. They are excluded from the row-count balance.
- Given the same raw rows are processed twice, When both runs finish, Then staging and the loaded fact rows are identical, with no duplicates.
- Given a date range or run ID, When reprocessing is requested, Then the transform rebuilds that range from raw alone, with no API call.
- Given a batch's transform starts, When it begins, Then staging rows for that run ID and that batch's symbols are cleared first, so leftovers from a failed attempt are not loaded. Other batches' staging rows are untouched.
- Given no raw rows for the run, When the transform runs, Then it finishes with zero rows and no error.

---

### E2-US8: Testable Transformation Logic
- **As a** Data Engineer,
- **I want to** run parsing, cleaning, metrics, and checks without the API or a production database,
- **So that** the logic is verifiable and CI can run it on every push.
- **Priority:** High
- **Covers:** NFR-9.1, NFR-9.2

**Acceptance Criteria:**
- Given sample data (DataFrames and JSON fixtures taken from the Twelve Data and EODHD examples), When the tests run, Then no network and no production database are needed.
- Given the test suite, When it runs, Then it covers normal data, missing values, duplicates, malformed payloads, empty payloads, rerun idempotency, batch-scoped staging clearing, and `row_origin` tagging.
- Given metric edge cases, When tests run, Then they cover a zero previous close, history shorter than the window, a missing trading day, zero volume, and a preliminary bar.
- Given the hand-calculated expected values in the tests, When the metrics run, Then results match them.
- Given a push or pull request, When CI runs (E6), Then these tests run and must pass.

---

## Remaining open questions
## Remaining open questions
- Should returns use `close` or `adjusted_close`? Splits and dividends can distort returns on unadjusted `close`, so confirm how each provider adjusts before finalizing.
- Where do the trading calendars and holiday lists come from (a calendar library or a config list), and for which exchanges? (They live in configuration, E1-US7.)
- Are the starting thresholds acceptable, especially the 5% missing-days fail level?

---

## Summary Table

| ID | Title | Priority | Covers |
| :--- | :--- | :--- | :--- |
| E2-US1 | JSON Parsing and Data Type Mapping | High | FR-4.1, FR-4.2 |
| E2-US2 | Data Cleaning and Standardization | High | FR-4.3 to 4.5 |
| E2-US3 | Financial and Time-Series Metrics | High | FR-4.6 to 4.9 |
| E2-US4 | Automated Data-Quality Checks | High | FR-7.1 |
| E2-US5 | Quality Report Storage | High | FR-7.2 |
| E2-US6 | Quality Thresholds and Run Outcome | Medium | FR-7.3 |
| E2-US7 | Incremental, Idempotent, and Reprocessable Transformation | High | FR-4.1, NFR-3.3, NFR-3.5 |
| E2-US8 | Testable Transformation Logic | High | NFR-9.1, NFR-9.2 |