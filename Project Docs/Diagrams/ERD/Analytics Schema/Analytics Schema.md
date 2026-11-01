# ERD: Analytics Schema

**Covers:** E3-US3, E3-US4, E3-US11, E2-US5 | FR-5.1, FR-5.4, FR-5.5, FR-7.2, FR-9.2 | NFR-4.1, NFR-4.2, NFR-4.3, NFR-7.3, NFR-8.1, NFR-8.3

## 1. Purpose

The `analytics` schema holds everything except the raw responses: the clean, final data, plus the helper tables the pipeline needs to run safely.

It is split into 3 modules:

| Module | Tables | Role |
|---|---|---|
| 1. Star schema | `dim_symbol`, `dim_date`, `fact_daily_prices` | The final analytics data |
| 2. Pipeline state | `symbol_watermark`, `api_usage`, `stg_api_responses`, `stg_daily_prices` | Helper tables that make the workflow safe and repeatable |
| 3. Run history and quality | `pipeline_runs`, `pipeline_run_symbols`, `quality_results` | Audit trail of every run and its data quality |

## 2. Rules for the whole schema

| Rule | Meaning |
|---|---|
| Never delete | No `DELETE` or `TRUNCATE` on fact, dimension, quality, run history, or API usage tables. |
| Only exception | The two scratch tables (`stg_api_responses`, `stg_daily_prices`) can be cleaned up after a configurable number of days. |
| Foreign keys | All foreign keys use `ON DELETE RESTRICT`. |
| Fact key | One row per `(symbol_id, trade_date)`. The database blocks duplicates. |
| Types | Prices `NUMERIC(18,6)`, volume `BIGINT`, returns/volatility/cumulative return `NUMERIC(12,8)`, rolling volume averages `NUMERIC(20,2)`, timestamps `TIMESTAMPTZ` in UTC. |
| Dimension changes | SCD Type 1 (overwrite in place). |
| Metric column names | Named by role (`ma_short`, `ma_long`), not by number, so changing window sizes needs no migration. |

---

## 3. Module 1: Star schema

### 3.1 `dim_symbol` (one row per symbol)

| Column | Description |
|---|---|
| `symbol_id` (PK) | Unique identifier. |
| `symbol` (unique) | The configured identifier, for example `AAPL` or `COMI.EGX`. |
| `name` | Company or instrument name. |
| `exchange` | Exchange name. |
| `mic_code` | Market identifier code. |
| `currency` | Trading currency. |
| `exchange_timezone` | Timezone of the exchange. |
| `instrument_type` | For example Common Stock. |
| `primary_provider` | Which provider supplies this symbol's data. |
| `is_active` | Whether the symbol is still being collected. |
| `created_at`, `updated_at` | Timestamps in UTC. |

### 3.2 `dim_date` (calendar)

| Column | Description |
|---|---|
| `full_date` (PK) | The calendar date. |
| `year`, `quarter`, `month`, `month_name` | Calendar parts. |
| `day`, `day_of_week`, `day_name` | Day parts. |
| `week_of_year` | Week number. |
| `is_weekend` | Plain Saturday/Sunday flag. Trading-day logic uses exchange calendars from configuration. |

### 3.3 `fact_daily_prices` (one row per symbol per trading day)

| Group | Columns |
|---|---|
| Key | `symbol_id` (FK → `dim_symbol`), `trade_date` (FK → `dim_date.full_date`). Together they form the PK. |
| Prices and volume | `open`, `high`, `low`, `close`, `adjusted_close`, `volume` |
| Metrics | `daily_return`, `ma_short`, `ma_long`, `vol_avg_short`, `vol_avg_long`, `volatility_long`, `cumulative_return`, `rolling_high_long`, `rolling_low_long` |
| Flags | `has_missing_values`, `is_preliminary` |
| Tracking | `run_id` (last run that wrote the row), `created_at`, `updated_at` |

---

## 4. Module 2: Pipeline state

### 4.1 `symbol_watermark` (last loaded date per symbol)

| Column | Description |
|---|---|
| `symbol_id` (PK, FK → `dim_symbol`) | One row per symbol. |
| `last_loaded_date` | The latest finalized trading date loaded. Next run asks only for newer data. |
| `run_id` | The run that last moved it. |
| `updated_at` | When it changed. |

It is updated in the same transaction as the fact upsert, so it can never get ahead of the data.

### 4.2 `api_usage` (daily API credits used)

| Column | Description |
|---|---|
| `provider` (PK part) | Twelve Data or EODHD. |
| `usage_date` (PK part) | The UTC date. |
| `credits_used` | Credits spent that day. |
| `updated_at` | When it changed. |

Credits are reserved one at a time in a single atomic statement, so parallel tasks can never exceed the daily limit.

### 4.3 `stg_api_responses` (landing table, temporary)

| Column | Description |
|---|---|
| `landing_id` (PK) | Unique ID per fetched response. Carried into the raw tables. |
| `run_id`, `batch_no` | Which run and symbol batch fetched it. |
| `provider`, `purpose` | Provider, and `primary` or `cross_check`. |
| `symbol`, `source_endpoint`, `request_params` | Request details, with no API key. |
| `http_status`, `fetched_at` | Response status and fetch time (UTC). |
| `has_preliminary_bar` | True if it includes today's unfinished bar. |
| `body` (TEXT) | The response as text. |
| `validation_status` | `pending`, `valid`, or `invalid`. |
| `failure_reason` | Why it was marked invalid. |
| `created_at` | When it was saved. |

Scratch space with no foreign keys.

### 4.4 `stg_daily_prices` (transform output waiting to load, temporary)

| Column | Description |
|---|---|
| PK: `run_id`, `symbol`, `trade_date` | One row per run, symbol, and day. |
| Same columns as `fact_daily_prices` | Except `symbol` is text instead of `symbol_id`. |
| `row_origin` | `new` (from this run's raw rows) or `recalculated` (older row whose metrics changed). |

Scratch space with no foreign keys.

---

## 5. Module 3: Run history and quality

### 5.1 `pipeline_runs` (one row per run)

| Group | Columns |
|---|---|
| Key | `run_id` (PK, UUID) |
| Airflow link | `airflow_dag_id`, `airflow_run_id` (unique) |
| How it started | `trigger_type` (scheduled / manual / api / backfill), `mode` (incremental / backfill / reprocess), `params` (JSONB) |
| Outcome | `status` (queued / running / success / failed), `quality_status` (pass / warn / fail / skipped, null while running) |
| Time | `started_at`, `ended_at`, `created_at` |
| Symbol counters | `symbols_requested`, `symbols_succeeded`, `symbols_no_new_data`, `symbols_failed`, `symbols_quarantined`, `symbols_deferred` |
| Row counters | `rows_extracted`, `rows_rejected`, `duplicates_removed`, `rows_inserted`, `rows_updated`, `rows_unchanged`, `rows_loaded`, `rows_recalculated` |
| Errors | `error_type`, `error_message` (sanitized, no secrets) |

### 5.2 `pipeline_run_symbols` (one row per symbol per run)

| Column | Description |
|---|---|
| `run_id` (PK part, FK → `pipeline_runs`) | The run. |
| `symbol` (PK part) | The symbol. |
| `provider`, `batch_no` | Provider and batch. |
| `status` | success / no_new_data / failed / quarantined / deferred. |
| Row counters | Same counters as the run table, but for this symbol. |
| `error_type`, `error_message` | Sanitized error details. |
| `updated_at` | When it changed. |

### 5.3 `quality_results` (one row per check per run)

| Column | Description |
|---|---|
| `result_id` (PK) | Unique identifier. |
| `run_id` (FK → `pipeline_runs`) | The run that was checked. |
| `check_name` | Which check ran. |
| `symbol_id` (nullable FK → `dim_symbol`) | The symbol, or empty for run-wide checks. |
| `measured_value` | The value measured. |
| `warn_threshold`, `fail_threshold` | Limits from configuration. |
| `status` | pass / warn / fail. |
| `checked_at` | When the check ran. |

Unique on (`run_id`, `check_name`, `symbol_id`), so saving the same check twice replaces it.

---

## 6. Relationships

| From | To | Type | Notes |
|---|---|---|---|
| `fact_daily_prices.symbol_id` | `dim_symbol.symbol_id` | many-to-one | Many days per symbol. |
| `fact_daily_prices.trade_date` | `dim_date.full_date` | many-to-one | Many symbols per date. |
| `symbol_watermark.symbol_id` | `dim_symbol.symbol_id` | one-to-one | One watermark per symbol. |
| `pipeline_run_symbols.run_id` | `pipeline_runs.run_id` | many-to-one | Many symbol rows per run. |
| `quality_results.run_id` | `pipeline_runs.run_id` | many-to-one | Many checks per run. |
| `quality_results.symbol_id` | `dim_symbol.symbol_id` | many-to-one (optional) | Empty for run-wide checks. |
| `raw.raw_responses.run_id` | `pipeline_runs.run_id` | many-to-one | From the raw schema. |
| `raw.quarantine.run_id` | `pipeline_runs.run_id` | many-to-one | From the raw schema. |

Tables with no foreign keys: `stg_api_responses`, `stg_daily_prices` (scratch space), and `api_usage` (standalone counter).

## 7. Keys summary

| Table | Primary key | Foreign keys |
|---|---|---|
| `dim_symbol` | `symbol_id` | none |
| `dim_date` | `full_date` | none |
| `fact_daily_prices` | (`symbol_id`, `trade_date`) | `symbol_id` → `dim_symbol`, `trade_date` → `dim_date` |
| `symbol_watermark` | `symbol_id` | `symbol_id` → `dim_symbol` |
| `api_usage` | (`provider`, `usage_date`) | none |
| `stg_api_responses` | `landing_id` | none |
| `stg_daily_prices` | (`run_id`, `symbol`, `trade_date`) | none |
| `pipeline_runs` | `run_id` | none |
| `pipeline_run_symbols` | (`run_id`, `symbol`) | `run_id` → `pipeline_runs` |
| `quality_results` | `result_id` | `run_id` → `pipeline_runs`, `symbol_id` → `dim_symbol` |

## 8. Open points for the team

- `run_id` on `fact_daily_prices` and `symbol_watermark` is listed in the docs but not defined as a foreign key. Decide whether to enforce it.
- Confirm the exact types for the ID columns (for example `BIGINT` vs `UUID` for `symbol_id`).
- PostgreSQL 15 or newer is needed for the `NULLS NOT DISTINCT` unique rule on `quality_results`.