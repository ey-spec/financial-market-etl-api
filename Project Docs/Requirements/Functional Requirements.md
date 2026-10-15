# Functional Requirements

**Legend:** Priority = **M** (Must) / **S** (Should) / **O** (Optional)

---

## Phase 1: Automated Financial Market Data Pipeline

### 1. Data Extraction

| ID | Requirement | Priority |
|---|---|---|
| FR-1.1 | The system shall consume a financial REST API to retrieve historical and daily market data (OHLCV) for a configurable list of symbols. | M |
| FR-1.2 | The system shall support a one-time historical backfill and recurring daily incremental extraction. | M |
| FR-1.3 | The system shall track the last loaded date per symbol (watermark) and request only newer data. | M |
| FR-1.4 | The system shall split symbols into batches that respect the API's per-minute and daily rate limits, pausing between batches. | M |
| FR-1.5 | The system shall retry failed API calls (timeouts, 429, 5xx) with exponential backoff up to a configurable maximum. | M |
| FR-1.6 | The system shall read the API key, base URL, and symbol list from configuration, never from code. | M |

### 2. Validation

| ID | Requirement | Priority |
|---|---|---|
| FR-2.1 | The system shall validate the HTTP status code of every response. | M |
| FR-2.2 | The system shall validate payload structure: required fields present, correct data types, no API error messages inside a 200 response. | M |
| FR-2.3 | The system shall reject or quarantine invalid payloads and log the reason without stopping the whole run. | M |

### 3. Raw Storage

| ID | Requirement | Priority |
|---|---|---|
| FR-3.1 | The system shall store each original API response unchanged in a PostgreSQL `raw` schema using a `JSONB` column. | M |
| FR-3.2 | Each raw record shall include metadata: symbol, fetch timestamp, run ID, source endpoint, and HTTP status. | M |
| FR-3.3 | Raw data shall be retained permanently to support audit and reprocessing without re-calling the API. | M |
| FR-3.4 | The system shall optionally write the same JSON documents to a MongoDB collection alongside the SQL raw layer. | O |

### 4. Transformation (Pandas)

| ID | Requirement | Priority |
|---|---|---|
| FR-4.1 | The system shall read from the raw layer, not from in-memory API responses, when transforming. | M |
| FR-4.2 | The system shall parse JSON into tabular form and cast fields to correct types. | M |
| FR-4.3 | The system shall standardize all timestamps to a single timezone and format (UTC, ISO 8601). | M |
| FR-4.4 | The system shall detect and remove duplicate records using the key (symbol, trade date). | M |
| FR-4.5 | The system shall handle missing values using documented rules (e.g. drop rows missing close price, forward-fill non-trading gaps where appropriate) and flag affected rows. | M |
| FR-4.6 | The system shall calculate daily returns per symbol. | M |
| FR-4.7 | The system shall calculate moving averages (e.g. 7-day and 30-day) per symbol. | M |
| FR-4.8 | The system shall calculate trading volume statistics (average, min, max, rolling average). | M |
| FR-4.9 | The system shall calculate basic time-series metrics (e.g. rolling volatility, cumulative return, high/low over a window). | M |

### 5. Loading and Database Design

| ID | Requirement | Priority |
|---|---|---|
| FR-5.1 | The system shall store transformed data in PostgreSQL in an `analytics` schema, separate from `raw`. | M |
| FR-5.2 | The system shall load data using upserts (`INSERT ... ON CONFLICT DO UPDATE`) so reruns never create duplicates. | M |
| FR-5.3 | The system shall preserve full history: loads append or update, and never delete past records. | M |
| FR-5.4 | The analytics layer shall be modeled as a star schema: `fact_daily_prices`, `dim_symbol`, `dim_date`. | S |
| FR-5.5 | The system shall define primary keys, foreign keys, and indexes (at minimum a composite index on symbol and date). | M |
| FR-5.6 | The system shall provide an ERD documenting all tables and relationships. | M |
| FR-5.7 | Each load shall be transactional: a failed load rolls back and leaves existing data intact. | M |

### 6. Analytics (SQL)

| ID | Requirement | Priority |
|---|---|---|
| FR-6.1 | The system shall provide analytical SQL queries using window functions (`LAG`, `AVG OVER`, `RANK`) and CTEs. | M |
| FR-6.2 | The queries shall answer at least: top gainers/losers, monthly average volume, rolling returns, and best/worst trading days per symbol. | M |
| FR-6.3 | The system shall expose common aggregations as views or materialized views in the `analytics` schema. | S |
| FR-6.4 | Materialized views shall be refreshed automatically after each successful load. | S |

### 7. Data Quality

| ID | Requirement | Priority |
|---|---|---|
| FR-7.1 | The system shall run data-quality checks after each load: null counts, duplicate counts, negative prices or volumes, date gaps, and row-count comparison with the raw layer. | M |
| FR-7.2 | The system shall generate a data-quality report per run and store its results in a table. | M |
| FR-7.3 | The system shall fail or warn the run when quality thresholds are breached (thresholds configurable). | S |

### 8. Orchestration (Airflow)

| ID | Requirement | Priority |
|---|---|---|
| FR-8.1 | The system shall provide an Airflow DAG running the chain: extract → validate → save raw → transform → load → quality checks → refresh views. | M |
| FR-8.2 | The DAG shall run on a configurable schedule (default: daily after market close). | M |
| FR-8.3 | The DAG shall define task-level retries and retry delays. | M |
| FR-8.4 | The DAG shall support backfills for past date ranges. | S |
| FR-8.5 | The DAG shall map one task per symbol batch to enable parallel and isolated retries. | S |

### 9. Logging and Visualization

| ID | Requirement | Priority |
|---|---|---|
| FR-9.1 | The system shall log every pipeline run: start/end time, status, symbols processed, rows extracted, loaded, rejected, and errors. | M |
| FR-9.2 | The system shall store run history in a `pipeline_runs` table. | M |
| FR-9.3 | The system shall produce a time-series visualization (close price with moving averages, optionally volume). | M |
| FR-9.4 | The visualization shall read from the analytics layer, not from raw data. | M |

---

## Phase 2: Production-Ready ETL API Platform

### 10. Modular ETL Application

| ID | Requirement | Priority |
|---|---|---|
| FR-10.1 | The system shall refactor Phase 1 scripts into separate modules: extract, validate, transform, load, quality, and config. | M |
| FR-10.2 | The system shall store all configuration (credentials, URLs, schedules, thresholds) in environment variables or config files, separate from code. | M |
| FR-10.3 | Each module shall be independently testable with unit tests. | S |

### 11. REST API

| ID | Requirement | Priority |
|---|---|---|
| FR-11.1 | `POST /pipeline/trigger`: starts a pipeline run (optional parameters: symbols, date range) and returns a run ID. | M |
| FR-11.2 | `GET /pipeline/status/{run_id}`: returns run state (queued, running, success, failed) with timestamps. | M |
| FR-11.3 | `GET /pipeline/latest-success`: returns details of the most recent successful run. | M |
| FR-11.4 | `GET /pipeline/stats`: returns statistics (total runs, success rate, average duration, rows loaded, last failure). | M |
| FR-11.5 | `GET /health`: returns service and dependency health (API, database, Airflow). | S |
| FR-11.6 | The API shall return consistent JSON responses and appropriate HTTP status codes. | M |
| FR-11.7 | The API shall prevent overlapping runs by rejecting a trigger while a run is active (409 Conflict). | S |
| FR-11.8 | The API shall validate all request parameters and return clear error messages. | M |

### 12. Airflow Integration

| ID | Requirement | Priority |
|---|---|---|
| FR-12.1 | The API shall trigger the Phase 1 DAG through the Airflow REST API. | M |
| FR-12.2 | The API shall read DAG run state from Airflow to report status. | M |
| FR-12.3 | Run records shall link the API run ID to the Airflow DAG run ID. | M |

### 13. Security

| ID | Requirement | Priority |
|---|---|---|
| FR-13.1 | The API shall require basic authentication on all endpoints except `/health`. | M |
| FR-13.2 | Credentials shall be loaded from configuration and stored hashed or as secrets, never hardcoded. | M |
| FR-13.3 | Unauthorized requests shall return 401 and be logged. | M |

### 14. Logging and Error Handling

| ID | Requirement | Priority |
|---|---|---|
| FR-14.1 | The system shall use structured logging with levels and run IDs across API and ETL. | M |
| FR-14.2 | The API shall handle exceptions globally and never expose stack traces to clients. | M |
| FR-14.3 | Pipeline failures shall be recorded with error type and message in the run history. | M |

### 15. Containerization

| ID | Requirement | Priority |
|---|---|---|
| FR-15.1 | The system shall provide a Dockerfile for the API/ETL application. | M |
| FR-15.2 | The system shall provide Docker Compose to run the API, Airflow, PostgreSQL, Prometheus, and Grafana together. | M |
| FR-15.3 | The full stack shall start with a single command and persist data in volumes. | M |

### 16. Version Control and CI/CD

| ID | Requirement | Priority |
|---|---|---|
| FR-16.1 | All code shall be stored in a Git repository with a clear structure and a `.gitignore` that excludes secrets. | M |
| FR-16.2 | A GitHub Actions workflow shall run lint and automated tests on every push and pull request. | M |
| FR-16.3 | The workflow shall build the Docker image on merge to the main branch. | M |
| FR-16.4 | The workflow shall optionally push the image to a registry or deploy to a target environment. | O |

### 17. Monitoring

| ID | Requirement | Priority |
|---|---|---|
| FR-17.1 | The API shall expose a `/metrics` endpoint in Prometheus format. | M |
| FR-17.2 | Metrics shall include: pipeline runs (total, success, failure), run duration, rows processed, API request count and latency, and last successful run timestamp. | M |
| FR-17.3 | A Grafana dashboard shall visualize those metrics with panels for success rate, durations, failures, and rows loaded. | M |
| FR-17.4 | Grafana shall optionally include alerts for failed runs or stale data. | O |
| FR-17.5 | A separate Grafana dashboard may show market analytics by reading the Postgres analytics views. | O |

### 18. Documentation

| ID | Requirement | Priority |
|---|---|---|
| FR-18.1 | The system shall provide API documentation (OpenAPI/Swagger) listing endpoints, parameters, responses, and auth. | M |
| FR-18.2 | The project shall include one unified architecture diagram: Financial API → Airflow → Raw (Postgres JSONB) → Transformation → Analytics (Postgres) → ETL REST API → Prometheus → Grafana. | M |
| FR-18.3 | The README shall cover setup, configuration, usage, and both phases. | M |

### 19. Optional Extensions

| ID | Requirement | Priority |
|---|---|---|
| FR-19.1 | MongoDB storage of original API JSON alongside SQL (Phase 1). | O |
| FR-19.2 | Terraform provisioning of the infrastructure that runs the application (Phase 2). | O |

---

## Requirement Summary

| Area | Must | Should | Optional |
|---|---|---|---|
| Phase 1 (FR-1 to FR-9) | 35 | 8 | 1 |
| Phase 2 (FR-10 to FR-19) | 31 | 4 | 5 |

---