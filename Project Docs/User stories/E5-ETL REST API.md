# E5: ETL REST API

**Phase:** 2 | **Covers:** FR-10, FR-11 (+ FR-9.2 read side, NFR-5.1, NFR-5.2, NFR-6.1, NFR-6.3, NFR-7.3, NFR-8.1, NFR-9.1, NFR-11.1) *(FR numbers to be confirmed against the Functional Requirements file)*

## Overview
This epic adds a REST API on top of the Phase 1 pipeline. It lets a client (1) trigger and monitor pipeline runs and (2) read the loaded market data and analytics. The API never extracts, transforms, or loads anything itself. It triggers the existing Airflow DAG, and reads only the analytics layer, so Phase 1 logic stays in one place.

## What this epic covers
- **Service foundation:** FastAPI app, config, health checks, OpenAPI docs, uniform errors
- **Security:** API keys with two roles (reader, operator), least-privilege database access
- **Run triggering:** validate, create a `queued` run, trigger the DAG, return `202`
- **Run monitoring:** run list, run detail, per-symbol results, quality results
- **Market data:** symbols and daily prices with metrics
- **Analytics:** top movers, monthly volume, best/worst days, volume stats (from the E3 views)
- **API hygiene:** pagination, validation, rate limiting, request IDs
- **Testing:** contract and integration tests that run without Airflow or the real API

## Depends on
- **E3:** `fact_daily_prices`, `dim_symbol`, the views (`mv_*`, `v_rolling_returns`), `pipeline_runs`, `pipeline_run_symbols`, `quality_results`, and the roles in E3-US1
- **E4:** the DAG `financial_market_pipeline`, the `dag_run.conf.run_id` contract, the `queued → running` transition in `start_run` (E4-US7), and the parameter validation rules (E4-US6)
- **E1:** the shared configuration mechanism (E1-US7) and the configured symbol list
- **E6:** Docker Compose service and CI for the API

Build order: E4 complete (Phase 1 done) → this epic. Read endpoints (US4 to US6) need no Airflow. Only US3 needs it.

## Decisions

### Rules

| Topic | Decision |
|---|---|
| Framework | FastAPI with Pydantic models. OpenAPI generated automatically. |
| Structure | Three layers: **router** (HTTP only) → **service** (rules) → **repository** (SQL only). Dependencies are injected, so each layer can be tested alone. No business logic in routers. |
| Base path | `/api/v1`. Breaking changes need a new version. |
| Read-only by default | Read endpoints use the `analytics_reader` role (E3-US1): select on `analytics` only, no access to `raw`, so raw and quarantine bodies are never exposed. |
| Write access | Only `POST /runs` writes, through a separate `api_operator` role that can insert/update `pipeline_runs` only. It cannot touch any other table (new, added to E3-US1). |
| Triggering | The API creates the run row and calls Airflow's REST API. It never runs ETL code. |
| Run ID | The API generates a UUID and passes it as `dag_run.conf.run_id`. It also sets the Airflow `dag_run_id` to `api__<run_id>` and stores it as `airflow_run_id` (unique). |
| Run states exposed | `queued`, `running`, `success`, `failed`, plus `quality_status` (`pass`, `warn`, `fail`, `skipped`). Same values as E3-US11. |
| Concurrency | `max_active_runs = 1` (E4). A second triggered run stays `queued` until the first ends. |
| Preliminary bars | Excluded by default. `include_preliminary=true` returns them with `is_preliminary` visible. |
| Data meaning | Returns are fractions (0.0123 = 1.23%). Volumes are not comparable across symbols or currencies (E3-US7). Both are stated in the API docs. |
| Auth | `X-API-Key` header. Keys come from configuration, are stored hashed, and carry a role. |
| Roles | `reader`: all GET endpoints. `operator`: everything `reader` can do, plus `POST /runs`. |
| Error format | One JSON shape for every error: `error.code`, `error.message`, `error.details`, `request_id`. Never contains secrets, SQL, or stack traces. |
| Pagination | `limit` (default 100, max 1000) and `offset`, with `total` in the response. Stable ordering is always defined. |
| Time and dates | Timestamps in UTC (ISO 8601). Trade dates are plain dates (`YYYY-MM-DD`), as in E2. |

### Endpoint catalog

| Method | Path | Role | Purpose |
|---|---|---|---|
| GET | `/health` | none | Process is alive |
| GET | `/ready` | none | Database reachable (Airflow status is reported but does not fail readiness) |
| POST | `/api/v1/runs` | operator | Trigger a run (incremental, backfill, reprocess) |
| GET | `/api/v1/runs` | reader | List runs, filter by status, mode, date |
| GET | `/api/v1/runs/{run_id}` | reader | Run detail with totals |
| GET | `/api/v1/runs/{run_id}/symbols` | reader | Per-symbol status and counters |
| GET | `/api/v1/runs/{run_id}/quality` | reader | Quality results of the run |
| GET | `/api/v1/symbols` | reader | Symbols from `dim_symbol` |
| GET | `/api/v1/symbols/{symbol}/prices` | reader | Daily prices and metrics for a date range |
| GET | `/api/v1/analytics/top-movers` | reader | Top gainers and losers |
| GET | `/api/v1/analytics/monthly-volume` | reader | Monthly average volume per symbol |
| GET | `/api/v1/analytics/best-worst-days` | reader | Best and worst daily returns per symbol |
| GET | `/api/v1/analytics/volume-stats` | reader | Average, min, max volume per symbol |

---

## User Stories

### E5-US1: API Foundation, Configuration, and Health
- **As a** Backend Engineer,
- **I want to** a FastAPI service with layered structure, shared configuration, and health endpoints,
- **So that** every endpoint is built on the same tested base and operations can monitor it.
- **Priority:** High
- **Covers:** NFR-5.1, NFR-5.2, NFR-6.1, NFR-6.3, NFR-11.1

**Acceptance Criteria:**
- Given the service starts, When configuration is loaded, Then database URLs, Airflow URL and credentials, API keys, rate limits, and pagination limits come from environment variables or the shared config mechanism (E1-US7). A missing required value fails fast with a message naming the setting, before the service accepts requests.
- Given the repository, When it is inspected, Then no secret is hardcoded, `.env.example` has placeholders only, and `.env` is in `.gitignore`.
- Given the code, When it is inspected, Then routers contain no SQL and no business rules. Services call repositories, and repositories contain all SQL.
- Given a request, When it is received, Then it gets a request ID, returned in the `X-Request-ID` header and included in every log line and error body.
- Given any failure, When an error response is built, Then it uses the uniform error shape and never contains secrets, SQL text, or stack traces.
- Given `GET /health`, When called, Then it returns 200 without touching the database.
- Given `GET /ready`, When the data database is reachable, Then it returns 200. When it is not, Then 503. Airflow's reachability is reported in the body but does not change the status code, because read endpoints work without Airflow.
- Given the running service, When `/docs` is opened, Then the OpenAPI documentation lists every endpoint, parameter, response, and error shape.
- Given logs, When they are written, Then they never contain API keys or full connection strings.

**Notes:** Pattern: layered architecture with dependency injection (router → service → repository). It keeps SQL replaceable and lets tests swap the database for a fake.

---

### E5-US2: Authentication and Authorization
- **As a** Backend Engineer,
- **I want to** protect every data and run endpoint with API keys and two roles,
- **So that** only known clients can read data and only operators can start runs.
- **Priority:** High
- **Covers:** NFR-5.1, NFR-5.2, NFR-7.3

**Acceptance Criteria:**
- Given a request to any `/api/v1` endpoint, When the `X-API-Key` header is missing or invalid, Then the response is 401 and no data is returned.
- Given a valid `reader` key, When it calls a GET endpoint, Then the request succeeds. When it calls `POST /runs`, Then the response is 403.
- Given a valid `operator` key, When it calls any endpoint, Then the request succeeds.
- Given the key store, When it is inspected, Then keys are stored as hashes (never plain text), each with a role and a name, and are compared in constant time.
- Given logs and errors, When an authentication failure is recorded, Then the key value is never logged. Only the key name (if known) and the request ID are.
- Given the database connections, When the API starts, Then read endpoints use the `analytics_reader` role and `POST /runs` uses the `api_operator` role, never a superuser and never `etl_writer`.
- Given the `analytics_reader` role, When an endpoint is built, Then no endpoint can read `raw`, `raw.quarantine`, or the staging tables (enforced by database permissions, not only by code).
- Given a key is added, removed, or rotated in configuration, When the service restarts, Then the change applies with no code edit.
- Given `/health` and `/ready`, When called without a key, Then they work (the only open endpoints).

**Notes:** Phase 2 uses API keys for simplicity. OAuth/JWT is out of scope (see open questions).

---

### E5-US3: Trigger a Pipeline Run
- **As an** Operator,
- **I want to** start an incremental, backfill, or reprocess run through the API,
- **So that** I can load or repair data without opening Airflow.
- **Priority:** High
- **Covers:** FR-10 (trigger), FR-8.4, FR-9.2

**Request body:** `mode` (`incremental`, `backfill`, `reprocess`), optional `symbols`, `start_date`, `end_date`, and `source_run_id` (reprocess only).

**Acceptance Criteria:**
- Given a valid request from an operator, When `POST /api/v1/runs` is called, Then the API creates a `pipeline_runs` row with `status = queued`, `trigger_type = api`, the requested `mode`, and `params`, then triggers the DAG with `dag_run.conf.run_id` and returns `202` with the run ID and a link to `GET /runs/{run_id}`.
- Given the request, When it is validated, Then it uses the same rules as E4-US6 (start not after end, no future end date, symbols in the configured list, known `mode`, dates required for backfill, `source_run_id` or range required for reprocess), implemented in one shared function used by both the API and `start_run`. An invalid request returns `422` with field-level details and creates no row.
- Given `mode = reprocess`, When the run is created, Then `params` records the range or `source_run_id` and no API credits are involved (E4-US6).
- Given an `Idempotency-Key` header, When the same key and the same body are sent again, Then the original run is returned and no second run is created. The same key with a different body returns `409`.
- Given the `pipeline_runs` row is created, When Airflow cannot be reached or rejects the trigger, Then the row is marked `failed` with a sanitized `error_type` and `error_message`, and the API returns `502`. A run is never left `queued` with no DAG run behind it.
- Given another run is active, When a new run is triggered, Then it is accepted and shown as `queued` until Airflow starts it (`max_active_runs = 1`, E4).
- Given the DAG's `start_run` begins, When it finds the `queued` row for this run ID, Then it moves the row to `running` (E4-US7), so the status the API shows is always current.
- Given the Airflow call, When it is made, Then it uses a configured timeout and credentials from configuration, and its URL and credentials never appear in logs or responses.
- Given a trigger, When it is recorded, Then the audit trail shows who started it (the API key name) in `params`, never the key itself.

**Notes:** Flow: validate → insert `queued` row → call Airflow → return `202`. The row is written first, so even a failed trigger leaves an audit record. This is the **command pattern**: the API only records the intent and hands execution to the orchestrator.

---

### E5-US4: Run Monitoring Endpoints
- **As a** Data Engineer,
- **I want to** query run history, run detail, per-symbol results, and quality results,
- **So that** I can see what happened in any run without database access.
- **Priority:** High
- **Covers:** FR-11 (monitoring read side), FR-9.2, NFR-8.1

**Acceptance Criteria:**
- Given `GET /runs`, When called, Then it returns runs newest first with `run_id`, `status`, `quality_status`, `mode`, `trigger_type`, `started_at`, `ended_at`, and duration, paginated, and filterable by `status`, `mode`, `trigger_type`, and a date range.
- Given `GET /runs/{run_id}`, When the run exists, Then it returns the full row: totals, counters, params, error fields, and the Airflow run ID.
- Given an unknown run ID, When requested, Then the response is 404 with the uniform error shape. A malformed UUID returns 422.
- Given `GET /runs/{run_id}/symbols`, When called, Then it returns one entry per symbol with `provider`, `batch_no`, `status`, row counters, and sanitized error fields. It can be filtered by `status`.
- Given `GET /runs/{run_id}/quality`, When called, Then it returns each check with measured value, warn and fail thresholds, status, and symbol (nullable), plus pass/warn/fail counts. A run with `quality_status = skipped` returns an empty list with that status stated.
- Given a run still in progress, When requested, Then it returns its current state, with `ended_at` empty and totals not yet final (totals are written by `finalize_run`, E4-US8). The response says so.
- Given a `queued` run, When requested, Then the symbol list is empty (symbol rows are created by `start_run`) and the status is `queued`.
- Given error fields, When returned, Then they are the sanitized values already stored (no traceback, no secrets).

---

### E5-US5: Symbols and Daily Price Endpoints
- **As a** Data Consumer,
- **I want to** list symbols and fetch daily prices with metrics for a date range,
- **So that** I can use the cleaned data in my own tools.
- **Priority:** High
- **Covers:** FR-11 (market data)

**Acceptance Criteria:**
- Given `GET /symbols`, When called, Then it returns active symbols from `dim_symbol` (symbol, name, exchange, currency, instrument type, provider), filterable by `exchange` and `is_active`.
- Given `GET /symbols/{symbol}/prices`, When called with `start_date` and `end_date`, Then it returns daily rows ordered by trade date ascending, with open, high, low, close, volume, and the E2 metrics under their role-based names (`daily_return`, `ma_short`, `ma_long`, `vol_avg_short`, `vol_avg_long`, `volatility_long`, `cumulative_return`, `rolling_high_long`, `rolling_low_long`).
- Given the dates are omitted, When called, Then the default is the last 90 calendar days, and the maximum range per request is configurable.
- Given an unknown symbol, When requested, Then the response is 404. A known symbol with no data in the range returns 200 with an empty list.
- Given `start_date` after `end_date`, or a malformed date, When requested, Then the response is 422.
- Given preliminary bars, When the request does not set `include_preliminary`, Then they are excluded. When it is `true`, Then they are returned with `is_preliminary = true`.
- Given null metrics (window not full), When serialized, Then they are `null`, never `0`.
- Given numeric columns, When serialized, Then prices and metrics keep their stored precision (decimals are not converted through lossy floats).
- Given the response, When returned, Then it includes `as_of`, the end time of the latest successful run, so clients can judge freshness.
- Given the query, When it runs, Then it uses the `(symbol_id, trade_date)` key and is paginated.

---

### E5-US6: Analytics Endpoints
- **As a** Data Analyst,
- **I want to** query the ready-made analytics through the API,
- **So that** dashboards and scripts get the same answers as the SQL queries.
- **Priority:** Medium
- **Covers:** FR-11 (analytics), FR-6.3

**Acceptance Criteria:**
- Given `GET /analytics/top-movers`, When called with an optional `n` (default 5), Then it returns the top `n` gainers and bottom `n` losers from `mv_top_movers_30d`, with period return as a fraction and the period dates.
- Given `GET /analytics/monthly-volume`, When called with `symbol` and an optional month range, Then it returns average volume per month and the change versus the previous month from `mv_monthly_volume`.
- Given `GET /analytics/best-worst-days`, When called with `symbol`, Then it returns the top 5 and bottom 5 daily returns from `mv_best_worst_days`, with ties sharing a rank.
- Given `GET /analytics/volume-stats`, When called, Then it returns average, min, and max volume per symbol from `mv_symbol_volume_stats`, optionally for one symbol.
- Given any analytics endpoint, When it runs, Then it reads only the E3 views (never recomputes from raw) and applies the same definitions as E3-US7 and E3-US8.
- Given the views were not refreshed because quality failed (E4-US8), When an endpoint is called, Then it still serves the last good data, and the response includes `as_of` so the client can see how fresh it is.
- Given a view is empty (nothing loaded yet), When called, Then the response is 200 with an empty list, not an error.
- Given a symbol parameter that does not exist, When called, Then the response is 404.
- Given the docs, When they describe volume results, Then they state that volumes are only comparable within one symbol.

---

### E5-US7: Validation, Pagination, and Rate Limiting
- **As a** Backend Engineer,
- **I want to** consistent input validation, pagination, and rate limits on every endpoint,
- **So that** the API is predictable and one client cannot overload the database.
- **Priority:** Medium
- **Covers:** NFR-7.3, NFR-6.3

**Acceptance Criteria:**
- Given any request, When parameters are invalid (type, range, enum, format), Then the response is 422 listing each invalid field, before any database call.
- Given a paginated endpoint, When `limit` exceeds the maximum or is below 1, Then the response is 422. The response always includes `total`, `limit`, and `offset`.
- Given a paginated list, When it is requested twice with no new data, Then the order and content are identical (a stable sort key is always defined).
- Given an API key, When it exceeds the configured requests per minute, Then the response is 429 with a `Retry-After` header. Limits are separate for `reader` and `operator` and come from configuration.
- Given `POST /runs`, When it is called, Then it has a stricter limit than the read endpoints.
- Given SQL parameters, When queries are built, Then all values are bound parameters. No user input is ever concatenated into SQL.
- Given sorting or filtering options, When a client sends a field name, Then only an allow-listed set of fields is accepted.
- Given a query that runs too long, When it exceeds the configured statement timeout, Then it is cancelled and the response is 503 or 504 with the uniform error shape.
- Given limit and timeout values, When they are changed in configuration, Then they apply with no code edit.

---

### E5-US8: API Testing and Contract Verification
- **As a** Backend Engineer,
- **I want to** run automated tests for every endpoint without Airflow or production data,
- **So that** the API is verifiable and CI can run it on every push.
- **Priority:** High
- **Covers:** NFR-9.1, NFR-9.4

**Acceptance Criteria:**
- Given the test suite, When it runs, Then it uses a test database built by the real migrations (E3-US1) with a small seeded dataset, and a fake Airflow client. No network and no real keys are needed.
- Given unit tests, When they run, Then services are tested with fake repositories (the layering makes this possible).
- Given the endpoint catalog, When tests run, Then every endpoint has tests for success, validation errors (422), missing or wrong key (401), wrong role (403), and not found (404).
- Given seeded data with known values, When the data and analytics endpoints are called, Then responses match hand-calculated expected values (returns, moving averages, top movers).
- Given `POST /runs`, When tested, Then the suite covers: valid trigger (row `queued`, Airflow called once), invalid parameters (no row), repeated `Idempotency-Key` (one run), Airflow down (row `failed`, response 502), and a second run while one is active (`queued`).
- Given the `analytics_reader` connection, When a test tries to read from `raw` or write to a fact table through the API's read connection, Then the database rejects it.
- Given the error and log output, When tests inspect them, Then no API key, password, or connection string appears.
- Given the generated OpenAPI document, When tests compare it with a stored copy, Then an unintended change to the API contract fails the build.
- Given a push or pull request, When CI runs (E6), Then these tests run and must pass.
- Given the end-to-end test of E4-US9, When it is extended for Phase 2, Then it includes one run triggered through `POST /runs` against the mocked API.

---

## Remaining open questions
- Which FR numbers cover the API (FR-10, FR-11 are assumed here)? Confirm against the Functional Requirements and update the Covers lines.
- Is a separate `api_operator` role acceptable? E3-US1 must add it, with insert/update on `pipeline_runs` only (and `pipeline_run_symbols` is untouched, since `start_run` creates those rows).
- Are API keys enough, or is OAuth/JWT required?
- Should the API offer a cancel-run endpoint, or is cancelling done in Airflow only?
- Should price endpoints support CSV output as well as JSON?
- Which Airflow REST API version is used (it depends on the Airflow major pinned in E4-US1)?
- Maximum date range and page size per request (starting values: 366 days, 1000 rows)?

---

## Summary Table

| ID | Title | Priority | Covers |
| :--- | :--- | :--- | :--- |
| E5-US1 | API Foundation, Configuration, and Health | High | NFR-5.1, NFR-5.2, NFR-6.1, NFR-6.3, NFR-11.1 |
| E5-US2 | Authentication and Authorization | High | NFR-5.1, NFR-5.2, NFR-7.3 |
| E5-US3 | Trigger a Pipeline Run | High | FR-10, FR-8.4, FR-9.2 |
| E5-US4 | Run Monitoring Endpoints | High | FR-11, FR-9.2, NFR-8.1 |
| E5-US5 | Symbols and Daily Price Endpoints | High | FR-11 |
| E5-US6 | Analytics Endpoints | Medium | FR-11, FR-6.3 |
| E5-US7 | Validation, Pagination, and Rate Limiting | Medium | NFR-7.3, NFR-6.3 |
| E5-US8 | API Testing and Contract Verification | High | NFR-9.1, NFR-9.4 |