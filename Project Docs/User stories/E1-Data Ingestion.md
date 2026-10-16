# E1: Data Ingestion

**Phase:** 1 | **Covers:** FR-1, FR-2, FR-3 (+ NFR-2.2, NFR-3.1, NFR-4.4, NFR-5.1, NFR-5.2, NFR-5.4, NFR-6.3, NFR-7.1, NFR-7.2)

## Overview
This epic gets market data from the external financial APIs into our system safely. It covers loading configuration, calling the API within its limits, checking that each response is valid, and saving the original response untouched.

## What this epic covers
- **Configuration:** API keys, URLs, symbols, batch size, pauses, retry limits, and timeouts all come from config, never from code
- **Extraction:** calling the REST API per symbol, one-time backfill plus daily incremental runs
- **Incremental logic:** a watermark per symbol, advanced only after a successful load
- **Rate-limit handling:** batches with pauses, plus a daily credit budget
- **Retry logic:** exponential backoff for transient failures only
- **Validation:** check HTTP status and payload structure (including errors hidden inside a 200 response), quarantine bad responses per symbol
- **Raw storage:** each original JSON response saved in PostgreSQL (`raw` schema, JSONB) with metadata, append-only
- **Secondary provider:** EODHD for EGX symbols and cross-checking (pending confirmation, see E1-US8)

## Depends on
- **E3:** DDL for `raw.raw_responses`, `raw.quarantine`, `analytics.symbol_watermark`, `analytics.stg_api_responses`, `analytics.api_usage`, and `analytics.pipeline_run_symbols`
- **E4:** the run ID and the `pipeline_runs` table. Until the DAG exists, E1 code accepts the run ID as a parameter. E1 reports per-symbol results to `pipeline_run_symbols`.

## Decisions (resolved from the project docs)
| Topic | Decision |
|---|---|
| Primary provider | Twelve Data, Basic plan: 8 credits/min, 800 credits/day, 1 credit per symbol request |
| Endpoint | `time_series` with `interval=1day`; auth via `apikey` from config |
| Valid payload | HTTP 200, `status == "ok"`, `meta` present, `values[]` entries with datetime/open/high/low/close/volume |
| Bad payload | HTTP 200 with an error body is still invalid, except the provider's documented "no data for this range" response, which counts as zero rows (see E1-US5) |
| Quarantine | `raw.quarantine` table in PostgreSQL |
| Watermark | `analytics.symbol_watermark` (symbol_id (PK, FK), last_loaded_date, run_id, updated_at) E1 only reads the watermark. / The write is owned by E3-US6, in the load transaction.|
| Retry policy | Retry on timeout, 429, and 5xx. Never retry other 4xx. Wait = base × 2^(attempt−1) + jitter. All values configurable. |
| Run ID | UUID created by E4 `start_run` and passed into every E1 function |
| Handing Over Tasks | `extract` writes responses to `stg_api_responses`, `validate` marks them valid or sends invalid ones to `raw.quarantine`, and `save_raw` copies valid ones to `raw.raw_responses`. A retry of `validate` or `save_raw` never re-calls the API. |
---

## User Stories

### E1-US1: One-Time Historical Backfill Extraction
- **As a** Data Engineer,
- **I want to** fetch full available history for each configured symbol in a one-time backfill,
- **So that** the system starts with complete foundational market data.
- **Priority:** High
- **Covers:** FR-1.1, FR-1.2

**Acceptance Criteria:**
- Given a configured symbol list, When the backfill runs, Then it requests daily history for each symbol from the configured start date.
- Given a symbol's history exceeds the API's per-request limit (5000 points), When the backfill runs, Then it splits the request into date ranges so no data is missed.
- Given one symbol's call fails after all retries, When the backfill continues, Then the other symbols are still processed.
- Given the backfill finishes, When the run ends, Then a completion record gives a status per symbol (`success`, `no_new_data`, `failed`, `quarantined`, or `deferred`) with row counts, written to `pipeline_run_symbols`.
- Given a backfill is rerun for the same symbols, When it completes, Then no duplicates are created (loading is idempotent, see E3).

**Notes:** Backfill and incremental runs share the same extraction code. A symbol with no watermark is treated as a backfill (see E1-US2).

---

### E1-US2: Daily Incremental Data Extraction
- **As a** Data Engineer,
- **I want to** read the last loaded date (watermark) per symbol and request only newer data on daily runs,
- **So that** we avoid redundant API calls and never skip days.
- **Priority:** High
- **Covers:** FR-1.2, FR-1.3

**Acceptance Criteria:**
- Given a symbol has a watermark and the run has no explicit date range, When the daily run executes, Then the request asks only for dates after the watermark (`start_date` = watermark + 1 day).
- Given a symbol has no watermark, When the run executes, Then it is treated as a historical backfill for that symbol, starting from the configured start date.
- Given a run carries explicit `start_date` and `end_date` parameters (backfill mode, E4-US6), When extraction runs, Then those parameters override the watermark.
- Given a run in `mode = reprocess` (E4-US6), When the DAG executes, Then E1 functions are not called and no API request is made.
- Given data was fetched for a symbol, When the load transaction commits successfully (E3-US6), Then the watermark moves to the latest finalized trading date loaded. If the load fails, the watermark does not change. E1 only reads the watermark and never writes it.
- Given a bar whose date is today in the symbol's exchange timezone and the exchange has not closed yet, When it is fetched, Then it is flagged `has_preliminary_bar = true` in the raw metadata, and the watermark stays at the last finalized day. The exchange timezone and close time come from configuration.
- Given no new data exists for a symbol, When the run executes, Then it records zero rows fetched, sets the symbol status to `no_new_data` in `pipeline_run_symbols`, leaves the watermark unchanged, and the run continues.

**Notes:** The watermark table (`analytics.symbol_watermark`) is defined in E3.

---

### E1-US3: Rate-Limit Handling with Credit-Based Batches, Pauses, and a Daily Budget
- **As a** Data Engineer,
- **I want to** group requests into credit-based batches, pause between them, and track the daily credit budget,
- **So that** we never exceed the API's per-minute or per-day limits, even with retries and long backfills.
- **Priority:** High
- **Covers:** FR-1.4, NFR-7.2

**Acceptance Criteria:**
- Given a list of symbols, When the job plans its requests, Then it groups them into batches whose total credits do not exceed the configured per-minute limit (8 for Twelve Data). Each request, including each date-range chunk of a long backfill, costs one credit.
- Given any API call attempt, including a retry (E1-US4), When it is about to be made, Then one credit is reserved atomically in `analytics.api_usage` (provider, usage date in UTC, credits used) before the call.
- Given a batch has finished, When moving to the next batch, Then the system waits at least the configured pause (60 seconds for Twelve Data). The pause happens inside `extract`, before the pool slot is released (E4-US5).
- Given a call returns 429, When the next attempt is scheduled, Then it waits at least until the next minute window starts.
- Given credits used today plus the next request would exceed the provider's daily limit (800 for Twelve Data), When the request is due, Then no API call is made, the remaining symbols are marked `deferred` in `pipeline_run_symbols`, and the reason is logged. The run does not crash and does not fail only because of deferral.
- Given symbols were deferred in an earlier run, When the next run plans its batches, Then the order is: deferred symbols first, then symbols with no watermark, then symbols with the oldest watermark.
- Given an empty symbol list, When planning runs, Then it completes with no API calls and no pauses.

**Notes:** E1 provides the functions (credit reservation, budget check, pause). E4-US5 plans the Airflow batches and pools. Batch size, pause, and daily limit are configurable per provider (E1-US7), so a different provider or plan needs no code change.

---

### E1-US4: Retry Failed API Calls with Exponential Backoff
- **As a** Data Engineer,
- **I want to** retry transient failures with exponential backoff,
- **So that** temporary network or API problems do not cause permanent data gaps.
- **Priority:** High
- **Covers:** FR-1.5, NFR-2.2, NFR-3.1, NFR-8.2

**Acceptance Criteria:**
- Given a call times out or returns 429 or 5xx, When the failure occurs, Then it is retried automatically.
- Given consecutive retries, When each one is triggered, Then the wait follows `base × 2^(attempt−1)` plus jitter, capped at a configured maximum.
- Given every retry attempt, When it is made, Then it reserves a credit like any other call (E1-US3), so retries can never exceed the rate limit or the daily budget.
- Given the maximum number of retries is reached, When the last attempt fails, Then the error is logged with the run ID and symbol, and the response is saved in `raw.quarantine` if one was received. The symbol is marked `failed` in `pipeline_run_symbols` and the run continues with the other symbols.
- Given a symbol-specific non-retriable error (400 or 404), When it is received, Then no retry happens, the error is logged immediately, and that symbol is marked `failed`. The run continues.
- Given an authentication error (401 or 403, meaning a bad or missing key), When it is received, Then no retry happens and the task fails immediately with a non-retryable error (`AirflowFailException`), because every symbol would fail the same way.
- Given every API call, When it is made, Then it uses a configured timeout so it can never hang.
- Given a retried call eventually succeeds, When it finishes, Then only the successful response continues to validation.
- Given logs and error messages, When they are written, Then they never contain the API key.

**Notes:**
- Defaults (adjust in config): base 2 seconds, max 5 retries, timeout 30 seconds.
- `extract` has 1 Airflow task retry only (E4-US4), because these HTTP-level retries already happen inside the task and every attempt costs a credit.

---

### E1-US5: Response Validation and Quarantine
- **As a** Data Engineer,
- **I want to** validate the HTTP status and payload of every response and quarantine bad ones,
- **So that** invalid data never reaches the raw layer or the analytics layer.
- **Priority:** High
- **Covers:** FR-2.1, FR-2.2, FR-2.3

**Acceptance Criteria:**
- Given responses saved by `extract` in `analytics.stg_api_responses`, When `validate` runs, Then it reads them from there and never calls the API again.
- Given a response, When it is inspected, Then both the HTTP status code and the payload structure are validated.
- Given HTTP 200, When the body has `status` other than `ok` or contains an error code or message, Then it is treated as invalid, unless it is the provider's documented "no data for this range" response (next criterion).
- Given the provider's documented "no data available" response for a range with no trading days (exact body to be confirmed in the Twelve Data docs), When it is validated, Then it is treated as zero rows, not as an error: the symbol status is `no_new_data` and nothing is quarantined.
- Given a Twelve Data payload, When validated, Then `meta` and `values[]` must exist, `meta.symbol` must match the requested symbol, and each entry must have datetime, open, high, low, close, and volume parseable as the expected types.
- Given a response fails validation, When detected, Then it is saved unchanged (as text) in `raw.quarantine` with run ID, symbol, fetch timestamp, source endpoint, HTTP status, and a specific failure reason, and the symbol status is `quarantined`.
- Given one symbol is quarantined, When the batch continues, Then the other symbols are processed normally.
- Given a valid response with an empty `values[]` for a valid date range, When validated, Then it is logged as zero rows (a warning), not quarantined. It is still passed on to raw storage (E2-US1 handles empty payloads) and the symbol status is `no_new_data`.
- Given a response passes, When validated, Then it is marked valid in the landing table and moves on to raw storage (E1-US6).

**Notes:** Quarantined data is never read by transformation.

---

### E1-US6: Raw Storage of JSON Responses
- **As a** Data Engineer,
- **I want to** save every valid original API response unchanged in PostgreSQL (`raw` schema, JSONB) with metadata,
- **So that** we have an immutable audit trail and can reprocess without re-calling the API.
- **Priority:** High
- **Covers:** FR-3.1, FR-3.2, FR-3.3, NFR-3.5, NFR-4.4, NFR-5.4

**Acceptance Criteria:**
- Given a valid response in the landing table, When `save_raw` runs, Then the stored JSONB has the same content as the response (key order and whitespace are not preserved).
- Given a response is saved, When the row is written, Then it includes symbol, fetch timestamp (UTC), run ID, provider, source endpoint, `request_params`, HTTP status, `has_preliminary_bar`, and `purpose` (`primary` or `cross_check`).
- Given `source_endpoint` and `request_params`, When they are stored, Then they never contain the API key or token.
- Given `save_raw` is retried for the same landing row, When it runs again, Then no duplicate is created (unique `landing_id`, inserted with `ON CONFLICT DO NOTHING`). Fetching the same data in a different run still creates a new row.
- Given a database write fails, When saving, Then the transaction rolls back and the error is logged against the run ID.
- Given raw rows exist, When any pipeline step runs, Then raw rows are never updated or deleted (append-only).
- Given raw rows for a date range exist, When the transform is rerun, Then it works from raw alone, with no API call.

**Notes:** The `raw` schema DDL is delivered by E3, and this story depends on it. E3 must add `landing_id` and `purpose` to `raw.raw_responses`.

---

### E1-US7: Configuration Management
- **As a** Data Engineer,
- **I want to** load the API key, base URL, symbols, and limits from environment variables or config files,
- **So that** no secret or setting is ever hardcoded and values change without code edits.
- **Priority:** High
- **Covers:** FR-1.6, NFR-5.1, NFR-5.2, NFR-5.4, NFR-6.3, NFR-7.1

**Acceptance Criteria:**
- Given the application starts, When configuration is loaded, Then API key, base URL, symbol list, `primary_provider` per symbol, start date, per-provider batch size, pause, and daily limit, pool names, exchange timezone and close time, trading calendar per exchange, max retries, backoff base, timeout, metric windows, and quality thresholds all come from environment variables or a config file.
- Given a required value is missing or invalid, When the application starts, Then it fails fast with a clear message naming the missing setting, before any API call.
- Given logs and error messages, When they are written, Then they never contain the API key or full connection strings.
- Given the repository, When it is inspected, Then it contains an `.env.example` with placeholders only, and `.env` is in `.gitignore`.
- Given the code runs inside Airflow, When secrets are needed, Then they come from Airflow Connections or environment variables (E4-US1).
- Given a symbol is added or removed in config, When the next run executes, Then it is picked up with no code change.

**Notes:** One shared config mechanism serves E1, E2, and E4.

---

### E1-US8: Secondary Provider (EODHD) for EGX and Cross-Checking (new, pending decision)
- **As a** Data Engineer,
- **I want to** optionally fetch end-of-day data from EODHD for EGX symbols and cross-check Twelve Data,
- **So that** we cover Egyptian Exchange symbols and can verify data accuracy.
- **Priority:** Medium (confirm before building)
- **Covers:** new FR-1.7 (to be added to the Functional Requirements)

**Acceptance Criteria:**
- Given each configured symbol, When extraction plans its requests, Then it uses the symbol's explicit `primary_provider` field from configuration (not the ticker format), and batches never mix providers.
- Given an EODHD symbol in `SYMBOL.EXCHANGE` format (for example `COMI.EGX`), When the request is built, Then that ticker is used in the URL path. The symbol stored in our tables is the configured identifier.
- Given an EODHD response (a JSON array with no `meta` or `status`), When validated, Then the array shape is checked and the symbol is taken from the request, not the payload.
- Given EODHD returns `[]`, When validated, Then it is logged as zero rows with the likely cause noted (weekend or holiday, reversed date range, unknown symbol, or unpublished bar), and the symbol status is `no_new_data`.
- Given the EODHD limit of 20 calls per day, When the job runs, Then the same credit reservation and daily budget logic as E1-US3 applies using the EODHD limit and the `eodhd_api` pool.
- Given EODHD data fetched as the primary source, When stored, Then the raw response goes to the same raw layer with the provider recorded in the metadata.
- Given a cross-check fetch (for example `AAPL.US` to compare with Twelve Data), When it is stored, Then the raw row is tagged `purpose = 'cross_check'`, counts against the 20 calls/day budget, and is never transformed or loaded into the fact table.
- Given symbol details (name, exchange, currency) that EODHD does not return, When symbols are set up, Then they come from configuration.

**Notes:**
- The free plan has 1 year of history, so backfill depth is limited for EODHD symbols.
- Where cross-check comparison results are stored is still open (E3 question).
- E3 needs a `purpose` column in `raw.raw_responses`.
- If you drop EODHD, delete this story and the related lines in Resources.

---

## Remaining open questions
- Is EODHD in scope (secondary source, cross-check only, or dropped)?
- What is the configured backfill start date and the final symbol list?
- Confirm Twelve Data's exact error body format against the official docs before writing the validator.
- Confirm Twelve Data's exact "no data for this range" response body (used by E1-US5).

---

## Summary Table

| ID | Title | Priority | Covers |
| :--- | :--- | :--- | :--- |
| E1-US1 | One-Time Historical Backfill Extraction | High | FR-1.1, FR-1.2 |
| E1-US2 | Daily Incremental Data Extraction | High | FR-1.2, FR-1.3 |
| E1-US3 | Rate-Limit Handling with Credit-Based Batches, Pauses, and a Daily Budget | High | FR-1.4, NFR-7.2 |
| E1-US4 | Retry Failed API Calls with Exponential Backoff | High | FR-1.5, NFR-2.2, NFR-3.1, NFR-8.2 |
| E1-US5 | Response Validation and Quarantine | High | FR-2.1 to 2.3 |
| E1-US6 | Raw Storage of JSON Responses | High | FR-3.1 to 3.3, NFR-3.5, NFR-4.4 |
| E1-US7 | Configuration Management | High | FR-1.6, NFR-5.x, NFR-6.3, NFR-7.1 |
| E1-US8 | Secondary Provider (EODHD) | Medium | FR-1.7 (new) |