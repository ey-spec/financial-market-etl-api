# E1: Data Ingestion

**Phase:** 1 | **Covers:** FR-1, FR-2, FR-3

## Overview
This epic gets market data from the external financial API into our system safely. It covers calling the API, checking that the response is valid, and saving the original response untouched.

## What this epic covers
- **Extraction:** calling the REST API for each symbol, one-time backfill plus daily incremental runs
- **Incremental logic:** remember the last loaded date per symbol and fetch only newer data
- **Rate-limit handling:** batches of symbols with pauses between them
- **Retry logic:** retry failed calls with exponential backoff
- **Validation:** check HTTP status and payload structure, quarantine bad responses
- **Raw storage:** save each original JSON response in PostgreSQL (`raw` schema, JSONB) with metadata

## Depends on
Nothing. This is the starting point.

---

## User Stories

### US-1: One-Time Historical Backfill Extraction
- **As a** Data Engineer,
- **I want to** call the external financial REST API for each symbol to perform a one-time historical backfill,
- **So that** we can populate our system with foundational historical market data.
- **Priority:** High

**Acceptance Criteria:**
- Given the ingestion job is initialized for a target list of symbols, When the one-time backfill routine runs, Then it calls the external financial REST API for each symbol to fetch historical records.
- Given a backfill request is sent, When the API returns a successful response, Then the historical data is processed for downstream storage.
- Given the backfill process encounters a communication error, When an API call fails, Then the failure is logged and handled without crashing the entire batch.
- Given the backfill process is completed for all symbols, When the run finishes, Then a completion log records the status per symbol.

**Notes:** Assumes a predefined list of active symbols is provided as input to the backfill job.

---

### US-2: Daily Incremental Data Extraction
- **As a** Data Engineer,
- **I want to** remember the last loaded date per symbol and fetch only newer data during daily runs,
- **So that** we avoid redundant data fetching and keep our database updated efficiently.
- **Priority:** High

**Acceptance Criteria:**
- Given a symbol has a recorded last loaded date, When the daily incremental run executes, Then it queries the external API requesting only data newer than the last loaded date.
- Given a symbol has no prior loaded date recorded, When the incremental run executes, Then the system flags or handles the missing state safely.
- Given new data is successfully fetched for a symbol, When the run concludes, Then the last loaded date for that symbol is updated to the latest fetched timestamp.
- Given no new data is available for a symbol since the last loaded date, When the run executes, Then the system records zero new records fetched and proceeds gracefully.

**Notes:** Assumes a state-tracking mechanism exists to store and retrieve the last loaded date per symbol.

---

### US-3: Rate-Limit Handling with Batches and Pauses
- **As a** Data Engineer,
- **I want to** process symbols in batches with configured pauses between them,
- **So that** we comply with external API rate limits and prevent request blocking.
- **Priority:** Medium

**Acceptance Criteria:**
- Given a list of symbols to ingest, When the ingestion job runs, Then it groups the symbols into defined batches.
- Given a batch of symbols has finished processing, When transitioning to the next batch, Then the system enforces a configured pause or delay.
- Given the rate-limit configuration specifies a pause duration, When the system pauses, Then execution suspends for the exact specified duration before resuming.
- Given an empty symbol list is provided, When batching is executed, Then the system handles it gracefully without triggering unnecessary pauses.

**Notes:** Assumes batch size and pause duration values are configurable via environment variables or configuration files.

---

### US-4: Retry Failed API Calls with Exponential Backoff
- **As a** Data Engineer,
- **I want to** retry failed API calls using exponential backoff,
- **So that** temporary network glitches or transient API errors do not result in permanent data loss.
- **Priority:** High

**Acceptance Criteria:**
- Given an API call fails due to a transient error or network timeout, When the failure occurs, Then the system automatically retries the call.
- Given multiple consecutive retry attempts occur, When each subsequent retry is triggered, Then the wait time increases exponentially according to a defined formula.
- Given the maximum number of retry attempts is reached without success, When the final retry fails, Then the error is logged and the failed call is marked for escalation or quarantine.
- Given an API call fails with a non-retriable error (e.g., 400 Bad Request), When the response is received, Then the system bypasses exponential backoff retries.

**Notes:** Assumes max retry counts and backoff multiplier parameters need definition.

---

### US-5: Response Validation and Quarantine
- **As a** Data Engineer,
- **I want to** check the HTTP status and payload structure of API responses and quarantine bad responses,
- **So that** corrupted or invalid data does not contaminate our core datasets.
- **Priority:** High

**Acceptance Criteria:**
- Given an API response is received, When the system inspects it, Then it validates both the HTTP status code and the expected payload structure.
- Given an API response passes status and structure checks, When validated, Then it is marked as valid for further raw storage processing.
- Given an API response fails validation (e.g., unexpected schema or error status code), When the failure is detected, Then the response is isolated and saved into a quarantine area.
- Given a response is quarantined, When logged, Then metadata detailing the specific validation failure reason is attached.

**Notes:** Assumes explicit JSON schemas or validation rules for expected payloads are documented separately.

---

### US-6: Raw Storage of JSON Responses in PostgreSQL
- **As a** Data Engineer,
- **I want to** save each original JSON response in PostgreSQL under the raw schema using JSONB with metadata,
- **So that** we retain an immutable, queryable audit trail of all raw ingested data.
- **Priority:** High

**Acceptance Criteria:**
- Given a valid API response is received, When saving to the database, Then the original JSON response is stored untouched within the raw schema using the PostgreSQL JSONB data type.
- Given the JSON response is stored, When writing the record, Then supplementary metadata (such as ingestion timestamp, symbol, and source endpoint) is included.
- Given a database connection failure occurs during write operations, When saving fails, Then the transaction rolls back or error handling is triggered safely.
- Given historical raw records are queried, When searching the JSONB column, Then structural queries can be executed successfully.

**Notes:** Assumes the PostgreSQL database instance and raw schema are already provisioned.

---

## Questions / Clarifications Needed
- What are the specific endpoints, authentication headers, and rate-limit thresholds required by the external financial API?
- What is the precise JSON schema structure expected for payload validation?
- Where should quarantined responses be physically stored (e.g., a specific table or schema within PostgreSQL)?
- What is the exact state-management strategy for tracking the "last loaded date" per symbol?

---

## Summary Table

| ID | Title | Priority |
| :--- | :--- | :--- |
| US-1 | One-Time Historical Backfill Extraction | High |
| US-2 | Daily Incremental Data Extraction | High |
| US-3 | Rate-Limit Handling with Batches and Pauses | Medium |
| US-4 | Retry Failed API Calls with Exponential Backoff | High |
| US-5 | Response Validation and Quarantine | High |
| US-6 | Raw Storage of JSON Responses in PostgreSQL | High |