# ERD: Raw Schema

**Covers:** E3-US2 | FR-3.1, FR-3.2, FR-3.3, FR-2.3 | NFR-4.4, NFR-12.1

## 1. Purpose

The `raw` schema is the pipeline's permanent record of what the financial APIs actually returned. It does not clean or change anything.

It exists for two reasons:
- **Audit:** we can always show exactly what the provider sent, and when.
- **Reprocessing:** if a transformation bug is found, we rebuild the analytics data from raw without calling the API again (this saves API credits).

The schema has two tables:
- `raw.raw_responses` keeps valid responses.
- `raw.quarantine` keeps invalid responses, so bad data never reaches transformation but is still available for investigation.

## 2. Rules for the whole schema

| Rule | Meaning |
|---|---|
| Append-only | Rows are never updated or deleted. A trigger blocks `UPDATE`, `DELETE`, and `TRUNCATE`, and the privileges are also revoked. |
| Permanent retention | No job ever purges raw or quarantine data. |
| No duplicates on retry | `landing_id` is unique, so a retried task cannot insert the same response twice. |
| No secrets | `source_endpoint` and `request_params` never contain the API key or token. |
| Same data fetched twice | Both responses are kept. De-duplication happens later, in the transform step, not in raw. |

## 3. Table: `raw.raw_responses`

Stores every valid API response, unchanged, with metadata.

| Column | Type | Description |
|---|---|---|
| `raw_id` | PK | Unique row identifier. |
| `landing_id` | unique | Comes from the landing table. Makes `save_raw` safe to retry. |
| `run_id` | FK | The pipeline run that fetched this response (→ `analytics.pipeline_runs`). |
| `provider` | text | Which provider returned it (Twelve Data or EODHD). Tells the transform step which parser to use. |
| `purpose` | text | `primary` (used for analytics) or `cross_check` (used only to compare providers, never transformed). |
| `symbol` | text | The requested symbol. Needed because some responses do not contain it. |
| `source_endpoint` | text | Which endpoint was called, with no key. |
| `request_params` | JSONB | The parameters sent (dates, interval), with no key. |
| `http_status` | int | HTTP status code of the response. |
| `fetched_at` | timestamptz | When it was fetched, in UTC. |
| `has_preliminary_bar` | boolean | True if the response includes today's unfinished bar. Used to flag preliminary rows later. |
| `payload` | JSONB | The original API response. Same content as received (JSONB does not keep key order or whitespace). |

## 4. Table: `raw.quarantine`

Stores every response that failed validation, with the reason.

| Column | Type | Description |
|---|---|---|
| `quarantine_id` | PK | Unique row identifier. |
| `landing_id` | unique | Same retry protection as above. |
| `run_id` | FK | The run that fetched it (→ `analytics.pipeline_runs`). |
| `provider` | text | Which provider returned it. |
| `symbol` | text | The requested symbol. |
| `source_endpoint` | text | Which endpoint was called, with no key. |
| `request_params` | JSONB | The parameters sent, with no key. |
| `http_status` | int | HTTP status code. |
| `fetched_at` | timestamptz | When it was fetched, in UTC. |
| `failure_reason` | text, NOT NULL | Why validation rejected it. Required, so every quarantined row is explainable. |
| `response_body` | TEXT | The raw body as text. TEXT and not JSONB, because a bad response may not be valid JSON. |

## 5. Relationships

| From | To | Type | Notes |
|---|---|---|---|
| `raw.raw_responses.run_id` | `analytics.pipeline_runs.run_id` | many-to-one | Many responses belong to one run. `ON DELETE RESTRICT`. |
| `raw.quarantine.run_id` | `analytics.pipeline_runs.run_id` | many-to-one | Many quarantined responses belong to one run. `ON DELETE RESTRICT`. |

Other points:
- The two raw tables are not related to each other. A response goes to exactly one of them.
- `symbol` has no foreign key to `dim_symbol`. Raw must store what was received, even for an unknown symbol.
- `landing_id` originates in `analytics.stg_api_responses`, but there is no foreign key, because that table is temporary and is cleaned up.
- `pipeline_runs` belongs to the `analytics` schema and is designed in the analytics ERD.

## 6. Indexes

| Table | Index | Supports |
|---|---|---|
| both | `(run_id)` | Finding everything fetched by one run |
| both | `(symbol, fetched_at DESC)` | Finding the latest responses for a symbol |
| both | unique on `landing_id` | Retry safety |

## 7. How data flows into these tables

1. `extract` saves each response in `analytics.stg_api_responses` (a temporary landing table).
2. `validate` checks it:
   - valid → marked valid
   - invalid → copied to `raw.quarantine`
3. `save_raw` copies valid responses into `raw.raw_responses`.
4. `transform` later reads only `raw.raw_responses` rows with `purpose = 'primary'`. It never reads quarantine or cross-check rows.


## 8. Keys: Primary Keys and Foreign Keys

### Primary keys

| Table | Primary key | Notes |
|---|---|---|
| `raw.raw_responses` | `raw_id` | Created by this table when the row is inserted. |
| `raw.quarantine` | `quarantine_id` | Created by this table when the row is inserted. |

### Foreign keys

| Foreign key (in this table) | Points to (where the PK lives) | Rule |
|---|---|---|
| `raw.raw_responses.run_id` | `analytics.pipeline_runs.run_id` (PK of `pipeline_runs`, analytics schema) | `ON DELETE RESTRICT` |
| `raw.quarantine.run_id` | `analytics.pipeline_runs.run_id` (PK of `pipeline_runs`, analytics schema) | `ON DELETE RESTRICT` |

`ON DELETE RESTRICT` means a run cannot be deleted while raw rows still reference it, which protects the audit trail.

### Unique keys (not primary keys)

| Table | Unique column | Purpose |
|---|---|---|
| `raw.raw_responses` | `landing_id` | Stops duplicates when `save_raw` is retried. |
| `raw.quarantine` | `landing_id` | Stops duplicates when `validate` is retried. |

### Columns that look like keys but are not foreign keys

| Column | Why there is no FK |
|---|---|
| `symbol` (both tables) | Raw must store what was received, even for a symbol not yet in `dim_symbol`. |
| `landing_id` (both tables) | It comes from `analytics.stg_api_responses`, which is temporary and gets cleaned up, so a FK would break after cleanup. |

### Summary

- Both raw tables have exactly one FK: `run_id`.
- That FK always points to `analytics.pipeline_runs`.
- No FK points into the raw tables, so nothing depends on them structurally. They are read by the transform step, but only through queries.