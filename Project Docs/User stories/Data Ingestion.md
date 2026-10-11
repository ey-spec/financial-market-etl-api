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