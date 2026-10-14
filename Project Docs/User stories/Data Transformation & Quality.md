# E2: Data Transformation & Quality

**Phase:** 1 | **Covers:** FR-4, FR-7

## Overview
This epic turns raw API JSON into clean, reliable, analysis-ready data, and checks that the result can be trusted.

## What this epic covers
- **Parsing and typing:** JSON to tables with correct data types
- **Cleaning:** standardize timestamps (UTC), remove duplicates, handle missing values
- **Metrics:** daily returns, moving averages, trading volume statistics, basic time-series metrics
- **Data-quality checks:** nulls, duplicates, negative values, date gaps, row-count comparison with raw
- **Quality report:** results stored per run, with thresholds that warn or fail the run

## Depends on
E1 (reads from the raw layer).

## User Stories

### User Story 1: JSON Parsing and Data Type Mapping

* **User Story:**
  * As a Data Engineer,
  * I want to parse raw API JSON data into structured tables with correct data types,
  * So that the data can be accurately queried and processed.

* **Acceptance Criteria:**
  * Given raw API JSON records from the E1 layer, when the parsing pipeline executes, then data is successfully converted into tabular format with correct data types.
  * Given incoming JSON data containing standard field types, when parsing runs, then strings, integers, floats, and booleans map to their respective target table columns accurately.
  * Given incoming JSON with malformed structures or unexpected data types, when the parser processes the record, then it captures the parsing error gracefully without crashing the entire run.
  * Given an empty JSON payload from the source, when parsing is triggered, then it handles the empty payload by outputting an empty table structure safely.

* **Priority:** High

* **Questions / Notes:**
  * What exact target schema or data type definitions should be applied to each specific JSON field? Are there fallback types for unrecognized formats?

---

### User Story 2: Data Cleaning and Standardization

* **User Story:**
  * As a Data Engineer,
  * I want to standardize timestamps to UTC, remove duplicate records, and handle missing values,
  * So that the dataset is clean, uniform, and reliable for subsequent analysis.

* **Acceptance Criteria:**
  * Given parsed tabular data with various time zones, when the cleaning process runs, then all timestamps are successfully standardized to UTC.
  * Given a dataset containing duplicate rows, when the deduplication process runs, then duplicate entries are successfully removed.
  * Given records containing missing or null values, when the missing value handler executes, then missing values are addressed according to defined rules.
  * Given records that already have correct UTC timestamps, no duplicates, and no missing values, when cleaning runs, then the dataset passes through unchanged and intact.

* **Priority:** High

* **Questions / Notes:**
  * How should missing values be handled (e.g., dropped entirely, or imputed with default/null placeholders)? What defines a duplicate record (exact match across all columns or a unique key)?

---

### User Story 3: Financial and Time-Series Metrics Calculation

* **User Story:**
  * As a Data Analyst,
  * I want to calculate daily returns, moving averages, trading volume statistics, and basic time-series metrics,
  * So that financial insights and analytics-ready metrics are available.

* **Acceptance Criteria:**
  * Given clean time-series data, when the metrics pipeline runs, then daily returns and moving averages are accurately computed.
  * Given trading or transaction data, when volume processing executes, then trading volume statistics and basic time-series metrics are generated.
  * Given insufficient historical data points to compute a moving average window, when the calculation runs, then it outputs null or an appropriate indicator for those initial periods instead of failing.
  * Given zero or negative trading volume figures in the raw data, when volume statistics are calculated, then edge cases are handled safely without causing mathematical exceptions (like division by zero).

* **Priority:** Medium

* **Questions / Notes:**
  * What are the exact mathematical formulas or window sizes (e.g., 30-day moving average) required for these metrics?

---

### User Story 4: Automated Data-Quality Validation Checks

* **User Story:**
  * As a Data Engineer,
  * I want to run automated quality checks for nulls, duplicates, negative values, date gaps, and row-count comparisons against raw data,
  * So that data anomalies and integrity issues can be detected.

* **Acceptance Criteria:**
  * Given transformed tables, when data-quality validation runs, then it checks for unexpected nulls, duplicates, and negative values.
  * Given time-series data streams, when the validation executes, then date gaps are identified and flagged.
  * Given the raw data layer and the transformed layer, when row counts are compared, then a row-count comparison metric is generated against the raw source.
  * Given a dataset with zero anomalies or discrepancies, when quality checks execute, then the validation status reports zero errors or warnings.

* **Priority:** High

* **Questions / Notes:**
  * Which specific fields are permitted to contain negative values (e.g., daily returns can be negative, but trading volume cannot)? What constitutes an unacceptable "date gap"?

---

### User Story 5: Quality Report Generation and Threshold Management

* **User Story:**
  * As a Data Quality Auditor,
  * I want to store quality check results per run and enforce thresholds that can warn or fail the run,
  * So that pipeline degradations and failures can be properly monitored and handled.

* **Acceptance Criteria:**
  * Given completed data-quality checks, when a pipeline run finishes, then the quality results are stored per run.
  * Given quality metrics that breach defined warning thresholds, when evaluation occurs, then a warning is triggered for the run.
  * Given quality metrics that breach critical failure thresholds, when evaluation occurs, then the pipeline run is marked as failed.
  * Given quality metrics that remain safely within acceptable limits, when thresholds are evaluated, then the run completes successfully without warnings or failures.

* **Priority:** Medium

* **Questions / Notes:**
  * What are the exact numerical or percentage thresholds for warnings versus failures? Where should the quality reports be stored?

---

## Summary Table

| ID | Title | Priority |
| :--- | :--- | :--- |
| US-1 | JSON Parsing and Data Type Mapping | High |
| US-2 | Data Cleaning and Standardization | High |
| US-3 | Compute Daily Returns, Moving Averages, and Volume Statistics | Medium |
| US-4 | Automated Data-Quality Validation Checks | High |
| US-5 | Data Quality Reporting and Threshold Management | Medium |