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