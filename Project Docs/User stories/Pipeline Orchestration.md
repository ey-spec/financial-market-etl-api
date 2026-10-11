# E4: Pipeline Orchestration

**Phase:** 1 | **Covers:** FR-8, FR-9.1, FR-9.2

## Overview
This epic automates the whole Phase 1 workflow so it runs on a schedule without manual work, and records what happened in each run.

## What this epic covers
- **DAG:** extract → validate → save raw → transform → load → quality checks → refresh views
- **Scheduling:** once per trading day after market close, configurable
- **Retries:** task-level retries and delays
- **Backfills:** rerun past date ranges
- **Parallelism:** one task per symbol batch for isolated retries
- **Run logging:** every run recorded (times, status, symbols, rows extracted, loaded, rejected, errors) in a `pipeline_runs` table

## Depends on
E1, E2, E3. **Completing this epic finishes Phase 1.**