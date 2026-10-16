# E5: ETL REST API

**Phase:** 2 | **Covers:** FR-10, FR-11, FR-12, FR-13, FR-14

## Overview
This epic turns the working Phase 1 pipeline into a service that other applications can control through a secured REST API.

## Goal
Let a client trigger the pipeline, check its status, and get results, without touching the scripts.

## What this epic covers
- **Refactor:** split the scripts into modules (extract, validate, transform, load, quality, config), with configuration kept out of the code
- **Endpoints:** trigger pipeline, get run status, get latest successful run, get pipeline statistics, health check
- **Airflow integration:** the API triggers the DAG and reads its run state, and API run IDs link to Airflow run IDs
- **Security:** basic authentication on all endpoints except health
- **Error handling and logging:** consistent responses and status codes, parameter validation, global exception handling, structured logs
- **Safety rules:** reject a trigger while a run is active
- 
## Depends on
E4 (the DAG the API controls).