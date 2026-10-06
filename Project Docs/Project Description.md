# Automated Financial Market Data Pipeline & Production-Ready ETL API Platform

## Problem Statement

A financial-data team needs to collect historical and regularly updated market data from an external API. Raw API responses are inconsistent and must be transformed into reliable analytical datasets for performance analysis.

Currently, the pipeline would only run as a local script. Other applications need a controlled way to trigger the pipeline, check its status, and retrieve its results.

## Project Objective

Build the project in two phases:

- **Phase 1 (Project 2):** Build a complete, scheduled ETL pipeline that retrieves financial data from an API, validates and transforms it, stores it in a database, and maintains historical records through incremental loading.
- **Phase 2 (Project 9):** Turn the finished pipeline into a containerized, API-accessible data service with orchestration, monitoring, and deployment automation.

Phase 2 starts only after Phase 1 is complete and working end to end.

---

## Phase 1: Automated Financial Market Data Pipeline

### Key Features / Scope

- Consume a financial REST API.
- Validate HTTP responses and API payloads.
- Parse JSON responses.
- Store raw data separately from transformed data.
- Clean and standardize timestamps.
- Handle missing and duplicate records.
- Implement incremental extraction.
- Store data in PostgreSQL/MySQL.
- Design appropriate tables and indexes.
- Use Pandas for transformations.
- Calculate:
  - Daily returns
  - Moving averages
  - Trading volume statistics
  - Basic time-series metrics
- Use SQL window functions and CTEs.
- Schedule the workflow through Airflow.
- Add retry logic for failed API calls.
- Log pipeline execution.

### Expected Deliverable

- Python ingestion and transformation scripts
- SQL database
- ERD
- Airflow DAG
- Data-quality report
- Analytical SQL queries
- Time-series visualization
- Architecture diagram
- README

### Optional Extension

Add a MongoDB collection for storing the original API JSON documents alongside the structured SQL representation.

---

## Phase 2: Production-Ready ETL API Platform

### Key Features / Scope

- Refactor the Phase 1 scripts into modular Python ETL components (extract, validate, transform, load).
- Store configuration separately from code (environment variables / config files).
- Create REST endpoints:
  - Trigger pipeline
  - Get pipeline status
  - Get latest successful run
  - Retrieve pipeline statistics
- Connect the API to Airflow (the API triggers and monitors the Phase 1 DAG).
- Implement basic authentication.
- Containerize the application using Docker (API, Airflow, database).
- Store code in Git.
- Create a CI/CD workflow with GitHub Actions.
- Add logging and error handling.
- Add Prometheus metrics.
- Create a Grafana monitoring dashboard.

### Expected Deliverable

- ETL application
- REST API
- Airflow integration
- Dockerfile
- GitHub repository
- CI/CD workflow
- Monitoring dashboard
- API documentation
- Architecture diagram

### Optional Extension

Use Terraform to provision the infrastructure required to run the application.

---

## Combined Final Deliverables

- Complete Phase 1 deliverables (pipeline, database, ERD, DAG, data-quality report, SQL queries, visualization)
- Complete Phase 2 deliverables (REST API, Docker setup, CI/CD, monitoring dashboard, API docs)
- One unified architecture diagram showing: Financial API → Airflow → Raw Storage → Transformation → PostgreSQL → ETL REST API → Prometheus → Grafana
- One unified README covering setup, usage, and both phases