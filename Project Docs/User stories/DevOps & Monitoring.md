# E6: DevOps, Monitoring & Documentation

**Phase:** 2 | **Covers:** FR-15, FR-16, FR-17, FR-18

## Overview
This epic makes the platform production-ready: packaged, automatically tested and built, monitored, and fully documented.

## Goal
Run the whole stack with one command, ship changes safely through CI/CD, and see pipeline health on a dashboard.

## What this epic covers
- **Containerization:** a Dockerfile for the app, and Docker Compose for API, Airflow, PostgreSQL, Prometheus, and Grafana, with persistent volumes
- **Version control:** Git repository with a clear structure and secrets excluded
- **CI/CD:** GitHub Actions running lint and tests on every push, and building the Docker image on merge
- **Monitoring:** `/metrics` endpoint (run counts, durations, rows processed, API latency, last success time), Prometheus collection, and a Grafana dashboard
- **Documentation:** API docs (OpenAPI/Swagger), one unified architecture diagram, and a README covering setup and both phases

## Depends on
E5. **Completing this epic finishes the project.**