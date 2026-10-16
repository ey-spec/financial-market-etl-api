# E6: DevOps, Monitoring & Documentation

**Phase:** 2 | **Covers:** FR-15, FR-16, FR-17, FR-18 (+ NFR-5.1, NFR-5.2, NFR-6.1, NFR-6.3, NFR-6.5, NFR-8.1, NFR-9.1, NFR-9.4, NFR-11.1, NFR-11.3) *(FR and NFR numbers to be confirmed, see open questions)*

## Overview
This epic makes the platform production-ready: packaged, automatically tested and built, monitored, and fully documented. It adds no new business logic. It wraps E1 to E5 so the whole system runs with one command, every change is checked before it merges, and the health of the pipeline is visible on a dashboard.

## Goal
Run the whole stack with one command, ship changes safely through CI/CD, and see pipeline health on a dashboard.

## What this epic covers
- **Containerization:** one Dockerfile with an `api` target and an `airflow` target, and Docker Compose for PostgreSQL, Airflow, the API, Prometheus, and Grafana, with persistent volumes
- **Version control:** Git repository with a clear structure, protected `main`, and secrets excluded
- **CI:** GitHub Actions running lint, secret scan, and every test suite from E2 to E5 on each push and pull request
- **CD:** building, scanning, and publishing the Docker images after a merge to `main`
- **Monitoring:** a `/metrics` endpoint (run counts, durations, rows processed, API latency, last success time), Prometheus collection, and a Grafana dashboard
- **Alerting:** alert rules for failed runs, stale data, and exhausted API credits
- **Documentation:** API docs (OpenAPI/Swagger), one unified architecture diagram, a README covering setup and both phases, and an operations runbook
- **Final acceptance:** a full-stack smoke test that proves the project works as a whole

## Depends on
- **E1:** the shared configuration mechanism and `.env.example` convention (E1-US7)
- **E2, E3, E4, E5:** the test suites that CI runs (E2-US8, E3-US6, E4-US9, E5-US8), the migrations and roles (E3-US1), the DAG and pools (E4-US1, E4-US2), and the API with its OpenAPI contract (E5-US1, E5-US8)
- **E3:** `pipeline_runs`, `pipeline_run_symbols`, `api_usage`, and the `analytics_reader` role, which the metrics read

**Completing this epic finishes the project.**
Build order: E5 complete, then US1 to US3 (image, stack, repository), then US4 (CI), then US6 to US8 (metrics, dashboard, alerts), then US9 and US10 (documentation), and last US5 (CD) and US11 (smoke test).

## Decisions

### Rules

| Topic | Decision |
|---|---|
| Images | One Dockerfile with two targets: `api` and `airflow`. Both install the same ETL package, so CI tests exactly the code that runs in production. |
| Pinning | Base images, Python, Airflow (installed with Airflow's official constraints file), and all dependencies are pinned. No `latest` tags in Compose. |
| Image safety | Multi-stage build, non-root user, no secrets in any layer (`.dockerignore` excludes `.env`). |
| Services | `postgres`, `migrate` (one-shot), `airflow-init` (one-shot), the Airflow processes required by the pinned version (scheduler, DAG processor, API server/web UI), `api`, `prometheus`, `grafana`. Phase 1's standalone Airflow (E4-US1) is folded in here. |
| Startup order | Healthchecks plus `depends_on: condition: service_healthy`. `postgres` → `migrate` → `airflow-init` → Airflow processes and `api`. |
| Database layout | One PostgreSQL server (version 15 or newer, because E3 needs `NULLS NOT DISTINCT`) with two databases: project data and Airflow metadata (E4-US1). They have different owners. |
| Database roles | Airflow connection uses `etl_writer`. API reads use `analytics_reader`, `POST /runs` uses `api_operator` (E5-US2). Grafana's SQL datasource and the metrics collector use `analytics_reader`. The owner role is used only by `migrate`, and no running service uses it. |
| Network and ports | One internal network. Published ports bind to `127.0.0.1` by default. PostgreSQL is not published unless an override file does it. |
| Time zone | Every container runs in UTC, matching the UTC timestamps and the 22:00 UTC schedule (E2, E4). |
| Persistence | Named volumes for PostgreSQL, Airflow logs, Prometheus, and Grafana. `docker compose down` keeps data. `down -v` deletes it and is documented as destructive. |
| Configuration | Everything comes from `.env` (copied from `.env.example`). No secret in Compose files, images, dashboards, or Git (E1-US7). |
| Everything as code | Compose files, Prometheus config, alert rules, Grafana datasources and dashboards, and CI workflows all live in Git. Nothing is configured by hand in a UI. |
| CI | GitHub Actions on every push and pull request. `main` is protected and requires the CI checks to pass. |
| CD scope | A merge to `main` builds, scans, and publishes images to GitHub Container Registry. Deployment is `docker compose pull && up -d`. There is no cloud deploy target in scope. |
| Image tags | Immutable commit-SHA tags, a `main` tag that moves, and version tags (`vX.Y.Z`) on releases. |
| Metrics source | Pipeline metrics are computed at scrape time from `pipeline_runs`, `pipeline_run_symbols`, and `api_usage` (single source of truth, survive restarts). HTTP metrics are counted in the API process. |
| Label policy | Labels have low cardinality only (status, mode, provider, route template). Never a run ID, symbol, API key, or raw path. |
| Scrape security | `/metrics` requires a bearer token (`METRICS_TOKEN`) that only Prometheus holds. It is the third endpoint outside the API-key scheme, next to `/health` and `/ready`. |
| Documentation | Markdown in `docs/` plus the README. Diagrams are Mermaid source in Git plus an exported image. |

### Metric catalog

| Metric | Type | Source | Meaning |
|---|---|---|---|
| `pipeline_runs_total{status,mode,trigger_type}` | counter | `pipeline_runs` | Runs by outcome (never decreases, because runs are never deleted, NFR-8.3) |
| `pipeline_run_duration_seconds` (sum and count) | summary | `pipeline_runs` | Duration of finished runs (`ended_at − started_at`) |
| `pipeline_rows_total{kind}` | counter | `pipeline_runs` | `extracted`, `loaded`, `rejected`, `duplicates_removed`, `recalculated` (same names as E4) |
| `pipeline_last_success_timestamp_seconds` | gauge | `pipeline_runs` | `ended_at` of the latest run with `status = success` |
| `pipeline_last_run_status{status}` | gauge (one-hot) | `pipeline_runs` | Status of the latest finished run |
| `pipeline_last_quality_status{status}` | gauge (one-hot) | `pipeline_runs` | `pass`, `warn`, `fail`, `skipped` of the latest finished run |
| `pipeline_last_run_symbols{status}` | gauge | `pipeline_run_symbols` | Symbols per status in the latest finished run (`deferred`, `failed`, `quarantined`, and so on) |
| `pipeline_api_credits_used{provider}` / `pipeline_api_credits_limit{provider}` | gauge | `api_usage` and config | Today's (UTC) credits against the daily limit |
| `pipeline_metrics_db_up` | gauge | collector | 1 if the collector could read the database, else 0 |
| `http_requests_total{method,route,status}` | counter | API process | Requests by route template and status code |
| `http_request_duration_seconds{method,route}` | histogram | API process | API latency |

---

## User Stories

### E6-US1: Dockerfile and Container Images
- **As a** DevOps Engineer,
- **I want to** build the API and Airflow images from one Dockerfile,
- **So that** every environment runs the same code and the build is reproducible.
- **Priority:** High
- **Covers:** FR-15, NFR-6.1, NFR-6.5, NFR-5.2

**Acceptance Criteria:**
- Given a clean checkout, When `docker build` runs for the `api` and `airflow` targets, Then both build with no manual steps.
- Given the images, When they are inspected, Then base images, the Python version, Airflow, and all dependencies are pinned, and Airflow dependencies are installed with the official constraints file for the pinned version (E4-US1).
- Given the ETL package, When the `airflow` image starts, Then the DAG imports it with no error (E4-US1). When the `api` image starts, Then the API imports the same package version.
- Given the build, When it finishes, Then the final image contains no build tools, runs as a non-root user, and contains no `.env`, key, or password in any layer (checked with `docker history`).
- Given a container starts with a required setting missing, When configuration loads, Then it exits non-zero with a message naming the setting (E1-US7, E5-US1).
- Given the `api` image, When it runs, Then its container healthcheck calls `/health` (E5-US1).
- Given logs, When containers write them, Then they go to stdout/stderr, include the run ID and request ID where relevant, and never contain API keys or connection strings (E4-US7, E5-US1).
- Given the repository, When it is inspected, Then `.dockerignore` excludes `.env`, `.git`, tests data volumes, and caches.

**Notes:** Pattern: multi-stage build with shared layers. The ETL package is installed as a package (not copied ad hoc), so the code in production is the code CI tested.

---

### E6-US2: Docker Compose Full Stack
- **As a** DevOps Engineer,
- **I want to** start the whole platform with one command,
- **So that** any teammate or grader gets a working system in minutes.
- **Priority:** High
- **Covers:** FR-15, NFR-6.1, NFR-6.5, NFR-5.1, NFR-5.2

**Acceptance Criteria:**
- Given a clean machine with Docker, When `.env.example` is copied to `.env` and `docker compose up -d` runs, Then PostgreSQL, the Airflow services, the API, Prometheus, and Grafana start and become healthy, with the documented URLs reachable.
- Given the startup order, When services start, Then each waits for its dependencies to be healthy, and the one-shot `migrate` service runs the E3 migrations, creates the roles from environment passwords, and seeds `dim_date` (E3-US1, E3-US5) before Airflow or the API start.
- Given `airflow-init`, When it runs, Then it creates the admin user, the data-database connection (using `etl_writer`), and the pools `twelvedata_api` and `eodhd_api` with 1 slot each (E4-US1).
- Given the stack is started a second time, When `migrate` and `airflow-init` run again, Then there is no error and no change (idempotent).
- Given the databases, When they are inspected, Then Airflow's metadata database is separate from the project data database (E4-US1).
- Given the services' database access, When it is checked, Then Airflow uses `etl_writer`, the API uses `analytics_reader` and `api_operator`, and no running service uses the owner or a superuser role (E3-US1, E5-US2).
- Given `docker compose down` then `up`, When the stack returns, Then PostgreSQL data, run history, Prometheus data, and Grafana dashboards are still there.
- Given Compose files, When they are inspected, Then every image tag is pinned, every service has a healthcheck and a restart policy, and `docker compose config` shows no hardcoded secret.
- Given published ports, When the stack runs, Then they bind to `127.0.0.1` only, and PostgreSQL is not published by default.
- Given a developer override file, When it is used, Then it mounts the source code for live editing without changing the production Compose file.
- Given the Compose profile `test`, When it is enabled, Then a mock market-data provider serving fixtures starts, so the stack can run with no real API key (used by E6-US11).

**Notes:** Pattern: infrastructure as code with health-gated startup. E4-US1 ran Airflow alone for Phase 1, and this story folds it into the full stack.

---

### E6-US3: Repository Structure and Secrets Hygiene
- **As a** Team Member,
- **I want to** a clear Git repository with protected `main` and no secrets in it,
- **So that** the code is easy to navigate, changes are reviewed, and credentials never leak.
- **Priority:** High
- **Covers:** FR-16, NFR-5.1, NFR-5.2

**Acceptance Criteria:**
- Given the repository, When it is inspected, Then it has these top-level areas, documented in the README: the ETL package (config, ingestion, transform, load, API, metrics), `dags/`, `migrations/`, `sql/` (E3-US7 queries), `tests/` (unit, integration, e2e, api), `docker/` (Dockerfile, Compose, Prometheus, Grafana), `docs/`, and `.github/`.
- Given `.gitignore`, When it is inspected, Then it excludes `.env`, volumes, caches, generated charts, and local data. `.env.example` contains every variable used by E1 to E6 with placeholders and one-line comments.
- Given a CI check, When it compares the variables read by the configuration code with `.env.example`, Then a variable missing from the example fails the build.
- Given a secret scanner in CI, When a key, token, or password is committed, Then the build fails. The full history is scanned once at setup.
- Given `main`, When changes are made, Then they go through pull requests only, with the CI checks required and at least one approval.
- Given the pull request template, When a PR is opened, Then it has a checklist: tests added, migration and ERD updated together (E3-US10), OpenAPI contract regenerated (E5-US8), docs updated.
- Given fixtures, When they are committed, Then they are small, sanitized sample payloads with no keys, and no real raw data or large files are in Git.
- Given the migrations, When they are inspected, Then they are numbered SQL files in `migrations/`, applied by one documented command (E3-US1).

---

### E6-US4: Continuous Integration
- **As a** Team Member,
- **I want to** run lint and every test suite automatically on each push and pull request,
- **So that** broken code, schema drift, and contract changes are caught before merge.
- **Priority:** High
- **Covers:** FR-16, NFR-9.1, NFR-9.4, NFR-11.3

**Acceptance Criteria:**
- Given a push or pull request, When the workflow starts, Then these jobs run: lint and format check, secret scan, unit tests, integration tests, DAG and end-to-end tests, API contract check, and Docker build.
- Given the unit tests (E2-US8), When they run, Then they need no network and no database, and they cover the transform and quality logic.
- Given the integration tests, When they run, Then a PostgreSQL service container is built by the real migrations (E3-US1) and the suite covers: migrations run twice with no change, idempotent reload and mid-batch rollback (E3-US6), role permissions (`analytics_reader` cannot read `raw`, `etl_writer` cannot delete from protected tables), and the API tests (E5-US8).
- Given the DAG tests (E4-US9), When they run, Then the DAG parses without import errors and the end-to-end run uses a mocked provider and no real key.
- Given the OpenAPI document, When it is generated and compared with the stored copy, Then an unintended change fails the build (E5-US8).
- Given the ERD source (E3-US10), When a script compares its tables and columns with the migrated database, Then any mismatch fails the build.
- Given the Docker build job, When it runs on a pull request, Then it builds both images without pushing, and `docker compose config` validates the Compose files.
- Given CI, When it runs, Then it uses dummy test values, never a real API key or production secret, and makes no call to a real provider.
- Given the workflow files, When they are inspected, Then actions are pinned to a version or commit SHA and dependencies are cached to keep runs fast.
- Given any job fails, When the pull request is viewed, Then merge is blocked and the failing job's log shows the reason.
- Given test reports, When a run ends, Then they are uploaded as artifacts.

**Notes:** Pattern: CI as a quality gate. This story implements every "CI runs (E6)" criterion in E2-US8, E3-US6, E4-US9, and E5-US8.

---

### E6-US5: Continuous Delivery of Images
- **As a** Team Member,
- **I want to** build and publish images automatically after a merge to `main`,
- **So that** the exact tested version can be pulled and run anywhere.
- **Priority:** Medium
- **Covers:** FR-16

**Acceptance Criteria:**
- Given a merge to `main`, When CI has passed, Then the workflow builds the `api` and `airflow` images and pushes them to GitHub Container Registry.
- Given the pushed images, When they are inspected, Then each has an immutable commit-SHA tag and the moving `main` tag. A release tag `vX.Y.Z` also produces a version tag.
- Given a build, When it runs, Then it uses the repository's built-in token and no stored personal credential, and no secret is baked into the image.
- Given an image scan, When it finds a critical vulnerability, Then the publish step fails and the findings are shown in the log.
- Given the Compose file, When a specific version is wanted, Then the image tag is set from an environment variable, so `pull` and `up -d` deploy exactly that version.
- Given a failed build or scan, When the workflow ends, Then nothing is published and the previous images stay available.

---

### E6-US6: Metrics Endpoint
- **As a** DevOps Engineer,
- **I want to** a `/metrics` endpoint exposing pipeline and API metrics,
- **So that** Prometheus can collect them without reading Airflow or the database directly.
- **Priority:** High
- **Covers:** FR-17, NFR-8.1

**Acceptance Criteria:**
- Given a request with the correct bearer token, When `GET /metrics` is called, Then it returns the metrics in Prometheus text format, including every metric in the Metric catalog.
- Given a missing or wrong token, When `/metrics` is called, Then the response is 401. The token is stored hashed, compared in constant time, and never logged.
- Given pipeline metrics, When they are produced, Then they are computed at scrape time from `pipeline_runs`, `pipeline_run_symbols`, and `api_usage` through the `analytics_reader` role, with a short cache (default 15 s) so frequent scrapes do not load the database.
- Given the same data, When the API restarts, Then the pipeline metrics have the same values (they do not depend on in-process counters).
- Given HTTP metrics, When requests are served, Then they are labeled by route template (for example `/api/v1/runs/{run_id}`), never by raw path, run ID, symbol, or key.
- Given the database is unreachable, When `/metrics` is called, Then it still returns the HTTP metrics, reports `pipeline_metrics_db_up 0`, and does not return a 500.
- Given a finished run, When the next scrape happens, Then `pipeline_rows_total` and `pipeline_runs_total` reflect it, and the row kinds balance as in E4-US7.
- Given the API runs with several workers, When metrics are collected, Then they are correct (single worker, or Prometheus multiprocess mode).
- Given the OpenAPI document, When it is generated, Then `/metrics` is excluded from the public docs, and the endpoint catalog and E5-US2 list it as an exception to the API-key scheme.
- Given the metrics output, When it is inspected, Then it contains no secrets, no symbols, and no run IDs.

**Notes:** Pattern: pull-based metrics with a custom collector. Reading the run tables at scrape time keeps one source of truth, so the dashboard always agrees with `GET /runs`.

---

### E6-US7: Prometheus and Grafana Dashboard
- **As a** Data Engineer,
- **I want to** see pipeline health on a Grafana dashboard fed by Prometheus,
- **So that** I notice problems and trends without querying the database.
- **Priority:** High
- **Covers:** FR-17

**Acceptance Criteria:**
- Given the Compose stack, When it starts, Then Prometheus scrapes the API's `/metrics` every 15 seconds using the bearer token from configuration, and the target shows as up.
- Given Grafana starts, When it is opened, Then the Prometheus datasource and the dashboard are already provisioned from files in Git, with no manual setup. Anonymous access is off and the admin password comes from configuration.
- Given the dashboard, When it is viewed, Then it shows at least: time since the last successful run, latest run status and quality status, runs by status over time, run duration, rows extracted, loaded, and rejected, symbols per status in the latest run (deferred, failed, quarantined), credits used against the daily limit per provider, API request rate, p95 latency, and 5xx rate.
- Given a failed run, When the dashboard refreshes, Then the failure is visible within two scrape intervals.
- Given the project database, When the optional market panel is used, Then it reads a read-only SQL datasource with `analytics_reader` and shows the close price with the short and long moving averages for a symbol chosen from a dropdown filled from `dim_symbol`. It reads only the analytics layer (E3-US9).
- Given Prometheus, When it is configured, Then retention is set in configuration, its data is on a named volume, and its config and rules are in Git.
- Given the dashboard JSON, When it is inspected, Then it contains no secrets and no hardcoded hostnames other than service names.
- Given `docker compose down` then `up`, When the stack returns, Then history is still shown (volumes persist).

---

### E6-US8: Alerting Rules
- **As a** Data Engineer,
- **I want to** alert rules for failed runs, stale data, and exhausted credits,
- **So that** problems are found without watching the dashboard all day.
- **Priority:** Medium
- **Covers:** FR-17

**Acceptance Criteria:**
- Given the latest finished run has `status = failed`, When Prometheus evaluates the rules, Then a `critical` alert fires.
- Given the latest finished run has `quality_status = fail`, When evaluated, Then a `critical` alert fires, because the views were not refreshed (E4-US8).
- Given no successful run for longer than a configurable threshold, When evaluated, Then a `warning` alert fires. The default (80 hours) covers the weekend gap of the Monday to Friday schedule (E4-US3) and is tuned in configuration.
- Given credits used reach a configurable share of the daily limit (default 90%), When evaluated, Then a `warning` alert fires. Deferred symbols are shown on the dashboard but are not alerts by themselves (E1-US3).
- Given the API target is down or the 5xx rate is above a threshold, When evaluated, Then an alert fires.
- Given `quality_status = warn`, When evaluated, Then no alert fires (the run is a success, E2-US6).
- Given the rule files, When they are inspected, Then they are in Git, each has a severity and a one-line summary with the likely first action, and thresholds come from configuration.
- Given rules, When they are tested, Then each rule has a unit test with sample series (`promtool test rules`) that runs in CI.
- Given an alert fires, When someone looks, Then it is visible in Prometheus and Grafana. External delivery (email or chat) is optional and configured by environment variables.

---

### E6-US9: API Documentation and Architecture Diagram
- **As a** New Team Member or Grader,
- **I want to** complete API docs and one diagram of the whole system,
- **So that** I can understand and use the platform without reading all the code.
- **Priority:** High
- **Covers:** FR-18, NFR-11.1, NFR-11.3

**Acceptance Criteria:**
- Given the running API, When `/docs` is opened, Then every endpoint is documented with its parameters, example request and response, error shapes, and required role (E5-US1).
- Given the docs, When data endpoints are described, Then they state that returns are fractions (0.0123 = 1.23%), volumes are comparable only within one symbol, preliminary bars are excluded by default, and `null` metrics mean the window is not full (E2, E3-US7).
- Given the exported `openapi.json`, When it is inspected, Then it is stored in the repository and matches the running API (checked in CI, E6-US4).
- Given the architecture diagram, When it is viewed, Then one diagram shows both phases: provider APIs, E1 extraction, `raw` and quarantine, E2 transform, E3 star schema and views, Airflow orchestration, the REST API, Prometheus and Grafana, and the data flow between them.
- Given the run-trigger flow, When it is documented, Then a sequence diagram shows `POST /runs` → `queued` row → Airflow → `start_run` → batches → `finalize_run` (E4, E5-US3).
- Given the ERD (E3-US10), When the docs are viewed, Then it is linked from the architecture page.
- Given the diagrams, When they are stored, Then both the editable source (Mermaid) and an exported image are in Git, and a change to the architecture updates them in the same pull request.
- Given the metric definitions (E2) and run modes (incremental, backfill, reprocess), When the docs are read, Then each is explained once, in one place, and linked from the README.

---

### E6-US10: README and Operations Runbook
- **As a** New Team Member,
- **I want to** a README for setup and a runbook for operations,
- **So that** I can run the project from a fresh clone and handle problems without asking anyone.
- **Priority:** High
- **Covers:** FR-18, NFR-6.1

**Acceptance Criteria:**
- Given a fresh clone, When a person follows the README only, Then they get the full stack running, a first pipeline run triggered, data queried through the API, and the dashboard open, with no undocumented step.
- Given the README, When it is read, Then it contains: what the project is, both phases, the architecture diagram, prerequisites, the quick start (copy `.env`, one command), service URLs and ports, a configuration table, how to run the tests, how to trigger incremental, backfill, and reprocess runs (with `curl` examples), the repository structure, and a troubleshooting section.
- Given the quick start, When it uses secrets, Then it shows placeholders only and explains how to generate keys and tokens.
- Given the runbook, When a run is `failed`, Then it explains how to find the cause (run detail, symbol rows, Airflow log) and how to recover (rerun, or reprocess from raw).
- Given the runbook, When symbols are `deferred`, Then it explains the credit budget and that the next run takes them first. When symbols are `quarantined`, Then it explains how to inspect the stored response with SQL (the API never exposes `raw`) and how to fix and reprocess.
- Given the runbook, When keys or passwords must change, Then it describes rotation for API keys, the metrics token, role passwords, and the provider key.
- Given the runbook, When data protection is needed, Then it documents a PostgreSQL backup (`pg_dump`) and restore, and warns that `docker compose down -v` deletes all data.
- Given the docs, When they are inspected, Then they contain no real secret, and every command in them has been run at least once in CI or the smoke test.

---

### E6-US11: Full-Stack Smoke Test
- **As a** Team Member,
- **I want to** prove the whole stack works from a clean start,
- **So that** the project is verified as one system before it is delivered.
- **Priority:** Medium
- **Covers:** FR-15, FR-17, NFR-9.4, NFR-6.1

**Acceptance Criteria:**
- Given a clean environment, When the smoke test script runs, Then it starts Compose with the `test` profile (mock provider, no real key), waits until all services are healthy, and fails with a clear message if any is not.
- Given the stack is up, When the script calls `POST /runs` with an operator key, Then it receives `202`, polls `GET /runs/{run_id}` until the run ends, and expects `status = success`.
- Given the finished run, When the script reads the data, Then `GET /symbols/{symbol}/prices` and an analytics endpoint return the expected fixture values.
- Given the run, When the script reads `/metrics`, Then `pipeline_runs_total` shows the new run and `pipeline_last_success_timestamp_seconds` is recent.
- Given Prometheus and Grafana, When the script checks them, Then the API target is up and the provisioned dashboard loads.
- Given the same run is triggered again, When it ends, Then no duplicates exist (idempotency, E3-US6).
- Given a `reader` key, When the script calls `POST /runs`, Then the response is 403, and `raw` is not readable through any endpoint (E5-US2).
- Given the test ends, When it cleans up, Then it removes its containers and volumes, and never touches a developer's real volumes.
- Given merges to `main` and before a release, When the pipeline runs, Then this test runs and must pass.

---

## Remaining open questions
- Which FR numbers cover this epic? The header uses FR-15 to FR-18, but E4 mentions FR-12.3 for the Airflow link. Confirm the mapping against the Functional Requirements and update the Covers lines.
- Which Airflow major version is pinned? It decides which Airflow processes Compose starts (E4 open question).
- Is PostgreSQL 16 acceptable? Any version 15 or newer answers E3's version question.
- Is publishing to GitHub Container Registry enough, or is a real deployment target (a VM or cloud) needed?
- Should alerts also be delivered by email or chat, or only shown in Prometheus and Grafana?
- Should provider API latency (Twelve Data call duration) be a metric? It needs E1 to record the duration of each call. "API latency" is currently the latency of our own REST API.
- Is a bearer token for `/metrics` acceptable, with E5-US2 updated to list it?
- Should backups stay documented only, or be automated (a scheduled `pg_dump` service)?
- Is a minimum test-coverage threshold required in CI?
- Should Grafana's market panel read PostgreSQL directly (current decision) or go through the API?

---

## Summary Table

| ID | Title | Priority | Covers |
| :--- | :--- | :--- | :--- |
| E6-US1 | Dockerfile and Container Images | High | FR-15, NFR-6.1, NFR-6.5, NFR-5.2 |
| E6-US2 | Docker Compose Full Stack | High | FR-15, NFR-6.1, NFR-6.5, NFR-5.1, NFR-5.2 |
| E6-US3 | Repository Structure and Secrets Hygiene | High | FR-16, NFR-5.1, NFR-5.2 |
| E6-US4 | Continuous Integration | High | FR-16, NFR-9.1, NFR-9.4, NFR-11.3 |
| E6-US5 | Continuous Delivery of Images | Medium | FR-16 |
| E6-US6 | Metrics Endpoint | High | FR-17, NFR-8.1 |
| E6-US7 | Prometheus and Grafana Dashboard | High | FR-17 |
| E6-US8 | Alerting Rules | Medium | FR-17 |
| E6-US9 | API Documentation and Architecture Diagram | High | FR-18, NFR-11.1, NFR-11.3 |
| E6-US10 | README and Operations Runbook | High | FR-18, NFR-6.1 |
| E6-US11 | Full-Stack Smoke Test | Medium | FR-15, FR-17, NFR-9.4, NFR-6.1 |