# Non-Functional Requirements (NFR)

**Purpose:** This section defines the quality attributes and operational constraints for the two-phase financial data engineering project. These requirements complement the Functional Requirements (FRs): they describe how well the system should operate, rather than the specific business functions it performs.

---

## 1. Priority Legend

**M** = Must (required for project acceptance) / **S** = Should (strongly recommended) / **O** = Optional (extension)

---

## 2. Performance and Efficiency

| ID | Requirement | Priority |
|---|---|---|
| NFR-2.1 | The pipeline should process the configured symbol list and requested historical date range within a documented target duration, under the selected API's rate limits. | S |
| NFR-2.2 | API calls, database operations, and transformations shall use timeouts or bounded execution settings where applicable, preventing tasks from hanging indefinitely. | M |
| NFR-2.3 | The API status and statistics endpoints should return responses within 2 seconds under normal local-demo load, excluding time spent waiting for an external pipeline run. | S |
| NFR-2.4 | Pandas transformations should avoid unnecessary repeated reads and copies of large datasets; batch processing may be used if the dataset grows beyond available memory. | S |

---

## 3. Reliability and Recovery

| ID | Requirement | Priority |
|---|---|---|
| NFR-3.1 | A transient external API failure shall not corrupt previously loaded analytical data. | M |
| NFR-3.2 | A failed database load shall roll back its transaction and preserve the previously committed state. | M |
| NFR-3.3 | The pipeline shall be safe to rerun for the same symbols and dates without creating duplicate analytical records. | M |
| NFR-3.4 | A failed task shall produce a clear error record and be eligible for configured retries; exhausted retries shall leave the run marked as failed. | M |
| NFR-3.5 | The system should be able to reprocess stored raw responses without calling the financial API again. | S |
| NFR-3.6 | The API shall reject overlapping pipeline triggers with HTTP 409 when a run is already active, if this behavior is implemented as planned. | S |

---

## 4. Data Integrity and Quality

| ID | Requirement | Priority |
|---|---|---|
| NFR-4.1 | Analytical records shall have a stable business key based on symbol and trading date, with database constraints preventing duplicates. | M |
| NFR-4.2 | All stored timestamps shall use a consistent timezone and documented representation; UTC shall be used for normalized timestamps. | M |
| NFR-4.3 | Price and volume fields shall use suitable numeric database types and documented precision; invalid values shall be rejected or flagged according to defined quality rules. | M |
| NFR-4.4 | Raw API responses shall remain unchanged in raw storage so that transformed results can be audited and reproduced. | M |
| NFR-4.5 | Data-quality checks and thresholds shall be documented, repeatable, and associated with the pipeline run that produced them. | M |
| NFR-4.6 | Financial calculations, including daily returns and rolling metrics, shall define their formulas, ordering, and treatment of missing trading days. | M |

---

## 5. Security and Secrets Management

| ID | Requirement | Priority |
|---|---|---|
| NFR-5.1 | API keys, database credentials, and API authentication credentials shall not be hardcoded or committed to version control. | M |
| NFR-5.2 | Secrets shall be supplied through environment variables or an approved local secret/configuration mechanism, with an example environment file containing placeholders only. | M |
| NFR-5.3 | Protected ETL API endpoints shall require authentication; credentials shall be compared/stored using an appropriate secure mechanism rather than exposed in source code. | M |
| NFR-5.4 | Logs and API error responses shall not expose passwords, API keys, tokens, or full connection strings. | M |
| NFR-5.5 | The application should validate API request parameters and restrict them to supported symbols, date ranges, and reasonable input sizes. | S |
| NFR-5.6 | Production deployment should use HTTPS and a secret-management mechanism appropriate to the hosting environment. | S |

---

## 6. Availability and Maintainability

| ID | Requirement | Priority |
|---|---|---|
| NFR-6.1 | The local development stack shall be startable using documented commands, with required dependencies and configuration clearly listed. | M |
| NFR-6.2 | The application shall separate extraction, validation, transformation, loading, quality checks, API routes, and configuration into maintainable modules. | M |
| NFR-6.3 | Configuration such as symbols, schedule, retry limits, and quality thresholds shall be changeable without editing business-logic code. | M |
| NFR-6.4 | The service should expose health information for the API and its key dependencies, including database and Airflow connectivity. | S |
| NFR-6.5 | The project should document supported Python and service versions and pin or constrain dependencies for reproducible setup. | S |

---

## 7. Scalability

| ID | Requirement | Priority |
|---|---|---|
| NFR-7.1 | The pipeline shall support a configurable list of symbols rather than requiring code changes for each new symbol. | M |
| NFR-7.2 | Extraction shall respect external API rate limits and use configurable batching so additional symbols can be added without uncontrolled request bursts. | M |
| NFR-7.3 | Database indexes shall support common queries filtered by symbol and trading date. | M |
| NFR-7.4 | The design should allow extraction batches or symbol groups to run independently where Airflow resources and API limits permit. | S |
| NFR-7.5 | The initial implementation may target a documented project/demo data volume; larger-scale throughput targets should be set only after the API, symbol count, and deployment environment are chosen. | S |

---

## 8. Observability and Auditability

| ID | Requirement | Priority |
|---|---|---|
| NFR-8.1 | Each pipeline run shall have a unique run ID that can be used to correlate ETL logs, API requests, Airflow runs, and database run records. | M |
| NFR-8.2 | Logs shall include timestamps, severity levels, run IDs, and actionable error context while avoiding secrets. | M |
| NFR-8.3 | The system shall retain run outcomes and key counters, including extracted, loaded, rejected, and failed records, for troubleshooting. | M |
| NFR-8.4 | Monitoring metrics shall use consistent names and units and distinguish successful runs, failed runs, duration, and row counts. | M |
| NFR-8.5 | The README should explain how to inspect logs, health endpoints, Prometheus metrics, and Grafana dashboards. | S |

---

## 9. Testability

| ID | Requirement | Priority |
|---|---|---|
| NFR-9.1 | Core transformation and validation logic shall be testable independently of live API calls and production infrastructure. | M |
| NFR-9.2 | Automated tests shall cover normal data, missing values, duplicates, malformed payloads, API errors, and rerun/idempotency behavior. | M |
| NFR-9.3 | The project should include integration tests for database loading and API-to-Airflow interaction using a test or local environment. | S |
| NFR-9.4 | CI shall run the configured automated test suite and lint checks on pushes and pull requests. | M |

---

## 10. Portability and Deployment

| ID | Requirement | Priority |
|---|---|---|
| NFR-10.1 | The application shall run in the documented local development environment and, in Phase 2, through Docker Compose with persistent data volumes. | M |
| NFR-10.2 | Environment-specific values shall be configurable without modifying application code or rebuilding source files. | M |
| NFR-10.3 | The container stack should use health checks and dependency readiness controls so services do not assume dependencies are ready merely because containers have started. | S |
| NFR-10.4 | The README shall provide clear setup, startup, configuration, test, and shutdown instructions. | M |

---

## 11. Documentation and Usability

| ID | Requirement | Priority |
|---|---|---|
| NFR-11.1 | A new team member shall be able to set up the project by following the README without relying on undocumented manual steps. | M |
| NFR-11.2 | The API shall provide OpenAPI/Swagger documentation with endpoint descriptions, authentication requirements, parameters, and example responses. | M |
| NFR-11.3 | Architecture diagrams, ERD, data-quality rules, and analytical metric definitions shall be kept consistent with the implemented system. | M |
| NFR-11.4 | Errors presented to API consumers shall be understandable and shall not expose internal stack traces. | M |

---

## 12. Backup and Retention

| ID | Requirement | Priority |
|---|---|---|
| NFR-12.1 | Raw market API responses and historical analytical records shall be retained according to the project's documented retention policy; the default project policy is to retain them for the life of the project. | M |
| NFR-12.2 | PostgreSQL data shall persist across container restarts through named or explicitly configured Docker volumes in Phase 2. | M |
| NFR-12.3 | The project should