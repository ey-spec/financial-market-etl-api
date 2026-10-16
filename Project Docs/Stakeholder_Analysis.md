# Stakeholder Analysis

**Project:** Financial Market Data Pipeline → ETL API Platform

## 1. Purpose

This document identifies the people and groups who affect, or are affected by, the two-phase data engineering project: an automated financial market ETL pipeline (Phase 1) and a containerized, monitored, API-accessible ETL platform (Phase 2). It records what each stakeholder expects, how much influence they hold, and how the team will engage with them. The results feed directly into the Task Assignment & Roles and Risk Assessment sections of the project plan.

## 2. Method

The analysis follows four steps:

1. **Identify** everyone who influences or is affected by the project.
2. **Analyze** each stakeholder's interest, expectations, and influence.
3. **Classify** stakeholders on a Power/Interest grid.
4. **Plan engagement** by defining how, and how often, the team communicates with each group.

## 3. Stakeholder Register

| # | Stakeholder | Type | Interest / Expectation | Influence | Interest | Engagement Strategy |
| --- | --- | --- | --- | --- | --- | --- |
| S1 | Instructor / evaluators | Internal | All Must requirements met, working end-to-end demo, clear documentation | High | High | Manage closely: milestone reviews and demo rehearsal |
| S2 | Program coordinator | Internal | On-time delivery that follows the project brief | High | Medium | Keep satisfied: submit each deliverable on schedule |
| S3 | Project lead | Internal | Plan on track, scope under control, risks managed | High | High | Weekly check-ins; owns the risk log |
| S4 | Team members (Data, Database, Orchestration, Backend, DevOps, Documentation) | Internal | Clear roles, fair workload, smooth integration between modules | Medium | High | Keep informed and involved: shared task board and Git repository |
| S5 | Data providers (Twelve Data, Finnhub) | External | Fair use of the free tier; respect for rate limits and terms of service | High | Low | Monitor: throttling, retries with backoff, raw data stored as fallback |
| S6 | Data analysts (consumers of the analytics layer) | End users | Accurate, deduplicated, documented data and ready-made views | Low | High | Keep informed: data-quality reports, ERD, example SQL queries |
| S7 | Pipeline operators / maintainers | End users | Clear logs, useful alerts, simple setup and deployment | Medium | High | Consult: README, API docs, Grafana dashboards |
| S8 | Future reviewers (portfolio audience, recruiters) | External | Clean repository, working CI/CD, readable architecture | Low | Medium | Monitor: polish README and architecture diagram |

## 4. Power/Interest Grid

|  | **Low Interest** | **High Interest** |
| --- | --- | --- |
| **High Power** | **Keep Satisfied** – Program coordinator (S2), Data providers (S5) | **Manage Closely** – Instructor / evaluators (S1), Project lead (S3) |
| **Low Power** | **Monitor** – Future reviewers (S8) | **Keep Informed** – Team members (S4), Data analysts (S6), Pipeline operators (S7) |

Operators (S7) have medium influence and are placed in Keep Informed because their needs are met mainly through documentation and dashboards.

## 5. Engagement and Communication Plan

| Stakeholder | Method | Frequency | Owner | Content |
| --- | --- | --- | --- | --- |
| Instructor / evaluators | Milestone review, demo | At each milestone (M1–M5) | Project lead | Progress against requirements, demo of working features |
| Program coordinator | Formal submission | Per deliverable deadline | Project lead | Completed documents and deliverables |
| Project lead | Check-in meeting | Weekly | Project lead | Status, blockers, risk log updates |
| Team members | Task board, repository, stand-up | Weekly, ongoing | Project lead | Task status, integration points, code reviews |
| Data providers | Usage tracking and monitoring | Every pipeline run | Data engineer | API call counts, rate-limit and error handling |
| Data analysts | Documentation | Per release | Database engineer | Data-quality report, ERD, SQL query examples |
| Pipeline operators | README, API docs, Grafana | Continuous | DevOps engineer | Setup guide, run status, failure alerts |
| Future reviewers | Repository and README | At final milestone | Documentation lead | Architecture diagram, usage guide |

## 6. Link to Project Risks

The stakeholder analysis explains several risks in the Risk Assessment:

| Stakeholder | Related risk | Reason |
| --- | --- | --- |
| Data providers (S5) | API rate limits or downtime (High impact) | They hold high power over data access but have no stake in the project; the free tier allows only 8 requests per minute (Twelve Data) or 60 per minute (Finnhub). |
| Team members (S4) | Team availability (Medium impact) | Delivery depends on every role being filled and active. |
| Instructor / evaluators (S1) | Scope creep (High impact) | Must requirements are fixed, so Phase 1 is completed before Phase 2. |
| Pipeline operators (S7) | Credentials exposed in code; deployment failures | Operators rely on secure configuration and tested releases. |
| Data analysts (S6) | Inconsistent data; duplicate records | Their trust depends on validation, upserts, and data-quality checks. |

## 7. Review

The register will be reviewed at each milestone (M1–M5). Stakeholders are added, removed, or reclassified when their influence or expectations change, for example when a different data provider is chosen or when roles move from TBD to named team members.
