# E3: Database & Analytics

**Phase:** 1 | **Covers:** FR-5, FR-6, FR-9.3, FR-9.4

## Overview
This epic designs the PostgreSQL database, loads the transformed data with full history, and builds the analytics on top of it.

## What this epic covers
- **Database design:** `raw` and `analytics` schemas, keys, indexes, and the ERD
- **Star schema:** `fact_daily_prices` with `dim_symbol` and `dim_date`
- **Loading:** upserts so reruns never create duplicates, history never deleted, transactional loads
- **Analytical SQL:** window functions and CTEs (top gainers/losers, monthly volume, rolling returns, best/worst days)
- **Views:** aggregations saved as views or materialized views, refreshed after each load
- **Visualization:** time-series chart (price with moving averages) reading from the analytics layer

## Depends on
E2 (transformed data to load).