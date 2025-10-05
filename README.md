# Batch ETL & Analytics Warehouse

**Theme**: Urban Mobility and Weather

**Goal**: Build a reliable, reproducible batch data platform that ingests raw public mobility data, models it into a star schema with dbt, validates quality with Great Expectations, and surfaces daily insights in Metabase. Designed for local dev via Docker Compose.

### 1) Domain & Datasets

#### Why Urban Mobility and Weather?

Rich, high-volume, time-partitionable data (great for Parquet + incremental models).
Realistic dimensions (zones, vendors, dates) and fact table (trips) with meaningful KPIs (utilization, demand, revenue).
Weather join adds causal/exogenous features and realistic data quality/keys challenges.

#### Primary sources

NYC TLC Trip Records (Yellow, Green, FHVs) — monthly Parquet files.
Hourly weather for NYC (NOAA/NWS or ERA5 proxy) — aggregated to hourly buckets for joins.

### 2) Architecture Overview

#### Local Dev Stack (Docker Compose):

Airflow (Scheduler, Webserver, Postgres metadata, Triggerer)

MinIO (object storage, S3-compatible) for bronze/staging file drops

Postgres (warehouse for silver/gold in dev)

Metabase (BI)

Prometheus scraping app metrics; Alertmanager placeholders

#### Flow

Ingest (Airflow DAG 1): Download raw TLC Parquet → store in MinIO under /bronze/trips/year=YYYY/month=MM/…. Load partition manifests.

Weather Ingest: Pull hourly weather → store as Parquet /bronze/weather/date=YYYY-MM-DD/….

dbt Transform (Airflow DAG 2):

Sources -> staging (silver) (type casting, dedup, basic constraints)

Build dim_date, dim_zone, dim_vendor; assemble fact_trips (incremental by pickup_date)

Build gold marts: demand_by_hour_zone, revenue_by_zone_day, pickup_dropoff_heatmap, service_level_kpis

Data Quality: GE suites (freshness, row count deltas, null %, accepted values); dbt tests on keys & relationships.

BI: Metabase models, metrics, and dashboard with filters (date range, zone, vendor).

#### Observability

Structured logs with structlog in Python tasks

Metrics via prometheus_client (task durations, row counts processed);

Airflow task SLAs; alerting placeholders (log to console + TODO: Pager duty/webhook).
