# 🚖 NYC Taxi Data Lake — Serverless Pipeline on AWS

> An end-to-end serverless data lake built on AWS S3, Glue, and Athena.
> Ingests raw NYC Taxi trip data, transforms it into a query-optimized analytics layer, and serves business insights through SQL.

---

## 📖 About the project

This project builds a **production-style serverless data lake** for the New York City Taxi & Limousine Commission (TLC) trip dataset. It demonstrates the core patterns of modern AWS-native data engineering — separating raw, cleaned, and analytics layers in S3, using Glue for schema management and transformation, and Athena for SQL-based analysis.

The architecture follows the **medallion pattern** (bronze → silver → gold), a widely adopted approach in data lake design where data progressively moves from raw ingestion through cleaned tables to business-ready analytics tables.

This was built as the capstone project of a self-directed learning journey through AWS data services. The `docs/` folder contains the full learning history — concept reports, Q&A logs, and supporting material — kept separate from the production pipeline code.

---

## 🎯 What this project demonstrates

- Designing a **layered data lake** (raw / clean / analytics) in S3
- Cataloging unstructured S3 data with **AWS Glue Crawlers**
- Transforming data with **Glue ETL jobs** (PySpark)
- Partitioning strategies for **query performance and cost optimization**
- Converting CSV → **Parquet** for 5–10× query efficiency
- Writing **cost-aware SQL** in Athena
- Infrastructure organized for repeatability and version control

---

## 🏗️ Architecture

```
                    ┌────────────────────────────────────────────────────┐
                    │              NYC TLC Public Dataset                │
                    └────────────────────────┬───────────────────────────┘
                                             │ ingest (CSV / Parquet)
                                             ▼
              ┌──────────────────────────────────────────────────────────┐
              │  S3 RAW ZONE                                             │
              │  s3://<bucket>/raw/yellow_tripdata/year=YYYY/month=MM/   │
              └──────────────────────────────┬───────────────────────────┘
                                             │ Glue Crawler → schema registered
                                             ▼
              ┌──────────────────────────────────────────────────────────┐
              │  GLUE DATA CATALOG  (database: nyc_taxi_raw)             │
              └──────────────────────────────┬───────────────────────────┘
                                             │ Glue ETL job (PySpark)
                                             │ clean • cast types • drop nulls
                                             │ partition by year/month
                                             ▼
              ┌──────────────────────────────────────────────────────────┐
              │  S3 CLEAN ZONE  (Parquet, partitioned)                   │
              │  s3://<bucket>/clean/yellow_tripdata/year=YYYY/month=MM/ │
              └──────────────────────────────┬───────────────────────────┘
                                             │ Glue Crawler → schema registered
                                             ▼
              ┌──────────────────────────────────────────────────────────┐
              │  GLUE DATA CATALOG  (database: nyc_taxi_clean)           │
              └──────────────────────────────┬───────────────────────────┘
                                             │ Athena CTAS queries
                                             │ aggregate to business tables
                                             ▼
              ┌──────────────────────────────────────────────────────────┐
              │  S3 ANALYTICS ZONE  (aggregates as Parquet)              │
              │  s3://<bucket>/analytics/<metric>/                       │
              └──────────────────────────────┬───────────────────────────┘
                                             │
                                             ▼
                              ┌────────────────────────────┐
                              │   Athena (SQL queries)     │
                              │   → business insights      │
                              └────────────────────────────┘
```

---

## 🧱 The three zones

| Zone | Purpose | Format | Partitioned by |
|---|---|---|---|
| **Raw** (bronze) | Untouched source data — full fidelity for replay | Original (CSV / Parquet) | Source-defined |
| **Clean** (silver) | Validated, typed, deduplicated | Parquet + Snappy | year, month |
| **Analytics** (gold) | Business-aggregated tables for fast querying | Parquet + Snappy | metric-specific |

**Why three zones?** If a transformation has a bug, you replay from raw — you never lose source data. The clean layer is the trustworthy "single source of truth." The analytics layer is what dashboards and analysts hit, optimized for speed.

---

## 📊 Business questions answered

The analytics layer is designed to answer questions like:

- What are the top 10 pickup zones by total revenue per month?
- How does average tip percentage vary by hour of day and day of week?
- What's the daily trip volume trend, week over week?
- Which routes (pickup → dropoff zone pairs) have the highest average fare?
- How has rideshare volume changed year over year?

---

## 🗂️ Repository structure

```
nyc-taxi-data-lake/
│
├── README.md                          ← you are here
├── .gitignore
├── LICENSE
│
├── docs/                              ← LEARNING ARTIFACTS (separate from pipeline)
│   ├── README.md                      ← guide to the docs folder
│   ├── reports/                       ← progress reports from learning phase
│   │   └── aws_data_lake_tutorial_report.md
│   ├── concepts/                      ← concept notes, Q&A logs, cheat sheets
│   │   ├── 01_data_lakes_vs_warehouses.md
│   │   ├── 02_schema_explained.md
│   │   ├── 03_serverless_explained.md
│   │   ├── 04_glue_catalog_vs_etl.md
│   │   └── ...
│   ├── pdfs/                          ← reference PDFs and saved AWS docs
│   ├── screenshots/                   ← console walkthroughs from learning
│   └── scratch/                       ← experimental queries and throwaway code
│
├── infrastructure/                    ← AWS resource definitions
│   ├── iam/
│   │   ├── glue_service_role.json
│   │   └── README.md
│   ├── s3/
│   │   └── bucket_setup.sh            ← CLI commands to create the data lake buckets
│   └── athena/
│       └── workgroup_setup.sh
│
├── ingestion/                         ← scripts that pull raw data into S3
│   ├── download_nyc_taxi.py           ← downloads source files
│   ├── upload_to_raw_zone.sh
│   └── README.md
│
├── glue_jobs/                         ← Glue ETL job source code
│   ├── raw_to_clean/
│   │   ├── job.py                     ← PySpark transformation logic
│   │   ├── job_config.json            ← Glue job parameters
│   │   └── README.md
│   └── clean_to_analytics/
│       ├── top_zones_by_revenue.py
│       ├── tip_patterns_by_hour.py
│       └── README.md
│
├── crawlers/                          ← Glue Crawler configurations
│   ├── raw_crawler_config.json
│   ├── clean_crawler_config.json
│   └── README.md
│
├── athena_queries/                    ← reusable SQL queries
│   ├── exploratory/                   ← ad-hoc queries from analysis
│   ├── analytics/                     ← production queries
│   │   ├── top_zones_by_revenue.sql
│   │   ├── tip_patterns.sql
│   │   └── daily_trip_volume.sql
│   └── ddl/                           ← table definitions
│       ├── create_database.sql
│       └── partition_projection_examples.sql
│
├── scripts/                           ← helper scripts
│   ├── cleanup_resources.sh           ← tear down to avoid AWS charges
│   ├── deploy.sh                      ← one-shot deploy of the whole stack
│   └── run_pipeline.sh                ← trigger a full pipeline run
│
└── tests/                             ← validation queries and data quality checks
    ├── data_quality/
    │   ├── check_nulls.sql
    │   └── check_row_counts.sql
    └── README.md
```

### Why this layout

- **`docs/` is deliberately quarantined** from the pipeline code. Learning material, screenshots, and personal notes live there. The rest of the repo is what would ship to production.
- **Each pipeline stage gets its own folder** (`ingestion/`, `glue_jobs/`, `crawlers/`, `athena_queries/`) so the dataflow is mirrored by the directory layout.
- **Infrastructure is split out** so you can later migrate to Terraform or CloudFormation without restructuring.
- **Tests live separately** because data quality should be a first-class concern, not buried inside transformation code.

---

## 🚀 Getting started

> ⚠️ **Status: under construction.** This README documents the planned project. Implementation begins after the learning phase is complete. See `docs/reports/` for current progress.

### Prerequisites

- AWS account with an IAM user (not root) having access to S3, Glue, Athena, IAM, CloudWatch
- AWS CLI v2 configured locally
- Python 3.9+ (for ingestion scripts)
- ~$2 of expected AWS spend for a full pipeline run on a single month of NYC Taxi data

### Once implementation is complete

```bash
# 1. Clone
git clone <repo-url>
cd nyc-taxi-data-lake

# 2. Configure
cp .env.example .env
# edit .env with your AWS account ID and bucket name

# 3. Deploy infrastructure
./scripts/deploy.sh

# 4. Run the pipeline end-to-end
./scripts/run_pipeline.sh

# 5. Query the results
aws athena start-query-execution \
  --query-string "$(cat athena_queries/analytics/top_zones_by_revenue.sql)" \
  --work-group nyc_taxi_workgroup

# 6. When done — tear down to stop charges
./scripts/cleanup_resources.sh
```

---

## 💰 Cost expectations

Designed to stay well within hobbyist budgets:

| Service | Estimated cost (1 month of taxi data) |
|---|---|
| S3 storage | < $0.50 |
| Glue Crawler runs | < $0.10 |
| Glue ETL job | $0.50 – $1.00 per run |
| Athena queries | < $0.10 (after Parquet conversion) |
| **Total per full pipeline run** | **~$1.50** |

The `scripts/cleanup_resources.sh` script removes all created resources to prevent residual charges.

---

## 🎓 What I learned building this

Tracked in detail under `docs/concepts/`. Highlights:

- **Schema-on-read** is what makes data lakes flexible — and the Glue Catalog is what makes it queryable.
- **Partitioning is the single biggest lever** for Athena cost. A poorly partitioned query can scan terabytes; a well-partitioned one scans gigabytes.
- **Parquet vs. CSV** is not a small optimization — it's typically 5–10× faster and cheaper for analytical workloads.
- **Serverless != free** — but it does mean you only pay for what you use, with no idle infrastructure.
- **Decoupling storage from compute** is the architectural insight that makes the whole stack work. Same files, queryable by Athena today, Spark tomorrow, anything else next year.

---

## 🛠️ Tech stack

- **Storage** — Amazon S3
- **Metadata** — AWS Glue Data Catalog
- **ETL** — AWS Glue (PySpark)
- **Query engine** — Amazon Athena (Presto/Trino)
- **Orchestration** — bash scripts (could be upgraded to Step Functions or Airflow)
- **Source data** — [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

---

## 🗺️ Roadmap

- [ ] Phase 1 — Learning phase (in progress, tracked in `docs/`)
- [ ] Phase 2 — Build raw ingestion + first Glue crawler
- [ ] Phase 3 — Build clean-zone ETL job (Parquet, partitioned)
- [ ] Phase 4 — Build analytics-zone aggregates
- [ ] Phase 5 — Add data quality tests
- [ ] Phase 6 — Migrate to Terraform for IaC
- [ ] Phase 7 — Add orchestration (Step Functions or Airflow)
- [ ] Phase 8 — Add dbt models on top of the analytics layer
- [ ] Phase 9 — Connect a BI tool (QuickSight or Metabase) for dashboards

---

## 📄 License

MIT — see `LICENSE` file.

---

## 🙋 Author

Built as a hands-on learning project to develop production-grade data engineering skills on AWS. Comments, suggestions, and pull requests welcome.
