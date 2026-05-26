# Phase 1 Report — Raw Ingestion + First Glue Crawler

**Project:** NYC Taxi Data Lake (serverless: S3 + Glue + Athena)
**Phase:** 1 of 8 — *Build raw ingestion + first Glue crawler*
**Date:** 2026-05-26
**Region:** `eu-west-1` · **Account:** `055622654653` · **IAM user:** `Marco`
**Status:** ✅ Complete and verified

---

## 1. Objective

Stand up the **bronze (raw) layer** of the medallion architecture and make it
queryable. Concretely, Phase 1 had to:

1. Provision the data lake's S3 bucket with raw / clean / analytics zones.
2. Land a month of NYC TLC yellow-taxi data, untouched, in the raw zone.
3. Run a Glue Crawler to infer the schema and register a table in the Glue Data
   Catalog (database `nyc_taxi_raw`).
4. Confirm the table is queryable from Athena.

Everything downstream (clean ETL, analytics aggregates) depends on this layer
being a faithful, replayable copy of the source.

---

## 2. Architecture slice delivered

```
NYC TLC CloudFront
      │  download_nyc_taxi.py
      ▼
  ./data/yellow_tripdata_2024-01.parquet
      │  upload_to_raw_zone.sh
      ▼
  s3://marco-data-darya/raw/yellow_tripdata/        ← RAW ZONE (bronze)
      │  Glue Crawler  (nyc-taxi-raw-yellow-crawler-cli)
      ▼
  Glue Data Catalog → nyc_taxi_raw.raw_yellow_tripdata
      │  Athena (Presto/Trino)
      ▼
  SELECT ... ✅ queryable
```

---

## 3. What was built (repository artifacts)

| Path | Purpose |
|---|---|
| `infrastructure/s3/bucket_setup.sh` | Create the bucket, block public access, enable SSE-S3, lay down zone markers |
| `infrastructure/iam/glue_service_role.json` | Trust policy letting Glue assume the service role |
| `infrastructure/iam/README.md` | How the Glue role is built; least-privilege bucket policy |
| `ingestion/download_nyc_taxi.py` | Fetch monthly TLC Parquet to a local folder |
| `ingestion/upload_to_raw_zone.sh` | Copy local files into the S3 raw zone |
| `ingestion/README.md` | Why download/upload are decoupled; raw-zone contents |
| `crawlers/raw_crawler_config.json` | Declarative crawler definition (source of truth for the live crawler) |
| `crawlers/README.md` | Crawler concepts, create/run commands, cost note |
| `athena_queries/ddl/create_database.sql` | Catalog database DDL (`nyc_taxi_raw`, `nyc_taxi_clean`) |
| `athena_queries/exploratory/raw_sanity_checks.sql` | Read-only checks to validate the raw table |
| `.gitignore` | Keeps data (`*.parquet`), `venv/`, and secrets out of git |
| `LICENSE` | MIT |

---

## 4. AWS resources provisioned

| Resource | Identifier |
|---|---|
| S3 bucket | `marco-data-darya` (eu-west-1) |
| Raw zone object | `raw/yellow_tripdata/yellow_tripdata_2024-01.parquet` (47.6 MiB) |
| Glue Catalog database | `nyc_taxi_raw` |
| Glue service role | `AWSGlueServiceRole-DataLakeTutorial` |
| Glue crawler | `nyc-taxi-raw-yellow-crawler-cli` |
| Catalog table | `nyc_taxi_raw.raw_yellow_tripdata` (19 columns, Parquet, unpartitioned) |

### Inferred schema (19 columns)

`vendorid` int · `tpep_pickup_datetime` timestamp · `tpep_dropoff_datetime` timestamp ·
`passenger_count` bigint · `trip_distance` double · `ratecodeid` bigint ·
`store_and_fwd_flag` string · `pulocationid` int · `dolocationid` int ·
`payment_type` bigint · `fare_amount` double · `extra` double · `mta_tax` double ·
`tip_amount` double · `tolls_amount` double · `improvement_surcharge` double ·
`total_amount` double · `congestion_surcharge` double · `airport_fee` double

---

## 5. Execution & verification

**Crawler run** — `aws glue start-crawler --name nyc-taxi-raw-yellow-crawler-cli`
→ completed with `LastCrawl.Status = SUCCEEDED`, table registered.

**Athena verification** — confirmed the table resolves and returns data:

```sql
SELECT count(*) AS trip_count,
       min(tpep_pickup_datetime) AS earliest,
       max(tpep_pickup_datetime) AS latest
FROM nyc_taxi_raw.raw_yellow_tripdata;
```

| Metric | Value |
|---|---|
| **trip_count** | **2,964,624** |
| earliest pickup | `2002-12-31 22:59:39` |
| latest pickup | `2024-02-01 00:01:15` |
| data scanned | 12.8 MB |
| engine time | 1.37 s |

---

## 6. Data-quality observations (feed Phase 2)

The raw layer is intentionally left dirty — the date range alone already exposes
two classic TLC artifacts that the clean layer must handle:

- **Out-of-range timestamps.** A pickup dated `2002-12-31` is clearly bogus; TLC
  files routinely contain a handful of garbage timestamps.
- **Month spillover.** The latest pickup is `2024-02-01`, so a January file
  carries a few trips that cross midnight into February.

These are *expected* and are exactly why the bronze layer exists: we keep the
flawed source intact and fix it downstream, rather than mutating the source. The
Phase 2 clean job will filter to the valid month window, drop impossible rows
(zero passengers, non-positive distance, negative fares), cast types, and
partition by `pickup_year` / `pickup_month`.

---

## 7. Cost

| Item | Actual |
|---|---|
| S3 storage (47.6 MiB) | < $0.01 / month |
| Crawler run (2 DPU, 10-min min) | ~$0.07–0.15 |
| Athena verification query (12.8 MB scanned) | ~$0.00006 |
| **Phase 1 total** | **≈ $0.15** |

Comfortably inside the project's ~$1.50/full-run budget. No idle/standing cost
remains — S3 storage is the only ongoing charge.

---

## 8. Design decisions & rationale

- **One bucket, prefix-based zones** (`raw/`, `clean/`, `analytics/`) instead of
  three buckets — simpler IAM, lifecycle, and policy management at this scale.
- **Schema-on-read via the crawler** — files stay byte-for-byte as published;
  the schema is metadata layered on top, so the same files stay queryable by
  Athena today and Spark tomorrow.
- **`raw_` table prefix** — keeps each zone's tables visually distinct in the
  catalog (`raw_yellow_tripdata` vs the future `clean_yellow_tripdata`).
- **`DEPRECATE_IN_DATABASE` on schema delete** — a vanished column is flagged,
  not dropped, which is the safer behaviour for a replayable source layer.
- **Least-privilege Glue role** — scoped to the one bucket, not `S3FullAccess`.
- **Source is already Parquet**, so there's no CSV→Parquet conversion at
  ingestion; that efficiency win is inherited, and Phase 2's value is typing,
  validation, and partitioning rather than format conversion.

---

## 9. Reproduce from scratch

```bash
# 1. Bucket + zones
bash infrastructure/s3/bucket_setup.sh

# 2. Catalog database (or run create_database.sql in Athena)
aws glue create-database --region eu-west-1 \
  --database-input '{"Name":"nyc_taxi_raw","Description":"Raw NYC TLC trip data, schema inferred from S3"}'

# 3. Ingest one month
python ingestion/download_nyc_taxi.py 2024-01
bash ingestion/upload_to_raw_zone.sh ./data/yellow_tripdata_2024-01.parquet

# 4. Create + run the crawler
aws glue create-crawler --region eu-west-1 --cli-input-json file://crawlers/raw_crawler_config.json
aws glue start-crawler --name nyc-taxi-raw-yellow-crawler-cli --region eu-west-1

# 5. Verify in Athena
#    -> run athena_queries/exploratory/raw_sanity_checks.sql
```

---

## 10. Next — Phase 2

Build the **clean-zone ETL job** (Glue PySpark): read `raw_yellow_tripdata`,
drop/repair the dirty rows surfaced above, enforce types, partition by
`pickup_year` / `pickup_month`, and write Snappy-compressed Parquet to
`s3://marco-data-darya/clean/yellow_tripdata/`. Then crawl it into
`nyc_taxi_clean.clean_yellow_tripdata`.
