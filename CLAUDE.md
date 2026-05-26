# NYC Taxi Data Lake — Claude Code Context

## What this project is
A learning-grade serverless data lake on AWS: S3 + Glue + Athena.
Pedagogical, not production. Cost-conscious throughout.

## My AWS setup
- Region: eu-west-1 (everything stays here)
- IAM user: Marco (already configured in ~/.aws/credentials)
- Bucket: marco-data-darya
- Glue databases: nyc_taxi_raw, nyc_taxi_clean
- Glue service role: AWSGlueServiceRole-DataLakeTutorial

## Naming conventions
- Glue databases: snake_case, lowercase, no hyphens
- Crawlers / jobs: kebab-case (hyphens OK)
- S3 zones: raw/, clean/, analytics/
- Hive-style partitions: pickup_year=YYYY/pickup_month=MM (not year/month)

## Layout I follow
ingestion/        - scripts to download/upload raw data
glue_jobs/        - PySpark scripts for ETL
crawlers/         - crawler definitions (JSON for the AWS CLI)
athena_queries/   - SQL files, organized by zone
infra/            - IAM, S3, future Terraform/CDK
scripts/          - utility shell scripts (deploy, cleanup)
tests/            - data quality checks
docs/             - learning notes and reports

## How to work with me
- Theory-first explanations before code, with the data-engineer's mindset
  (cost, scale, schema, partitioning).
- Always show the AWS CLI equivalent for anything done via console.
- Always flag cost implications when running Glue jobs or scanning S3.
- Never delete S3 data or Catalog tables without explicit confirmation.
- After making changes, list what was changed.

## What's already done
- Crawler nyc-taxi-raw-yellow-crawler exists, produced raw_yellow_tripdata
- ETL job nyc-yellow-clean-etl-script exists, writes clean_yellow_tripdata
- Both tables registered in the Glue Catalog