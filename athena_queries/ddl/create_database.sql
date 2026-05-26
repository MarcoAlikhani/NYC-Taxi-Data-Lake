-- create_database.sql
-- Glue Catalog databases for the data lake. In Athena, "database" == Glue
-- Catalog database -- a namespace for tables. Creating it is free; it's just
-- a metadata container, no storage is provisioned.
--
-- These can be created either via Athena DDL (below) or via the AWS CLI
-- (`aws glue create-database --database-input file://database.json`). This
-- project originally created nyc_taxi_raw via the CLI; the DDL here is the
-- equivalent and is kept as the source of truth.

-- Bronze: raw, untouched source data (schema inferred by the crawler).
CREATE DATABASE IF NOT EXISTS nyc_taxi_raw
COMMENT 'Raw NYC TLC trip data, schema inferred from S3';

-- Silver: cleaned, typed, partitioned data (created in Phase 2).
CREATE DATABASE IF NOT EXISTS nyc_taxi_clean
COMMENT 'Validated, typed, partitioned NYC TLC trip data';
