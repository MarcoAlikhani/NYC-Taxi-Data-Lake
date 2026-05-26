# Crawlers — schema inference for the raw zone

A **Glue Crawler** connects to an S3 path, samples the files it finds, infers a
schema, and writes a table definition into the Glue Data Catalog. That catalog
table is what makes the raw files queryable by Athena — the files themselves
never move. This is **schema-on-read**: the data sits as-is in S3, and the
schema is metadata layered on top.

## `raw_crawler_config.json`

| Setting | Value | Why |
|---|---|---|
| Target | `s3://marco-data-darya/raw/yellow_tripdata/` | The raw-zone prefix to scan |
| Database | `nyc_taxi_raw` | Where the inferred table is registered |
| Table prefix | `raw_` | Yields table name `raw_yellow_tripdata` — keeps zones distinct in the catalog |
| Schema update | `UPDATE_IN_DATABASE` | New/changed columns update the table in place |
| Schema delete | `DEPRECATE_IN_DATABASE` | Vanished columns are marked deprecated, not dropped — safer for replay |
| Recrawl | `CRAWL_EVERYTHING` | Re-scan the whole prefix each run (fine at this data size) |

## Create / update the crawler from this config

```bash
aws glue create-crawler \
  --region eu-west-1 \
  --cli-input-json file://raw_crawler_config.json

# If it already exists, update it instead:
aws glue update-crawler \
  --region eu-west-1 \
  --cli-input-json file://raw_crawler_config.json
```

## Run it

```bash
aws glue start-crawler --name nyc-taxi-raw-yellow-crawler-cli --region eu-west-1

# Poll until State returns to READY (it goes RUNNING -> STOPPING -> READY)
aws glue get-crawler --name nyc-taxi-raw-yellow-crawler-cli --region eu-west-1 \
  --query "Crawler.{State:State,LastStatus:LastCrawl.Status}"
```

> **Cost:** crawlers bill at $0.44 per DPU-hour, 2 DPUs, **10-minute minimum**.
> One run over this small dataset is roughly **$0.07–$0.15**. Don't put it on a
> schedule unless the raw data actually changes.

## Result

Produces table **`nyc_taxi_raw.raw_yellow_tripdata`**: 19 columns, Parquet,
unpartitioned (the raw file is a single month). Verify with:

```bash
aws glue get-table --database-name nyc_taxi_raw --name raw_yellow_tripdata \
  --region eu-west-1 --query "Table.StorageDescriptor.Columns[].Name"
```
