# Ingestion — getting raw data into the lake

This stage is deliberately dumb: **download the source files, put them in the
raw zone, change nothing.** The raw (bronze) zone is the replay buffer for the
whole pipeline — if a downstream transform has a bug, we re-run it against raw
and lose nothing.

## Two steps, decoupled on purpose

```
NYC TLC CloudFront  ──download──▶  ./data/*.parquet  ──upload──▶  s3://marco-data-darya/raw/yellow_tripdata/
        (1) download_nyc_taxi.py                       (2) upload_to_raw_zone.sh
```

Download and upload are separate scripts so a flaky network download doesn't
force a re-upload, and a re-upload doesn't force a re-download.

## 1. Download

```bash
python download_nyc_taxi.py 2024-01
# -> ./data/yellow_tripdata_2024-01.parquet  (~48 MB)
```

The TLC publishes monthly Parquet at a stable URL:
`https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`

## 2. Upload to the raw zone

```bash
./upload_to_raw_zone.sh ./data/yellow_tripdata_2024-01.parquet
```

This lands the file at:
`s3://marco-data-darya/raw/yellow_tripdata/yellow_tripdata_2024-01.parquet`

## A note on the source format

The TLC already publishes Parquet, so the raw zone here is Parquet, not CSV.
In a more typical "raw" scenario you'd land CSV/JSON exactly as received. The
README architecture diagram shows CSV→Parquet conversion happening at the clean
layer; because our source is already Parquet, the clean-layer job (Phase 2)
focuses on **typing, validation, dedup, and partitioning** rather than format
conversion.

## What's currently in the raw zone

| File | Size | Rows (approx) |
|---|---|---|
| `yellow_tripdata_2024-01.parquet` | 47.6 MiB | ~2.96 M |
