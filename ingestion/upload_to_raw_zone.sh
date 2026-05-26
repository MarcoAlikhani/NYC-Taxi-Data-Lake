#!/usr/bin/env bash
#
# upload_to_raw_zone.sh — copy local taxi files into the S3 raw zone.
#
# The raw zone is the bronze layer: source data, untouched, full fidelity.
# We keep the original TLC Parquet exactly as published so we can always replay
# downstream transforms from a known-good source.
#
# Usage:
#   ./upload_to_raw_zone.sh ./data/yellow_tripdata_2024-01.parquet
#   ./upload_to_raw_zone.sh ./data/*.parquet
#
# Cost: S3 PUT + storage. ~48 MB/month of taxi data is well under $0.01/month.
set -euo pipefail

REGION="eu-west-1"
BUCKET="marco-data-darya"
RAW_PREFIX="raw/yellow_tripdata"

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <file.parquet> [more.parquet ...]" >&2
  exit 1
fi

for f in "$@"; do
  if [[ ! -f "$f" ]]; then
    echo "  [skip] not a file: $f" >&2
    continue
  fi
  base="$(basename "$f")"
  dest="s3://${BUCKET}/${RAW_PREFIX}/${base}"
  echo "  [put ] ${f} -> ${dest}"
  aws s3 cp "$f" "$dest" --region "${REGION}"
done

echo ">> Raw zone now contains:"
aws s3 ls "s3://${BUCKET}/${RAW_PREFIX}/" --region "${REGION}" --human-readable
