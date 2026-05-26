#!/usr/bin/env python3
"""
download_nyc_taxi.py — fetch NYC TLC yellow-taxi trip files to a local folder.

The TLC publishes monthly trip records as Parquet on a public CloudFront
distribution. This script just pulls the files down; uploading them to the S3
raw zone is a separate step (upload_to_raw_zone.sh) so that download and upload
can be retried independently.

Usage:
    python download_nyc_taxi.py 2024-01
    python download_nyc_taxi.py 2024-01 2024-02 2024-03
    python download_nyc_taxi.py --dest ./data 2024-01

Each <YYYY-MM> argument fetches yellow_tripdata_<YYYY-MM>.parquet.
"""
from __future__ import annotations

import argparse
import sys
import urllib.request
from pathlib import Path

# Public TLC distribution. Pattern is stable across months.
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
FILENAME = "yellow_tripdata_{month}.parquet"


def download_month(month: str, dest: Path) -> Path:
    """Download a single yellow_tripdata_<YYYY-MM>.parquet into dest."""
    fname = FILENAME.format(month=month)
    url = f"{BASE_URL}/{fname}"
    out_path = dest / fname

    if out_path.exists():
        print(f"  [skip] {fname} already present ({out_path.stat().st_size/1e6:.1f} MB)")
        return out_path

    print(f"  [get ] {url}")
    dest.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(url, out_path)
    print(f"  [ok  ] {out_path} ({out_path.stat().st_size/1e6:.1f} MB)")
    return out_path


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Download NYC TLC yellow-taxi data.")
    parser.add_argument("months", nargs="+", help="Months as YYYY-MM (e.g. 2024-01)")
    parser.add_argument("--dest", default="./data", help="Local download folder (default ./data)")
    args = parser.parse_args(argv)

    dest = Path(args.dest)
    print(f"Downloading {len(args.months)} file(s) to {dest.resolve()}")
    for month in args.months:
        try:
            download_month(month, dest)
        except Exception as exc:  # noqa: BLE001 - surface the URL that failed
            print(f"  [FAIL] {month}: {exc}", file=sys.stderr)
            return 1
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
