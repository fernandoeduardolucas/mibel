#!/usr/bin/env python3
"""
Fetch meteorological data from the Open-Meteo archive API and upload to MinIO.

Fetches hourly data for Lisbon (38.72N, 9.14W) from 2023-01-01 to today.
Variables: temperature_2m, precipitation, wind_speed_10m, shortwave_radiation, cloud_cover.

Usage:
    python fetch_open_meteo.py --out-dir ./output --upload

Environment variables for upload:
    S3_ENDPOINT_URL=http://localhost:9000
    AWS_ACCESS_KEY_ID=minioadmin
    AWS_SECRET_ACCESS_KEY=minioadmin
    S3_BUCKET=warehouse
"""
from __future__ import annotations

import argparse
import os
from datetime import date, datetime
from pathlib import Path

LATITUDE = 38.72
LONGITUDE = -9.14
DATE_FROM = "2023-01-01"
HOURLY_VARS = [
    "temperature_2m",
    "precipitation",
    "wind_speed_10m",
    "shortwave_radiation",
    "cloud_cover",
]
S3_RAW_KEY = "bronze/raw/meteo_open_meteo/open-meteo-portugal-hourly.csv"
S3_CLEAN_KEY = "bronze/clean/meteo_open_meteo/open-meteo-portugal-hourly.parquet"


def fetch_open_meteo(date_from: str, date_to: str) -> "pd.DataFrame":
    import pandas as pd
    import urllib.request
    import json

    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={LATITUDE}&longitude={LONGITUDE}"
        f"&start_date={date_from}&end_date={date_to}"
        f"&hourly={','.join(HOURLY_VARS)}"
        "&timezone=UTC"
    )

    print(f">>> Fetching Open-Meteo: {url}")
    with urllib.request.urlopen(url, timeout=120) as response:
        data = json.loads(response.read())

    hourly = data.get("hourly", {})
    if not hourly or "time" not in hourly:
        raise ValueError("Open-Meteo returned unexpected structure — no 'hourly.time' key.")

    df = pd.DataFrame(hourly)
    df.rename(columns={"time": "ts_utc"}, inplace=True)
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)

    df["year"] = df["ts_utc"].dt.year.astype("Int64")
    df["month"] = df["ts_utc"].dt.month.astype("Int64")
    df["day"] = df["ts_utc"].dt.day.astype("Int64")
    df["hour"] = df["ts_utc"].dt.hour.astype("Int64")
    df["latitude"] = float(data.get("latitude", LATITUDE))
    df["longitude"] = float(data.get("longitude", LONGITUDE))
    df["elevation_m"] = float(data.get("elevation", 0.0))
    df["_source_file"] = "open-meteo-archive-api"
    df["_ingested_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    for col in HOURLY_VARS:
        df[col] = df[col].astype("float64")

    return df.sort_values("ts_utc").reset_index(drop=True)


def write_csv(df: "pd.DataFrame", out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_csv = df.copy()
    df_csv["ts_utc"] = df_csv["ts_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    df_csv.to_csv(out_path, index=False)
    print(f">>> Wrote CSV: {out_path}  ({len(df_csv):,} rows)")


def write_parquet(df: "pd.DataFrame", out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(out_path, index=False, engine="pyarrow")
    except ImportError as exc:
        raise SystemExit("Falta a dependência 'pyarrow'. Instala com: pip install pyarrow") from exc
    print(f">>> Wrote Parquet: {out_path}")


def upload_file(local_path: Path, bucket: str, key: str) -> None:
    try:
        import boto3
    except ImportError as exc:
        raise SystemExit("Falta a dependência 'boto3'. Instala com: pip install boto3") from exc

    endpoint = os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    s3.upload_file(str(local_path), bucket, key)
    print(f">>> Uploaded: s3://{bucket}/{key}")


def print_quality_report(df: "pd.DataFrame") -> None:
    print("\n=== Quality Report - Open-Meteo Bronze ===")
    print(f"Rows       : {len(df):,}")
    print(f"Date range : {df['ts_utc'].min()} -> {df['ts_utc'].max()}")
    for col in HOURLY_VARS:
        null_count = int(df[col].isna().sum())
        print(f"  {col:30s}  nulls={null_count:,}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Open-Meteo historical data for Portugal.")
    parser.add_argument("--date-from", default=DATE_FROM, help="Start date YYYY-MM-DD")
    parser.add_argument("--date-to", default=date.today().isoformat(), help="End date YYYY-MM-DD")
    parser.add_argument("--out-dir", default=Path("./output"), type=Path)
    parser.add_argument("--upload", action="store_true", help="Upload to MinIO after writing")
    args = parser.parse_args()

    df = fetch_open_meteo(args.date_from, args.date_to)

    csv_out = args.out_dir / "open-meteo-portugal-hourly.csv"
    parquet_out = args.out_dir / "open-meteo-portugal-hourly.parquet"

    write_csv(df, csv_out)
    write_parquet(df, parquet_out)
    print_quality_report(df)

    if args.upload:
        bucket = os.environ.get("S3_BUCKET", "warehouse")
        upload_file(csv_out, bucket, S3_RAW_KEY)
        upload_file(parquet_out, bucket, S3_CLEAN_KEY)


if __name__ == "__main__":
    main()
