#!/usr/bin/env python3
"""
Upload Bronze para MinIO/S3:
- raw CSV (consumo + produção)
- clean Parquet (consumo + produção)
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path


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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--consumo-raw", required=True, type=Path)
    parser.add_argument("--producao-raw", required=True, type=Path)
    parser.add_argument("--consumo-clean", required=True, type=Path)
    parser.add_argument("--producao-clean", required=True, type=Path)
    args = parser.parse_args()

    bucket = os.environ.get("S3_BUCKET", "warehouse")
    prefix = os.environ.get("S3_PREFIX", "bronze/clean").strip("/")

    raw_consumo_key = "bronze/raw/consumo_total_nacional/consumo-total-nacional.csv"
    raw_producao_key = "bronze/raw/energia_produzida_total_nacional/energia-produzida-total-nacional.csv"
    clean_consumo_key = f"{prefix}/consumo_total_nacional/{args.consumo_clean.name}"
    clean_producao_key = f"{prefix}/energia_produzida_total_nacional/{args.producao_clean.name}"

    upload_file(args.consumo_raw, bucket, raw_consumo_key)
    upload_file(args.producao_raw, bucket, raw_producao_key)
    upload_file(args.consumo_clean, bucket, clean_consumo_key)
    upload_file(args.producao_clean, bucket, clean_producao_key)

    print(f"Uploaded raw:   s3://{bucket}/{raw_consumo_key}")
    print(f"Uploaded raw:   s3://{bucket}/{raw_producao_key}")
    print(f"Uploaded clean: s3://{bucket}/{clean_consumo_key}")
    print(f"Uploaded clean: s3://{bucket}/{clean_producao_key}")


if __name__ == "__main__":
    main()
