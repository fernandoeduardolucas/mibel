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
    base_dir = Path(__file__).resolve().parents[2]
    default_raw_dir = base_dir / "01_bronze" / "data" / "raw"
    default_clean_dir = base_dir / "scripts" / "python" / "output"

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--consumo-raw",
        default=default_raw_dir / "consumo-total-nacional.csv",
        type=Path,
        help="Caminho para o CSV raw de consumo (default: %(default)s)",
    )
    parser.add_argument(
        "--producao-raw",
        default=default_raw_dir / "energia-produzida-total-nacional.csv",
        type=Path,
        help="Caminho para o CSV raw de produção (default: %(default)s)",
    )
    parser.add_argument(
        "--consumo-clean",
        default=default_clean_dir / "consumo_total_nacional_clean.parquet",
        type=Path,
        help="Caminho para o Parquet clean de consumo (default: %(default)s)",
    )
    parser.add_argument(
        "--producao-clean",
        default=default_clean_dir / "energia_produzida_total_nacional_clean.parquet",
        type=Path,
        help="Caminho para o Parquet clean de produção (default: %(default)s)",
    )
    args = parser.parse_args()

    missing_files = [
        path for path in [args.consumo_raw, args.producao_raw, args.consumo_clean, args.producao_clean] if not path.exists()
    ]
    if missing_files:
        formatted = "\n".join(f" - {path}" for path in missing_files)
        raise SystemExit(f"Ficheiros não encontrados:\n{formatted}")

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
