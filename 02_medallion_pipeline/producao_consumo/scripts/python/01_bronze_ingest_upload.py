#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd


def read_raw_exact(path: Path, origem: str) -> pd.DataFrame:
    # Bronze: preservar valores exatamente como lidos (sem casts/trim/normalização).
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df["origem_ficheiro"] = origem
    df["ingest_ts_utc"] = pd.Timestamp.now("UTC").isoformat()
    return df


def write_parquet(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False, engine="pyarrow")


def upload_file(local_path: Path, bucket: str, key: str) -> None:
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )
    s3.upload_file(str(local_path), bucket, key)


def main() -> None:
    parser = argparse.ArgumentParser(description="Passo Bronze: ingestão bruta com metadados")
    parser.add_argument("--consumo", required=True, type=Path)
    parser.add_argument("--producao", required=True, type=Path)
    parser.add_argument("--out-dir", default=Path("./output"), type=Path)
    parser.add_argument("--upload", action="store_true")
    args = parser.parse_args()

    consumo_df = read_raw_exact(args.consumo, args.consumo.name)
    producao_df = read_raw_exact(args.producao, args.producao.name)

    consumo_out = args.out_dir / "consumo_total_nacional_bronze_raw.parquet"
    producao_out = args.out_dir / "energia_produzida_total_nacional_bronze_raw.parquet"
    write_parquet(consumo_df, consumo_out)
    write_parquet(producao_df, producao_out)

    if args.upload:
        bucket = os.environ.get("S3_BUCKET", "warehouse")
        prefix = os.environ.get("S3_PREFIX", "bronze/clean").strip("/")
        upload_file(args.consumo, bucket, "bronze/raw/consumo_total_nacional/consumo-total-nacional.csv")
        upload_file(args.producao, bucket, "bronze/raw/energia_produzida_total_nacional/energia-produzida-total-nacional.csv")
        upload_file(consumo_out, bucket, f"{prefix}/consumo_total_nacional/{consumo_out.name}")
        upload_file(producao_out, bucket, f"{prefix}/energia_produzida_total_nacional/{producao_out.name}")

    print(f"Bronze concluído. Output: {consumo_out} | {producao_out}")


if __name__ == "__main__":
    main()
