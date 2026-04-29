#!/usr/bin/env python3
"""Carrega apenas CSV raw para MinIO e materializa tabelas raw no Trino."""
from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path


def run(cmd: list[str], *, cwd: Path | None = None, input_text: str | None = None) -> None:
    print(f"\n>>> {' '.join(cmd)}")
    subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        text=True,
        input=input_text,
        check=True,
    )


def minio_client():
    try:
        import boto3
    except ImportError as exc:
        raise SystemExit("Falta a dependência 'boto3'. Instala com: pip install boto3") from exc

    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )


def upload_file(s3, local_path: Path, bucket: str, key: str) -> None:
    s3.upload_file(str(local_path), bucket, key)


def clear_prefix(s3, bucket: str, prefix: str) -> int:
    deleted = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if not keys:
            continue
        for i in range(0, len(keys), 1000):
            batch = keys[i : i + 1000]
            s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
            deleted += len(batch)
    return deleted


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload raw CSV para MinIO e load para tabelas raw")
    parser.add_argument("--bucket", default=os.environ.get("S3_BUCKET", "warehouse"))
    parser.add_argument("--skip-clear", action="store_true")
    args = parser.parse_args()

    script_path = Path(__file__).resolve()
    pipeline_root = script_path.parent.parent
    bronze_dir = pipeline_root / "01_bronze"
    repo_root = pipeline_root.parent.parent

    consumo_csv = bronze_dir / "data" / "raw" / "consumo-total-nacional.csv"
    producao_csv = bronze_dir / "data" / "raw" / "energia-produzida-total-nacional.csv"
    raw_sql = pipeline_root / "run" / "raw_tables_trino.sql"
    compose_file = repo_root / "01_bootstrap" / "tead_2.0_v1.2" / "docker-compose.yml"

    s3 = minio_client()
    s3.head_bucket(Bucket=args.bucket)

    if not args.skip_clear:
        raw_deleted = clear_prefix(s3, args.bucket, "bronze/raw/")
        print(f">>> Objetos removidos raw: {raw_deleted}")

    raw_consumo_key = "bronze/raw/consumo_total_nacional/consumo-total-nacional.csv"
    raw_producao_key = "bronze/raw/energia_produzida_total_nacional/energia-produzida-total-nacional.csv"

    upload_file(s3, consumo_csv, args.bucket, raw_consumo_key)
    upload_file(s3, producao_csv, args.bucket, raw_producao_key)

    print(f">>> Uploaded: s3://{args.bucket}/{raw_consumo_key}")
    print(f">>> Uploaded: s3://{args.bucket}/{raw_producao_key}")

    sql_text = raw_sql.read_text(encoding="utf-8").replace("__BUCKET__", args.bucket)

    run(
        ["docker", "compose", "-f", str(compose_file), "exec", "-T", "trino", "trino"],
        input_text=sql_text,
    )

    print("\nCarga RAW concluída com sucesso.")


if __name__ == "__main__":
    main()
