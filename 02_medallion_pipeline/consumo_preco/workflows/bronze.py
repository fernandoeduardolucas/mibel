#!/usr/bin/env python3
"""
Bronze ingest — consumo_preco.

Lê os CSVs históricos de consumo e preços do MinIO e carrega
nas tabelas iceberg.bronze.consumo_raw / preco_raw via Trino.

Uso:
    python bronze.py [opções]

Opções:
    --trino-host          Host Trino (default: localhost)
    --minio-endpoint      URL MinIO S3 (default: http://localhost:9000)
    --minio-access-key    Credencial MinIO (default: minioadmin)
    --minio-secret-key    Credencial MinIO (default: minioadmin)
    --bucket              Bucket MinIO (default: warehouse)
    --preco-key           Chave S3 do CSV de preços (auto-detecta se omitido)
"""

from __future__ import annotations

import argparse
from io import BytesIO

import boto3
import pandas as pd
import trino

BATCH_SIZE = 4000  # linhas por INSERT — equilibra payload e número de round-trips


# ---------------------------------------------------------------------------
# Ligações
# ---------------------------------------------------------------------------

def _trino(host: str, port: int = 8080) -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=host, port=port, user="admin",
        catalog="iceberg", schema="bronze",
        request_timeout=600,
    )


def _s3(endpoint: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3", endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def _exec(cur, sql: str) -> None:
    cur.execute(sql)
    cur.fetchall()


# ---------------------------------------------------------------------------
# INSERT em lotes
# ---------------------------------------------------------------------------

def _insert_batches(cur, table: str, columns: str, rows: list[str]) -> int:
    total = 0
    n = len(rows)
    for i in range(0, n, BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        cur.execute(f"INSERT INTO {table} ({columns}) VALUES {', '.join(batch)}")
        cur.fetchall()
        total += len(batch)
        print(f"  [{table}] {total}/{n}")
    return total


# ---------------------------------------------------------------------------
# Ingestão: consumo (15 min → bronze.consumo_raw)
# ---------------------------------------------------------------------------

def ingest_consumo_full(
    trino_host: str,
    minio_endpoint: str,
    access_key: str,
    secret_key: str,
    bucket: str = "warehouse",
) -> int:
    s3 = _s3(minio_endpoint, access_key, secret_key)
    obj = s3.get_object(Bucket=bucket, Key="raw/consumo-total-nacional.csv")

    df = pd.read_csv(BytesIO(obj["Body"].read()), encoding="utf-8-sig")
    df.columns = [c.strip().lower() for c in df.columns]
    df["datahora"] = pd.to_datetime(df["datahora"], utc=True)
    df["proc_date"] = df["datahora"].dt.date.astype(str)

    def flt(v) -> str:
        try:
            return str(float(v))
        except (TypeError, ValueError):
            return "NULL"

    def integer(v) -> str:
        try:
            return str(int(float(v)))
        except (TypeError, ValueError):
            return "NULL"

    # Sort by timestamp so rows within each batch span at most 1-2 Iceberg partitions,
    # avoiding ICEBERG_TOO_MANY_OPEN_PARTITIONS (limit: 100 writers)
    df = df.sort_values("datahora").reset_index(drop=True)

    cols = "datahora, dia, mes, ano, date_raw, time_raw, bt, mt, at, mat, total, process_date"
    rows: list[str] = []
    for r in df.to_dict("records"):
        ts = r["datahora"]
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S.000000 UTC")
        date_raw = str(r.get("date", r.get("date_raw", ""))).replace("'", "")
        time_raw = str(r.get("time", r.get("time_raw", ""))).replace("'", "")
        rows.append(
            f"(TIMESTAMP '{ts_str}', {integer(r.get('dia'))}, {integer(r.get('mes'))}, "
            f"{integer(r.get('ano'))}, '{date_raw}', '{time_raw}', "
            f"{flt(r.get('bt'))}, {flt(r.get('mt'))}, {flt(r.get('at'))}, "
            f"{flt(r.get('mat'))}, {flt(r.get('total'))}, DATE '{r['proc_date']}')"
        )

    conn = _trino(trino_host)
    cur = conn.cursor()
    _exec(cur, "DELETE FROM iceberg.bronze.consumo_raw WHERE 1=1")
    total = _insert_batches(cur, "iceberg.bronze.consumo_raw", cols, rows)
    conn.close()
    print(f"[bronze] consumo_raw: {total} registos inseridos.")
    return total


# ---------------------------------------------------------------------------
# Ingestão: preços day-ahead (horário → bronze.preco_raw)
# ---------------------------------------------------------------------------

def ingest_preco_full(
    trino_host: str,
    minio_endpoint: str,
    access_key: str,
    secret_key: str,
    bucket: str = "warehouse",
    preco_key: str = "",
) -> int:
    s3 = _s3(minio_endpoint, access_key, secret_key)

    if not preco_key:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix="raw/Day-ahead")
        objects = resp.get("Contents", [])
        if not objects:
            raise FileNotFoundError(
                "Nenhum ficheiro de preços encontrado em raw/Day-ahead* no MinIO.\n"
                "Carrega o CSV OMIE em s3://warehouse/raw/ antes de correr o pipeline."
            )
        preco_key = sorted(objects, key=lambda x: x["LastModified"], reverse=True)[0]["Key"]
        print(f"[bronze] Usando ficheiro de preços: {preco_key}")

    obj = s3.get_object(Bucket=bucket, Key=preco_key)
    df = pd.read_csv(BytesIO(obj["Body"].read()), sep=";", skiprows=2, encoding="utf-8-sig")
    df.columns = [c.strip() for c in df.columns]

    # Normaliza nomes de colunas independente de capitalização
    col_lower = {c.lower(): c for c in df.columns}
    rename = {}
    if "date" in col_lower:
        rename[col_lower["date"]] = "date_raw"
    if "hour" in col_lower:
        rename[col_lower["hour"]] = "hour"
    if "portugal" in col_lower:
        rename[col_lower["portugal"]] = "price_portugal_raw"
    if "spain" in col_lower:
        rename[col_lower["spain"]] = "price_spain_raw"
    df = df.rename(columns=rename)

    needed = [c for c in ["date_raw", "hour", "price_portugal_raw", "price_spain_raw"] if c in df.columns]
    df = df[needed].copy()
    df["date_raw"] = df["date_raw"].astype(str).str.strip()
    # Descarta linhas sem data válida (ex: totais, cabeçalhos extra)
    df = df[df["date_raw"].str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)]

    # Sort by date/hour so batches stay within a single Iceberg partition
    df = df.sort_values(["date_raw", "hour"]).reset_index(drop=True)

    cols = "date_raw, hour, price_portugal_raw, price_spain_raw, process_date"
    rows: list[str] = []
    for r in df.to_dict("records"):
        try:
            h = int(r["hour"])
        except (TypeError, ValueError):
            continue
        try:
            pt = str(float(r["price_portugal_raw"]))
        except (TypeError, ValueError):
            pt = "NULL"
        try:
            es = str(float(r.get("price_spain_raw", "NULL")))
        except (TypeError, ValueError):
            es = "NULL"
        date_raw = str(r["date_raw"]).replace("'", "")
        rows.append(f"('{date_raw}', {h}, {pt}, {es}, DATE '{date_raw}')")

    conn = _trino(trino_host)
    cur = conn.cursor()
    _exec(cur, "DELETE FROM iceberg.bronze.preco_raw WHERE 1=1")
    total = _insert_batches(cur, "iceberg.bronze.preco_raw", cols, rows)
    conn.close()
    print(f"[bronze] preco_raw: {total} registos inseridos.")
    return total


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Bronze ingest — consumo_preco")
    parser.add_argument("--trino-host",       default="localhost")
    parser.add_argument("--trino-port",       type=int, default=8080)
    parser.add_argument("--minio-endpoint",   default="http://localhost:9000")
    parser.add_argument("--minio-access-key", default="minioadmin")
    parser.add_argument("--minio-secret-key", default="minioadmin")
    parser.add_argument("--bucket",           default="warehouse")
    parser.add_argument("--preco-key",        default="",
                        help="Chave S3 do CSV de preços (auto-detecta se omitido)")
    args = parser.parse_args()

    ingest_consumo_full(
        args.trino_host, args.minio_endpoint,
        args.minio_access_key, args.minio_secret_key, args.bucket,
    )
    ingest_preco_full(
        args.trino_host, args.minio_endpoint,
        args.minio_access_key, args.minio_secret_key, args.bucket, args.preco_key,
    )


if __name__ == "__main__":
    main()
