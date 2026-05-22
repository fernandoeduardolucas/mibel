"""
Workflow Flyte: ingestão de CSVs para a camada Bronze — consumo_preco.

Lê os CSVs históricos completos de consumo e preços do MinIO (raw/)
e insere na camada Bronze do lakehouse Iceberg via Trino.

Os CSVs raw são lidos do MinIO bucket 'warehouse' nos caminhos:
  - raw/consumo-total-nacional.csv
  - raw/Day-ahead Market Prices_20230101_20260311.csv

Fontes:
  - Consumo: REN (Redes Energéticas Nacionais) — granularidade 15 minutos
  - Preços: OMIE day-ahead market prices PT+ES — granularidade horária (horas 1-25)

Execução:
    pyflyte run workflows/flyte_ingest_bronze.py ingest_bronze_full
"""

from __future__ import annotations

import os
from datetime import date
from io import BytesIO

import boto3
import pandas as pd
import trino
from flytekit import task, workflow

# ---------------------------------------------------------------------------
# Configuração (via variáveis de ambiente para portabilidade)
# ---------------------------------------------------------------------------
TRINO_HOST       = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT       = int(os.getenv("TRINO_PORT", "8080"))

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
RAW_BUCKET       = os.getenv("RAW_BUCKET", "warehouse")

CONSUMO_KEY = os.getenv("CONSUMO_KEY", "raw/consumo-total-nacional.csv")
PRECO_KEY   = os.getenv("PRECO_KEY",   "raw/Day-ahead Market Prices_20230101_20260311.csv")

BATCH_SIZE = 5000               # linhas por INSERT para evitar payloads excessivos
MAX_PARTITIONS_PER_INSERT = 60  # abaixo do limite habitual do Trino/Iceberg


def _trino_conn() -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="admin",
        catalog="iceberg",
        schema="bronze",
    )


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def _insert_partition_batches(
    cur,
    table: str,
    columns: str,
    daily_rows: list[tuple[date | str, list[str]]],
) -> int:
    """
    Executa INSERTs agrupando vários dias por statement.

    Mantém o número de partições por INSERT controlado para evitar o limite de
    writers do Trino/Iceberg, reduzindo bastante o número de commits face a
    inserir dia a dia.
    """
    inserted = 0
    batch_rows: list[str] = []
    batch_partitions: set[str] = set()

    def flush() -> None:
        nonlocal inserted, batch_rows, batch_partitions
        if not batch_rows:
            return
        cur.execute(f"INSERT INTO {table} ({columns}) VALUES {', '.join(batch_rows)}")
        cur.fetchall()
        inserted += len(batch_rows)
        print(
            f"[insert] {table}: {len(batch_rows)} linhas, "
            f"{len(batch_partitions)} partições"
        )
        batch_rows = []
        batch_partitions = set()

    for partition, rows in daily_rows:
        partition_key = str(partition)
        would_exceed_partitions = (
            partition_key not in batch_partitions
            and len(batch_partitions) >= MAX_PARTITIONS_PER_INSERT
        )
        would_exceed_rows = len(batch_rows) + len(rows) > BATCH_SIZE
        if would_exceed_partitions or would_exceed_rows:
            flush()

        batch_partitions.add(partition_key)
        batch_rows.extend(rows)

    flush()
    return inserted


def _exec(cur, sql: str) -> None:
    cur.execute(sql)
    cur.fetchall()


# ---------------------------------------------------------------------------
# Task: ingestão completa de consumo (todos os dias)
# ---------------------------------------------------------------------------
@task(retries=3)
def ingest_consumo_full() -> int:
    """
    Lê consumo-total-nacional.csv completo do MinIO e carrega em Bronze.
    Insere data a data para evitar o limite de partições abertas do Iceberg.
    process_date é derivado da coluna datahora de cada registo.

    Origem: REN CSV — granularidade 15 minutos, unidades kW.
    Schema Bronze preserva todas as colunas originais mais metadados de ingestão.
    """
    s3 = _s3_client()
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=CONSUMO_KEY)

    df = pd.read_csv(
        BytesIO(obj["Body"].read()),
        encoding="utf-8-sig",
        parse_dates=["datahora"],
    )
    df.columns = [c.strip().lower() for c in df.columns]
    df["datahora"] = pd.to_datetime(df["datahora"], utc=True)
    df["_date"] = df["datahora"].dt.date

    def _float(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return "NULL"

    def _int(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return "NULL"

    conn = _trino_conn()
    cur = conn.cursor()
    _exec(cur, "DELETE FROM iceberg.bronze.consumo_raw WHERE 1=1")

    cols = "datahora, dia, mes, ano, date_raw, time_raw, bt, mt, at, mat, total, process_date"
    daily_rows: list[tuple[date, list[str]]] = []
    for proc, df_day in df.groupby("_date"):
        rows = []
        for _, r in df_day.iterrows():
            ts = r["datahora"]
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S.000000 UTC")
            rows.append(
                f"(TIMESTAMP '{ts_str}', "
                f"{_int(r.get('dia', 'NULL'))}, "
                f"{_int(r.get('mes', 'NULL'))}, "
                f"{_int(r.get('ano', 'NULL'))}, "
                f"'{r.get('date', '')}', "
                f"'{r.get('time', '')}', "
                f"{_float(r.get('bt', 'NULL'))}, "
                f"{_float(r.get('mt', 'NULL'))}, "
                f"{_float(r.get('at', 'NULL'))}, "
                f"{_float(r.get('mat', 'NULL'))}, "
                f"{_float(r.get('total', 'NULL'))}, "
                f"DATE '{proc}')"
            )
        daily_rows.append((proc, rows))
        print(f"[consumo_full] preparado {proc}: {len(rows)} registos.")

    total = _insert_partition_batches(cur, "iceberg.bronze.consumo_raw", cols, daily_rows)

    conn.close()
    print(f"[consumo_full] Total: {total} registos inseridos.")
    return total


# ---------------------------------------------------------------------------
# Task: ingestão completa de preços (todos os dias)
# ---------------------------------------------------------------------------
@task(retries=3)
def ingest_preco_full() -> int:
    """
    Lê o CSV de preços day-ahead completo do MinIO e carrega em Bronze.
    Insere data a data para evitar o limite de partições abertas do Iceberg.
    process_date é derivado da coluna date_raw de cada registo.

    Origem: OMIE day-ahead market prices — granularidade horária, horas 1-25.
    Hora 25 (DST outono) é preservada no Bronze e filtrada na Silver.
    """
    s3 = _s3_client()
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=PRECO_KEY)

    df = pd.read_csv(
        BytesIO(obj["Body"].read()),
        sep=";",
        skiprows=2,
        encoding="utf-8-sig",
    )
    df.columns = [c.strip() for c in df.columns]

    col_map = {c.lower(): c for c in df.columns}
    df = df.rename(columns={
        col_map.get("date", "Date"): "date_raw",
        col_map.get("hour", "Hour"): "hour",
        col_map.get("portugal", "Portugal"): "price_portugal_raw",
        col_map.get("spain", "Spain"): "price_spain_raw",
    })

    conn = _trino_conn()
    cur = conn.cursor()
    _exec(cur, "DELETE FROM iceberg.bronze.preco_raw WHERE 1=1")

    cols = "date_raw, hour, price_portugal_raw, price_spain_raw, process_date"
    daily_rows: list[tuple[str, list[str]]] = []
    for proc, df_day in df.groupby("date_raw"):
        rows = []
        for _, r in df_day.iterrows():
            try:
                hour_val = int(r["hour"])
            except (TypeError, ValueError):
                continue

            try:
                pt = float(r["price_portugal_raw"])
            except (TypeError, ValueError):
                pt = "NULL"

            try:
                es = float(r["price_spain_raw"])
            except (TypeError, ValueError):
                es = "NULL"

            rows.append(
                f"('{r['date_raw']}', {hour_val}, {pt}, {es}, DATE '{r['date_raw']}')"
            )
        if rows:
            daily_rows.append((proc, rows))
            print(f"[preco_full] preparado {proc}: {len(rows)} registos.")

    total = _insert_partition_batches(cur, "iceberg.bronze.preco_raw", cols, daily_rows)

    conn.close()
    print(f"[preco_full] Total: {total} registos inseridos.")
    return total


# ---------------------------------------------------------------------------
# Workflows
# ---------------------------------------------------------------------------
@workflow
def ingest_bronze_full() -> None:
    """
    Ingestão Bronze completa (todos os dados dos CSVs raw).
    Trunca as tabelas Bronze e re-insere tudo a partir dos ficheiros em MinIO.

    Pré-requisito: CSVs devem estar em MinIO warehouse/raw/ (upload feito pelo run script).

    Execução directa:
        pyflyte run workflows/flyte_ingest_bronze.py ingest_bronze_full
    """
    ingest_consumo_full()
    ingest_preco_full()
