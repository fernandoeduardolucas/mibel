"""
Workflow Flyte: ingestão de CSVs para a camada Bronze — consumo_preco.

Lê os CSVs de consumo e preços do MinIO (raw/), filtra pelo process_date
e insere na camada Bronze do lakehouse Iceberg via Trino.

Execução:
    pyflyte run workflows/flyte_ingest_bronze.py ingest_bronze --process_date 2023-01-01

Backfill (exemplo em bash):
    for d in $(seq 0 364); do
        pyflyte run workflows/flyte_ingest_bronze.py ingest_bronze \
            --process_date $(date -d "2023-01-01 + $d days" +%F)
    done
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

CONSUMO_KEY      = "raw/consumo-total-nacional.csv"
PRECO_KEY        = os.getenv("PRECO_KEY", "raw/Day-ahead Market Prices_20230101_20260311.csv")

BATCH_SIZE = 500  # linhas por INSERT para evitar payloads excessivos


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


def _insert_batches(cur, table: str, columns: str, rows: list[str]) -> int:
    """Executa INSERTs em lotes de BATCH_SIZE linhas. Retorna total inserido."""
    inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i: i + BATCH_SIZE]
        cur.execute(f"INSERT INTO {table} ({columns}) VALUES {', '.join(batch)}")
        cur.fetchall()
        inserted += len(batch)
    return inserted


def _exec(cur, sql: str) -> None:
    cur.execute(sql)
    cur.fetchall()


# ---------------------------------------------------------------------------
# Task 1: ingestão de consumo
# ---------------------------------------------------------------------------
@task(retries=3)
def ingest_consumo(process_date: date) -> int:
    """
    Lê consumo-total-nacional.csv do MinIO, filtra pelo process_date
    e carrega na tabela iceberg.bronze.consumo_raw.

    Idempotente: apaga registos existentes para process_date antes de inserir.
    Granularidade: 15 minutos (preservada em Bronze, sem transformação).
    """
    s3 = _s3_client()
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=CONSUMO_KEY)

    df = pd.read_csv(
        BytesIO(obj["Body"].read()),
        encoding="utf-8-sig",  # remove BOM
        parse_dates=["datahora"],
    )

    # Normaliza nomes de colunas (remove espaços e BOM residual)
    df.columns = [c.strip().lower() for c in df.columns]

    # Filtra pelo dia do process_date
    df["_date"] = pd.to_datetime(df["datahora"], utc=True).dt.date
    df_day = df[df["_date"] == process_date].copy()

    if df_day.empty:
        print(f"[consumo] Sem dados para {process_date}.")
        return 0

    conn = _trino_conn()
    cur = conn.cursor()

    # Idempotência: remove registos do mesmo process_date antes de inserir
    _exec(cur, f"DELETE FROM iceberg.bronze.consumo_raw WHERE process_date = DATE '{process_date}'")

    cols = "datahora, dia, mes, ano, date_raw, time_raw, bt, mt, at, mat, total, process_date"
    rows = []
    for _, r in df_day.iterrows():
        ts = pd.Timestamp(r["datahora"])
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S.000000 UTC")

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
            f"DATE '{process_date}')"
        )

    n = _insert_batches(cur, "iceberg.bronze.consumo_raw", cols, rows)
    conn.close()
    print(f"[consumo] {n} registos inseridos para {process_date}.")
    return n


# ---------------------------------------------------------------------------
# Task 2: ingestão de preços
# ---------------------------------------------------------------------------
@task(retries=3)
def ingest_preco(process_date: date) -> int:
    """
    Lê o CSV de preços day-ahead do MinIO, filtra pelo process_date
    e carrega na tabela iceberg.bronze.preco_raw.

    Idempotente: apaga registos existentes para process_date antes de inserir.
    Preserva hora original OMIE (1-25) sem interpretação UTC — feita em Silver.
    """
    s3 = _s3_client()
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=PRECO_KEY)

    df = pd.read_csv(
        BytesIO(obj["Body"].read()),
        sep=";",
        skiprows=2,          # ignora linhas de metadados (Units e Information)
        encoding="utf-8-sig",
    )
    df.columns = [c.strip() for c in df.columns]

    # Garante nomes esperados independentemente de capitalização
    col_map = {c.lower(): c for c in df.columns}
    df = df.rename(columns={
        col_map.get("date", "Date"): "date_raw",
        col_map.get("hour", "Hour"): "hour",
        col_map.get("portugal", "Portugal"): "price_portugal_raw",
        col_map.get("spain", "Spain"): "price_spain_raw",
    })

    # Filtra pelo process_date (date_raw está em formato YYYY-MM-DD)
    df_day = df[df["date_raw"] == str(process_date)].copy()

    if df_day.empty:
        print(f"[preco] Sem dados para {process_date}.")
        return 0

    conn = _trino_conn()
    cur = conn.cursor()

    # Idempotência
    _exec(cur, f"DELETE FROM iceberg.bronze.preco_raw WHERE process_date = DATE '{process_date}'")

    cols = "date_raw, hour, price_portugal_raw, price_spain_raw, process_date"
    rows = []
    for _, r in df_day.iterrows():
        try:
            hour_val = int(r["hour"])
        except (TypeError, ValueError):
            continue  # ignora linhas com hora inválida

        try:
            pt = float(r["price_portugal_raw"])
        except (TypeError, ValueError):
            pt = "NULL"

        try:
            es = float(r["price_spain_raw"])
        except (TypeError, ValueError):
            es = "NULL"

        rows.append(
            f"('{r['date_raw']}', {hour_val}, {pt}, {es}, DATE '{process_date}')"
        )

    n = _insert_batches(cur, "iceberg.bronze.preco_raw", cols, rows)
    conn.close()
    print(f"[preco] {n} registos inseridos para {process_date}.")
    return n


# ---------------------------------------------------------------------------
# Workflow: orquestra ambas as tarefas em paralelo
# ---------------------------------------------------------------------------
@workflow
def ingest_bronze(process_date: date = date(2023, 1, 1)) -> None:
    """
    Ingestão diária Bronze para consumo_preco.
    As duas tarefas são independentes e correm em paralelo.

    Execução:
        pyflyte run workflows/flyte_ingest_bronze.py ingest_bronze --process_date 2023-01-01
    """
    ingest_consumo(process_date=process_date)
    ingest_preco(process_date=process_date)
