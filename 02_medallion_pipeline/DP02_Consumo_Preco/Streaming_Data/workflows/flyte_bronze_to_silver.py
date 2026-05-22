"""
Workflow Flyte: Bronze → Silver — Streaming_Data (DP-02 API).

Lê de iceberg.bronze.consumo_api_raw e iceberg.bronze.preco_api_raw,
normaliza e escreve em iceberg.silver.consumo_api_hourly e preco_api_hourly.

Diferença face ao pipeline estático:
  - Bronze API já tem ts_utc normalizado (não precisa de date_raw+hour).
  - Consumo já está em granularidade horária (MW por hora = MWh direto).
  - Silver faz apenas deduplicação e filtragem de nulos.

Execução diária:
    pyflyte run workflows/flyte_bronze_to_silver.py bronze_to_silver_api \\
        --process_date 2024-01-15

Execução completa:
    pyflyte run workflows/flyte_bronze_to_silver.py bronze_to_silver_api_full
"""

from __future__ import annotations

import os
from datetime import date, timedelta

import trino
from flytekit import task, workflow

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))


def _trino_conn() -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="admin",
        catalog="iceberg",
        schema="silver",
    )


def _exec(cur, sql: str) -> None:
    cur.execute(sql)
    cur.fetchall()


def _day_bounds(process_date: date) -> tuple[str, str]:
    next_day = process_date + timedelta(days=1)
    return (
        f"{process_date} 00:00:00 UTC",
        f"{next_day} 00:00:00 UTC",
    )


# ---------------------------------------------------------------------------
# Task 1: consumo Bronze → Silver (diário)
# ---------------------------------------------------------------------------
@task(retries=3)
def transform_consumo_api_silver(process_date: date) -> int:
    """
    Promove consumo Bronze → Silver para o process_date indicado.

    Bronze já está em MW horário. Silver converte MW → MWh (1:1 pois é horário)
    e filtra registos com ts_utc nulo. Deduplicação por GROUP BY + AVG.

    Idempotente: apaga as horas do dia antes de inserir.
    """
    conn = _trino_conn()
    cur  = conn.cursor()

    start_ts, end_ts = _day_bounds(process_date)

    _exec(cur, f"""
        DELETE FROM iceberg.silver.consumo_api_hourly
        WHERE ts_utc >= TIMESTAMP '{start_ts}'
          AND ts_utc <  TIMESTAMP '{end_ts}'
    """)

    _exec(cur, f"""
        INSERT INTO iceberg.silver.consumo_api_hourly (ts_utc, total_mwh, year, month)
        SELECT
            ts_utc,
            ROUND(AVG(total), 3)   AS total_mwh,
            YEAR(ts_utc)           AS year,
            MONTH(ts_utc)          AS month
        FROM iceberg.bronze.consumo_api_raw
        WHERE process_date = DATE '{process_date}'
          AND ts_utc IS NOT NULL
          AND total   IS NOT NULL
          AND total > 0
        GROUP BY ts_utc
    """)

    cur.execute(f"""
        SELECT COUNT(*) FROM iceberg.silver.consumo_api_hourly
        WHERE ts_utc >= TIMESTAMP '{start_ts}'
          AND ts_utc <  TIMESTAMP '{end_ts}'
    """)
    n = cur.fetchone()[0]
    conn.close()
    print(f"[silver consumo api] {n} horas inseridas para {process_date}.")
    return n


# ---------------------------------------------------------------------------
# Task 2: preços Bronze → Silver (diário)
# ---------------------------------------------------------------------------
@task(retries=3)
def transform_preco_api_silver(process_date: date) -> int:
    """
    Promove preços Bronze → Silver para o process_date indicado.

    Bronze já tem ts_utc normalizado. Silver filtra nulos e deduplica por GROUP BY + AVG.

    Idempotente: apaga as horas do dia antes de inserir.
    """
    conn = _trino_conn()
    cur  = conn.cursor()

    start_ts, end_ts = _day_bounds(process_date)

    _exec(cur, f"""
        DELETE FROM iceberg.silver.preco_api_hourly
        WHERE ts_utc >= TIMESTAMP '{start_ts}'
          AND ts_utc <  TIMESTAMP '{end_ts}'
    """)

    _exec(cur, f"""
        INSERT INTO iceberg.silver.preco_api_hourly
            (ts_utc, price_portugal_eur_mwh, price_spain_eur_mwh, year, month)
        SELECT
            ts_utc,
            ROUND(AVG(price_portugal_eur_mwh), 2) AS price_portugal_eur_mwh,
            ROUND(AVG(price_spain_eur_mwh), 2)    AS price_spain_eur_mwh,
            YEAR(ts_utc)                           AS year,
            MONTH(ts_utc)                          AS month
        FROM iceberg.bronze.preco_api_raw
        WHERE process_date = DATE '{process_date}'
          AND ts_utc IS NOT NULL
          AND price_portugal_eur_mwh IS NOT NULL
        GROUP BY ts_utc
    """)

    cur.execute(f"""
        SELECT COUNT(*) FROM iceberg.silver.preco_api_hourly
        WHERE ts_utc >= TIMESTAMP '{start_ts}'
          AND ts_utc <  TIMESTAMP '{end_ts}'
    """)
    n = cur.fetchone()[0]
    conn.close()
    print(f"[silver preco api] {n} horas inseridas para {process_date}.")
    return n


# ---------------------------------------------------------------------------
# Task 3: consumo Bronze → Silver completo
# ---------------------------------------------------------------------------
@task(retries=3)
def transform_consumo_api_silver_full() -> int:
    """Materializa todo o histórico Bronze → Silver consumo. Idempotente."""
    conn = _trino_conn()
    cur  = conn.cursor()

    _exec(cur, "DELETE FROM iceberg.silver.consumo_api_hourly WHERE 1=1")

    _exec(cur, """
        INSERT INTO iceberg.silver.consumo_api_hourly (ts_utc, total_mwh, year, month)
        SELECT
            ts_utc,
            ROUND(AVG(total), 3) AS total_mwh,
            YEAR(ts_utc)         AS year,
            MONTH(ts_utc)        AS month
        FROM iceberg.bronze.consumo_api_raw
        WHERE ts_utc IS NOT NULL
          AND total IS NOT NULL
          AND total > 0
        GROUP BY ts_utc
    """)

    cur.execute("SELECT COUNT(*) FROM iceberg.silver.consumo_api_hourly")
    n = cur.fetchone()[0]
    conn.close()
    print(f"[silver consumo api full] {n} horas inseridas.")
    return n


# ---------------------------------------------------------------------------
# Task 4: preços Bronze → Silver completo
# ---------------------------------------------------------------------------
@task(retries=3)
def transform_preco_api_silver_full() -> int:
    """Materializa todo o histórico Bronze → Silver preços. Idempotente."""
    conn = _trino_conn()
    cur  = conn.cursor()

    _exec(cur, "DELETE FROM iceberg.silver.preco_api_hourly WHERE 1=1")

    _exec(cur, """
        INSERT INTO iceberg.silver.preco_api_hourly
            (ts_utc, price_portugal_eur_mwh, price_spain_eur_mwh, year, month)
        SELECT
            ts_utc,
            ROUND(AVG(price_portugal_eur_mwh), 2) AS price_portugal_eur_mwh,
            ROUND(AVG(price_spain_eur_mwh), 2)    AS price_spain_eur_mwh,
            YEAR(ts_utc)                           AS year,
            MONTH(ts_utc)                          AS month
        FROM iceberg.bronze.preco_api_raw
        WHERE ts_utc IS NOT NULL
          AND price_portugal_eur_mwh IS NOT NULL
        GROUP BY ts_utc
    """)

    cur.execute("SELECT COUNT(*) FROM iceberg.silver.preco_api_hourly")
    n = cur.fetchone()[0]
    conn.close()
    print(f"[silver preco api full] {n} horas inseridas.")
    return n


# ---------------------------------------------------------------------------
# Workflows
# ---------------------------------------------------------------------------
@workflow
def bronze_to_silver_api(process_date: date = date(2024, 1, 1)) -> None:
    """
    Transforma Bronze → Silver para o process_date indicado (consumo + preço em paralelo).

    Execução:
        pyflyte run workflows/flyte_bronze_to_silver.py bronze_to_silver_api \\
            --process_date 2024-01-15
    """
    transform_consumo_api_silver(process_date=process_date)
    transform_preco_api_silver(process_date=process_date)


@workflow
def bronze_to_silver_api_full() -> None:
    """
    Transforma todo o Bronze disponível → Silver (consumo + preço em paralelo).

    Execução:
        pyflyte run workflows/flyte_bronze_to_silver.py bronze_to_silver_api_full
    """
    transform_consumo_api_silver_full()
    transform_preco_api_silver_full()
