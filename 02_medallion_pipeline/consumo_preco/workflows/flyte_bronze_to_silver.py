"""
Workflow Flyte: transformação Bronze → Silver — consumo_preco.

Agrega consumo de 15 min para horário (kW → MWh) e normaliza preços
para timestamp UTC. Ambas as tarefas são parametrizadas por process_date
e são idempotentes ao nível diário (DELETE + INSERT no mesmo dia lógico).

Execução diária:
    pyflyte run workflows/flyte_bronze_to_silver.py bronze_to_silver --process_date 2023-01-01

Execução completa:
    pyflyte run workflows/flyte_bronze_to_silver.py bronze_to_silver_full

Backfill (exemplo em bash):
    for d in $(seq 0 364); do
        pyflyte run workflows/flyte_bronze_to_silver.py bronze_to_silver \
            --process_date $(date -d "2023-01-01 + $d days" +%F)
    done
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
    """Executa SQL e faz fetch para garantir que o statement completou."""
    cur.execute(sql)
    cur.fetchall()


def _day_bounds(process_date: date) -> tuple[str, str]:
    """Devolve limites [start, end) do dia em formato Trino UTC."""
    next_day = process_date + timedelta(days=1)
    return (
        f"{process_date} 00:00:00 UTC",
        f"{next_day} 00:00:00 UTC",
    )


# ---------------------------------------------------------------------------
# Task 1: consumo Bronze → Silver
# ---------------------------------------------------------------------------
@task(retries=3)
def transform_consumo_silver(process_date: date) -> int:
    """
    Agrega os registos de 15 min do Bronze num único registo horário UTC.

    Conversão de unidades: o campo `total` em Bronze está em kW por intervalo
    de 15 minutos. SUM(total) / 1000 dá a energia horária em MWh.

    Idempotente: apaga as horas do mesmo dia lógico antes de inserir.
    Só afecta as partições Silver do dia processado.
    """
    conn = _trino_conn()
    cur = conn.cursor()

    start_ts, end_ts = _day_bounds(process_date)

    # Idempotência: remove apenas as horas do dia em processamento
    _exec(cur, f"""
        DELETE FROM iceberg.silver.consumo_hourly
        WHERE ts_utc >= TIMESTAMP '{start_ts}'
          AND ts_utc <  TIMESTAMP '{end_ts}'
    """)

    # Agrega 15 min → 1 hora; kW → MWh (÷ 1000)
    _exec(cur, f"""
        INSERT INTO iceberg.silver.consumo_hourly (ts_utc, total_mwh, year, month)
        SELECT
            DATE_TRUNC('hour', datahora)        AS ts_utc,
            SUM(total) / 1000.0                 AS total_mwh,
            YEAR(DATE_TRUNC('hour', datahora))  AS year,
            MONTH(DATE_TRUNC('hour', datahora)) AS month
        FROM iceberg.bronze.consumo_raw
        WHERE process_date = DATE '{process_date}'
        GROUP BY DATE_TRUNC('hour', datahora)
    """)

    cur.execute(f"""
        SELECT COUNT(*) FROM iceberg.silver.consumo_hourly
        WHERE ts_utc >= TIMESTAMP '{start_ts}'
          AND ts_utc <  TIMESTAMP '{end_ts}'
    """)
    n = cur.fetchone()[0]
    conn.close()
    print(f"[silver consumo] {n} horas inseridas para {process_date}.")
    return n


# ---------------------------------------------------------------------------
# Task 2: preços Bronze → Silver
# ---------------------------------------------------------------------------
@task(retries=3)
def transform_preco_silver(process_date: date) -> int:
    """
    Normaliza preços day-ahead para timestamp UTC.

    Mapeamento: Hour 1-24 → ts_utc = date + (hour - 1) horas.
    Hour 25 (hora extra nos dias de mudança DST de outono) é descartada
    com o filtro AND hour BETWEEN 1 AND 24.

    Em dias de mudança DST de primavera, a hora 2 local não existe — o CSV
    não a inclui, por isso não é necessário tratamento especial.

    Idempotente: apaga as horas do mesmo dia lógico antes de inserir.
    O GROUP BY + AVG absorve eventuais duplicados residuais na Bronze.
    """
    conn = _trino_conn()
    cur = conn.cursor()

    start_ts, end_ts = _day_bounds(process_date)

    _exec(cur, f"""
        DELETE FROM iceberg.silver.preco_hourly
        WHERE ts_utc >= TIMESTAMP '{start_ts}'
          AND ts_utc <  TIMESTAMP '{end_ts}'
    """)

    # date_raw (VARCHAR) + hour (1-24) → TIMESTAMP WITH TIME ZONE UTC
    # date_add('hour', hour - 1, ...) converte hora OMIE para 0-based UTC
    _exec(cur, f"""
        INSERT INTO iceberg.silver.preco_hourly
            (ts_utc, price_portugal_eur_mwh, price_spain_eur_mwh, year, month)
        SELECT
            ts_utc,
            ROUND(AVG(price_portugal_raw), 2) AS price_portugal_eur_mwh,
            ROUND(AVG(price_spain_raw), 2)    AS price_spain_eur_mwh,
            YEAR(ts_utc)                      AS year,
            MONTH(ts_utc)                     AS month
        FROM (
            SELECT
                date_add('hour', hour - 1,
                    CAST(DATE_PARSE(date_raw, '%Y-%m-%d') AS TIMESTAMP(6))
                ) AT TIME ZONE 'UTC'  AS ts_utc,
                price_portugal_raw,
                price_spain_raw
            FROM iceberg.bronze.preco_raw
            WHERE process_date = DATE '{process_date}'
              AND hour BETWEEN 1 AND 24
        ) AS normalized
        GROUP BY ts_utc
    """)

    cur.execute(f"""
        SELECT COUNT(*) FROM iceberg.silver.preco_hourly
        WHERE ts_utc >= TIMESTAMP '{start_ts}'
          AND ts_utc <  TIMESTAMP '{end_ts}'
    """)
    n = cur.fetchone()[0]
    conn.close()
    print(f"[silver preco] {n} horas inseridas para {process_date}.")
    return n


# ---------------------------------------------------------------------------
# Task 3: consumo Bronze → Silver completo
# ---------------------------------------------------------------------------
@task(retries=3)
def transform_consumo_silver_full() -> int:
    """
    Agrega todo o histórico de consumo Bronze para Silver.

    Idempotente para execução full: limpa a tabela Silver de consumo e volta a
    materializar todas as horas disponíveis no ficheiro raw de consumo.
    """
    conn = _trino_conn()
    cur = conn.cursor()

    _exec(cur, "DELETE FROM iceberg.silver.consumo_hourly WHERE 1=1")

    _exec(cur, """
        INSERT INTO iceberg.silver.consumo_hourly (ts_utc, total_mwh, year, month)
        SELECT
            DATE_TRUNC('hour', datahora)        AS ts_utc,
            SUM(total) / 1000.0                 AS total_mwh,
            YEAR(DATE_TRUNC('hour', datahora))  AS year,
            MONTH(DATE_TRUNC('hour', datahora)) AS month
        FROM iceberg.bronze.consumo_raw
        GROUP BY DATE_TRUNC('hour', datahora)
    """)

    cur.execute("SELECT COUNT(*) FROM iceberg.silver.consumo_hourly")
    n = cur.fetchone()[0]
    conn.close()
    print(f"[silver consumo full] {n} horas inseridas.")
    return n


# ---------------------------------------------------------------------------
# Task 4: preços Bronze → Silver completo
# ---------------------------------------------------------------------------
@task(retries=3)
def transform_preco_silver_full() -> int:
    """
    Normaliza todo o histórico de preços Bronze para Silver.

    Idempotente para execução full: limpa a tabela Silver de preços e volta a
    materializar todas as horas disponíveis no ficheiro raw de preços.
    """
    conn = _trino_conn()
    cur = conn.cursor()

    _exec(cur, "DELETE FROM iceberg.silver.preco_hourly WHERE 1=1")

    _exec(cur, """
        INSERT INTO iceberg.silver.preco_hourly
            (ts_utc, price_portugal_eur_mwh, price_spain_eur_mwh, year, month)
        SELECT
            ts_utc,
            ROUND(AVG(price_portugal_raw), 2) AS price_portugal_eur_mwh,
            ROUND(AVG(price_spain_raw), 2)    AS price_spain_eur_mwh,
            YEAR(ts_utc)                      AS year,
            MONTH(ts_utc)                     AS month
        FROM (
            SELECT
                date_add('hour', hour - 1,
                    CAST(DATE_PARSE(date_raw, '%Y-%m-%d') AS TIMESTAMP(6))
                ) AT TIME ZONE 'UTC'  AS ts_utc,
                price_portugal_raw,
                price_spain_raw
            FROM iceberg.bronze.preco_raw
            WHERE hour BETWEEN 1 AND 24
        ) AS normalized
        GROUP BY ts_utc
    """)

    cur.execute("SELECT COUNT(*) FROM iceberg.silver.preco_hourly")
    n = cur.fetchone()[0]
    conn.close()
    print(f"[silver preco full] {n} horas inseridas.")
    return n


# ---------------------------------------------------------------------------
# Workflows
# ---------------------------------------------------------------------------
@workflow
def bronze_to_silver(process_date: date = date(2023, 1, 1)) -> None:
    """
    Transforma Bronze → Silver para o process_date indicado.
    As duas transformações são independentes e correm em paralelo.

    Execução:
        pyflyte run workflows/flyte_bronze_to_silver.py bronze_to_silver --process_date 2023-01-01
    """
    transform_consumo_silver(process_date=process_date)
    transform_preco_silver(process_date=process_date)


@workflow
def bronze_to_silver_full() -> None:
    """
    Transforma Bronze → Silver para todo o período disponível nos dois raw files.

    Execução:
        pyflyte run workflows/flyte_bronze_to_silver.py bronze_to_silver_full
    """
    transform_consumo_silver_full()
    transform_preco_silver_full()
