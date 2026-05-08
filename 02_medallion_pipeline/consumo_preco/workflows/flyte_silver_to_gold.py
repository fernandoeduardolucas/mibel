"""
Workflow Flyte: transformação Silver → Gold — consumo_preco.

Constrói os dois data products da camada Gold:
  - dp_energy_market_hourly      : produto analítico principal (consumo + preço + features)
  - feat_load_forecasting_hourly : feature table para ML (inclui target consumo_next_hour)

As window functions (LAG, AVG rolling) operam sobre o histórico completo da Silver
para garantir que os valores de lag/rolling não ficam truncados nas fronteiras de mês.
Só a inserção final é filtrada para o year/month solicitado.

A feature table é derivada do produto analítico Gold para manter um único
contrato semântico entre serving e ML.

Execução mensal:
    pyflyte run workflows/flyte_silver_to_gold.py silver_to_gold --year 2023 --month 1

Execução completa:
    pyflyte run workflows/flyte_silver_to_gold.py silver_to_gold_full
"""

from __future__ import annotations

import os
from datetime import date

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
        schema="gold",
    )


def _exec(cur, sql: str) -> None:
    cur.execute(sql)
    cur.fetchall()


# ---------------------------------------------------------------------------
# Task 1: produto analítico principal
# ---------------------------------------------------------------------------
@task(retries=3)
def build_dp_energy_market(year: int, month: int) -> int:
    """
    Constrói gold.dp_energy_market_hourly para o year/month indicado.

    Passos:
    1. JOIN INNER Silver consumo × Silver preço por ts_utc
    2. Features de calendário: hora, dia_semana, is_weekend
    3. Lags: consumo_lag_1h (LAG 1), consumo_lag_24h (LAG 24), price_lag_1h (LAG 1)
    4. Rolling averages de 24h: consumo e preço (janela de 24 registos)

    As window functions usam histórico completo (sem filtro de data) para que
    lag e rolling sejam correctos nas primeiras horas do mês.
    A inserção final filtra apenas o year/month alvo (partition pruning).

    Idempotente: DELETE da partição year/month antes de INSERT.
    """
    conn = _trino_conn()
    cur = conn.cursor()

    _exec(cur, f"""
        DELETE FROM iceberg.gold.dp_energy_market_hourly
        WHERE year = {year} AND month = {month}
    """)

    _exec(cur, f"""
        INSERT INTO iceberg.gold.dp_energy_market_hourly (
            ts_utc, consumo_total, market_price_pt,
            hora, dia_semana, is_weekend,
            consumo_lag_1h, consumo_lag_24h, price_lag_1h,
            rolling_avg_consumo_24h, rolling_avg_price_24h,
            process_date, year, month
        )
        WITH joined AS (
            SELECT
                c.ts_utc,
                c.total_mwh                                  AS consumo_total,
                p.price_portugal_eur_mwh                     AS market_price_pt,
                HOUR(c.ts_utc)                               AS hora,
                DAY_OF_WEEK(c.ts_utc) - 1                   AS dia_semana,
                DAY_OF_WEEK(c.ts_utc) >= 6                  AS is_weekend,
                YEAR(c.ts_utc)                               AS year,
                MONTH(c.ts_utc)                              AS month
            FROM iceberg.silver.consumo_hourly c
            INNER JOIN iceberg.silver.preco_hourly p ON c.ts_utc = p.ts_utc
        ),
        with_windows AS (
            SELECT
                *,
                LAG(consumo_total, 1)  OVER (ORDER BY ts_utc) AS consumo_lag_1h,
                LAG(consumo_total, 24) OVER (ORDER BY ts_utc) AS consumo_lag_24h,
                LAG(market_price_pt, 1) OVER (ORDER BY ts_utc) AS price_lag_1h,
                AVG(consumo_total) OVER (
                    ORDER BY ts_utc ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
                ) AS rolling_avg_consumo_24h,
                AVG(market_price_pt) OVER (
                    ORDER BY ts_utc ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
                ) AS rolling_avg_price_24h
            FROM joined
        )
        SELECT
            ts_utc, consumo_total, market_price_pt,
            hora, dia_semana, is_weekend,
            consumo_lag_1h, consumo_lag_24h, price_lag_1h,
            rolling_avg_consumo_24h, rolling_avg_price_24h,
            CURRENT_DATE AS process_date,
            year, month
        FROM with_windows
        WHERE year = {year} AND month = {month}
    """)

    cur.execute(f"""
        SELECT COUNT(*) FROM iceberg.gold.dp_energy_market_hourly
        WHERE year = {year} AND month = {month}
    """)
    n = cur.fetchone()[0]
    conn.close()
    print(f"[gold dp_energy_market] {n} horas inseridas para {year}-{month:02d}.")
    return n


# ---------------------------------------------------------------------------
# Task 2: feature table ML
# ---------------------------------------------------------------------------
@task(retries=3)
def build_feat_load_forecasting(year: int, month: int, upstream_rows: int) -> int:
    """
    Constrói gold.feat_load_forecasting_hourly para o year/month indicado.

    Derivada de dp_energy_market_hourly com adição da variável alvo:
      consumo_next_hour = LEAD(consumo_total, 1) — consumo da hora seguinte.

    A última linha de cada execução é descartada (sem target disponível).
    Linhas com nulos nas features principais também são excluídas
    (primeiras horas sem histórico suficiente para lags/rolling).

    Idempotente: DELETE da partição year/month antes de INSERT.
    O parâmetro upstream_rows cria dependência explícita sobre o produto analítico.
    """
    _ = upstream_rows  # dependência explícita para sequenciar após dp_energy_market

    conn = _trino_conn()
    cur = conn.cursor()

    _exec(cur, f"""
        DELETE FROM iceberg.gold.feat_load_forecasting_hourly
        WHERE year = {year} AND month = {month}
    """)

    _exec(cur, f"""
        INSERT INTO iceberg.gold.feat_load_forecasting_hourly (
            ts_utc, consumo_total, market_price_pt,
            hora, dia_semana, is_weekend,
            consumo_lag_1h, consumo_lag_24h, price_lag_1h,
            rolling_avg_consumo_24h, rolling_avg_price_24h,
            consumo_next_hour, process_date, year, month
        )
        WITH with_lead AS (
            SELECT
                ts_utc,
                consumo_total,
                market_price_pt,
                hora,
                dia_semana,
                is_weekend,
                consumo_lag_1h,
                consumo_lag_24h,
                price_lag_1h,
                rolling_avg_consumo_24h,
                rolling_avg_price_24h,
                LEAD(consumo_total, 1) OVER (ORDER BY ts_utc) AS consumo_next_hour,
                process_date,
                year,
                month
            FROM iceberg.gold.dp_energy_market_hourly
        )
        SELECT
            ts_utc, consumo_total, market_price_pt,
            hora, dia_semana, is_weekend,
            consumo_lag_1h, consumo_lag_24h, price_lag_1h,
            rolling_avg_consumo_24h, rolling_avg_price_24h,
            consumo_next_hour, process_date, year, month
        FROM with_lead
        WHERE year = {year} AND month = {month}
          AND consumo_next_hour IS NOT NULL
          AND consumo_lag_1h IS NOT NULL
          AND consumo_lag_24h IS NOT NULL
          AND price_lag_1h IS NOT NULL
    """)

    cur.execute(f"""
        SELECT COUNT(*) FROM iceberg.gold.feat_load_forecasting_hourly
        WHERE year = {year} AND month = {month}
    """)
    n = cur.fetchone()[0]
    conn.close()
    print(f"[gold feat_load_forecasting] {n} exemplos inseridos para {year}-{month:02d}.")
    return n


# ---------------------------------------------------------------------------
# Task 3: produto analítico principal completo
# ---------------------------------------------------------------------------
@task(retries=3)
def build_dp_energy_market_full() -> int:
    """
    Constrói gold.dp_energy_market_hourly para todo o histórico Silver.

    Idempotente para execução full: limpa o produto Gold e volta a materializar
    todas as horas comuns entre consumo e preços.
    """
    conn = _trino_conn()
    cur = conn.cursor()

    _exec(cur, "DELETE FROM iceberg.gold.dp_energy_market_hourly WHERE 1=1")

    _exec(cur, """
        INSERT INTO iceberg.gold.dp_energy_market_hourly (
            ts_utc, consumo_total, market_price_pt,
            hora, dia_semana, is_weekend,
            consumo_lag_1h, consumo_lag_24h, price_lag_1h,
            rolling_avg_consumo_24h, rolling_avg_price_24h,
            process_date, year, month
        )
        WITH joined AS (
            SELECT
                c.ts_utc,
                c.total_mwh                                  AS consumo_total,
                p.price_portugal_eur_mwh                     AS market_price_pt,
                HOUR(c.ts_utc)                               AS hora,
                DAY_OF_WEEK(c.ts_utc) - 1                   AS dia_semana,
                DAY_OF_WEEK(c.ts_utc) >= 6                  AS is_weekend,
                YEAR(c.ts_utc)                               AS year,
                MONTH(c.ts_utc)                              AS month
            FROM iceberg.silver.consumo_hourly c
            INNER JOIN iceberg.silver.preco_hourly p ON c.ts_utc = p.ts_utc
        ),
        with_windows AS (
            SELECT
                *,
                LAG(consumo_total, 1)  OVER (ORDER BY ts_utc) AS consumo_lag_1h,
                LAG(consumo_total, 24) OVER (ORDER BY ts_utc) AS consumo_lag_24h,
                LAG(market_price_pt, 1) OVER (ORDER BY ts_utc) AS price_lag_1h,
                AVG(consumo_total) OVER (
                    ORDER BY ts_utc ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
                ) AS rolling_avg_consumo_24h,
                AVG(market_price_pt) OVER (
                    ORDER BY ts_utc ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
                ) AS rolling_avg_price_24h
            FROM joined
        )
        SELECT
            ts_utc, consumo_total, market_price_pt,
            hora, dia_semana, is_weekend,
            consumo_lag_1h, consumo_lag_24h, price_lag_1h,
            rolling_avg_consumo_24h, rolling_avg_price_24h,
            CURRENT_DATE AS process_date,
            year, month
        FROM with_windows
    """)

    cur.execute("SELECT COUNT(*) FROM iceberg.gold.dp_energy_market_hourly")
    n = cur.fetchone()[0]
    conn.close()
    print(f"[gold dp_energy_market full] {n} horas inseridas.")
    return n


# ---------------------------------------------------------------------------
# Task 4: feature table ML completa
# ---------------------------------------------------------------------------
@task(retries=3)
def build_feat_load_forecasting_full(upstream_rows: int) -> int:
    """
    Constrói gold.feat_load_forecasting_hourly para todo o histórico Gold.

    O target consumo_next_hour é calculado sobre o histórico completo, evitando
    truncagens artificiais nas fronteiras mensais.
    """
    _ = upstream_rows

    conn = _trino_conn()
    cur = conn.cursor()

    _exec(cur, "DELETE FROM iceberg.gold.feat_load_forecasting_hourly WHERE 1=1")

    _exec(cur, """
        INSERT INTO iceberg.gold.feat_load_forecasting_hourly (
            ts_utc, consumo_total, market_price_pt,
            hora, dia_semana, is_weekend,
            consumo_lag_1h, consumo_lag_24h, price_lag_1h,
            rolling_avg_consumo_24h, rolling_avg_price_24h,
            consumo_next_hour, process_date, year, month
        )
        WITH with_lead AS (
            SELECT
                ts_utc,
                consumo_total,
                market_price_pt,
                hora,
                dia_semana,
                is_weekend,
                consumo_lag_1h,
                consumo_lag_24h,
                price_lag_1h,
                rolling_avg_consumo_24h,
                rolling_avg_price_24h,
                LEAD(consumo_total, 1) OVER (ORDER BY ts_utc) AS consumo_next_hour,
                process_date,
                year,
                month
            FROM iceberg.gold.dp_energy_market_hourly
        )
        SELECT
            ts_utc, consumo_total, market_price_pt,
            hora, dia_semana, is_weekend,
            consumo_lag_1h, consumo_lag_24h, price_lag_1h,
            rolling_avg_consumo_24h, rolling_avg_price_24h,
            consumo_next_hour, process_date, year, month
        FROM with_lead
        WHERE consumo_next_hour IS NOT NULL
          AND consumo_lag_1h IS NOT NULL
          AND consumo_lag_24h IS NOT NULL
          AND price_lag_1h IS NOT NULL
    """)

    cur.execute("SELECT COUNT(*) FROM iceberg.gold.feat_load_forecasting_hourly")
    n = cur.fetchone()[0]
    conn.close()
    print(f"[gold feat_load_forecasting full] {n} exemplos inseridos.")
    return n


# ---------------------------------------------------------------------------
# Workflows
# ---------------------------------------------------------------------------
@workflow
def silver_to_gold(year: int = 2023, month: int = 1) -> None:
    """
    Constrói os dois produtos Gold para o year/month indicado.
    A feature table depende do produto analítico e corre depois dele.

    Execução:
        pyflyte run workflows/flyte_silver_to_gold.py silver_to_gold --year 2023 --month 1
    """
    dp_rows = build_dp_energy_market(year=year, month=month)
    build_feat_load_forecasting(year=year, month=month, upstream_rows=dp_rows)


@workflow
def silver_to_gold_full() -> None:
    """
    Constrói os dois produtos Gold para todo o período disponível na Silver.

    Execução:
        pyflyte run workflows/flyte_silver_to_gold.py silver_to_gold_full
    """
    dp_rows = build_dp_energy_market_full()
    build_feat_load_forecasting_full(upstream_rows=dp_rows)
