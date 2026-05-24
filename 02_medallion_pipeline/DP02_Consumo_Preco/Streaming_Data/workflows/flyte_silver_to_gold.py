"""
Workflow Flyte: Silver → Gold — Streaming_Data (DP-02 API).

Constrói os dois produtos Gold sobre todo o histórico Silver API:
  - dp_energy_market_api_hourly      : produto analítico (consumo + preço + features)
  - feat_load_forecasting_api_hourly : feature table ML (+ target consumo_next_hour)

Schema idêntico ao pipeline estático — permite comparação direta entre fontes.

As window functions operam sobre o histórico completo para garantir lags corretos.

Execução:
    pyflyte run workflows/flyte_silver_to_gold.py silver_to_gold_api_full
"""

from __future__ import annotations

import os

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
        request_timeout=300,
    )


def _exec(cur, sql: str) -> None:
    cur.execute(sql)
    cur.fetchall()


# ---------------------------------------------------------------------------
# Task 1: produto analítico principal — histórico completo
# ---------------------------------------------------------------------------
@task(retries=3)
def build_dp_energy_market_api_full() -> int:
    """
    Constrói gold.dp_energy_market_api_hourly para todo o histórico Silver API.

    Passos:
    1. INNER JOIN Silver consumo_api_hourly × preco_api_hourly por ts_utc
    2. Features calendário: hora (0-23), dia_semana (0=Segunda), is_weekend
    3. Lags: consumo_lag_1h, consumo_lag_24h, price_lag_1h
    4. Rolling averages 24h: consumo e preço

    Idempotente: limpa e volta a materializar todo o histórico.
    """
    conn = _trino_conn()
    cur  = conn.cursor()
    try:
        _exec(cur, "DELETE FROM iceberg.gold.dp_energy_market_api_hourly WHERE 1=1")

        _exec(cur, """
            INSERT INTO iceberg.gold.dp_energy_market_api_hourly (
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
                FROM iceberg.silver.consumo_api_hourly c
                INNER JOIN iceberg.silver.preco_api_hourly p ON c.ts_utc = p.ts_utc
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

        cur.execute("SELECT COUNT(*) FROM iceberg.gold.dp_energy_market_api_hourly")
        n = cur.fetchone()[0]
    finally:
        conn.close()
    print(f"[gold dp_energy_market_api full] {n} horas inseridas.")
    return n


# ---------------------------------------------------------------------------
# Task 2: feature table ML — histórico completo
# ---------------------------------------------------------------------------
@task(retries=3)
def build_feat_load_forecasting_api_full(upstream_rows: int) -> int:
    """
    Constrói gold.feat_load_forecasting_api_hourly.

    Derivada de dp_energy_market_api_hourly com adição de:
      consumo_next_hour = LEAD(consumo_total, 1)

    Última linha (sem target) e linhas com lags nulos são excluídas.
    O parâmetro upstream_rows cria dependência explícita sobre o produto analítico.

    Idempotente: limpa e volta a materializar.
    """
    _ = upstream_rows

    conn = _trino_conn()
    cur  = conn.cursor()
    try:
        _exec(cur, "DELETE FROM iceberg.gold.feat_load_forecasting_api_hourly WHERE 1=1")

        _exec(cur, """
            INSERT INTO iceberg.gold.feat_load_forecasting_api_hourly (
                ts_utc, consumo_total, market_price_pt,
                hora, dia_semana, is_weekend,
                consumo_lag_1h, consumo_lag_24h, price_lag_1h,
                rolling_avg_consumo_24h, rolling_avg_price_24h,
                consumo_next_hour, process_date, year, month
            )
            WITH with_lead AS (
                SELECT
                    ts_utc, consumo_total, market_price_pt,
                    hora, dia_semana, is_weekend,
                    consumo_lag_1h, consumo_lag_24h, price_lag_1h,
                    rolling_avg_consumo_24h, rolling_avg_price_24h,
                    LEAD(consumo_total, 1) OVER (ORDER BY ts_utc) AS consumo_next_hour,
                    process_date, year, month
                FROM iceberg.gold.dp_energy_market_api_hourly
            )
            SELECT
                ts_utc, consumo_total, market_price_pt,
                hora, dia_semana, is_weekend,
                consumo_lag_1h, consumo_lag_24h, price_lag_1h,
                rolling_avg_consumo_24h, rolling_avg_price_24h,
                consumo_next_hour, process_date, year, month
            FROM with_lead
            WHERE consumo_next_hour IS NOT NULL
              AND consumo_lag_1h    IS NOT NULL
              AND consumo_lag_24h   IS NOT NULL
              AND price_lag_1h      IS NOT NULL
        """)

        cur.execute("SELECT COUNT(*) FROM iceberg.gold.feat_load_forecasting_api_hourly")
        n = cur.fetchone()[0]
    finally:
        conn.close()
    print(f"[gold feat_load_forecasting_api full] {n} exemplos inseridos.")
    return n


# ---------------------------------------------------------------------------
# Workflow
# ---------------------------------------------------------------------------
@workflow
def silver_to_gold_api_full() -> None:
    """
    Constrói os dois produtos Gold API para todo o período Silver disponível.

    Execução:
        pyflyte run workflows/flyte_silver_to_gold.py silver_to_gold_api_full
    """
    dp_rows = build_dp_energy_market_api_full()
    build_feat_load_forecasting_api_full(upstream_rows=dp_rows)
