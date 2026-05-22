#!/usr/bin/env python3
"""
Silver → Gold — consumo_preco.

Constrói dois produtos Gold sobre todo o histórico Silver:
  - dp_energy_market_hourly       : produto analítico (consumo + preço + features)
  - feat_load_forecasting_hourly  : feature table para ML (inclui target consumo_next_hour)

As window functions (LAG, rolling AVG) operam sobre o histórico completo da Silver
para garantir que os valores de lag não ficam truncados nas fronteiras de mês.

Uso:
    python gold.py [--trino-host localhost]
"""

from __future__ import annotations

import argparse
import os

import trino


def _trino(host: str, port: int = 8080) -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=host, port=port, user="admin",
        catalog="iceberg", schema="gold",
        request_timeout=900,
    )


def _exec(cur, sql: str) -> None:
    cur.execute(sql)
    cur.fetchall()


# ---------------------------------------------------------------------------
# Produto analítico principal
# ---------------------------------------------------------------------------

def build_dp_energy_market(trino_host: str, port: int = 8080) -> int:
    """
    Constrói gold.dp_energy_market_hourly para todo o histórico Silver.

    JOIN INNER consumo × preço por ts_utc, depois:
    - Features de calendário: hora (0-23), dia_semana (0=Seg), is_weekend
    - Lags: consumo_lag_1h, consumo_lag_24h, price_lag_1h
    - Rolling 24h: rolling_avg_consumo_24h, rolling_avg_price_24h

    Idempotente: limpa a tabela e volta a materializar.
    """
    conn = _trino(trino_host, port)
    cur = conn.cursor()
    try:
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
                    c.total_mwh                             AS consumo_total,
                    p.price_portugal_eur_mwh                AS market_price_pt,
                    HOUR(c.ts_utc)                          AS hora,
                    DAY_OF_WEEK(c.ts_utc) - 1              AS dia_semana,
                    DAY_OF_WEEK(c.ts_utc) >= 6             AS is_weekend,
                    YEAR(c.ts_utc)                          AS year,
                    MONTH(c.ts_utc)                         AS month
                FROM iceberg.silver.consumo_hourly c
                INNER JOIN iceberg.silver.preco_hourly p ON c.ts_utc = p.ts_utc
            ),
            with_windows AS (
                SELECT
                    *,
                    LAG(consumo_total,   1) OVER (ORDER BY ts_utc) AS consumo_lag_1h,
                    LAG(consumo_total,  24) OVER (ORDER BY ts_utc) AS consumo_lag_24h,
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
    finally:
        conn.close()
    print(f"[gold] dp_energy_market_hourly: {n} horas inseridas.")
    return n


# ---------------------------------------------------------------------------
# Feature table para ML
# ---------------------------------------------------------------------------

def build_feat_load_forecasting(trino_host: str, port: int = 8080) -> int:
    """
    Constrói gold.feat_load_forecasting_hourly derivada do produto analítico.

    Adiciona consumo_next_hour = LEAD(consumo_total, 1) como variável alvo.
    Exclui linhas sem target (última hora) e linhas sem features de lag
    (primeiras 24 horas do histórico).
    Idempotente: limpa a tabela e volta a materializar.
    """
    conn = _trino(trino_host, port)
    cur = conn.cursor()
    try:
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
                    *,
                    LEAD(consumo_total, 1) OVER (ORDER BY ts_utc) AS consumo_next_hour
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
              AND consumo_lag_1h    IS NOT NULL
              AND consumo_lag_24h   IS NOT NULL
              AND price_lag_1h      IS NOT NULL
        """)
        cur.execute("SELECT COUNT(*) FROM iceberg.gold.feat_load_forecasting_hourly")
        n = cur.fetchone()[0]
    finally:
        conn.close()
    print(f"[gold] feat_load_forecasting_hourly: {n} exemplos inseridos.")
    return n


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Silver → Gold — consumo_preco")
    parser.add_argument("--trino-host", default=os.getenv("TRINO_HOST", "localhost"))
    parser.add_argument("--trino-port", type=int, default=int(os.getenv("TRINO_PORT", "8080")))
    args = parser.parse_args()

    build_dp_energy_market(args.trino_host, args.trino_port)
    build_feat_load_forecasting(args.trino_host, args.trino_port)


if __name__ == "__main__":
    main()
