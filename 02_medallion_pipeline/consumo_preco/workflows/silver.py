#!/usr/bin/env python3
"""
Bronze → Silver — consumo_preco.

Agrega consumo de 15 min para horário (kW → MWh) e normaliza preços
para timestamp UTC. Ambas as transformações são idempotentes (DELETE + INSERT).

Uso:
    python silver.py [--trino-host localhost]
"""

from __future__ import annotations

import argparse
import os

import trino


def _trino(host: str, port: int = 8080) -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=host, port=port, user="admin",
        catalog="iceberg", schema="silver",
        request_timeout=600,
    )


def _exec(cur, sql: str) -> None:
    cur.execute(sql)
    cur.fetchall()


# ---------------------------------------------------------------------------
# Consumo: Bronze → Silver (histórico completo)
# ---------------------------------------------------------------------------

def transform_consumo_full(trino_host: str, port: int = 8080) -> int:
    """
    Agrega todo o histórico de consumo Bronze → Silver.
    Granularidade: 15 min → 1 hora. Unidades: kW → MWh (÷ 1000).
    Idempotente: limpa a tabela Silver antes de materializar.
    """
    conn = _trino(trino_host, port)
    cur = conn.cursor()
    try:
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
    finally:
        conn.close()
    print(f"[silver] consumo_hourly: {n} horas inseridas.")
    return n


# ---------------------------------------------------------------------------
# Preços: Bronze → Silver (histórico completo)
# ---------------------------------------------------------------------------

def transform_preco_full(trino_host: str, port: int = 8080) -> int:
    """
    Normaliza todo o histórico de preços Bronze → Silver.
    Mapeamento: date_raw (VARCHAR) + hour (1-24) → ts_utc (TIMESTAMP UTC).
    Hora 25 (DST outono) é descartada com filtro hour BETWEEN 1 AND 24.
    GROUP BY absorve eventuais duplicados com AVG.
    Idempotente: limpa a tabela Silver antes de materializar.
    """
    conn = _trino(trino_host, port)
    cur = conn.cursor()
    try:
        _exec(cur, "DELETE FROM iceberg.silver.preco_hourly WHERE 1=1")
        _exec(cur, """
            INSERT INTO iceberg.silver.preco_hourly
                (ts_utc, price_portugal_eur_mwh, price_spain_eur_mwh, year, month)
            SELECT
                ts_utc,
                ROUND(AVG(price_portugal_raw), 2) AS price_portugal_eur_mwh,
                ROUND(AVG(price_spain_raw),    2) AS price_spain_eur_mwh,
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
    finally:
        conn.close()
    print(f"[silver] preco_hourly: {n} horas inseridas.")
    return n


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Bronze → Silver — consumo_preco")
    parser.add_argument("--trino-host", default=os.getenv("TRINO_HOST", "localhost"))
    parser.add_argument("--trino-port", type=int, default=int(os.getenv("TRINO_PORT", "8080")))
    args = parser.parse_args()

    transform_consumo_full(args.trino_host, args.trino_port)
    transform_preco_full(args.trino_host, args.trino_port)


if __name__ == "__main__":
    main()
