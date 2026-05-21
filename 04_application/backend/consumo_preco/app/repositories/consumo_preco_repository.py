"""Repositório — consultas à tabela Gold dp_energy_market_hourly."""
from __future__ import annotations

from app.config import GOLD_TABLE
from app.db.trino_client import TrinoClient


class ConsumoPrecoeRepository:
    def __init__(self) -> None:
        self._client = TrinoClient()

    def get_overview(self) -> dict:
        rows = self._client.run_query(f"""
            SELECT
                COUNT(*)                                                    AS registos,
                CAST(MIN(ts_utc) AS VARCHAR)                                AS inicio,
                CAST(MAX(ts_utc) AS VARCHAR)                                AS fim,
                ROUND(SUM(consumo_total), 3)                                AS consumo_total_mwh,
                ROUND(SUM(consumo_total * market_price_pt), 2)              AS custo_estimado_total_eur,
                ROUND(AVG(market_price_pt), 4)                              AS preco_medio_simples_eur_mwh,
                ROUND(
                    SUM(consumo_total * market_price_pt)
                    / NULLIF(SUM(consumo_total), 0),
                4)                                                          AS preco_medio_ponderado_eur_mwh,
                CAST(MAX(ts_utc) AS VARCHAR)                                AS ultimo_ts
            FROM {GOLD_TABLE}
        """)
        return rows[0] if rows else {}

    def get_timeseries_daily(self) -> list[dict]:
        return self._client.run_query(f"""
            SELECT
                CAST(CAST(ts_utc AS DATE) AS VARCHAR)                       AS periodo,
                ROUND(SUM(consumo_total), 3)                                AS consumo_mwh,
                ROUND(SUM(consumo_total * market_price_pt), 2)              AS custo_estimado_eur,
                ROUND(AVG(market_price_pt), 4)                              AS preco_medio_simples_eur_mwh,
                ROUND(
                    SUM(consumo_total * market_price_pt)
                    / NULLIF(SUM(consumo_total), 0),
                4)                                                          AS preco_medio_ponderado_eur_mwh,
                COUNT(*)                                                    AS leituras
            FROM {GOLD_TABLE}
            GROUP BY CAST(ts_utc AS DATE)
            ORDER BY 1
        """)

    def get_timeseries_monthly(self) -> list[dict]:
        return self._client.run_query(f"""
            SELECT
                CAST(year AS VARCHAR) || '-' || LPAD(CAST(month AS VARCHAR), 2, '0')
                                                                            AS periodo,
                ROUND(SUM(consumo_total), 3)                                AS consumo_mwh,
                ROUND(SUM(consumo_total * market_price_pt), 2)              AS custo_estimado_eur,
                ROUND(AVG(market_price_pt), 4)                              AS preco_medio_simples_eur_mwh,
                ROUND(
                    SUM(consumo_total * market_price_pt)
                    / NULLIF(SUM(consumo_total), 0),
                4)                                                          AS preco_medio_ponderado_eur_mwh,
                COUNT(*)                                                    AS leituras
            FROM {GOLD_TABLE}
            GROUP BY year, month
            ORDER BY year, month
        """)

    def get_analytics(self) -> dict:
        perfil_horario = self._client.run_query(f"""
            SELECT
                hora,
                ROUND(AVG(consumo_total), 3)    AS consumo_medio_mwh,
                ROUND(AVG(market_price_pt), 4)  AS preco_medio_eur_mwh
            FROM {GOLD_TABLE}
            WHERE hora IS NOT NULL
            GROUP BY hora
            ORDER BY hora
        """)
        perfil_semanal = self._client.run_query(f"""
            SELECT
                dia_semana,
                ROUND(AVG(consumo_total), 3)    AS consumo_medio_mwh,
                ROUND(AVG(market_price_pt), 4)  AS preco_medio_eur_mwh
            FROM {GOLD_TABLE}
            WHERE dia_semana IS NOT NULL
            GROUP BY dia_semana
            ORDER BY dia_semana
        """)
        return {
            "perfil_horario": perfil_horario,
            "perfil_semanal": perfil_semanal,
        }

    def test_connection(self) -> dict:
        try:
            rows = self._client.run_query(f"SELECT COUNT(*) AS total FROM {GOLD_TABLE}")
            return {"status": "ok", "total_rows": rows[0]["total"] if rows else 0, "table": GOLD_TABLE}
        except Exception as exc:
            return {"status": "error", "error": str(exc), "table": GOLD_TABLE}
