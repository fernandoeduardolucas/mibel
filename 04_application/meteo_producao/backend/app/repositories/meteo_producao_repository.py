from __future__ import annotations

from datetime import date, datetime

from app.config import BASE_QUERY, GOLD_TABLE
from app.db.trino_client import TrinoClient
from app.models.meteo_point import MeteoPoint


class MeteoProducaoRepository:
    """Camada repositório: traduz rows Trino para MeteoPoint."""

    def __init__(self, client: TrinoClient):
        self.client = client

    def test_connection(self) -> dict[str, str | bool]:
        return self.client.test_connection()

    @staticmethod
    def _float(value: object | None) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        return float(text) if text else None

    @staticmethod
    def _int(value: object | None) -> int | None:
        if value is None:
            return None
        if isinstance(value, int):
            return value
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _bool(value: object | None) -> bool | None:
        if isinstance(value, bool):
            return value
        if value is None:
            return None
        return str(value).strip().lower() == "true"

    @staticmethod
    def _parse_date(value: object) -> date:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        raw = str(value).strip()[:10]
        return date.fromisoformat(raw)

    def list_daily(self) -> list[MeteoPoint]:
        rows = self.client.run_query(BASE_QUERY)
        points: list[MeteoPoint] = []
        for row in rows:
            raw_date = row.get("data_dia")
            if raw_date is None:
                continue
            points.append(MeteoPoint(
                data_dia=self._parse_date(raw_date),
                year=self._int(row.get("year")),
                month=self._int(row.get("month")),
                dia_semana=self._int(row.get("dia_semana")),
                is_weekend=self._bool(row.get("is_weekend")),
                estacao=self._int(row.get("estacao")),
                temperature_mean_c=self._float(row.get("temperature_mean_c")),
                temperature_min_c=self._float(row.get("temperature_min_c")),
                temperature_max_c=self._float(row.get("temperature_max_c")),
                precipitation_total_mm=self._float(row.get("precipitation_total_mm")),
                wind_speed_mean_ms=self._float(row.get("wind_speed_mean_ms")),
                wind_speed_max_ms=self._float(row.get("wind_speed_max_ms")),
                radiation_mean_wm2=self._float(row.get("radiation_mean_wm2")),
                radiation_total_kwh_m2=self._float(row.get("radiation_total_kwh_m2")),
                cloud_cover_mean_pct=self._float(row.get("cloud_cover_mean_pct")),
                producao_total_daily_mwh=self._float(row.get("producao_total_daily_mwh")),
                consumo_total_daily_mwh=self._float(row.get("consumo_total_daily_mwh")),
                saldo_daily_mwh=self._float(row.get("saldo_daily_mwh")),
                preco_spot_medio_eur_mwh=self._float(row.get("preco_spot_medio_eur_mwh")),
                preco_spot_max_eur_mwh=self._float(row.get("preco_spot_max_eur_mwh")),
                preco_spot_min_eur_mwh=self._float(row.get("preco_spot_min_eur_mwh")),
                temp_lag_1d=self._float(row.get("temp_lag_1d")),
                wind_lag_1d=self._float(row.get("wind_lag_1d")),
                radiation_lag_1d=self._float(row.get("radiation_lag_1d")),
                producao_lag_1d=self._float(row.get("producao_lag_1d")),
                preco_lag_1d=self._float(row.get("preco_lag_1d")),
                temp_rolling_7d_avg=self._float(row.get("temp_rolling_7d_avg")),
                wind_rolling_7d_avg=self._float(row.get("wind_rolling_7d_avg")),
                radiation_rolling_7d_avg=self._float(row.get("radiation_rolling_7d_avg")),
                producao_rolling_7d_avg=self._float(row.get("producao_rolling_7d_avg")),
            ))
        return points
