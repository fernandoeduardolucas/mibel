from __future__ import annotations

import time
from collections import defaultdict
from datetime import date

from app.config import CACHE_TTL_SECONDS
from app.models.meteo_point import MeteoPoint
from app.repositories.meteo_producao_repository import MeteoProducaoRepository


class MeteoProducaoService:
    """Cache + analytics for dp_meteo_producao_daily_features."""

    def __init__(self, repository: MeteoProducaoRepository):
        self.repository = repository
        self._cache: list[MeteoPoint] | None = None
        self._cache_loaded_at: float | None = None

    def _refresh_if_needed(self) -> None:
        now = time.monotonic()
        stale = (
            self._cache is None
            or self._cache_loaded_at is None
            or (now - self._cache_loaded_at) >= CACHE_TTL_SECONDS
        )
        if stale:
            self._cache = self.repository.list_daily()
            self._cache_loaded_at = now

    def _point_to_dict(self, p: MeteoPoint) -> dict:
        return {
            "data_dia": p.data_dia.isoformat(),
            "year": p.year,
            "month": p.month,
            "dia_semana": p.dia_semana,
            "is_weekend": p.is_weekend,
            "estacao": p.estacao,
            "temperature_mean_c": p.temperature_mean_c,
            "temperature_min_c": p.temperature_min_c,
            "temperature_max_c": p.temperature_max_c,
            "precipitation_total_mm": p.precipitation_total_mm,
            "wind_speed_mean_ms": p.wind_speed_mean_ms,
            "wind_speed_max_ms": p.wind_speed_max_ms,
            "radiation_mean_wm2": p.radiation_mean_wm2,
            "radiation_total_kwh_m2": p.radiation_total_kwh_m2,
            "cloud_cover_mean_pct": p.cloud_cover_mean_pct,
            "producao_total_daily_mwh": p.producao_total_daily_mwh,
            "consumo_total_daily_mwh": p.consumo_total_daily_mwh,
            "saldo_daily_mwh": p.saldo_daily_mwh,
            "preco_spot_medio_eur_mwh": p.preco_spot_medio_eur_mwh,
            "preco_spot_max_eur_mwh": p.preco_spot_max_eur_mwh,
            "preco_spot_min_eur_mwh": p.preco_spot_min_eur_mwh,
            "temp_lag_1d": p.temp_lag_1d,
            "wind_lag_1d": p.wind_lag_1d,
            "radiation_lag_1d": p.radiation_lag_1d,
            "producao_lag_1d": p.producao_lag_1d,
            "preco_lag_1d": p.preco_lag_1d,
            "temp_rolling_7d_avg": p.temp_rolling_7d_avg,
            "wind_rolling_7d_avg": p.wind_rolling_7d_avg,
            "radiation_rolling_7d_avg": p.radiation_rolling_7d_avg,
            "producao_rolling_7d_avg": p.producao_rolling_7d_avg,
        }

    def daily(self, start: date | None = None, end: date | None = None) -> list[dict]:
        self._refresh_if_needed()
        assert self._cache is not None
        return [
            self._point_to_dict(p)
            for p in self._cache
            if (start is None or p.data_dia >= start)
            and (end is None or p.data_dia <= end)
        ]

    def latest(self, n: int = 30) -> list[dict]:
        self._refresh_if_needed()
        assert self._cache is not None
        return [self._point_to_dict(p) for p in self._cache[-n:]]

    def correlations(self) -> dict:
        """Pearson r between each weather variable and both production and price."""
        self._refresh_if_needed()
        assert self._cache is not None

        weather_cols = [
            "temperature_mean_c", "precipitation_total_mm",
            "wind_speed_mean_ms", "radiation_mean_wm2", "cloud_cover_mean_pct",
        ]
        target_cols = ["producao_total_daily_mwh", "preco_spot_medio_eur_mwh"]

        data = defaultdict(list)
        for p in self._cache:
            for col in weather_cols + target_cols:
                data[col].append(getattr(p, col))

        def pearson(xs: list, ys: list) -> float | None:
            pairs = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
            if len(pairs) < 3:
                return None
            n = len(pairs)
            mean_x = sum(x for x, _ in pairs) / n
            mean_y = sum(y for _, y in pairs) / n
            num = sum((x - mean_x) * (y - mean_y) for x, y in pairs)
            den_x = (sum((x - mean_x) ** 2 for x, _ in pairs)) ** 0.5
            den_y = (sum((y - mean_y) ** 2 for _, y in pairs)) ** 0.5
            if den_x == 0 or den_y == 0:
                return None
            return round(num / (den_x * den_y), 4)

        result: dict[str, dict[str, float | None]] = {}
        for wcol in weather_cols:
            result[wcol] = {}
            for tcol in target_cols:
                result[wcol][tcol] = pearson(data[wcol], data[tcol])

        return result

    def analytics(self) -> dict:
        self._refresh_if_needed()
        assert self._cache is not None
        cache = self._cache
        n = len(cache)
        if n == 0:
            return {}

        def _avg(vals):
            filtered = [v for v in vals if v is not None]
            return round(sum(filtered) / len(filtered), 4) if filtered else None

        return {
            "total_dias": n,
            "data_inicio": cache[0].data_dia.isoformat() if cache else None,
            "data_fim": cache[-1].data_dia.isoformat() if cache else None,
            "temperatura_media_c": _avg(p.temperature_mean_c for p in cache),
            "precipitacao_media_mm": _avg(p.precipitation_total_mm for p in cache),
            "vento_medio_ms": _avg(p.wind_speed_mean_ms for p in cache),
            "radiacao_media_wm2": _avg(p.radiation_mean_wm2 for p in cache),
            "producao_media_diaria_mwh": _avg(p.producao_total_daily_mwh for p in cache),
            "preco_medio_eur_mwh": _avg(p.preco_spot_medio_eur_mwh for p in cache),
            "correlacoes": self.correlations(),
        }

    def test_database_connection(self) -> dict[str, str | bool]:
        return self.repository.test_connection()
