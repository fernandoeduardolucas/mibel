from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass
class MeteoPoint:
    data_dia: date
    year: int | None
    month: int | None
    dia_semana: int | None
    is_weekend: bool | None
    estacao: int | None
    temperature_mean_c: float | None
    temperature_min_c: float | None
    temperature_max_c: float | None
    precipitation_total_mm: float | None
    wind_speed_mean_ms: float | None
    wind_speed_max_ms: float | None
    radiation_mean_wm2: float | None
    radiation_total_kwh_m2: float | None
    cloud_cover_mean_pct: float | None
    producao_total_daily_mwh: float | None
    consumo_total_daily_mwh: float | None
    saldo_daily_mwh: float | None
    preco_spot_medio_eur_mwh: float | None
    preco_spot_max_eur_mwh: float | None
    preco_spot_min_eur_mwh: float | None
    temp_lag_1d: float | None
    wind_lag_1d: float | None
    radiation_lag_1d: float | None
    producao_lag_1d: float | None
    preco_lag_1d: float | None
    temp_rolling_7d_avg: float | None
    wind_rolling_7d_avg: float | None
    radiation_rolling_7d_avg: float | None
    producao_rolling_7d_avg: float | None
