from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]

GOLD_SQL_PATH = (
    PROJECT_ROOT
    / "02_medallion_pipeline/meteo_producao/03_gold/sql/01_gold_trino.sql"
)

TRINO_HOST    = "localhost"
TRINO_PORT    = 8080
TRINO_USER    = "trino"
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA  = "gold"

GOLD_TABLE = "iceberg.gold.dp_meteo_producao_daily_features"

BASE_QUERY = f"""
SELECT
    CAST(data_dia AS VARCHAR)   AS data_dia,
    year,
    month,
    dia_semana,
    is_weekend,
    estacao,
    temperature_mean_c,
    temperature_min_c,
    temperature_max_c,
    precipitation_total_mm,
    wind_speed_mean_ms,
    wind_speed_max_ms,
    radiation_mean_wm2,
    radiation_total_kwh_m2,
    cloud_cover_mean_pct,
    producao_total_daily_mwh,
    consumo_total_daily_mwh,
    saldo_daily_mwh,
    preco_spot_medio_eur_mwh,
    preco_spot_max_eur_mwh,
    preco_spot_min_eur_mwh,
    temp_lag_1d,
    wind_lag_1d,
    radiation_lag_1d,
    producao_lag_1d,
    preco_lag_1d,
    temp_rolling_7d_avg,
    wind_rolling_7d_avg,
    radiation_rolling_7d_avg,
    producao_rolling_7d_avg
FROM {GOLD_TABLE}
ORDER BY data_dia
"""

CACHE_TTL_SECONDS = 300  # 5 minutes — daily data, less volatile than hourly
HOST = "0.0.0.0"
PORT = 8083
