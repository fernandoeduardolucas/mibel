from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]

GOLD_SQL_PATH = (
    PROJECT_ROOT
    / "02_medallion_pipeline/producao_consumo/03_gold/sql/01_gold_trino.sql"
)

TRINO_HOST = "localhost"
TRINO_PORT = 8080
TRINO_USER = "trino"
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "gold"

GOLD_TABLE = "iceberg.gold.producao_vs_consumo_hourly"

BASE_QUERY = f"""
SELECT
    timestamp_utc,
    consumo_total_kwh,
    producao_total_kwh,
    producao_dgm_kwh,
    producao_pre_kwh,
    saldo_kwh,
    ratio_producao_consumo,
    flag_defice,
    flag_excedente,
    flag_missing_source
FROM {GOLD_TABLE}
ORDER BY timestamp_utc
"""

CACHE_TTL_SECONDS = 60
HOST = "0.0.0.0"
PORT = 8081
