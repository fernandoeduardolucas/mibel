-- =============================================================================
-- SILVER DDL — meteo_producao
-- Entrada:  iceberg.bronze.meteo_open_meteo_hourly
-- Saída:    iceberg.silver.meteo_open_meteo_hourly
-- =============================================================================
-- Transformações:
--   1) Deduplicação por ts_utc (mantém registo mais recente por _ingested_at)
--   2) Validação de intervalos físicos razoáveis para Portugal
--   3) Flag de qualidade ('ok' | 'out_of_range' | 'null_values')
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.silver
WITH (location = 's3a://warehouse/silver/');

DROP TABLE IF EXISTS iceberg.silver.meteo_open_meteo_hourly;

CREATE TABLE iceberg.silver.meteo_open_meteo_hourly
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month'],
    location = 's3a://warehouse/silver/meteo_open_meteo_hourly/'
) AS
WITH deduped AS (
    SELECT
        ts_utc,
        year,
        month,
        day,
        hour,
        temperature_2m,
        precipitation,
        wind_speed_10m,
        shortwave_radiation,
        cloud_cover,
        latitude,
        longitude,
        elevation_m,
        _source_file,
        _ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY ts_utc
            ORDER BY _ingested_at DESC
        ) AS _pick_rank
    FROM iceberg.bronze.meteo_open_meteo_hourly
    WHERE ts_utc IS NOT NULL
),
validated AS (
    SELECT
        ts_utc,
        year,
        month,
        day,
        hour,
        temperature_2m,
        precipitation,
        wind_speed_10m,
        shortwave_radiation,
        cloud_cover,
        latitude,
        longitude,
        elevation_m,
        _source_file,
        _ingested_at,
        -- Quality flag: physical plausibility for mainland Portugal
        CASE
            WHEN temperature_2m IS NULL
              OR precipitation IS NULL
              OR wind_speed_10m IS NULL
              OR shortwave_radiation IS NULL
              OR cloud_cover IS NULL
            THEN 'null_values'
            WHEN temperature_2m   < -10.0 OR temperature_2m   > 50.0
              OR precipitation     < 0.0  OR precipitation     > 200.0
              OR wind_speed_10m    < 0.0  OR wind_speed_10m    > 80.0
              OR shortwave_radiation < 0.0
              OR cloud_cover       < 0.0  OR cloud_cover       > 100.0
            THEN 'out_of_range'
            ELSE 'ok'
        END AS _quality_flag
    FROM deduped
    WHERE _pick_rank = 1
)
SELECT
    ts_utc,
    year,
    month,
    day,
    hour,
    temperature_2m,
    precipitation,
    wind_speed_10m,
    shortwave_radiation,
    -- Convert W/m² accumulated per hour to kWh/m² (÷ 1000)
    shortwave_radiation / 1000.0 AS radiation_kwh_m2,
    cloud_cover,
    latitude,
    longitude,
    elevation_m,
    _source_file,
    CAST(_ingested_at AS VARCHAR) AS _ingested_at,
    _quality_flag
FROM validated;


-- =============================================================================
-- VALIDAÇÃO
-- =============================================================================

SELECT COUNT(*) AS linhas_silver
FROM iceberg.silver.meteo_open_meteo_hourly;

SELECT
    MIN(ts_utc) AS min_ts,
    MAX(ts_utc) AS max_ts
FROM iceberg.silver.meteo_open_meteo_hourly;

SELECT
    _quality_flag,
    COUNT(*) AS linhas
FROM iceberg.silver.meteo_open_meteo_hourly
GROUP BY _quality_flag
ORDER BY _quality_flag;

SELECT *
FROM iceberg.silver.meteo_open_meteo_hourly
WHERE _quality_flag <> 'ok'
LIMIT 10;
