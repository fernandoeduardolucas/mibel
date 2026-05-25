-- =============================================================================
-- Quality checks — Silver (meteo_producao)
-- Varredura única via CTE. Sem multiple scans nem GROUP BY top-level.
-- Retorna: check_name | status | valor_pct | threshold_pct | detalhe
-- =============================================================================

WITH
meteo AS (
    SELECT
        COUNT(*)                                                                                AS total,
        COUNT(DISTINCT ts_utc)                                                                  AS distinct_ts,
        SUM(CASE WHEN ts_utc IS NULL                                       THEN 1 ELSE 0 END)  AS null_ts,
        SUM(CASE WHEN temperature_2m IS NULL                               THEN 1 ELSE 0 END)  AS null_temp,
        SUM(CASE WHEN shortwave_radiation IS NULL                           THEN 1 ELSE 0 END)  AS null_radiation,
        SUM(CASE WHEN wind_speed_10m IS NULL                               THEN 1 ELSE 0 END)  AS null_wind,
        SUM(CASE WHEN temperature_2m < -10 OR temperature_2m > 50          THEN 1 ELSE 0 END)  AS bad_temp,
        SUM(CASE WHEN precipitation < 0 OR precipitation > 200             THEN 1 ELSE 0 END)  AS bad_precip,
        SUM(CASE WHEN wind_speed_10m < 0 OR wind_speed_10m > 80            THEN 1 ELSE 0 END)  AS bad_wind,
        SUM(CASE WHEN shortwave_radiation < 0                              THEN 1 ELSE 0 END)  AS bad_radiation,
        SUM(CASE WHEN cloud_cover < 0 OR cloud_cover > 100                 THEN 1 ELSE 0 END)  AS bad_cloud,
        SUM(CASE WHEN minute(ts_utc) <> 0 OR second(ts_utc) <> 0          THEN 1 ELSE 0 END)  AS bad_alignment,
        SUM(CASE WHEN _quality_flag = 'ok'           THEN 1 ELSE 0 END)                        AS ok_count,
        SUM(CASE WHEN _quality_flag = 'null_values'  THEN 1 ELSE 0 END)                        AS null_flag_count,
        SUM(CASE WHEN _quality_flag = 'out_of_range' THEN 1 ELSE 0 END)                        AS range_flag_count
    FROM iceberg.silver.meteo_open_meteo_hourly
)

-- 1. row_count > 0
SELECT
    'silver.meteo_open_meteo_hourly » row_count > 0'                          AS check_name,
    CASE WHEN total > 0 THEN 'PASS' ELSE 'FAIL' END                           AS status,
    CAST(total AS DECIMAL(18, 2))                                              AS valor_pct,
    1.0                                                                        AS threshold_pct,
    CAST(total AS VARCHAR) || ' horas'                                         AS detalhe
FROM meteo

UNION ALL

-- 2. null_rate: ts_utc
SELECT
    'silver.meteo_open_meteo_hourly » null_rate » ts_utc',
    CASE WHEN null_ts = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_ts / NULLIF(total, 0), 2),
    0.0,
    CAST(null_ts AS VARCHAR) || ' nulos em ' || CAST(total AS VARCHAR) || ' linhas'
FROM meteo

UNION ALL

-- 3. null_rate: temperature_2m
SELECT
    'silver.meteo_open_meteo_hourly » null_rate » temperature_2m',
    CASE WHEN null_temp = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_temp / NULLIF(total, 0), 2),
    0.0,
    CAST(null_temp AS VARCHAR) || ' nulos'
FROM meteo

UNION ALL

-- 4. null_rate: shortwave_radiation
SELECT
    'silver.meteo_open_meteo_hourly » null_rate » shortwave_radiation',
    CASE WHEN null_radiation = 0 THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * null_radiation / NULLIF(total, 0), 2),
    0.0,
    CAST(null_radiation AS VARCHAR) || ' nulos'
FROM meteo

UNION ALL

-- 5. null_rate: wind_speed_10m
SELECT
    'silver.meteo_open_meteo_hourly » null_rate » wind_speed_10m',
    CASE WHEN null_wind = 0 THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * null_wind / NULLIF(total, 0), 2),
    0.0,
    CAST(null_wind AS VARCHAR) || ' nulos'
FROM meteo

UNION ALL

-- 6. range: temperature_2m [-10, 50]
SELECT
    'silver.meteo_open_meteo_hourly » range » temperature_2m IN [-10, 50]',
    CASE WHEN bad_temp = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * bad_temp / NULLIF(total, 0), 4),
    0.0,
    CAST(bad_temp AS VARCHAR) || ' registos fora do intervalo físico [-10, 50] °C'
FROM meteo

UNION ALL

-- 7. range: precipitation [0, 200]
SELECT
    'silver.meteo_open_meteo_hourly » range » precipitation IN [0, 200]',
    CASE WHEN bad_precip = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * bad_precip / NULLIF(total, 0), 4),
    0.0,
    CAST(bad_precip AS VARCHAR) || ' registos fora do intervalo físico [0, 200] mm'
FROM meteo

UNION ALL

-- 8. range: wind_speed_10m [0, 80]
SELECT
    'silver.meteo_open_meteo_hourly » range » wind_speed_10m IN [0, 80]',
    CASE WHEN bad_wind = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * bad_wind / NULLIF(total, 0), 4),
    0.0,
    CAST(bad_wind AS VARCHAR) || ' registos fora do intervalo físico [0, 80] m/s'
FROM meteo

UNION ALL

-- 9. range: shortwave_radiation >= 0
SELECT
    'silver.meteo_open_meteo_hourly » range » shortwave_radiation >= 0',
    CASE WHEN bad_radiation = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * bad_radiation / NULLIF(total, 0), 4),
    0.0,
    CAST(bad_radiation AS VARCHAR) || ' registos com radiação negativa'
FROM meteo

UNION ALL

-- 10. range: cloud_cover [0, 100]
SELECT
    'silver.meteo_open_meteo_hourly » range » cloud_cover IN [0, 100]',
    CASE WHEN bad_cloud = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * bad_cloud / NULLIF(total, 0), 4),
    0.0,
    CAST(bad_cloud AS VARCHAR) || ' registos fora do intervalo [0, 100] %'
FROM meteo

UNION ALL

-- 11. uniqueness: ts_utc único (COUNT vs COUNT DISTINCT — sem GROUP BY)
SELECT
    'silver.meteo_open_meteo_hourly » uniqueness » ts_utc',
    CASE WHEN total = distinct_ts THEN 'PASS' ELSE 'FAIL' END,
    CAST(total - distinct_ts AS DECIMAL(18, 2)),
    0.0,
    CAST(total - distinct_ts AS VARCHAR) || ' ts_utc duplicados'
FROM meteo

UNION ALL

-- 12. temporal: ts_utc alinhado à hora
SELECT
    'silver.meteo_open_meteo_hourly » temporal » ts_utc alinhado à hora',
    CASE WHEN bad_alignment = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * bad_alignment / NULLIF(total, 0), 4),
    0.0,
    CAST(bad_alignment AS VARCHAR) || ' registos com timestamp não alinhado à hora UTC'
FROM meteo

UNION ALL

-- 13. quality_flag: taxa ok >= 95%
SELECT
    'silver.meteo_open_meteo_hourly » quality_flag » pct_ok >= 95%',
    CASE WHEN ROUND(100.0 * ok_count / NULLIF(total, 0), 2) >= 95.0 THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * (null_flag_count + range_flag_count) / NULLIF(total, 0), 2),
    5.0,
    CAST(ok_count AS VARCHAR) || ' ok | ' ||
    CAST(null_flag_count AS VARCHAR) || ' null_values | ' ||
    CAST(range_flag_count AS VARCHAR) || ' out_of_range'
FROM meteo

ORDER BY status DESC, check_name;
