-- =============================================================================
-- CHECKS DE QUALIDADE — CAMADA SILVER (meteo_producao)
-- Executa no Trino. Retorna uma linha por verificação com colunas:
--   check_name | status (PASS/FAIL/WARN) | valor_pct | threshold_pct | detalhe
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. TAXA DE NULOS — ts_utc em meteo_open_meteo_hourly (chave temporal crítica)
-- -----------------------------------------------------------------------------
SELECT
    'silver.meteo_open_meteo_hourly » null_rate » ts_utc'               AS check_name,
    CASE WHEN SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END                                    AS status,
    ROUND(100.0 * SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                                     AS valor_pct,
    0.0                                                                 AS threshold_pct,
    CONCAT(CAST(SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')          AS detalhe
FROM iceberg.silver.meteo_open_meteo_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 2. TAXA DE NULOS — temperature_2m (variável meteorológica principal)
-- -----------------------------------------------------------------------------
SELECT
    'silver.meteo_open_meteo_hourly » null_rate » temperature_2m',
    CASE WHEN SUM(CASE WHEN temperature_2m IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN temperature_2m IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN temperature_2m IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.silver.meteo_open_meteo_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 3. TAXA DE NULOS — shortwave_radiation (relevante para solar)
-- -----------------------------------------------------------------------------
SELECT
    'silver.meteo_open_meteo_hourly » null_rate » shortwave_radiation',
    CASE WHEN SUM(CASE WHEN shortwave_radiation IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN shortwave_radiation IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN shortwave_radiation IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.silver.meteo_open_meteo_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 4. TAXA DE NULOS — wind_speed_10m
-- -----------------------------------------------------------------------------
SELECT
    'silver.meteo_open_meteo_hourly » null_rate » wind_speed_10m',
    CASE WHEN SUM(CASE WHEN wind_speed_10m IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN wind_speed_10m IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN wind_speed_10m IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.silver.meteo_open_meteo_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 5. RANGE — temperature_2m: Portugal continental [-10, 50] °C
-- -----------------------------------------------------------------------------
SELECT
    'silver.meteo_open_meteo_hourly » range » temperature_2m IN [-10, 50]',
    CASE WHEN SUM(CASE WHEN temperature_2m < -10 OR temperature_2m > 50 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN temperature_2m < -10 OR temperature_2m > 50 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN temperature_2m < -10 OR temperature_2m > 50 THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos fora do intervalo físico [-10, 50] °C')
FROM iceberg.silver.meteo_open_meteo_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 6. RANGE — precipitation: [0, 200] mm (precipitação nunca negativa)
-- -----------------------------------------------------------------------------
SELECT
    'silver.meteo_open_meteo_hourly » range » precipitation IN [0, 200]',
    CASE WHEN SUM(CASE WHEN precipitation < 0 OR precipitation > 200 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN precipitation < 0 OR precipitation > 200 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN precipitation < 0 OR precipitation > 200 THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos fora do intervalo físico [0, 200] mm')
FROM iceberg.silver.meteo_open_meteo_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 7. RANGE — wind_speed_10m: [0, 80] m/s
-- -----------------------------------------------------------------------------
SELECT
    'silver.meteo_open_meteo_hourly » range » wind_speed_10m IN [0, 80]',
    CASE WHEN SUM(CASE WHEN wind_speed_10m < 0 OR wind_speed_10m > 80 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN wind_speed_10m < 0 OR wind_speed_10m > 80 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN wind_speed_10m < 0 OR wind_speed_10m > 80 THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos fora do intervalo físico [0, 80] m/s')
FROM iceberg.silver.meteo_open_meteo_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 8. RANGE — shortwave_radiation >= 0 (radiação nunca negativa)
-- -----------------------------------------------------------------------------
SELECT
    'silver.meteo_open_meteo_hourly » range » shortwave_radiation >= 0',
    CASE WHEN SUM(CASE WHEN shortwave_radiation < 0 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN shortwave_radiation < 0 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN shortwave_radiation < 0 THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos com radiação negativa (fisicamente impossível)')
FROM iceberg.silver.meteo_open_meteo_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 9. RANGE — cloud_cover: [0, 100] %
-- -----------------------------------------------------------------------------
SELECT
    'silver.meteo_open_meteo_hourly » range » cloud_cover IN [0, 100]',
    CASE WHEN SUM(CASE WHEN cloud_cover < 0 OR cloud_cover > 100 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN cloud_cover < 0 OR cloud_cover > 100 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN cloud_cover < 0 OR cloud_cover > 100 THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos fora do intervalo [0, 100] %')
FROM iceberg.silver.meteo_open_meteo_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 10. UNICIDADE — ts_utc único em meteo_open_meteo_hourly (após de-duplicação)
-- -----------------------------------------------------------------------------
SELECT
    'silver.meteo_open_meteo_hourly » uniqueness » ts_utc',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' ts_utc duplicados após de-duplicação Silver')
FROM (
    SELECT SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS dup_groups
    FROM (
        SELECT ts_utc, COUNT(*) AS cnt
        FROM iceberg.silver.meteo_open_meteo_hourly
        GROUP BY ts_utc
    ) AS grouped
) AS dup_summary

UNION ALL

-- -----------------------------------------------------------------------------
-- 11. ALINHAMENTO TEMPORAL — ts_utc deve ser fronteira horária (minuto=0, segundo=0)
-- -----------------------------------------------------------------------------
SELECT
    'silver.meteo_open_meteo_hourly » temporal » ts_utc alinhado à hora',
    CASE WHEN SUM(CASE WHEN minute(ts_utc) <> 0 OR second(ts_utc) <> 0
                       THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN minute(ts_utc) <> 0 OR second(ts_utc) <> 0
                            THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN minute(ts_utc) <> 0 OR second(ts_utc) <> 0
                          THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos com timestamp não alinhado à hora UTC')
FROM iceberg.silver.meteo_open_meteo_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 12. DISTRIBUIÇÃO DE _quality_flag — taxa de registos 'ok' >= 95%
--     O Silver classifica: 'ok' | 'null_values' | 'out_of_range'
-- -----------------------------------------------------------------------------
SELECT
    'silver.meteo_open_meteo_hourly » quality_flag » pct_ok >= 95%',
    CASE WHEN ROUND(100.0 * SUM(CASE WHEN _quality_flag = 'ok' THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0), 2) >= 95.0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN _quality_flag <> 'ok' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    5.0,
    CONCAT(CAST(SUM(CASE WHEN _quality_flag = 'ok'          THEN 1 ELSE 0 END) AS VARCHAR), ' ok | ',
           CAST(SUM(CASE WHEN _quality_flag = 'null_values' THEN 1 ELSE 0 END) AS VARCHAR), ' null_values | ',
           CAST(SUM(CASE WHEN _quality_flag = 'out_of_range' THEN 1 ELSE 0 END) AS VARCHAR), ' out_of_range')
FROM iceberg.silver.meteo_open_meteo_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 13. COMPLETUDE HORÁRIA — >= 23 horas por dia (tolera DST e falhas pontuais)
-- -----------------------------------------------------------------------------
SELECT
    'silver.meteo_open_meteo_hourly » completeness » horas_dia >= 23',
    CASE WHEN SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    CAST(SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS VARCHAR),
           ' dias com menos de 23 horas de dados meteorológicos')
FROM (
    SELECT CAST(ts_utc AS DATE) AS dia, COUNT(*) AS cnt
    FROM iceberg.silver.meteo_open_meteo_hourly
    GROUP BY CAST(ts_utc AS DATE)
) AS daily_counts

ORDER BY status DESC, check_name;


-- =============================================================================
-- DETALHE: distribuição de _quality_flag por mês
-- =============================================================================
SELECT
    year,
    month,
    SUM(CASE WHEN _quality_flag = 'ok'           THEN 1 ELSE 0 END) AS ok,
    SUM(CASE WHEN _quality_flag = 'null_values'  THEN 1 ELSE 0 END) AS null_values,
    SUM(CASE WHEN _quality_flag = 'out_of_range' THEN 1 ELSE 0 END) AS out_of_range,
    COUNT(*)                                                          AS total,
    ROUND(100.0 * SUM(CASE WHEN _quality_flag = 'ok' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_ok
FROM iceberg.silver.meteo_open_meteo_hourly
GROUP BY year, month
ORDER BY year, month;
