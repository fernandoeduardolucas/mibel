-- =============================================================================
-- Quality checks — Gold (consumo_preco)
-- Varredura única por tabela via CTE. Sem self-JOINs nem GROUP BY.
-- Retorna: check_name | status | valor_pct | threshold_pct | detalhe
-- =============================================================================

WITH
dp AS (
    SELECT
        COUNT(*)                                                               AS total,
        SUM(CASE WHEN ts_utc IS NULL                    THEN 1 ELSE 0 END)    AS null_ts,
        SUM(CASE WHEN consumo_total IS NULL             THEN 1 ELSE 0 END)    AS null_consumo,
        SUM(CASE WHEN market_price_pt IS NULL           THEN 1 ELSE 0 END)    AS null_price,
        SUM(CASE WHEN hora < 0 OR hora > 23             THEN 1 ELSE 0 END)   AS bad_hora,
        SUM(CASE WHEN dia_semana < 0 OR dia_semana > 6  THEN 1 ELSE 0 END)   AS bad_dia
    FROM iceberg.gold.dp_energy_market_hourly
),
feat AS (
    SELECT
        COUNT(*)                                                               AS total,
        SUM(CASE WHEN ts_utc IS NULL             THEN 1 ELSE 0 END)           AS null_ts,
        SUM(CASE WHEN consumo_next_hour IS NULL  THEN 1 ELSE 0 END)           AS null_target,
        SUM(CASE WHEN consumo_lag_1h IS NULL     THEN 1 ELSE 0 END)           AS null_lag1,
        SUM(CASE WHEN consumo_lag_24h IS NULL    THEN 1 ELSE 0 END)           AS null_lag24,
        SUM(CASE WHEN price_lag_1h IS NULL       THEN 1 ELSE 0 END)           AS null_plagl
    FROM iceberg.gold.feat_load_forecasting_hourly
)

-- 1. dp_energy_market_hourly tem registos
SELECT
    'gold.dp_energy_market_hourly » row_count > 0'                       AS check_name,
    CASE WHEN total > 0 THEN 'PASS' ELSE 'FAIL' END                     AS status,
    CAST(total AS DECIMAL(18, 2))                                        AS valor_pct,
    1.0                                                                  AS threshold_pct,
    CAST(total AS VARCHAR) || ' horas'                                   AS detalhe
FROM dp

UNION ALL

-- 2. null_rate: ts_utc
SELECT
    'gold.dp_energy_market_hourly » null_rate » ts_utc',
    CASE WHEN null_ts = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_ts / NULLIF(total, 0), 2),
    0.0,
    CAST(null_ts AS VARCHAR) || ' nulos em ' || CAST(total AS VARCHAR) || ' linhas'
FROM dp

UNION ALL

-- 3. null_rate: consumo_total
SELECT
    'gold.dp_energy_market_hourly » null_rate » consumo_total',
    CASE WHEN null_consumo = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_consumo / NULLIF(total, 0), 2),
    0.0,
    CAST(null_consumo AS VARCHAR) || ' nulos'
FROM dp

UNION ALL

-- 4. null_rate: market_price_pt
SELECT
    'gold.dp_energy_market_hourly » null_rate » market_price_pt',
    CASE WHEN null_price = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_price / NULLIF(total, 0), 2),
    0.0,
    CAST(null_price AS VARCHAR) || ' nulos'
FROM dp

UNION ALL

-- 5. range: hora BETWEEN 0 AND 23
SELECT
    'gold.dp_energy_market_hourly » range » hora [0,23]',
    CASE WHEN bad_hora = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * bad_hora / NULLIF(total, 0), 4),
    0.0,
    CAST(bad_hora AS VARCHAR) || ' registos com hora fora de [0,23]'
FROM dp

UNION ALL

-- 6. range: dia_semana BETWEEN 0 AND 6
SELECT
    'gold.dp_energy_market_hourly » range » dia_semana [0,6]',
    CASE WHEN bad_dia = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * bad_dia / NULLIF(total, 0), 4),
    0.0,
    CAST(bad_dia AS VARCHAR) || ' registos com dia_semana fora de [0,6]'
FROM dp

UNION ALL

-- 7. feat tem registos
SELECT
    'gold.feat_load_forecasting_hourly » row_count > 0',
    CASE WHEN total > 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(total AS DECIMAL(18, 2)),
    1.0,
    CAST(total AS VARCHAR) || ' exemplos ML'
FROM feat

UNION ALL

-- 8. null_rate: consumo_next_hour (target ML)
SELECT
    'gold.feat_load_forecasting_hourly » null_rate » consumo_next_hour',
    CASE WHEN null_target = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_target / NULLIF(total, 0), 4),
    0.0,
    CAST(null_target AS VARCHAR) || ' exemplos sem target'
FROM feat

UNION ALL

-- 9. null_rate: features de lag
SELECT
    'gold.feat_load_forecasting_hourly » null_rate » lag features',
    CASE WHEN null_lag1 = 0 AND null_lag24 = 0 AND null_plagl = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * (null_lag1 + null_lag24 + null_plagl) / NULLIF(3 * total, 0), 4),
    0.0,
    'lag1h=' || CAST(null_lag1 AS VARCHAR) ||
    ' lag24h=' || CAST(null_lag24 AS VARCHAR) ||
    ' price_lag=' || CAST(null_plagl AS VARCHAR)
FROM feat

ORDER BY status DESC, check_name;
