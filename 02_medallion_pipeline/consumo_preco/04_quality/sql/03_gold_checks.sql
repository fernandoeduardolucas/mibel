-- =============================================================================
-- CHECKS DE QUALIDADE — CAMADA GOLD (consumo_preco)
-- Cada tabela é varrida uma única vez via CTE para reduzir pressão de memória.
-- O self-join de lag consistency é feito uma única vez para ambos os checks.
-- Retorna uma linha por verificação:
--   check_name | status (PASS/FAIL/WARN) | valor_pct | threshold_pct | detalhe
-- =============================================================================

WITH
dp_stats AS (
    SELECT
        COUNT(*)                                                                        AS total,
        SUM(CASE WHEN ts_utc IS NULL                         THEN 1 ELSE 0 END)        AS null_ts_utc,
        SUM(CASE WHEN consumo_total IS NULL                  THEN 1 ELSE 0 END)        AS null_consumo,
        SUM(CASE WHEN market_price_pt IS NULL                THEN 1 ELSE 0 END)        AS null_price,
        SUM(CASE WHEN hora < 0 OR hora > 23                 THEN 1 ELSE 0 END)        AS bad_hora,
        SUM(CASE WHEN dia_semana < 0 OR dia_semana > 6      THEN 1 ELSE 0 END)        AS bad_dia_semana
    FROM iceberg.gold.dp_energy_market_hourly
),
feat_stats AS (
    SELECT
        COUNT(*)                                                                        AS total,
        SUM(CASE WHEN ts_utc IS NULL             THEN 1 ELSE 0 END)                    AS null_ts_utc,
        SUM(CASE WHEN consumo_next_hour IS NULL  THEN 1 ELSE 0 END)                    AS null_next_hour,
        SUM(CASE WHEN consumo_lag_1h IS NULL     THEN 1 ELSE 0 END)                    AS null_lag_1h,
        SUM(CASE WHEN consumo_lag_24h IS NULL    THEN 1 ELSE 0 END)                    AS null_lag_24h,
        SUM(CASE WHEN price_lag_1h IS NULL       THEN 1 ELSE 0 END)                    AS null_price_lag
    FROM iceberg.gold.feat_load_forecasting_hourly
),
dp_dups AS (
    SELECT SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS dup_groups
    FROM (
        SELECT ts_utc, COUNT(*) AS cnt
        FROM iceberg.gold.dp_energy_market_hourly
        GROUP BY ts_utc
    ) AS g
),
feat_dups AS (
    SELECT SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS dup_groups
    FROM (
        SELECT ts_utc, COUNT(*) AS cnt
        FROM iceberg.gold.feat_load_forecasting_hourly
        GROUP BY ts_utc
    ) AS g
),
lag_consistency AS (
    -- Self-join único que serve tanto o check de consumo_lag_1h como price_lag_1h
    SELECT
        COUNT(*)                                                                        AS checked,
        SUM(CASE WHEN ABS(dp.consumo_lag_1h  - prev.consumo_total)   > 0.01
                 THEN 1 ELSE 0 END)                                                    AS bad_consumo_lag,
        SUM(CASE WHEN ABS(dp.price_lag_1h    - prev.market_price_pt) > 0.01
                 THEN 1 ELSE 0 END)                                                    AS bad_price_lag,
        ROUND(100.0 * SUM(CASE WHEN ABS(dp.consumo_lag_1h  - prev.consumo_total)   > 0.01
                               THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 4)           AS pct_bad_consumo,
        ROUND(100.0 * SUM(CASE WHEN ABS(dp.price_lag_1h    - prev.market_price_pt) > 0.01
                               THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 4)           AS pct_bad_price
    FROM iceberg.gold.dp_energy_market_hourly AS dp
    JOIN iceberg.gold.dp_energy_market_hourly AS prev
        ON dp.ts_utc = date_add('hour', 1, prev.ts_utc)
    WHERE dp.consumo_lag_1h IS NOT NULL
)

-- 1. null_rate: ts_utc em dp_energy_market_hourly
SELECT
    'gold.dp_energy_market_hourly » null_rate » ts_utc'                 AS check_name,
    CASE WHEN null_ts_utc = 0 THEN 'PASS' ELSE 'FAIL' END              AS status,
    ROUND(100.0 * null_ts_utc / NULLIF(total, 0), 2)                   AS valor_pct,
    0.0                                                                 AS threshold_pct,
    CONCAT(CAST(null_ts_utc AS VARCHAR), ' nulos em ',
           CAST(total AS VARCHAR), ' linhas')                           AS detalhe
FROM dp_stats

UNION ALL

-- 2. null_rate: consumo_total
SELECT
    'gold.dp_energy_market_hourly » null_rate » consumo_total',
    CASE WHEN null_consumo = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_consumo / NULLIF(total, 0), 2),
    0.0,
    CONCAT(CAST(null_consumo AS VARCHAR), ' nulos em ', CAST(total AS VARCHAR), ' linhas')
FROM dp_stats

UNION ALL

-- 3. null_rate: market_price_pt
SELECT
    'gold.dp_energy_market_hourly » null_rate » market_price_pt',
    CASE WHEN null_price = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_price / NULLIF(total, 0), 2),
    0.0,
    CONCAT(CAST(null_price AS VARCHAR), ' nulos em ', CAST(total AS VARCHAR), ' linhas')
FROM dp_stats

UNION ALL

-- 4. range: hora BETWEEN 0 AND 23
SELECT
    'gold.dp_energy_market_hourly » range » hora BETWEEN 0 AND 23',
    CASE WHEN bad_hora = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * bad_hora / NULLIF(total, 0), 4),
    0.0,
    CONCAT(CAST(bad_hora AS VARCHAR), ' registos com hora fora de [0,23]')
FROM dp_stats

UNION ALL

-- 5. range: dia_semana BETWEEN 0 AND 6
SELECT
    'gold.dp_energy_market_hourly » range » dia_semana BETWEEN 0 AND 6',
    CASE WHEN bad_dia_semana = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * bad_dia_semana / NULLIF(total, 0), 4),
    0.0,
    CONCAT(CAST(bad_dia_semana AS VARCHAR), ' registos com dia_semana fora de [0,6]')
FROM dp_stats

UNION ALL

-- 6. uniqueness: ts_utc em dp_energy_market_hourly
SELECT
    'gold.dp_energy_market_hourly » uniqueness » ts_utc',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' ts_utc duplicados')
FROM dp_dups

UNION ALL

-- 7. uniqueness: ts_utc em feat_load_forecasting_hourly
SELECT
    'gold.feat_load_forecasting_hourly » uniqueness » ts_utc',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' ts_utc duplicados')
FROM feat_dups

UNION ALL

-- 8. ML target: consumo_next_hour sem nulos
SELECT
    'gold.feat_load_forecasting_hourly » null_rate » consumo_next_hour',
    CASE WHEN null_next_hour = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_next_hour / NULLIF(total, 0), 4),
    0.0,
    CONCAT(CAST(null_next_hour AS VARCHAR), ' exemplos sem target (consumo_next_hour)')
FROM feat_stats

UNION ALL

-- 9a. ML features: consumo_lag_1h sem nulos
SELECT
    'gold.feat_load_forecasting_hourly » null_rate » consumo_lag_1h',
    CASE WHEN null_lag_1h = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_lag_1h / NULLIF(total, 0), 4),
    0.0,
    CONCAT(CAST(null_lag_1h AS VARCHAR), ' exemplos sem consumo_lag_1h')
FROM feat_stats

UNION ALL

-- 9b. ML features: consumo_lag_24h sem nulos
SELECT
    'gold.feat_load_forecasting_hourly » null_rate » consumo_lag_24h',
    CASE WHEN null_lag_24h = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_lag_24h / NULLIF(total, 0), 4),
    0.0,
    CONCAT(CAST(null_lag_24h AS VARCHAR), ' exemplos sem consumo_lag_24h')
FROM feat_stats

UNION ALL

-- 9c. ML features: price_lag_1h sem nulos
SELECT
    'gold.feat_load_forecasting_hourly » null_rate » price_lag_1h',
    CASE WHEN null_price_lag = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_price_lag / NULLIF(total, 0), 4),
    0.0,
    CONCAT(CAST(null_price_lag AS VARCHAR), ' exemplos sem price_lag_1h')
FROM feat_stats

UNION ALL

-- 10. paridade de linhas: dp vs feat (diferença <= 48)
SELECT
    'gold » row_count_parity » dp vs feat (diferença <= 48)',
    CASE WHEN ABS(dp_count - feat_count) <= 48 THEN 'PASS' ELSE 'WARN' END,
    CAST(ABS(dp_count - feat_count) AS DECIMAL(18,2)),
    48.0,
    CONCAT('dp=', CAST(dp_count AS VARCHAR), ' feat=', CAST(feat_count AS VARCHAR),
           ' diferença=', CAST(dp_count - feat_count AS VARCHAR))
FROM (
    SELECT
        (SELECT total FROM dp_stats)   AS dp_count,
        (SELECT total FROM feat_stats) AS feat_count
) AS counts

UNION ALL

-- 11. lag consistency: consumo_lag_1h
SELECT
    'gold.dp_energy_market_hourly » lag_consistency » consumo_lag_1h',
    CASE WHEN pct_bad_consumo < 0.1 THEN 'PASS' ELSE 'FAIL' END,
    pct_bad_consumo,
    0.1,
    CONCAT(CAST(bad_consumo_lag AS VARCHAR), ' de ', CAST(checked AS VARCHAR),
           ' registos com lag_1h inconsistente (tolerância 0.01 MWh)')
FROM lag_consistency

UNION ALL

-- 12. lag consistency: price_lag_1h
SELECT
    'gold.dp_energy_market_hourly » lag_consistency » price_lag_1h',
    CASE WHEN pct_bad_price < 0.1 THEN 'PASS' ELSE 'FAIL' END,
    pct_bad_price,
    0.1,
    CONCAT(CAST(bad_price_lag AS VARCHAR), ' de ', CAST(checked AS VARCHAR),
           ' registos com price_lag_1h inconsistente (tolerância 0.01 €/MWh)')
FROM lag_consistency

ORDER BY status DESC, check_name;
