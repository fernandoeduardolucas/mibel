-- =============================================================================
-- CHECKS DE QUALIDADE — CAMADA SILVER (consumo_preco)
-- Cada tabela é varrida uma única vez via CTE para reduzir pressão de memória.
-- Retorna uma linha por verificação:
--   check_name | status (PASS/FAIL/WARN) | valor_pct | threshold_pct | detalhe
-- =============================================================================

WITH
consumo_stats AS (
    SELECT
        COUNT(*)                                                                       AS total,
        SUM(CASE WHEN ts_utc IS NULL                                   THEN 1 ELSE 0 END) AS null_ts_utc,
        SUM(CASE WHEN total_mwh IS NULL                                THEN 1 ELSE 0 END) AS null_total_mwh,
        SUM(CASE WHEN total_mwh <= 0                                   THEN 1 ELSE 0 END) AS nonpos_mwh,
        SUM(CASE WHEN MINUTE(ts_utc) <> 0 OR SECOND(ts_utc) <> 0     THEN 1 ELSE 0 END) AS bad_temporal
    FROM iceberg.silver.consumo_hourly
),
preco_stats AS (
    SELECT
        COUNT(*)                                                                       AS total,
        SUM(CASE WHEN ts_utc IS NULL                   THEN 1 ELSE 0 END)             AS null_ts_utc,
        SUM(CASE WHEN price_portugal_eur_mwh IS NULL   THEN 1 ELSE 0 END)             AS null_price_pt,
        SUM(CASE WHEN price_spain_eur_mwh IS NULL      THEN 1 ELSE 0 END)             AS null_price_es,
        SUM(CASE WHEN price_portugal_eur_mwh < 0       THEN 1 ELSE 0 END)             AS negative_price
    FROM iceberg.silver.preco_hourly
),
consumo_dups AS (
    SELECT SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS dup_groups
    FROM (
        SELECT ts_utc, COUNT(*) AS cnt
        FROM iceberg.silver.consumo_hourly
        GROUP BY ts_utc
    ) AS g
),
preco_dups AS (
    SELECT SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS dup_groups
    FROM (
        SELECT ts_utc, COUNT(*) AS cnt
        FROM iceberg.silver.preco_hourly
        GROUP BY ts_utc
    ) AS g
),
join_stats AS (
    SELECT
        COUNT(c.ts_utc)                                                          AS total_consumo,
        COUNT(p.ts_utc)                                                          AS matched,
        ROUND(100.0 * COUNT(p.ts_utc) / NULLIF(COUNT(c.ts_utc), 0), 2)         AS coverage_pct
    FROM iceberg.silver.consumo_hourly AS c
    LEFT JOIN iceberg.silver.preco_hourly AS p ON c.ts_utc = p.ts_utc
),
consumo_daily AS (
    SELECT SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS days_low
    FROM (
        SELECT CAST(ts_utc AS DATE) AS dt, COUNT(*) AS cnt
        FROM iceberg.silver.consumo_hourly
        GROUP BY CAST(ts_utc AS DATE)
    ) AS g
),
preco_daily AS (
    SELECT SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS days_low
    FROM (
        SELECT CAST(ts_utc AS DATE) AS dt, COUNT(*) AS cnt
        FROM iceberg.silver.preco_hourly
        GROUP BY CAST(ts_utc AS DATE)
    ) AS g
)

-- 1. null_rate: ts_utc em consumo_hourly
SELECT
    'silver.consumo_hourly » null_rate » ts_utc'                        AS check_name,
    CASE WHEN null_ts_utc = 0 THEN 'PASS' ELSE 'FAIL' END              AS status,
    ROUND(100.0 * null_ts_utc / NULLIF(total, 0), 2)                   AS valor_pct,
    0.0                                                                 AS threshold_pct,
    CONCAT(CAST(null_ts_utc AS VARCHAR), ' nulos em ',
           CAST(total AS VARCHAR), ' linhas')                           AS detalhe
FROM consumo_stats

UNION ALL

-- 2. null_rate: total_mwh
SELECT
    'silver.consumo_hourly » null_rate » total_mwh',
    CASE WHEN null_total_mwh = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_total_mwh / NULLIF(total, 0), 2),
    0.0,
    CONCAT(CAST(null_total_mwh AS VARCHAR), ' nulos em ', CAST(total AS VARCHAR), ' linhas')
FROM consumo_stats

UNION ALL

-- 3. null_rate: ts_utc em preco_hourly
SELECT
    'silver.preco_hourly » null_rate » ts_utc',
    CASE WHEN null_ts_utc = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_ts_utc / NULLIF(total, 0), 2),
    0.0,
    CONCAT(CAST(null_ts_utc AS VARCHAR), ' nulos em ', CAST(total AS VARCHAR), ' linhas')
FROM preco_stats

UNION ALL

-- 4. null_rate: price_portugal_eur_mwh
SELECT
    'silver.preco_hourly » null_rate » price_portugal_eur_mwh',
    CASE WHEN null_price_pt = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_price_pt / NULLIF(total, 0), 2),
    0.0,
    CONCAT(CAST(null_price_pt AS VARCHAR), ' nulos em ', CAST(total AS VARCHAR), ' linhas')
FROM preco_stats

UNION ALL

-- 5. null_rate: price_spain_eur_mwh
SELECT
    'silver.preco_hourly » null_rate » price_spain_eur_mwh',
    CASE WHEN null_price_es = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_price_es / NULLIF(total, 0), 2),
    0.0,
    CONCAT(CAST(null_price_es AS VARCHAR), ' nulos em ', CAST(total AS VARCHAR), ' linhas')
FROM preco_stats

UNION ALL

-- 6. range: consumo horário > 0
SELECT
    'silver.consumo_hourly » range » total_mwh > 0',
    CASE WHEN nonpos_mwh = 0 THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * nonpos_mwh / NULLIF(total, 0), 4),
    0.0,
    CONCAT(CAST(nonpos_mwh AS VARCHAR), ' horas com consumo <= 0 MWh')
FROM consumo_stats

UNION ALL

-- 7. range: preço PT >= 0
SELECT
    'silver.preco_hourly » range » price_portugal_eur_mwh >= 0',
    CASE WHEN negative_price = 0 THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * negative_price / NULLIF(total, 0), 4),
    0.0,
    CONCAT(CAST(negative_price AS VARCHAR),
           ' horas com preço PT negativo (possível: mercado negativo)')
FROM preco_stats

UNION ALL

-- 8. uniqueness: ts_utc em consumo_hourly
SELECT
    'silver.consumo_hourly » uniqueness » ts_utc',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' horas duplicadas')
FROM consumo_dups

UNION ALL

-- 9. uniqueness: ts_utc em preco_hourly
SELECT
    'silver.preco_hourly » uniqueness » ts_utc',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' horas duplicadas')
FROM preco_dups

UNION ALL

-- 10. alinhamento temporal: ts_utc em fronteiras de hora exactas
SELECT
    'silver.consumo_hourly » temporal » ts_utc em fronteira de hora',
    CASE WHEN bad_temporal = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * bad_temporal / NULLIF(total, 0), 4),
    0.0,
    CONCAT(CAST(bad_temporal AS VARCHAR),
           ' registos fora de fronteira de hora (minuto ou segundo != 0)')
FROM consumo_stats

UNION ALL

-- 11. cobertura do join: >= 95% de horas de consumo com preço
SELECT
    'silver » join_coverage » consumo_hourly com preco_hourly >= 95%',
    CASE WHEN coverage_pct >= 95.0 THEN 'PASS' ELSE 'WARN' END,
    coverage_pct,
    95.0,
    CONCAT(CAST(matched AS VARCHAR), ' de ', CAST(total_consumo AS VARCHAR),
           ' horas com preço correspondente')
FROM join_stats

UNION ALL

-- 12. completude diária: >= 23 horas/dia em consumo_hourly
SELECT
    'silver.consumo_hourly » completeness » horas_por_dia >= 23',
    CASE WHEN days_low = 0 THEN 'PASS' ELSE 'WARN' END,
    CAST(days_low AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(days_low AS VARCHAR), ' dias com menos de 23 horas de consumo')
FROM consumo_daily

UNION ALL

-- 13. completude diária: >= 23 horas/dia em preco_hourly
SELECT
    'silver.preco_hourly » completeness » horas_por_dia >= 23',
    CASE WHEN days_low = 0 THEN 'PASS' ELSE 'WARN' END,
    CAST(days_low AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(days_low AS VARCHAR), ' dias com menos de 23 horas de preços')
FROM preco_daily

ORDER BY status DESC, check_name;
