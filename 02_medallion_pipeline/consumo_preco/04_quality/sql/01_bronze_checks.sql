-- =============================================================================
-- CHECKS DE QUALIDADE — CAMADA BRONZE (consumo_preco)
-- Cada tabela é varrida uma única vez via CTE para reduzir pressão de memória.
-- Retorna uma linha por verificação:
--   check_name | status (PASS/FAIL/WARN) | valor_pct | threshold_pct | detalhe
-- =============================================================================

WITH
consumo_stats AS (
    SELECT
        COUNT(*)                                                          AS total,
        SUM(CASE WHEN datahora IS NULL          THEN 1 ELSE 0 END)       AS null_datahora,
        SUM(CASE WHEN total IS NULL             THEN 1 ELSE 0 END)       AS null_total,
        SUM(CASE WHEN total <= 0               THEN 1 ELSE 0 END)       AS nonpos_total
    FROM iceberg.bronze.consumo_raw
),
preco_stats AS (
    SELECT
        COUNT(*)                                                          AS total,
        SUM(CASE WHEN price_portugal_raw IS NULL THEN 1 ELSE 0 END)      AS null_price_pt,
        SUM(CASE WHEN price_spain_raw IS NULL    THEN 1 ELSE 0 END)      AS null_price_es,
        SUM(CASE WHEN hour < 1 OR hour > 25     THEN 1 ELSE 0 END)      AS invalid_hour,
        SUM(CASE WHEN price_portugal_raw < 0    THEN 1 ELSE 0 END)      AS negative_price
    FROM iceberg.bronze.preco_raw
),
consumo_dups AS (
    SELECT SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS dup_groups
    FROM (
        SELECT datahora, process_date, COUNT(*) AS cnt
        FROM iceberg.bronze.consumo_raw
        GROUP BY datahora, process_date
    ) AS g
),
preco_dups AS (
    SELECT SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS dup_groups
    FROM (
        SELECT date_raw, hour, process_date, COUNT(*) AS cnt
        FROM iceberg.bronze.preco_raw
        GROUP BY date_raw, hour, process_date
    ) AS g
),
consumo_daily AS (
    SELECT SUM(CASE WHEN cnt < 80 THEN 1 ELSE 0 END) AS days_low
    FROM (
        SELECT process_date, COUNT(*) AS cnt
        FROM iceberg.bronze.consumo_raw
        GROUP BY process_date
    ) AS g
),
preco_daily AS (
    SELECT SUM(CASE WHEN cnt < 23 THEN 1 ELSE 0 END) AS days_low
    FROM (
        SELECT date_raw, COUNT(*) AS cnt
        FROM iceberg.bronze.preco_raw
        GROUP BY date_raw
    ) AS g
)

-- 1. null_rate: datahora
SELECT
    'bronze.consumo_raw » null_rate » datahora'                          AS check_name,
    CASE WHEN null_datahora = 0 THEN 'PASS' ELSE 'FAIL' END             AS status,
    ROUND(100.0 * null_datahora / NULLIF(total, 0), 2)                  AS valor_pct,
    0.0                                                                  AS threshold_pct,
    CONCAT(CAST(null_datahora AS VARCHAR), ' nulos em ',
           CAST(total AS VARCHAR), ' linhas')                            AS detalhe
FROM consumo_stats

UNION ALL

-- 2. null_rate: total
SELECT
    'bronze.consumo_raw » null_rate » total',
    CASE WHEN null_total = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_total / NULLIF(total, 0), 2),
    0.0,
    CONCAT(CAST(null_total AS VARCHAR), ' nulos em ', CAST(total AS VARCHAR), ' linhas')
FROM consumo_stats

UNION ALL

-- 3. null_rate: price_portugal_raw
SELECT
    'bronze.preco_raw » null_rate » price_portugal_raw',
    CASE WHEN null_price_pt = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_price_pt / NULLIF(total, 0), 2),
    0.0,
    CONCAT(CAST(null_price_pt AS VARCHAR), ' nulos em ', CAST(total AS VARCHAR), ' linhas')
FROM preco_stats

UNION ALL

-- 4. null_rate: price_spain_raw
SELECT
    'bronze.preco_raw » null_rate » price_spain_raw',
    CASE WHEN null_price_es = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_price_es / NULLIF(total, 0), 2),
    0.0,
    CONCAT(CAST(null_price_es AS VARCHAR), ' nulos em ', CAST(total AS VARCHAR), ' linhas')
FROM preco_stats

UNION ALL

-- 5. range: consumo total > 0
SELECT
    'bronze.consumo_raw » range » total > 0',
    CASE WHEN nonpos_total = 0 THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * nonpos_total / NULLIF(total, 0), 4),
    0.0,
    CONCAT(CAST(nonpos_total AS VARCHAR), ' registos com total <= 0 kW')
FROM consumo_stats

UNION ALL

-- 6. range: hour BETWEEN 1 AND 25
SELECT
    'bronze.preco_raw » range » hour BETWEEN 1 AND 25',
    CASE WHEN invalid_hour = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * invalid_hour / NULLIF(total, 0), 4),
    0.0,
    CONCAT(CAST(invalid_hour AS VARCHAR), ' registos fora do intervalo 1-25')
FROM preco_stats

UNION ALL

-- 7. range: price_portugal_raw >= 0
SELECT
    'bronze.preco_raw » range » price_portugal_raw >= 0',
    CASE WHEN negative_price = 0 THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * negative_price / NULLIF(total, 0), 4),
    0.0,
    CONCAT(CAST(negative_price AS VARCHAR),
           ' registos com preço negativo (mercado pode ter preços negativos)')
FROM preco_stats

UNION ALL

-- 8. uniqueness: (datahora, process_date)
SELECT
    'bronze.consumo_raw » uniqueness » (datahora, process_date)',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'WARN' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' grupos de timestamps duplicados')
FROM consumo_dups

UNION ALL

-- 9. uniqueness: (date_raw, hour, process_date)
SELECT
    'bronze.preco_raw » uniqueness » (date_raw, hour, process_date)',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' grupos de (data, hora) duplicados')
FROM preco_dups

UNION ALL

-- 10. completeness: dias com >= 80 registos de consumo
SELECT
    'bronze.consumo_raw » completeness » registos_por_dia >= 80',
    CASE WHEN days_low = 0 THEN 'PASS' ELSE 'WARN' END,
    CAST(days_low AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(days_low AS VARCHAR),
           ' dias com menos de 80 registos de consumo (esperados ~96/dia)')
FROM consumo_daily

UNION ALL

-- 11. completeness: dias com >= 23 registos de preços
SELECT
    'bronze.preco_raw » completeness » registos_por_dia >= 23',
    CASE WHEN days_low = 0 THEN 'PASS' ELSE 'WARN' END,
    CAST(days_low AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(days_low AS VARCHAR), ' dias com menos de 23 registos de preços')
FROM preco_daily

ORDER BY status DESC, check_name;
