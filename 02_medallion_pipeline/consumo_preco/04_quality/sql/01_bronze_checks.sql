-- =============================================================================
-- Quality checks — Bronze (consumo_preco)
-- Varredura única por tabela via CTE. Sem GROUP BY, sem JOINs.
-- Retorna: check_name | status | valor_pct | threshold_pct | detalhe
-- =============================================================================

WITH
consumo AS (
    SELECT
        COUNT(*)                                                          AS total,
        SUM(CASE WHEN datahora IS NULL        THEN 1 ELSE 0 END)         AS null_ts,
        SUM(CASE WHEN total IS NULL           THEN 1 ELSE 0 END)         AS null_total,
        SUM(CASE WHEN total IS NOT NULL
                  AND total <= 0             THEN 1 ELSE 0 END)          AS nonpos_total
    FROM iceberg.bronze.consumo_raw
),
preco AS (
    SELECT
        COUNT(*)                                                          AS total,
        SUM(CASE WHEN price_portugal_raw IS NULL THEN 1 ELSE 0 END)      AS null_price,
        SUM(CASE WHEN hour < 1 OR hour > 25      THEN 1 ELSE 0 END)     AS bad_hour,
        SUM(CASE WHEN price_portugal_raw < 0     THEN 1 ELSE 0 END)     AS neg_price
    FROM iceberg.bronze.preco_raw
)

-- 1. consumo_raw tem registos
SELECT
    'bronze.consumo_raw » row_count > 0'                                 AS check_name,
    CASE WHEN total > 0 THEN 'PASS' ELSE 'FAIL' END                     AS status,
    CAST(total AS DECIMAL(18, 2))                                        AS valor_pct,
    1.0                                                                  AS threshold_pct,
    CAST(total AS VARCHAR) || ' registos'                                AS detalhe
FROM consumo

UNION ALL

-- 2. null_rate: datahora
SELECT
    'bronze.consumo_raw » null_rate » datahora',
    CASE WHEN null_ts = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_ts / NULLIF(total, 0), 2),
    0.0,
    CAST(null_ts AS VARCHAR) || ' nulos em ' || CAST(total AS VARCHAR) || ' linhas'
FROM consumo

UNION ALL

-- 3. null_rate: total
SELECT
    'bronze.consumo_raw » null_rate » total',
    CASE WHEN null_total = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_total / NULLIF(total, 0), 2),
    0.0,
    CAST(null_total AS VARCHAR) || ' nulos em ' || CAST(total AS VARCHAR) || ' linhas'
FROM consumo

UNION ALL

-- 4. range: consumo > 0
SELECT
    'bronze.consumo_raw » range » total > 0',
    CASE WHEN nonpos_total = 0 THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * nonpos_total / NULLIF(total, 0), 4),
    0.0,
    CAST(nonpos_total AS VARCHAR) || ' registos com total <= 0 kW'
FROM consumo

UNION ALL

-- 5. preco_raw tem registos
SELECT
    'bronze.preco_raw » row_count > 0',
    CASE WHEN total > 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(total AS DECIMAL(18, 2)),
    1.0,
    CAST(total AS VARCHAR) || ' registos'
FROM preco

UNION ALL

-- 6. null_rate: price_portugal_raw
SELECT
    'bronze.preco_raw » null_rate » price_portugal_raw',
    CASE WHEN null_price = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_price / NULLIF(total, 0), 2),
    0.0,
    CAST(null_price AS VARCHAR) || ' nulos em ' || CAST(total AS VARCHAR) || ' linhas'
FROM preco

UNION ALL

-- 7. range: hour BETWEEN 1 AND 25
SELECT
    'bronze.preco_raw » range » hour BETWEEN 1 AND 25',
    CASE WHEN bad_hour = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * bad_hour / NULLIF(total, 0), 4),
    0.0,
    CAST(bad_hour AS VARCHAR) || ' registos com hora fora de [1,25]'
FROM preco

UNION ALL

-- 8. range: price_portugal_raw >= 0
SELECT
    'bronze.preco_raw » range » price >= 0',
    CASE WHEN neg_price = 0 THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * neg_price / NULLIF(total, 0), 4),
    0.0,
    CAST(neg_price AS VARCHAR) || ' registos com preco negativo'
FROM preco

ORDER BY status DESC, check_name;
