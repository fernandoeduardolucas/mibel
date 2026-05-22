-- =============================================================================
-- Quality checks — Silver (consumo_preco)
-- Varredura única por tabela via CTE. Sem JOINs entre tabelas.
-- Cobertura join estimada por comparação de COUNT (sem LEFT JOIN).
-- Retorna: check_name | status | valor_pct | threshold_pct | detalhe
-- =============================================================================

WITH
consumo AS (
    SELECT
        COUNT(*)                                                           AS total,
        SUM(CASE WHEN ts_utc IS NULL      THEN 1 ELSE 0 END)              AS null_ts,
        SUM(CASE WHEN total_mwh IS NULL   THEN 1 ELSE 0 END)              AS null_mwh,
        SUM(CASE WHEN total_mwh IS NOT NULL
                  AND total_mwh <= 0     THEN 1 ELSE 0 END)               AS nonpos_mwh
    FROM iceberg.silver.consumo_hourly
),
preco AS (
    SELECT
        COUNT(*)                                                           AS total,
        SUM(CASE WHEN ts_utc IS NULL                   THEN 1 ELSE 0 END) AS null_ts,
        SUM(CASE WHEN price_portugal_eur_mwh IS NULL   THEN 1 ELSE 0 END) AS null_price,
        SUM(CASE WHEN price_portugal_eur_mwh < 0       THEN 1 ELSE 0 END) AS neg_price
    FROM iceberg.silver.preco_hourly
),
counts AS (
    SELECT
        (SELECT total FROM consumo) AS consumo_total,
        (SELECT total FROM preco)   AS preco_total
)

-- 1. consumo_hourly tem registos
SELECT
    'silver.consumo_hourly » row_count > 0'                              AS check_name,
    CASE WHEN total > 0 THEN 'PASS' ELSE 'FAIL' END                     AS status,
    CAST(total AS DECIMAL(18, 2))                                        AS valor_pct,
    1.0                                                                  AS threshold_pct,
    CAST(total AS VARCHAR) || ' horas'                                   AS detalhe
FROM consumo

UNION ALL

-- 2. null_rate: ts_utc consumo
SELECT
    'silver.consumo_hourly » null_rate » ts_utc',
    CASE WHEN null_ts = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_ts / NULLIF(total, 0), 2),
    0.0,
    CAST(null_ts AS VARCHAR) || ' nulos em ' || CAST(total AS VARCHAR) || ' linhas'
FROM consumo

UNION ALL

-- 3. null_rate: total_mwh
SELECT
    'silver.consumo_hourly » null_rate » total_mwh',
    CASE WHEN null_mwh = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_mwh / NULLIF(total, 0), 2),
    0.0,
    CAST(null_mwh AS VARCHAR) || ' nulos em ' || CAST(total AS VARCHAR) || ' linhas'
FROM consumo

UNION ALL

-- 4. range: consumo > 0
SELECT
    'silver.consumo_hourly » range » total_mwh > 0',
    CASE WHEN nonpos_mwh = 0 THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * nonpos_mwh / NULLIF(total, 0), 4),
    0.0,
    CAST(nonpos_mwh AS VARCHAR) || ' horas com consumo <= 0 MWh'
FROM consumo

UNION ALL

-- 5. preco_hourly tem registos
SELECT
    'silver.preco_hourly » row_count > 0',
    CASE WHEN total > 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(total AS DECIMAL(18, 2)),
    1.0,
    CAST(total AS VARCHAR) || ' horas'
FROM preco

UNION ALL

-- 6. null_rate: ts_utc preco
SELECT
    'silver.preco_hourly » null_rate » ts_utc',
    CASE WHEN null_ts = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_ts / NULLIF(total, 0), 2),
    0.0,
    CAST(null_ts AS VARCHAR) || ' nulos em ' || CAST(total AS VARCHAR) || ' linhas'
FROM preco

UNION ALL

-- 7. null_rate: price_portugal_eur_mwh
SELECT
    'silver.preco_hourly » null_rate » price_portugal_eur_mwh',
    CASE WHEN null_price = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_price / NULLIF(total, 0), 2),
    0.0,
    CAST(null_price AS VARCHAR) || ' nulos em ' || CAST(total AS VARCHAR) || ' linhas'
FROM preco

UNION ALL

-- 8. range: price >= 0
SELECT
    'silver.preco_hourly » range » price >= 0',
    CASE WHEN neg_price = 0 THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * neg_price / NULLIF(total, 0), 4),
    0.0,
    CAST(neg_price AS VARCHAR) || ' horas com preco negativo'
FROM preco

UNION ALL

-- 9. cobertura: preco >= 90% de consumo (COUNT sem JOIN)
SELECT
    'silver » coverage » preco >= 90% de consumo',
    CASE
        WHEN consumo_total = 0 THEN 'FAIL'
        WHEN ROUND(100.0 * preco_total / consumo_total, 2) >= 90.0 THEN 'PASS'
        ELSE 'WARN'
    END,
    ROUND(100.0 * preco_total / NULLIF(consumo_total, 0), 2),
    90.0,
    'consumo=' || CAST(consumo_total AS VARCHAR) ||
    ' preco=' || CAST(preco_total AS VARCHAR)
FROM counts

ORDER BY status DESC, check_name;
