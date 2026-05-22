-- =============================================================================
-- CHECKS DE QUALIDADE — CAMADA SILVER (Streaming_Data / API)
-- Tabelas: iceberg.silver.consumo_api_hourly + iceberg.silver.preco_api_hourly
-- =============================================================================

-- 1. TAXA DE NULOS — ts_utc em consumo_api_hourly
SELECT
    'silver.consumo_api_hourly » null_rate » ts_utc'        AS check_name,
    CASE WHEN SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END                        AS status,
    ROUND(100.0 * SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                         AS valor_pct,
    0.0                                                     AS threshold_pct,
    CONCAT(CAST(SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas') AS detalhe
FROM iceberg.silver.consumo_api_hourly

UNION ALL

-- 2. TAXA DE NULOS — total_mwh em consumo_api_hourly
SELECT
    'silver.consumo_api_hourly » null_rate » total_mwh',
    CASE WHEN SUM(CASE WHEN total_mwh IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN total_mwh IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN total_mwh IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.silver.consumo_api_hourly

UNION ALL

-- 3. TAXA DE NULOS — ts_utc em preco_api_hourly
SELECT
    'silver.preco_api_hourly » null_rate » ts_utc',
    CASE WHEN SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN ts_utc IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.silver.preco_api_hourly

UNION ALL

-- 4. TAXA DE NULOS — price_portugal_eur_mwh em preco_api_hourly
SELECT
    'silver.preco_api_hourly » null_rate » price_portugal_eur_mwh',
    CASE WHEN SUM(CASE WHEN price_portugal_eur_mwh IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN price_portugal_eur_mwh IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN price_portugal_eur_mwh IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.silver.preco_api_hourly

UNION ALL

-- 5. RANGE — consumo positivo (MWh)
SELECT
    'silver.consumo_api_hourly » range » total_mwh > 0',
    CASE WHEN SUM(CASE WHEN total_mwh <= 0 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN total_mwh <= 0 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN total_mwh <= 0 THEN 1 ELSE 0 END) AS VARCHAR),
           ' horas com total_mwh <= 0')
FROM iceberg.silver.consumo_api_hourly

UNION ALL

-- 6. UNICIDADE — sem ts_utc duplicados em consumo_api_hourly
SELECT
    'silver.consumo_api_hourly » uniqueness » ts_utc',
    CASE WHEN dup_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_count AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_count AS VARCHAR), ' timestamps duplicados')
FROM (
    SELECT COUNT(*) AS dup_count
    FROM (
        SELECT ts_utc
        FROM iceberg.silver.consumo_api_hourly
        GROUP BY ts_utc
        HAVING COUNT(*) > 1
    ) AS dups
) AS dup_summary

UNION ALL

-- 7. UNICIDADE — sem ts_utc duplicados em preco_api_hourly
SELECT
    'silver.preco_api_hourly » uniqueness » ts_utc',
    CASE WHEN dup_count = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_count AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_count AS VARCHAR), ' timestamps duplicados')
FROM (
    SELECT COUNT(*) AS dup_count
    FROM (
        SELECT ts_utc
        FROM iceberg.silver.preco_api_hourly
        GROUP BY ts_utc
        HAVING COUNT(*) > 1
    ) AS dups
) AS dup_summary

UNION ALL

-- 8. INTEGRIDADE REFERENCIAL — horas de consumo sem par de preço
SELECT
    'silver » join_integrity » consumo_sem_preco',
    CASE WHEN orphan_count = 0 THEN 'PASS' ELSE 'WARN' END,
    CAST(orphan_count AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(orphan_count AS VARCHAR), ' horas de consumo sem preço correspondente')
FROM (
    SELECT COUNT(*) AS orphan_count
    FROM iceberg.silver.consumo_api_hourly c
    LEFT JOIN iceberg.silver.preco_api_hourly p ON c.ts_utc = p.ts_utc
    WHERE p.ts_utc IS NULL
) AS orphan_summary

UNION ALL

-- 9. INTEGRIDADE REFERENCIAL — horas de preço sem par de consumo
SELECT
    'silver » join_integrity » preco_sem_consumo',
    CASE WHEN orphan_count = 0 THEN 'PASS' ELSE 'WARN' END,
    CAST(orphan_count AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(orphan_count AS VARCHAR), ' horas de preço sem consumo correspondente')
FROM (
    SELECT COUNT(*) AS orphan_count
    FROM iceberg.silver.preco_api_hourly p
    LEFT JOIN iceberg.silver.consumo_api_hourly c ON p.ts_utc = c.ts_utc
    WHERE c.ts_utc IS NULL
) AS orphan_summary

UNION ALL

-- 10. DEDUPLICAÇÃO — verifica que Silver tem <= Bronze (nenhum registo gerado)
SELECT
    'silver.consumo_api_hourly » dedup » count_vs_bronze',
    CASE WHEN silver_count <= bronze_horas THEN 'PASS' ELSE 'FAIL' END,
    CAST(silver_count AS DECIMAL(18,2)),
    CAST(bronze_horas AS DECIMAL(18,2)),
    CONCAT('Silver: ', CAST(silver_count AS VARCHAR), ' horas | Bronze: ', CAST(bronze_horas AS VARCHAR), ' registos')
FROM (
    SELECT
        (SELECT COUNT(*) FROM iceberg.silver.consumo_api_hourly) AS silver_count,
        (SELECT COUNT(*) FROM iceberg.bronze.consumo_api_raw)    AS bronze_horas
) AS counts

ORDER BY status DESC, check_name;
