-- =============================================================================
-- CHECKS DE QUALIDADE — CAMADA SILVER (producao_consumo)
-- Executa no Trino. Retorna uma linha por verificação com colunas:
--   check_name | status (PASS/FAIL/WARN) | valor_pct | threshold_pct | detalhe
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. TAXA DE NULOS — timestamp_utc em consumo_total_nacional_15min (chave temporal)
-- -----------------------------------------------------------------------------
SELECT
    'silver.consumo_total_nacional_15min » null_rate » timestamp_utc'   AS check_name,
    CASE WHEN SUM(CASE WHEN timestamp_utc IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END                                    AS status,
    ROUND(100.0 * SUM(CASE WHEN timestamp_utc IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                                     AS valor_pct,
    0.0                                                                 AS threshold_pct,
    CONCAT(CAST(SUM(CASE WHEN timestamp_utc IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')          AS detalhe
FROM iceberg.silver.consumo_total_nacional_15min

UNION ALL

-- -----------------------------------------------------------------------------
-- 2. TAXA DE NULOS — consumo_total_kwh em consumo_total_nacional_15min
-- -----------------------------------------------------------------------------
SELECT
    'silver.consumo_total_nacional_15min » null_rate » consumo_total_kwh',
    CASE WHEN SUM(CASE WHEN consumo_total_kwh IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN consumo_total_kwh IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN consumo_total_kwh IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.silver.consumo_total_nacional_15min

UNION ALL

-- -----------------------------------------------------------------------------
-- 3. TAXA DE NULOS — timestamp_utc em energia_produzida_total_nacional_15min
-- -----------------------------------------------------------------------------
SELECT
    'silver.energia_produzida_total_nacional_15min » null_rate » timestamp_utc',
    CASE WHEN SUM(CASE WHEN timestamp_utc IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN timestamp_utc IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN timestamp_utc IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.silver.energia_produzida_total_nacional_15min

UNION ALL

-- -----------------------------------------------------------------------------
-- 4. TAXA DE NULOS — producao_total_kwh em energia_produzida_total_nacional_15min
-- -----------------------------------------------------------------------------
SELECT
    'silver.energia_produzida_total_nacional_15min » null_rate » producao_total_kwh',
    CASE WHEN SUM(CASE WHEN producao_total_kwh IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN producao_total_kwh IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN producao_total_kwh IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.silver.energia_produzida_total_nacional_15min

UNION ALL

-- -----------------------------------------------------------------------------
-- 5. RANGE — consumo_total_kwh > 0 (Portugal nacional nunca deve ser zero)
-- -----------------------------------------------------------------------------
SELECT
    'silver.consumo_total_nacional_15min » range » consumo_total_kwh > 0',
    CASE WHEN SUM(CASE WHEN consumo_total_kwh <= 0 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN consumo_total_kwh <= 0 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN consumo_total_kwh <= 0 THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos com consumo <= 0 kWh')
FROM iceberg.silver.consumo_total_nacional_15min

UNION ALL

-- -----------------------------------------------------------------------------
-- 6. RANGE — producao_total_kwh >= 0 (produção nunca negativa)
-- -----------------------------------------------------------------------------
SELECT
    'silver.energia_produzida_total_nacional_15min » range » producao_total_kwh >= 0',
    CASE WHEN SUM(CASE WHEN producao_total_kwh < 0 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN producao_total_kwh < 0 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN producao_total_kwh < 0 THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos com produção negativa')
FROM iceberg.silver.energia_produzida_total_nacional_15min

UNION ALL

-- -----------------------------------------------------------------------------
-- 7. UNICIDADE — timestamp_utc único em consumo_total_nacional_15min
--    A Silver já de-duplica, mas verificamos o resultado final
-- -----------------------------------------------------------------------------
SELECT
    'silver.consumo_total_nacional_15min » uniqueness » timestamp_utc',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' timestamps duplicados após de-duplicação Silver')
FROM (
    SELECT SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS dup_groups
    FROM (
        SELECT timestamp_utc, COUNT(*) AS cnt
        FROM iceberg.silver.consumo_total_nacional_15min
        GROUP BY timestamp_utc
    ) AS grouped
) AS dup_summary

UNION ALL

-- -----------------------------------------------------------------------------
-- 8. UNICIDADE — timestamp_utc único em energia_produzida_total_nacional_15min
-- -----------------------------------------------------------------------------
SELECT
    'silver.energia_produzida_total_nacional_15min » uniqueness » timestamp_utc',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' timestamps duplicados após de-duplicação Silver')
FROM (
    SELECT SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS dup_groups
    FROM (
        SELECT timestamp_utc, COUNT(*) AS cnt
        FROM iceberg.silver.energia_produzida_total_nacional_15min
        GROUP BY timestamp_utc
    ) AS grouped
) AS dup_summary

UNION ALL

-- -----------------------------------------------------------------------------
-- 9. ALINHAMENTO TEMPORAL — timestamps devem ser fronteiras de 15 min (minuto ∈ {0,15,30,45})
-- -----------------------------------------------------------------------------
SELECT
    'silver.consumo_total_nacional_15min » temporal » minuto IN (0,15,30,45)',
    CASE WHEN SUM(CASE WHEN minute(timestamp_utc) NOT IN (0, 15, 30, 45) THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN minute(timestamp_utc) NOT IN (0, 15, 30, 45) THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN minute(timestamp_utc) NOT IN (0, 15, 30, 45) THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos com minuto não alinhado ao intervalo de 15 min')
FROM iceberg.silver.consumo_total_nacional_15min

UNION ALL

-- -----------------------------------------------------------------------------
-- 10. COMPLETUDE — completude diária (espera-se 96 registos/dia; tolera >= 90)
-- -----------------------------------------------------------------------------
SELECT
    'silver.consumo_total_nacional_15min » completeness » registos_dia >= 90',
    CASE WHEN SUM(CASE WHEN cnt < 90 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    CAST(SUM(CASE WHEN cnt < 90 THEN 1 ELSE 0 END) AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN cnt < 90 THEN 1 ELSE 0 END) AS VARCHAR),
           ' dias com menos de 90 registos (esperados 96/dia)')
FROM (
    SELECT CAST(timestamp_utc AS DATE) AS dia, COUNT(*) AS cnt
    FROM iceberg.silver.consumo_total_nacional_15min
    GROUP BY CAST(timestamp_utc AS DATE)
) AS daily_counts

UNION ALL

-- -----------------------------------------------------------------------------
-- 11. CONSISTÊNCIA DE COMPONENTES — flag_component_sum_mismatch (desvio > 0.001 kWh)
-- -----------------------------------------------------------------------------
SELECT
    'silver.consumo_total_nacional_15min » schema » flag_component_sum_mismatch < 1%',
    CASE WHEN ROUND(100.0 * SUM(CASE WHEN flag_component_sum_mismatch THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0), 2) < 1.0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN flag_component_sum_mismatch THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    1.0,
    CONCAT(CAST(SUM(CASE WHEN flag_component_sum_mismatch THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos com mismatch de componentes BT+MT+AT+MAT ≠ Total')
FROM iceberg.silver.consumo_total_nacional_15min

ORDER BY status DESC, check_name;


-- =============================================================================
-- DETALHE: dias com cobertura incompleta em consumo (análise exploratória)
-- =============================================================================
SELECT
    CAST(timestamp_utc AS DATE)          AS dia,
    COUNT(*)                             AS registos_existentes,
    96 - COUNT(*)                        AS registos_em_falta,
    ROUND(100.0 * COUNT(*) / 96.0, 1)   AS pct_completo
FROM iceberg.silver.consumo_total_nacional_15min
GROUP BY CAST(timestamp_utc AS DATE)
HAVING COUNT(*) < 90
ORDER BY dia;
