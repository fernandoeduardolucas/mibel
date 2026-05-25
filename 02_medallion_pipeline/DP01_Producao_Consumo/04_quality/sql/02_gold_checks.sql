-- =============================================================================
-- CHECKS DE QUALIDADE — CAMADA GOLD (producao_consumo)
-- Executa no Trino. Retorna uma linha por verificação com colunas:
--   check_name | status (PASS/FAIL/WARN) | valor_pct | threshold_pct | detalhe
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. TAXA DE NULOS — timestamp_utc em dp_energia_balance_hourly (chave de negócio)
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_energia_balance_hourly » null_rate » timestamp_utc'        AS check_name,
    CASE WHEN SUM(CASE WHEN timestamp_utc IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END                                    AS status,
    ROUND(100.0 * SUM(CASE WHEN timestamp_utc IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                                     AS valor_pct,
    0.0                                                                 AS threshold_pct,
    CONCAT(CAST(SUM(CASE WHEN timestamp_utc IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')          AS detalhe
FROM iceberg.gold.dp_energia_balance_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 2. TAXA DE NULOS — consumo_total_kwh em dp_energia_balance_hourly
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_energia_balance_hourly » null_rate » consumo_total_kwh',
    CASE WHEN SUM(CASE WHEN consumo_total_kwh IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN consumo_total_kwh IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN consumo_total_kwh IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' horas sem dados de consumo (FULL OUTER JOIN com producao)')
FROM iceberg.gold.dp_energia_balance_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 3. TAXA DE NULOS — producao_total_kwh em dp_energia_balance_hourly
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_energia_balance_hourly » null_rate » producao_total_kwh',
    CASE WHEN SUM(CASE WHEN producao_total_kwh IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN producao_total_kwh IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN producao_total_kwh IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' horas sem dados de produção (FULL OUTER JOIN com consumo)')
FROM iceberg.gold.dp_energia_balance_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 4. UNICIDADE — timestamp_utc único em dp_energia_balance_hourly
--    Cada hora UTC deve ter exatamente 1 registo (agregação de 4×15min)
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_energia_balance_hourly » uniqueness » timestamp_utc',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' timestamp_utc duplicados na Gold')
FROM (
    SELECT SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS dup_groups
    FROM (
        SELECT timestamp_utc, COUNT(*) AS cnt
        FROM iceberg.gold.dp_energia_balance_hourly
        GROUP BY timestamp_utc
    ) AS grouped
) AS dup_summary

UNION ALL

-- -----------------------------------------------------------------------------
-- 5. ALINHAMENTO TEMPORAL — timestamp_utc deve ser fronteira horária (min=0, seg=0)
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_energia_balance_hourly » temporal » timestamp_utc alinhado à hora',
    CASE WHEN SUM(CASE WHEN minute(timestamp_utc) <> 0 OR second(timestamp_utc) <> 0
                       THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN minute(timestamp_utc) <> 0 OR second(timestamp_utc) <> 0
                            THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN minute(timestamp_utc) <> 0 OR second(timestamp_utc) <> 0
                          THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos com timestamp não alinhado à hora UTC')
FROM iceberg.gold.dp_energia_balance_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 6. RANGE — saldo_kwh = producao - consumo (verificação de consistência interna)
--    Tolerância de 0.01 kWh para arredondamentos em ponto flutuante
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_energia_balance_hourly » consistency » saldo = producao - consumo',
    CASE WHEN pct_inconsistent < 0.1 THEN 'PASS' ELSE 'FAIL' END,
    pct_inconsistent,
    0.1,
    CONCAT(CAST(inconsistent AS VARCHAR), ' de ', CAST(checked AS VARCHAR),
           ' registos com saldo_kwh inconsistente (tolerância 0.01 kWh)')
FROM (
    SELECT
        COUNT(*)                                                                AS checked,
        SUM(CASE WHEN ABS(saldo_kwh - (producao_total_kwh - consumo_total_kwh)) > 0.01
                 THEN 1 ELSE 0 END)                                             AS inconsistent,
        ROUND(100.0 * SUM(CASE WHEN ABS(saldo_kwh - (producao_total_kwh - consumo_total_kwh)) > 0.01
                               THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0), 4)                                         AS pct_inconsistent
    FROM iceberg.gold.dp_energia_balance_hourly
    WHERE consumo_total_kwh IS NOT NULL AND producao_total_kwh IS NOT NULL
) AS saldo_check

UNION ALL

-- -----------------------------------------------------------------------------
-- 7. SCHEMA CONTRACT — flag_missing_source correto: TRUE quando consumo OU producao NULL
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_energia_balance_hourly » schema » flag_missing_source coerente',
    CASE WHEN SUM(CASE WHEN flag_missing_source <> (consumo_total_kwh IS NULL OR producao_total_kwh IS NULL)
                       THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN flag_missing_source <> (consumo_total_kwh IS NULL OR producao_total_kwh IS NULL)
                            THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN flag_missing_source <> (consumo_total_kwh IS NULL OR producao_total_kwh IS NULL)
                          THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos com flag_missing_source incoerente')
FROM iceberg.gold.dp_energia_balance_hourly

UNION ALL

-- -----------------------------------------------------------------------------
-- 8. COMPLETUDE HORÁRIA — cobertara >= 95% das horas no intervalo de datas observado
--    O FULL OUTER JOIN pode criar horas sem ambos os lados; verifica não exceder 5%
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_energia_balance_hourly » completeness » cobertura_horaria >= 95%',
    CASE WHEN pct_com_ambos >= 95.0 THEN 'PASS' ELSE 'WARN' END,
    100.0 - pct_com_ambos,
    5.0,
    CONCAT(CAST(ROUND(pct_com_ambos, 1) AS VARCHAR),
           '% das horas têm consumo E produção (threshold: >= 95%)')
FROM (
    SELECT
        ROUND(100.0 * SUM(CASE WHEN consumo_total_kwh IS NOT NULL AND producao_total_kwh IS NOT NULL
                               THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0), 2) AS pct_com_ambos
    FROM iceberg.gold.dp_energia_balance_hourly
) AS coverage

UNION ALL

-- -----------------------------------------------------------------------------
-- 9. RANGE — flag_defice coerente (TRUE apenas quando producao < consumo)
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_energia_balance_hourly » schema » flag_defice coerente',
    CASE WHEN SUM(CASE WHEN flag_defice <> (producao_total_kwh < consumo_total_kwh)
                       THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN flag_defice <> (producao_total_kwh < consumo_total_kwh)
                            THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN flag_defice <> (producao_total_kwh < consumo_total_kwh)
                          THEN 1 ELSE 0 END) AS VARCHAR),
           ' registos com flag_defice incoerente com saldo')
FROM iceberg.gold.dp_energia_balance_hourly
WHERE consumo_total_kwh IS NOT NULL AND producao_total_kwh IS NOT NULL

UNION ALL

-- -----------------------------------------------------------------------------
-- 10. FRESHNESS — tabela Gold tem dados recentes (< 400 dias de atraso)
--     Portugal: dados históricos de 2022-2024; não exige atualização em tempo real
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_energia_balance_hourly » freshness » dados recentes (< 400 dias)',
    CASE WHEN date_diff('day', MAX(CAST(timestamp_utc AS DATE)), CURRENT_DATE) < 400
         THEN 'PASS' ELSE 'WARN' END,
    CAST(date_diff('day', MAX(CAST(timestamp_utc AS DATE)), CURRENT_DATE) AS DECIMAL(18,2)),
    400.0,
    CONCAT('Último registo: ', CAST(MAX(CAST(timestamp_utc AS DATE)) AS VARCHAR),
           ' (', CAST(date_diff('day', MAX(CAST(timestamp_utc AS DATE)), CURRENT_DATE) AS VARCHAR),
           ' dias atrás)')
FROM iceberg.gold.dp_energia_balance_hourly

ORDER BY status DESC, check_name;


-- =============================================================================
-- DETALHE: distribuição mensal de flags e saldo energético
-- =============================================================================
SELECT
    ano,
    mes,
    COUNT(*)                                                                        AS horas_totais,
    SUM(CASE WHEN flag_defice         THEN 1 ELSE 0 END)                            AS horas_defice,
    SUM(CASE WHEN flag_excedente      THEN 1 ELSE 0 END)                            AS horas_excedente,
    SUM(CASE WHEN flag_missing_source THEN 1 ELSE 0 END)                            AS horas_fonte_em_falta,
    ROUND(SUM(consumo_total_kwh)   / 1e6, 2)                                        AS consumo_total_gwh,
    ROUND(SUM(producao_total_kwh)  / 1e6, 2)                                        AS producao_total_gwh,
    ROUND(SUM(saldo_kwh)           / 1e6, 2)                                        AS saldo_total_gwh
FROM iceberg.gold.dp_energia_balance_hourly
GROUP BY ano, mes
ORDER BY ano, mes;
