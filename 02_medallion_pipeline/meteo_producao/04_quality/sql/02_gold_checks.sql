-- =============================================================================
-- CHECKS DE QUALIDADE — CAMADA GOLD (meteo_producao)
-- Executa no Trino. Retorna uma linha por verificação com colunas:
--   check_name | status (PASS/FAIL/WARN) | valor_pct | threshold_pct | detalhe
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. TAXA DE NULOS — data_dia em dp_meteo_producao_daily_features (chave de negócio)
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_meteo_producao_daily_features » null_rate » data_dia'      AS check_name,
    CASE WHEN SUM(CASE WHEN data_dia IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END                                    AS status,
    ROUND(100.0 * SUM(CASE WHEN data_dia IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                                     AS valor_pct,
    0.0                                                                 AS threshold_pct,
    CONCAT(CAST(SUM(CASE WHEN data_dia IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')          AS detalhe
FROM iceberg.gold.dp_meteo_producao_daily_features

UNION ALL

-- -----------------------------------------------------------------------------
-- 2. TAXA DE NULOS — temperature_mean_c (feature meteorológica principal)
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_meteo_producao_daily_features » null_rate » temperature_mean_c',
    CASE WHEN SUM(CASE WHEN temperature_mean_c IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN temperature_mean_c IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN temperature_mean_c IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' nulos em ', CAST(COUNT(*) AS VARCHAR), ' linhas')
FROM iceberg.gold.dp_meteo_producao_daily_features

UNION ALL

-- -----------------------------------------------------------------------------
-- 3. TAXA DE NULOS — producao_total_daily_mwh (join com DP-01 Gold)
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_meteo_producao_daily_features » null_rate » producao_total_daily_mwh',
    CASE WHEN SUM(CASE WHEN producao_total_daily_mwh IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN producao_total_daily_mwh IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN producao_total_daily_mwh IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' dias sem dados de produção (join com DP-01 Gold)')
FROM iceberg.gold.dp_meteo_producao_daily_features

UNION ALL

-- -----------------------------------------------------------------------------
-- 4. TAXA DE NULOS — preco_spot_medio_eur_mwh (target ML; join com DP-02 Gold)
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_meteo_producao_daily_features » null_rate » preco_spot_medio_eur_mwh',
    CASE WHEN SUM(CASE WHEN preco_spot_medio_eur_mwh IS NULL THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN preco_spot_medio_eur_mwh IS NULL THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN preco_spot_medio_eur_mwh IS NULL THEN 1 ELSE 0 END) AS VARCHAR),
           ' dias sem preço spot (join com DP-02 Gold)')
FROM iceberg.gold.dp_meteo_producao_daily_features

UNION ALL

-- -----------------------------------------------------------------------------
-- 5. UNICIDADE — data_dia único em dp_meteo_producao_daily_features (grão diário)
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_meteo_producao_daily_features » uniqueness » data_dia',
    CASE WHEN dup_groups = 0 THEN 'PASS' ELSE 'FAIL' END,
    CAST(dup_groups AS DECIMAL(18,2)),
    0.0,
    CONCAT(CAST(dup_groups AS VARCHAR), ' data_dia duplicados na Gold')
FROM (
    SELECT SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS dup_groups
    FROM (
        SELECT data_dia, COUNT(*) AS cnt
        FROM iceberg.gold.dp_meteo_producao_daily_features
        GROUP BY data_dia
    ) AS grouped
) AS dup_summary

UNION ALL

-- -----------------------------------------------------------------------------
-- 6. RANGE — temperature_mean_c: Portugal continental [-10, 50] °C
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_meteo_producao_daily_features » range » temperature_mean_c IN [-10, 50]',
    CASE WHEN SUM(CASE WHEN temperature_mean_c < -10 OR temperature_mean_c > 50 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN temperature_mean_c < -10 OR temperature_mean_c > 50 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN temperature_mean_c < -10 OR temperature_mean_c > 50 THEN 1 ELSE 0 END) AS VARCHAR),
           ' dias fora do intervalo físico [-10, 50] °C')
FROM iceberg.gold.dp_meteo_producao_daily_features

UNION ALL

-- -----------------------------------------------------------------------------
-- 7. RANGE — producao_total_daily_mwh >= 0 (produção nacional nunca negativa)
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_meteo_producao_daily_features » range » producao_total_daily_mwh >= 0',
    CASE WHEN SUM(CASE WHEN producao_total_daily_mwh < 0 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * SUM(CASE WHEN producao_total_daily_mwh < 0 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN producao_total_daily_mwh < 0 THEN 1 ELSE 0 END) AS VARCHAR),
           ' dias com produção negativa (fisicamente impossível)')
FROM iceberg.gold.dp_meteo_producao_daily_features

UNION ALL

-- -----------------------------------------------------------------------------
-- 8. RANGE — estacao IN (1, 2, 3, 4) — estações do ano codificadas
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_meteo_producao_daily_features » range » estacao IN (1,2,3,4)',
    CASE WHEN SUM(CASE WHEN estacao NOT IN (1, 2, 3, 4) THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN estacao NOT IN (1, 2, 3, 4) THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN estacao NOT IN (1, 2, 3, 4) THEN 1 ELSE 0 END) AS VARCHAR),
           ' dias com estacao fora de {1=Inv, 2=Pri, 3=Ver, 4=Out}')
FROM iceberg.gold.dp_meteo_producao_daily_features

UNION ALL

-- -----------------------------------------------------------------------------
-- 9. RANGE — dia_semana IN (0..6) — convenção 0=Seg, 6=Dom
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_meteo_producao_daily_features » range » dia_semana IN (0..6)',
    CASE WHEN SUM(CASE WHEN dia_semana < 0 OR dia_semana > 6 THEN 1 ELSE 0 END) = 0
         THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * SUM(CASE WHEN dia_semana < 0 OR dia_semana > 6 THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 4),
    0.0,
    CONCAT(CAST(SUM(CASE WHEN dia_semana < 0 OR dia_semana > 6 THEN 1 ELSE 0 END) AS VARCHAR),
           ' dias com dia_semana fora de [0, 6]')
FROM iceberg.gold.dp_meteo_producao_daily_features

UNION ALL

-- -----------------------------------------------------------------------------
-- 10. CONSISTÊNCIA DE LAGS — temp_lag_1d deve corresponder à temperatura do dia anterior
--     Tolerância de 0.01 °C para arredondamentos em ponto flutuante
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_meteo_producao_daily_features » lag_consistency » temp_lag_1d',
    CASE WHEN pct_inconsistent < 0.1 THEN 'PASS' ELSE 'FAIL' END,
    pct_inconsistent,
    0.1,
    CONCAT(CAST(inconsistent AS VARCHAR), ' de ', CAST(checked AS VARCHAR),
           ' dias com temp_lag_1d inconsistente (tolerância 0.01 °C)')
FROM (
    SELECT
        COUNT(*)                                                                    AS checked,
        SUM(CASE WHEN ABS(d.temp_lag_1d - prev.temperature_mean_c) > 0.01
                 THEN 1 ELSE 0 END)                                                 AS inconsistent,
        ROUND(100.0 * SUM(CASE WHEN ABS(d.temp_lag_1d - prev.temperature_mean_c) > 0.01
                               THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0), 4)                                             AS pct_inconsistent
    FROM iceberg.gold.dp_meteo_producao_daily_features AS d
    JOIN iceberg.gold.dp_meteo_producao_daily_features AS prev
        ON d.data_dia = date_add('day', 1, prev.data_dia)
    WHERE d.temp_lag_1d IS NOT NULL AND prev.temperature_mean_c IS NOT NULL
) AS lag_check

UNION ALL

-- -----------------------------------------------------------------------------
-- 11. COBERTURA CROSS-DP — % dias com dados de produção (DP-01) E preço (DP-02)
--     Espera-se que a maioria dos dias tenha ambos (joins LEFT)
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_meteo_producao_daily_features » cross_dp » cobertura_total >= 90%',
    CASE WHEN pct_completos >= 90.0 THEN 'PASS' ELSE 'WARN' END,
    100.0 - pct_completos,
    10.0,
    CONCAT(CAST(ROUND(pct_completos, 1) AS VARCHAR),
           '% dos dias têm meteo + producao + preco (threshold >= 90%)')
FROM (
    SELECT
        ROUND(100.0 * SUM(CASE WHEN producao_total_daily_mwh IS NOT NULL
                               AND preco_spot_medio_eur_mwh IS NOT NULL
                               THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0), 2) AS pct_completos
    FROM iceberg.gold.dp_meteo_producao_daily_features
) AS coverage

UNION ALL

-- -----------------------------------------------------------------------------
-- 12. FRESHNESS — tabela Gold tem dados recentes (< 400 dias de atraso)
-- -----------------------------------------------------------------------------
SELECT
    'gold.dp_meteo_producao_daily_features » freshness » dados recentes (< 400 dias)',
    CASE WHEN date_diff('day', MAX(data_dia), CURRENT_DATE) < 400
         THEN 'PASS' ELSE 'WARN' END,
    CAST(date_diff('day', MAX(data_dia), CURRENT_DATE) AS DECIMAL(18,2)),
    400.0,
    CONCAT('Último registo: ', CAST(MAX(data_dia) AS VARCHAR),
           ' (', CAST(date_diff('day', MAX(data_dia), CURRENT_DATE) AS VARCHAR),
           ' dias atrás)')
FROM iceberg.gold.dp_meteo_producao_daily_features

ORDER BY status DESC, check_name;


-- =============================================================================
-- DETALHE: estatísticas mensais de qualidade e cobertura cross-DP
-- =============================================================================
SELECT
    year,
    month,
    COUNT(*)                                                            AS dias_totais,
    SUM(CASE WHEN producao_total_daily_mwh IS NOT NULL THEN 1 ELSE 0 END)   AS dias_com_producao,
    SUM(CASE WHEN preco_spot_medio_eur_mwh IS NOT NULL THEN 1 ELSE 0 END)   AS dias_com_preco,
    SUM(CASE WHEN producao_total_daily_mwh IS NOT NULL
             AND preco_spot_medio_eur_mwh IS NOT NULL THEN 1 ELSE 0 END)    AS dias_completos,
    ROUND(AVG(temperature_mean_c), 1)                                   AS temp_media,
    ROUND(AVG(producao_total_daily_mwh), 0)                             AS producao_media_mwh,
    ROUND(AVG(preco_spot_medio_eur_mwh), 2)                             AS preco_medio_eur_mwh
FROM iceberg.gold.dp_meteo_producao_daily_features
GROUP BY year, month
ORDER BY year, month;
