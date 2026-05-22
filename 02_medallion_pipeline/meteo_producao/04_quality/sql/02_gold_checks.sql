-- =============================================================================
-- Quality checks — Gold (meteo_producao)
-- Varredura única via CTE. Sem self-JOINs nem múltiplos scans.
-- Retorna: check_name | status | valor_pct | threshold_pct | detalhe
-- =============================================================================

WITH
gold AS (
    SELECT
        COUNT(*)                                                                                        AS total,
        COUNT(DISTINCT data_dia)                                                                        AS distinct_dias,
        SUM(CASE WHEN data_dia IS NULL                                          THEN 1 ELSE 0 END)      AS null_data_dia,
        SUM(CASE WHEN temperature_mean_c IS NULL                                THEN 1 ELSE 0 END)      AS null_temp,
        SUM(CASE WHEN producao_total_daily_mwh IS NULL                          THEN 1 ELSE 0 END)      AS null_producao,
        SUM(CASE WHEN preco_spot_medio_eur_mwh IS NULL                          THEN 1 ELSE 0 END)      AS null_preco,
        SUM(CASE WHEN temperature_mean_c < -10 OR temperature_mean_c > 50       THEN 1 ELSE 0 END)      AS bad_temp,
        SUM(CASE WHEN producao_total_daily_mwh < 0                              THEN 1 ELSE 0 END)      AS bad_producao,
        SUM(CASE WHEN estacao NOT IN (1, 2, 3, 4)                               THEN 1 ELSE 0 END)      AS bad_estacao,
        SUM(CASE WHEN dia_semana < 0 OR dia_semana > 6                          THEN 1 ELSE 0 END)      AS bad_dia_semana,
        SUM(CASE WHEN producao_total_daily_mwh IS NOT NULL
                  AND preco_spot_medio_eur_mwh IS NOT NULL                       THEN 1 ELSE 0 END)      AS dias_completos,
        MAX(data_dia)                                                                                   AS max_data_dia
    FROM iceberg.gold.dp_meteo_producao_daily_features
)

-- 1. row_count > 0
SELECT
    'gold.dp_meteo_producao_daily_features » row_count > 0'               AS check_name,
    CASE WHEN total > 0 THEN 'PASS' ELSE 'FAIL' END                       AS status,
    CAST(total AS DECIMAL(18, 2))                                          AS valor_pct,
    1.0                                                                    AS threshold_pct,
    CAST(total AS VARCHAR) || ' dias'                                      AS detalhe
FROM gold

UNION ALL

-- 2. null_rate: data_dia
SELECT
    'gold.dp_meteo_producao_daily_features » null_rate » data_dia',
    CASE WHEN null_data_dia = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_data_dia / NULLIF(total, 0), 2),
    0.0,
    CAST(null_data_dia AS VARCHAR) || ' nulos em ' || CAST(total AS VARCHAR) || ' linhas'
FROM gold

UNION ALL

-- 3. null_rate: temperature_mean_c
SELECT
    'gold.dp_meteo_producao_daily_features » null_rate » temperature_mean_c',
    CASE WHEN null_temp = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * null_temp / NULLIF(total, 0), 2),
    0.0,
    CAST(null_temp AS VARCHAR) || ' nulos'
FROM gold

UNION ALL

-- 4. null_rate: producao_total_daily_mwh (join com DP-01, pode ser NULL)
SELECT
    'gold.dp_meteo_producao_daily_features » null_rate » producao_total_daily_mwh',
    CASE WHEN null_producao = 0 THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * null_producao / NULLIF(total, 0), 2),
    0.0,
    CAST(null_producao AS VARCHAR) || ' dias sem dados de produção'
FROM gold

UNION ALL

-- 5. null_rate: preco_spot_medio_eur_mwh (join com DP-02, pode ser NULL)
SELECT
    'gold.dp_meteo_producao_daily_features » null_rate » preco_spot_medio_eur_mwh',
    CASE WHEN null_preco = 0 THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * null_preco / NULLIF(total, 0), 2),
    0.0,
    CAST(null_preco AS VARCHAR) || ' dias sem preço spot'
FROM gold

UNION ALL

-- 6. uniqueness: data_dia único (COUNT vs COUNT DISTINCT — sem GROUP BY)
SELECT
    'gold.dp_meteo_producao_daily_features » uniqueness » data_dia',
    CASE WHEN total = distinct_dias THEN 'PASS' ELSE 'FAIL' END,
    CAST(total - distinct_dias AS DECIMAL(18, 2)),
    0.0,
    CAST(total - distinct_dias AS VARCHAR) || ' data_dia duplicados'
FROM gold

UNION ALL

-- 7. range: temperature_mean_c [-10, 50]
SELECT
    'gold.dp_meteo_producao_daily_features » range » temperature_mean_c IN [-10, 50]',
    CASE WHEN bad_temp = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * bad_temp / NULLIF(total, 0), 4),
    0.0,
    CAST(bad_temp AS VARCHAR) || ' dias fora do intervalo físico [-10, 50] °C'
FROM gold

UNION ALL

-- 8. range: producao_total_daily_mwh >= 0
SELECT
    'gold.dp_meteo_producao_daily_features » range » producao_total_daily_mwh >= 0',
    CASE WHEN bad_producao = 0 THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * bad_producao / NULLIF(total, 0), 4),
    0.0,
    CAST(bad_producao AS VARCHAR) || ' dias com produção negativa'
FROM gold

UNION ALL

-- 9. range: estacao IN (1,2,3,4)
SELECT
    'gold.dp_meteo_producao_daily_features » range » estacao IN (1,2,3,4)',
    CASE WHEN bad_estacao = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * bad_estacao / NULLIF(total, 0), 4),
    0.0,
    CAST(bad_estacao AS VARCHAR) || ' dias com estacao fora de {1,2,3,4}'
FROM gold

UNION ALL

-- 10. range: dia_semana [0, 6]
SELECT
    'gold.dp_meteo_producao_daily_features » range » dia_semana IN (0..6)',
    CASE WHEN bad_dia_semana = 0 THEN 'PASS' ELSE 'FAIL' END,
    ROUND(100.0 * bad_dia_semana / NULLIF(total, 0), 4),
    0.0,
    CAST(bad_dia_semana AS VARCHAR) || ' dias com dia_semana fora de [0, 6]'
FROM gold

UNION ALL

-- 11. cross-DP coverage: dias com meteo + producao + preco >= 90%
SELECT
    'gold.dp_meteo_producao_daily_features » cross_dp » cobertura_total >= 90%',
    CASE WHEN ROUND(100.0 * dias_completos / NULLIF(total, 0), 2) >= 90.0 THEN 'PASS' ELSE 'WARN' END,
    ROUND(100.0 * (total - dias_completos) / NULLIF(total, 0), 2),
    10.0,
    CAST(ROUND(100.0 * dias_completos / NULLIF(total, 0), 1) AS VARCHAR) ||
    '% dos dias têm meteo + producao + preco'
FROM gold

UNION ALL

-- 12. freshness: dados recentes (< 400 dias de atraso)
SELECT
    'gold.dp_meteo_producao_daily_features » freshness » dados recentes (< 400 dias)',
    CASE WHEN date_diff('day', max_data_dia, CURRENT_DATE) < 400 THEN 'PASS' ELSE 'WARN' END,
    CAST(date_diff('day', max_data_dia, CURRENT_DATE) AS DECIMAL(18, 2)),
    400.0,
    'Último registo: ' || CAST(max_data_dia AS VARCHAR) ||
    ' (' || CAST(date_diff('day', max_data_dia, CURRENT_DATE) AS VARCHAR) || ' dias atrás)'
FROM gold

ORDER BY status DESC, check_name;
