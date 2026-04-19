-- ============================================
-- GOLD QUALITY CHECKS (producao_vs_consumo_hourly)
-- Objetivo: detetar meses com cobertura incompleta,
-- desbalanceamento anómalo de saldo e lacunas por fonte.
-- ============================================

-- 1) Cobertura mensal em horas (esperadas vs observadas)
--    Nota: expected_hours usa o intervalo real min->max do mês,
--    para evitar falsos positivos em meses parciais.
WITH month_bounds AS (
    SELECT
        date_trunc('month', timestamp_utc) AS mes,
        min(timestamp_utc) AS min_ts,
        max(timestamp_utc) AS max_ts
    FROM iceberg.gold.producao_vs_consumo_hourly
    GROUP BY 1
),
month_counts AS (
    SELECT
        date_trunc('month', timestamp_utc) AS mes,
        count(*) AS horas_observadas,
        sum(CASE WHEN consumo_total_kwh IS NULL THEN 1 ELSE 0 END) AS horas_sem_consumo,
        sum(CASE WHEN producao_total_kwh IS NULL THEN 1 ELSE 0 END) AS horas_sem_producao,
        sum(CASE WHEN flag_missing_source THEN 1 ELSE 0 END) AS horas_com_fonte_em_falta
    FROM iceberg.gold.producao_vs_consumo_hourly
    GROUP BY 1
)
SELECT
    c.mes,
    c.horas_observadas,
    CAST(date_diff('hour', b.min_ts, b.max_ts) + 1 AS bigint) AS horas_esperadas_no_intervalo,
    (CAST(date_diff('hour', b.min_ts, b.max_ts) + 1 AS bigint) - c.horas_observadas) AS horas_em_falta_no_intervalo,
    c.horas_sem_consumo,
    c.horas_sem_producao,
    c.horas_com_fonte_em_falta
FROM month_counts c
JOIN month_bounds b ON b.mes = c.mes
ORDER BY c.mes;

-- 2) Série mensal agregada para auditoria de drift de saldo/rácio
SELECT
    date_trunc('month', timestamp_utc) AS mes,
    sum(consumo_total_kwh) AS consumo_kwh,
    sum(producao_total_kwh) AS producao_kwh,
    sum(saldo_kwh) AS saldo_kwh,
    CASE
        WHEN sum(consumo_total_kwh) = 0 OR sum(consumo_total_kwh) IS NULL THEN NULL
        ELSE sum(producao_total_kwh) / sum(consumo_total_kwh)
    END AS ratio_pc,
    sum(CASE WHEN flag_defice THEN 1 ELSE 0 END) AS horas_defice,
    sum(CASE WHEN flag_excedente THEN 1 ELSE 0 END) AS horas_excedente,
    count(*) AS horas_lidas
FROM iceberg.gold.producao_vs_consumo_hourly
GROUP BY 1
ORDER BY 1;

-- 3) Horas com maiores desvios absolutos (top 100)
SELECT
    timestamp_utc,
    consumo_total_kwh,
    producao_total_kwh,
    saldo_kwh,
    ratio_producao_consumo,
    flag_missing_source
FROM iceberg.gold.producao_vs_consumo_hourly
ORDER BY abs(saldo_kwh) DESC NULLS LAST
LIMIT 100;
