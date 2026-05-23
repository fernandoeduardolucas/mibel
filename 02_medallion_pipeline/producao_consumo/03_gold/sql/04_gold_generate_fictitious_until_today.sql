-- =====================================================
-- GERAR DADOS FICTÍCIOS NA GOLD ATÉ HOJE (UTC)
-- Objetivo:
--   Inserir horas fictícias da hora seguinte ao último registro
--   da tabela gold até a hora atual (UTC), preenchendo TODAS as colunas.
-- Regra adicional:
--   Não gerar valores numéricos a zero para linhas fictícias.
-- Estratégia de realismo:
--   1) Aprende perfis horários e semanais com histórico real (flag_missing_source=false)
--   2) Preserva relação produção/consumo observada na mesma hora
--   3) Adiciona ruído determinístico suave (sem quebras abruptas)
-- Tabela alvo:
--   iceberg.gold.dp_energia_balance_hourly
-- =====================================================

INSERT INTO iceberg.gold.dp_energia_balance_hourly (
    timestamp_utc,
    consumo_total_kwh,
    producao_total_kwh,
    producao_dgm_kwh,
    producao_pre_kwh,
    saldo_kwh,
    ratio_producao_consumo,
    flag_defice,
    flag_excedente,
    flag_missing_source,
    ano,
    mes
)
WITH
last_ts AS (
    SELECT MAX(timestamp_utc) AS max_ts
    FROM iceberg.gold.dp_energia_balance_hourly
),
range_limits AS (
    SELECT
        date_add('hour', 1, date_trunc('hour', max_ts)) AS start_ts,
        CAST(date_trunc('hour', current_timestamp AT TIME ZONE 'UTC') AS timestamp(6)) AS end_ts
    FROM last_ts
    WHERE max_ts IS NOT NULL
),
range_hours AS (
    SELECT ts AS timestamp_utc
    FROM range_limits
    CROSS JOIN UNNEST(
        IF(
            start_ts <= end_ts,
            sequence(start_ts, end_ts, INTERVAL '1' HOUR),
            CAST(ARRAY[] AS ARRAY(timestamp(6)))
        )
    ) AS t(ts)
),
hist AS (
    SELECT
        timestamp_utc,
        hour(timestamp_utc) AS h,
        day_of_week(timestamp_utc) AS dow,
        month(timestamp_utc) AS m,
        consumo_total_kwh,
        producao_total_kwh,
        producao_dgm_kwh,
        producao_pre_kwh,
        GREATEST(0.000001, producao_total_kwh / NULLIF(consumo_total_kwh, 0.0)) AS ratio_pc,
        GREATEST(0.0, producao_dgm_kwh / NULLIF(producao_total_kwh, 0.0)) AS share_dgm
    FROM iceberg.gold.dp_energia_balance_hourly
    WHERE flag_missing_source = false
      AND consumo_total_kwh > 0
      AND producao_total_kwh > 0
),
global_stats AS (
    SELECT
        COALESCE(avg(consumo_total_kwh), 50000.0) AS avg_consumo,
        COALESCE(avg(producao_total_kwh), 62000.0) AS avg_producao,
        COALESCE(avg(ratio_pc), 1.12) AS avg_ratio_pc,
        COALESCE(avg(share_dgm), 0.47) AS avg_share_dgm
    FROM hist
),
profile_hour AS (
    SELECT
        h,
        avg(consumo_total_kwh) AS avg_consumo_h,
        avg(ratio_pc) AS avg_ratio_pc_h,
        avg(share_dgm) AS avg_share_dgm_h
    FROM hist
    GROUP BY 1
),
profile_dow AS (
    SELECT
        dow,
        avg(consumo_total_kwh) AS avg_consumo_dow
    FROM hist
    GROUP BY 1
),
profile_month AS (
    SELECT
        m,
        avg(consumo_total_kwh) AS avg_consumo_m
    FROM hist
    GROUP BY 1
),
seasonal_factors AS (
    SELECT
        h.h,
        h.avg_consumo_h / NULLIF(g.avg_consumo, 0.0) AS fator_hora,
        h.avg_ratio_pc_h AS ratio_pc_h,
        h.avg_share_dgm_h AS share_dgm_h
    FROM profile_hour h
    CROSS JOIN global_stats g
),
synthetic AS (
    SELECT
        r.timestamp_utc,
        -- Consumo com fatores por hora + dia da semana + mês + ruído determinístico suave
        GREATEST(
            100.0,
            g.avg_consumo
            * COALESCE(sf.fator_hora, 1.0)
            * COALESCE(pd.avg_consumo_dow / NULLIF(g.avg_consumo, 0.0), 1.0)
            * COALESCE(pm.avg_consumo_m / NULLIF(g.avg_consumo, 0.0), 1.0)
            * (
                1.0
                + 0.018 * sin(2 * pi() * (hour(r.timestamp_utc) / 24.0))
                + 0.012 * (
                    (abs(from_big_endian_64(xxhash64(to_utf8(CAST(r.timestamp_utc AS varchar))))) % 1000) / 1000.0
                    - 0.5
                )
            )
        ) AS consumo_total_kwh,
        COALESCE(sf.ratio_pc_h, g.avg_ratio_pc) AS ratio_pc,
        COALESCE(sf.share_dgm_h, g.avg_share_dgm) AS share_dgm
    FROM range_hours r
    CROSS JOIN global_stats g
    LEFT JOIN seasonal_factors sf ON sf.h = hour(r.timestamp_utc)
    LEFT JOIN profile_dow pd ON pd.dow = day_of_week(r.timestamp_utc)
    LEFT JOIN profile_month pm ON pm.m = month(r.timestamp_utc)
),
final_values AS (
    SELECT
        timestamp_utc,
        consumo_total_kwh,
        GREATEST(consumo_total_kwh * 1.01, consumo_total_kwh * ratio_pc) AS producao_total_kwh,
        LEAST(0.95, GREATEST(0.05, share_dgm)) AS share_dgm
    FROM synthetic
)
SELECT
    timestamp_utc,
    consumo_total_kwh,
    producao_total_kwh,
    producao_total_kwh * share_dgm AS producao_dgm_kwh,
    producao_total_kwh * (1.0 - share_dgm) AS producao_pre_kwh,
    producao_total_kwh - consumo_total_kwh AS saldo_kwh,
    GREATEST(0.000001, producao_total_kwh / consumo_total_kwh) AS ratio_producao_consumo,
    false AS flag_defice,
    true AS flag_excedente,
    true AS flag_missing_source,
    year(timestamp_utc) AS ano,
    month(timestamp_utc) AS mes
FROM final_values;

-- Check rápido de cobertura gerada
SELECT
    MIN(timestamp_utc) AS min_inserted_ts,
    MAX(timestamp_utc) AS max_inserted_ts,
    COUNT(*) AS rows_inserted,
    SUM(CASE WHEN consumo_total_kwh IS NULL OR consumo_total_kwh = 0 THEN 1 ELSE 0 END) AS zero_or_null_consumo,
    SUM(CASE WHEN producao_total_kwh IS NULL OR producao_total_kwh = 0 THEN 1 ELSE 0 END) AS zero_or_null_producao_total,
    SUM(CASE WHEN producao_dgm_kwh IS NULL OR producao_dgm_kwh = 0 THEN 1 ELSE 0 END) AS zero_or_null_producao_dgm,
    SUM(CASE WHEN producao_pre_kwh IS NULL OR producao_pre_kwh = 0 THEN 1 ELSE 0 END) AS zero_or_null_producao_pre,
    SUM(CASE WHEN saldo_kwh IS NULL OR saldo_kwh = 0 THEN 1 ELSE 0 END) AS zero_or_null_saldo,
    SUM(CASE WHEN ratio_producao_consumo IS NULL OR ratio_producao_consumo = 0 THEN 1 ELSE 0 END) AS zero_or_null_ratio,
    SUM(CASE WHEN flag_defice IS NULL THEN 1 ELSE 0 END) AS null_flag_defice,
    SUM(CASE WHEN flag_excedente IS NULL THEN 1 ELSE 0 END) AS null_flag_excedente,
    SUM(CASE WHEN flag_missing_source IS NULL THEN 1 ELSE 0 END) AS null_flag_missing_source
FROM iceberg.gold.dp_energia_balance_hourly
WHERE flag_missing_source = true
  AND timestamp_utc >= (
      SELECT date_add('hour', 1, date_trunc('hour', MAX(timestamp_utc)))
      FROM iceberg.gold.dp_energia_balance_hourly
      WHERE flag_missing_source = false
  );
