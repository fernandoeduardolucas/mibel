-- =====================================================
-- GERAR DADOS FICTÍCIOS NA GOLD ATÉ HOJE (UTC)
-- Objetivo:
--   Inserir horas fictícias da hora seguinte ao último registro
--   da tabela gold até a hora atual (UTC).
-- Tabela alvo:
--   iceberg.gold.producao_vs_consumo_hourly
-- =====================================================

INSERT INTO iceberg.gold.producao_vs_consumo_hourly
WITH
last_row AS (
    SELECT
        timestamp_utc AS last_ts,
        consumo_total_kwh AS base_consumo,
        producao_total_kwh AS base_producao,
        producao_dgm_kwh AS base_dgm,
        producao_pre_kwh AS base_pre
    FROM iceberg.gold.producao_vs_consumo_hourly
    ORDER BY timestamp_utc DESC
    LIMIT 1
),
range_hours AS (
    SELECT ts AS timestamp_utc
    FROM last_row
    CROSS JOIN UNNEST(
        sequence(
            date_add('hour', 1, date_trunc('hour', last_ts)),
            CAST(date_trunc('hour', current_timestamp AT TIME ZONE 'UTC') AS timestamp(6)),
            INTERVAL '1' HOUR
        )
    ) AS t(ts)
),
synthetic AS (
    SELECT
        r.timestamp_utc,
        -- Sazonalidade horária + leve variação determinística
        GREATEST(0.0,
            l.base_consumo
            * (1 + 0.10 * sin(2 * pi() * (hour(r.timestamp_utc) / 24.0)))
            * (1 + 0.02 * sin(2 * pi() * (day_of_year(r.timestamp_utc) / 365.0)))
        ) AS consumo_total_kwh,

        GREATEST(0.0,
            l.base_producao
            * (1 + 0.12 * sin(2 * pi() * ((hour(r.timestamp_utc) - 2) / 24.0)))
            * (1 + 0.02 * sin(2 * pi() * (day_of_year(r.timestamp_utc) / 365.0)))
        ) AS producao_total_kwh,

        GREATEST(0.0,
            l.base_dgm
            * (1 + 0.11 * sin(2 * pi() * ((hour(r.timestamp_utc) - 1) / 24.0)))
            * (1 + 0.02 * sin(2 * pi() * (day_of_year(r.timestamp_utc) / 365.0)))
        ) AS producao_dgm_kwh,

        GREATEST(0.0,
            l.base_pre
            * (1 + 0.13 * sin(2 * pi() * ((hour(r.timestamp_utc) - 3) / 24.0)))
            * (1 + 0.02 * sin(2 * pi() * (day_of_year(r.timestamp_utc) / 365.0)))
        ) AS producao_pre_kwh,

        true AS flag_missing_source
    FROM range_hours r
    CROSS JOIN last_row l
)
SELECT
    timestamp_utc,
    consumo_total_kwh,
    producao_total_kwh,
    producao_dgm_kwh,
    producao_pre_kwh,
    producao_total_kwh - consumo_total_kwh AS saldo_kwh,
    CASE
        WHEN consumo_total_kwh = 0 THEN NULL
        ELSE producao_total_kwh / consumo_total_kwh
    END AS ratio_producao_consumo,
    producao_total_kwh < consumo_total_kwh AS flag_defice,
    producao_total_kwh > consumo_total_kwh AS flag_excedente,
    flag_missing_source
FROM synthetic;

-- Check rápido de cobertura gerada
SELECT
    MIN(timestamp_utc) AS min_inserted_ts,
    MAX(timestamp_utc) AS max_inserted_ts,
    COUNT(*) AS rows_inserted
FROM iceberg.gold.producao_vs_consumo_hourly
WHERE flag_missing_source = true
  AND timestamp_utc >= (
      SELECT date_add('hour', 1, date_trunc('hour', MAX(timestamp_utc)))
      FROM iceberg.gold.producao_vs_consumo_hourly
      WHERE flag_missing_source = false
  );
