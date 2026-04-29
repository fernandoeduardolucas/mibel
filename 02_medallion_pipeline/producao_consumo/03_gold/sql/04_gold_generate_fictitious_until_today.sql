-- =====================================================
-- GERAR DADOS FICTÍCIOS NA GOLD ATÉ HOJE (UTC)
-- Objetivo:
--   Inserir horas fictícias da hora seguinte ao último registro
--   da tabela gold até a hora atual (UTC), preenchendo TODAS as colunas.
-- Tabela alvo:
--   iceberg.gold.producao_vs_consumo_hourly
-- =====================================================

INSERT INTO iceberg.gold.producao_vs_consumo_hourly
WITH
last_ts AS (
    SELECT MAX(timestamp_utc) AS max_ts
    FROM iceberg.gold.producao_vs_consumo_hourly
),
base_values AS (
    SELECT
        COALESCE(
            max_by(consumo_total_kwh, timestamp_utc) FILTER (WHERE consumo_total_kwh IS NOT NULL),
            50000.0
        ) AS base_consumo,
        COALESCE(
            max_by(producao_total_kwh, timestamp_utc) FILTER (WHERE producao_total_kwh IS NOT NULL),
            48000.0
        ) AS base_producao,
        COALESCE(
            max_by(producao_dgm_kwh, timestamp_utc) FILTER (WHERE producao_dgm_kwh IS NOT NULL),
            22000.0
        ) AS base_dgm,
        COALESCE(
            max_by(producao_pre_kwh, timestamp_utc) FILTER (WHERE producao_pre_kwh IS NOT NULL),
            26000.0
        ) AS base_pre
    FROM iceberg.gold.producao_vs_consumo_hourly
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
synthetic AS (
    SELECT
        r.timestamp_utc,

        GREATEST(
            100.0,
            b.base_consumo
            * (1 + 0.10 * sin(2 * pi() * (hour(r.timestamp_utc) / 24.0)))
            * (1 + 0.02 * sin(2 * pi() * (day_of_year(r.timestamp_utc) / 365.0)))
        ) AS consumo_total_kwh,

        GREATEST(
            100.0,
            b.base_producao
            * (1 + 0.12 * sin(2 * pi() * ((hour(r.timestamp_utc) - 2) / 24.0)))
            * (1 + 0.02 * sin(2 * pi() * (day_of_year(r.timestamp_utc) / 365.0)))
        ) AS producao_total_kwh,

        GREATEST(
            50.0,
            b.base_dgm
            * (1 + 0.11 * sin(2 * pi() * ((hour(r.timestamp_utc) - 1) / 24.0)))
            * (1 + 0.02 * sin(2 * pi() * (day_of_year(r.timestamp_utc) / 365.0)))
        ) AS producao_dgm_kwh,

        GREATEST(
            50.0,
            b.base_pre
            * (1 + 0.13 * sin(2 * pi() * ((hour(r.timestamp_utc) - 3) / 24.0)))
            * (1 + 0.02 * sin(2 * pi() * (day_of_year(r.timestamp_utc) / 365.0)))
        ) AS producao_pre_kwh,

        true AS flag_missing_source
    FROM range_hours r
    CROSS JOIN base_values b
)
SELECT
    timestamp_utc,
    consumo_total_kwh,
    producao_total_kwh,
    producao_dgm_kwh,
    producao_pre_kwh,
    producao_total_kwh - consumo_total_kwh AS saldo_kwh,
    producao_total_kwh / NULLIF(consumo_total_kwh, 0) AS ratio_producao_consumo,
    producao_total_kwh < consumo_total_kwh AS flag_defice,
    producao_total_kwh > consumo_total_kwh AS flag_excedente,
    flag_missing_source
FROM synthetic;

-- Check rápido de cobertura gerada
SELECT
    MIN(timestamp_utc) AS min_inserted_ts,
    MAX(timestamp_utc) AS max_inserted_ts,
    COUNT(*) AS rows_inserted,
    SUM(CASE WHEN consumo_total_kwh IS NULL THEN 1 ELSE 0 END) AS null_consumo,
    SUM(CASE WHEN producao_total_kwh IS NULL THEN 1 ELSE 0 END) AS null_producao_total,
    SUM(CASE WHEN producao_dgm_kwh IS NULL THEN 1 ELSE 0 END) AS null_producao_dgm,
    SUM(CASE WHEN producao_pre_kwh IS NULL THEN 1 ELSE 0 END) AS null_producao_pre,
    SUM(CASE WHEN saldo_kwh IS NULL THEN 1 ELSE 0 END) AS null_saldo,
    SUM(CASE WHEN ratio_producao_consumo IS NULL THEN 1 ELSE 0 END) AS null_ratio,
    SUM(CASE WHEN flag_defice IS NULL THEN 1 ELSE 0 END) AS null_flag_defice,
    SUM(CASE WHEN flag_excedente IS NULL THEN 1 ELSE 0 END) AS null_flag_excedente,
    SUM(CASE WHEN flag_missing_source IS NULL THEN 1 ELSE 0 END) AS null_flag_missing_source
FROM iceberg.gold.producao_vs_consumo_hourly
WHERE flag_missing_source = true
  AND timestamp_utc >= (
      SELECT date_add('hour', 1, date_trunc('hour', MAX(timestamp_utc)))
      FROM iceberg.gold.producao_vs_consumo_hourly
      WHERE flag_missing_source = false
  );
