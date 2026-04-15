-- ============================================
-- PROJETO GOLD
-- Entrada:
--   iceberg.silver.consumo_total_nacional_15min
--   iceberg.silver.energia_produzida_total_nacional_15min
-- Saída:
--   iceberg.gold.producao_vs_consumo_hourly
-- ============================================

CREATE SCHEMA IF NOT EXISTS iceberg.gold
WITH (location = 's3a://warehouse/gold/');

DROP TABLE IF EXISTS iceberg.gold.producao_vs_consumo_hourly;

CREATE TABLE iceberg.gold.producao_vs_consumo_hourly
WITH (format = 'PARQUET') AS
WITH consumo_hourly AS (
    SELECT
        date_trunc('hour', timestamp_utc) AS timestamp_utc,
        SUM(consumo_total_kwh) AS consumo_total_kwh
    FROM iceberg.silver.consumo_total_nacional_15min
    GROUP BY 1
),
producao_hourly AS (
    SELECT
        date_trunc('hour', timestamp_utc) AS timestamp_utc,
        SUM(producao_total_kwh) AS producao_total_kwh,
        SUM(producao_dgm_kwh) AS producao_dgm_kwh,
        SUM(producao_pre_kwh) AS producao_pre_kwh
    FROM iceberg.silver.energia_produzida_total_nacional_15min
    GROUP BY 1
)
SELECT
    COALESCE(c.timestamp_utc, p.timestamp_utc) AS timestamp_utc,
    c.consumo_total_kwh,
    p.producao_total_kwh,
    p.producao_dgm_kwh,
    p.producao_pre_kwh,
    p.producao_total_kwh - c.consumo_total_kwh AS saldo_kwh,
    CASE
        WHEN c.consumo_total_kwh IS NULL OR c.consumo_total_kwh = 0 THEN NULL
        ELSE p.producao_total_kwh / c.consumo_total_kwh
    END AS ratio_producao_consumo,
    CASE
        WHEN c.consumo_total_kwh IS NOT NULL
         AND p.producao_total_kwh IS NOT NULL
         AND p.producao_total_kwh < c.consumo_total_kwh
        THEN true ELSE false
    END AS flag_defice,
    CASE
        WHEN c.consumo_total_kwh IS NOT NULL
         AND p.producao_total_kwh IS NOT NULL
         AND p.producao_total_kwh > c.consumo_total_kwh
        THEN true ELSE false
    END AS flag_excedente,
    CASE
        WHEN c.timestamp_utc IS NULL OR p.timestamp_utc IS NULL THEN true
        ELSE false
    END AS flag_missing_source
FROM consumo_hourly c
FULL OUTER JOIN producao_hourly p
    ON c.timestamp_utc = p.timestamp_utc
ORDER BY 1;

-- ============================================
-- VALIDACAO
-- ============================================
SELECT COUNT(*) AS linhas_gold
FROM iceberg.gold.producao_vs_consumo_hourly;

SELECT MIN(timestamp_utc) AS min_ts, MAX(timestamp_utc) AS max_ts
FROM iceberg.gold.producao_vs_consumo_hourly;

SELECT
    SUM(CASE WHEN flag_defice THEN 1 ELSE 0 END) AS horas_com_defice,
    SUM(CASE WHEN flag_excedente THEN 1 ELSE 0 END) AS horas_com_excedente,
    SUM(CASE WHEN flag_missing_source THEN 1 ELSE 0 END) AS horas_com_fonte_em_falta
FROM iceberg.gold.producao_vs_consumo_hourly;

SELECT *
FROM iceberg.gold.producao_vs_consumo_hourly
ORDER BY timestamp_utc
LIMIT 24;
