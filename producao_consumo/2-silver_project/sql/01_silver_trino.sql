-- ============================================
-- PROJETO SILVER
-- Entrada:
--   iceberg.bronze.consumo_total_nacional
--   iceberg.bronze.energia_produzida_total_nacional
-- Saída:
--   iceberg.silver.consumo_total_nacional_15min
--   iceberg.silver.energia_produzida_total_nacional_15min
-- ============================================

CREATE SCHEMA IF NOT EXISTS iceberg.silver
WITH (location = 's3a://warehouse/silver/');

-- ============================================
-- 1) SILVER CONSUMO 15 MIN
-- ============================================
DROP TABLE IF EXISTS iceberg.silver.consumo_total_nacional_15min;

CREATE TABLE iceberg.silver.consumo_total_nacional_15min
WITH (format = 'PARQUET') AS
WITH ranked AS (
    SELECT
        b.*,
        COALESCE(consumo_bt_kwh, 0) + COALESCE(consumo_mt_kwh, 0) + COALESCE(consumo_at_kwh, 0) + COALESCE(consumo_mat_kwh, 0) AS consumo_componentes_kwh,
        ABS(
            COALESCE(consumo_total_kwh, 0)
            - (
                COALESCE(consumo_bt_kwh, 0)
                + COALESCE(consumo_mt_kwh, 0)
                + COALESCE(consumo_at_kwh, 0)
                + COALESCE(consumo_mat_kwh, 0)
            )
        ) AS diff_componentes_total_kwh,
        ROW_NUMBER() OVER (
            PARTITION BY timestamp_utc
            ORDER BY
                CASE WHEN flag_zero_row THEN 1 ELSE 0 END,
                consumo_total_kwh DESC,
                duplicate_rank ASC,
                ingest_ts_utc DESC
        ) AS silver_pick_rank
    FROM iceberg.bronze.consumo_total_nacional b
    WHERE flag_bad_timestamp = false
      AND flag_bad_total = false
)
SELECT
    timestamp_utc,
    dia,
    mes,
    ano,
    data_local,
    hora_local_raw,
    consumo_bt_kwh,
    consumo_mt_kwh,
    consumo_at_kwh,
    consumo_mat_kwh,
    consumo_total_kwh,
    consumo_componentes_kwh,
    diff_componentes_total_kwh,
    diff_componentes_total_kwh > 0.001 AS flag_component_sum_mismatch,
    flag_duplicate_timestamp,
    duplicate_count,
    duplicate_rank AS bronze_duplicate_rank,
    flag_zero_row,
    datahora_raw,
    ingest_ts_utc
FROM ranked
WHERE silver_pick_rank = 1;

-- ============================================
-- 2) SILVER PRODUCAO 15 MIN
-- ============================================
DROP TABLE IF EXISTS iceberg.silver.energia_produzida_total_nacional_15min;

CREATE TABLE iceberg.silver.energia_produzida_total_nacional_15min
WITH (format = 'PARQUET') AS
WITH ranked AS (
    SELECT
        b.*,
        COALESCE(producao_dgm_kwh, 0) + COALESCE(producao_pre_kwh, 0) AS producao_componentes_kwh,
        ABS(
            COALESCE(producao_total_kwh, 0)
            - (
                COALESCE(producao_dgm_kwh, 0)
                + COALESCE(producao_pre_kwh, 0)
            )
        ) AS diff_componentes_total_kwh,
        ROW_NUMBER() OVER (
            PARTITION BY timestamp_utc
            ORDER BY
                CASE WHEN flag_zero_row THEN 1 ELSE 0 END,
                producao_total_kwh DESC,
                duplicate_rank ASC,
                ingest_ts_utc DESC
        ) AS silver_pick_rank
    FROM iceberg.bronze.energia_produzida_total_nacional b
    WHERE flag_bad_timestamp = false
      AND flag_bad_total = false
)
SELECT
    timestamp_utc,
    dia,
    mes,
    ano,
    data_local,
    hora_local_raw,
    producao_dgm_kwh,
    producao_pre_kwh,
    producao_total_kwh,
    producao_componentes_kwh,
    diff_componentes_total_kwh,
    diff_componentes_total_kwh > 0.001 AS flag_component_sum_mismatch,
    flag_duplicate_timestamp,
    duplicate_count,
    duplicate_rank AS bronze_duplicate_rank,
    flag_zero_row,
    datahora_raw,
    ingest_ts_utc
FROM ranked
WHERE silver_pick_rank = 1;

-- ============================================
-- 3) VALIDACAO
-- ============================================
SELECT COUNT(*) AS linhas_consumo_silver
FROM iceberg.silver.consumo_total_nacional_15min;

SELECT COUNT(*) AS linhas_producao_silver
FROM iceberg.silver.energia_produzida_total_nacional_15min;

SELECT
    COUNT(*) AS total_linhas,
    SUM(CASE WHEN flag_duplicate_timestamp THEN 1 ELSE 0 END) AS linhas_que_vinham_duplicadas,
    SUM(CASE WHEN flag_zero_row THEN 1 ELSE 0 END) AS linhas_zero_mantidas,
    SUM(CASE WHEN flag_component_sum_mismatch THEN 1 ELSE 0 END) AS linhas_com_inconsistencia_componentes
FROM iceberg.silver.consumo_total_nacional_15min;

SELECT
    COUNT(*) AS total_linhas,
    SUM(CASE WHEN flag_duplicate_timestamp THEN 1 ELSE 0 END) AS linhas_que_vinham_duplicadas,
    SUM(CASE WHEN flag_zero_row THEN 1 ELSE 0 END) AS linhas_zero_mantidas,
    SUM(CASE WHEN flag_component_sum_mismatch THEN 1 ELSE 0 END) AS linhas_com_inconsistencia_componentes
FROM iceberg.silver.energia_produzida_total_nacional_15min;

SELECT timestamp_utc, consumo_total_kwh
FROM iceberg.silver.consumo_total_nacional_15min
ORDER BY timestamp_utc
LIMIT 20;

SELECT timestamp_utc, producao_total_kwh
FROM iceberg.silver.energia_produzida_total_nacional_15min
ORDER BY timestamp_utc
LIMIT 20;
