-- ============================================
-- BACKFILL GOLD - preencher horas em falta
-- Tabela alvo:
--   iceberg.gold.producao_vs_consumo_hourly
-- Estratégia (sem sequence para evitar limite de 10k no Trino):
--   1) Reagrega as fontes Silver por hora
--   2) Recalcula a Gold completa hora a hora
--   3) Faz UNION com a Gold atual e mantém 1 linha por timestamp
--      (priorizando a linha recalculada)
-- ============================================

CREATE TABLE iceberg.gold.producao_vs_consumo_hourly_backfilled
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
),
recomputed_from_silver AS (
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
        END AS flag_missing_source,
        1 AS priority
    FROM consumo_hourly c
    FULL OUTER JOIN producao_hourly p
      ON c.timestamp_utc = p.timestamp_utc
),
existing_gold AS (
    SELECT
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
        2 AS priority
    FROM iceberg.gold.producao_vs_consumo_hourly
),
unioned AS (
    SELECT * FROM recomputed_from_silver
    UNION ALL
    SELECT * FROM existing_gold
),
deduplicated AS (
    SELECT
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
        row_number() OVER (PARTITION BY timestamp_utc ORDER BY priority) AS rn
    FROM unioned
)
SELECT
    timestamp_utc,
    consumo_total_kwh,
    producao_total_kwh,
    producao_dgm_kwh,
    producao_pre_kwh,
    saldo_kwh,
    ratio_producao_consumo,
    flag_defice,
    flag_excedente,
    flag_missing_source
FROM deduplicated
WHERE rn = 1;

-- Opcional: substituir a tabela oficial pela versão preenchida
-- DROP TABLE iceberg.gold.producao_vs_consumo_hourly;
-- ALTER TABLE iceberg.gold.producao_vs_consumo_hourly_backfilled
-- RENAME TO iceberg.gold.producao_vs_consumo_hourly;

-- Check rápido de cobertura
SELECT
    COUNT(*) AS total_linhas,
    MIN(timestamp_utc) AS min_ts,
    MAX(timestamp_utc) AS max_ts,
    SUM(CASE WHEN flag_missing_source THEN 1 ELSE 0 END) AS linhas_missing_source
FROM iceberg.gold.producao_vs_consumo_hourly_backfilled;
