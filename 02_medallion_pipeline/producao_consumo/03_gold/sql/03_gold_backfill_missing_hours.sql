-- ============================================
-- BACKFILL GOLD - preencher horas em falta
-- Tabela alvo:
--   iceberg.gold.producao_vs_consumo_hourly
-- Estratégia:
--   1) Gera calendário horário contínuo entre MIN e MAX timestamp_utc
--   2) Junta com a Gold existente
--   3) Para horas sem registo, faz imputação simples por média
--      entre valor anterior e seguinte (quando ambos existem)
--   4) Recalcula saldo, ratio e flags
-- ============================================

CREATE TABLE iceberg.gold.producao_vs_consumo_hourly_backfilled
WITH (format = 'PARQUET') AS
WITH limits AS (
    SELECT
        date_trunc('hour', MIN(timestamp_utc)) AS min_ts,
        date_trunc('hour', MAX(timestamp_utc)) AS max_ts
    FROM iceberg.gold.producao_vs_consumo_hourly
),
hourly_calendar AS (
    SELECT ts AS timestamp_utc
    FROM limits
    CROSS JOIN UNNEST(sequence(min_ts, max_ts, INTERVAL '1' HOUR)) AS t(ts)
),
base AS (
    SELECT
        cal.timestamp_utc,
        g.consumo_total_kwh,
        g.producao_total_kwh,
        g.producao_dgm_kwh,
        g.producao_pre_kwh,
        g.flag_missing_source,
        CASE WHEN g.timestamp_utc IS NULL THEN true ELSE false END AS row_was_missing
    FROM hourly_calendar cal
    LEFT JOIN iceberg.gold.producao_vs_consumo_hourly g
      ON g.timestamp_utc = cal.timestamp_utc
),
with_neighbors AS (
    SELECT
        *,
        lag(consumo_total_kwh) OVER (ORDER BY timestamp_utc) AS prev_consumo_total_kwh,
        lead(consumo_total_kwh) OVER (ORDER BY timestamp_utc) AS next_consumo_total_kwh,
        lag(producao_total_kwh) OVER (ORDER BY timestamp_utc) AS prev_producao_total_kwh,
        lead(producao_total_kwh) OVER (ORDER BY timestamp_utc) AS next_producao_total_kwh,
        lag(producao_dgm_kwh) OVER (ORDER BY timestamp_utc) AS prev_producao_dgm_kwh,
        lead(producao_dgm_kwh) OVER (ORDER BY timestamp_utc) AS next_producao_dgm_kwh,
        lag(producao_pre_kwh) OVER (ORDER BY timestamp_utc) AS prev_producao_pre_kwh,
        lead(producao_pre_kwh) OVER (ORDER BY timestamp_utc) AS next_producao_pre_kwh
    FROM base
),
filled AS (
    SELECT
        timestamp_utc,
        COALESCE(
            consumo_total_kwh,
            CASE
                WHEN prev_consumo_total_kwh IS NOT NULL AND next_consumo_total_kwh IS NOT NULL
                    THEN (prev_consumo_total_kwh + next_consumo_total_kwh) / 2.0
                ELSE prev_consumo_total_kwh
            END,
            next_consumo_total_kwh
        ) AS consumo_total_kwh,
        COALESCE(
            producao_total_kwh,
            CASE
                WHEN prev_producao_total_kwh IS NOT NULL AND next_producao_total_kwh IS NOT NULL
                    THEN (prev_producao_total_kwh + next_producao_total_kwh) / 2.0
                ELSE prev_producao_total_kwh
            END,
            next_producao_total_kwh
        ) AS producao_total_kwh,
        COALESCE(
            producao_dgm_kwh,
            CASE
                WHEN prev_producao_dgm_kwh IS NOT NULL AND next_producao_dgm_kwh IS NOT NULL
                    THEN (prev_producao_dgm_kwh + next_producao_dgm_kwh) / 2.0
                ELSE prev_producao_dgm_kwh
            END,
            next_producao_dgm_kwh
        ) AS producao_dgm_kwh,
        COALESCE(
            producao_pre_kwh,
            CASE
                WHEN prev_producao_pre_kwh IS NOT NULL AND next_producao_pre_kwh IS NOT NULL
                    THEN (prev_producao_pre_kwh + next_producao_pre_kwh) / 2.0
                ELSE prev_producao_pre_kwh
            END,
            next_producao_pre_kwh
        ) AS producao_pre_kwh,
        row_was_missing
    FROM with_neighbors
)
SELECT
    timestamp_utc,
    consumo_total_kwh,
    producao_total_kwh,
    producao_dgm_kwh,
    producao_pre_kwh,
    producao_total_kwh - consumo_total_kwh AS saldo_kwh,
    CASE
        WHEN consumo_total_kwh IS NULL OR consumo_total_kwh = 0 THEN NULL
        ELSE producao_total_kwh / consumo_total_kwh
    END AS ratio_producao_consumo,
    CASE
        WHEN consumo_total_kwh IS NOT NULL
         AND producao_total_kwh IS NOT NULL
         AND producao_total_kwh < consumo_total_kwh
        THEN true ELSE false
    END AS flag_defice,
    CASE
        WHEN consumo_total_kwh IS NOT NULL
         AND producao_total_kwh IS NOT NULL
         AND producao_total_kwh > consumo_total_kwh
        THEN true ELSE false
    END AS flag_excedente,
    row_was_missing AS flag_missing_source
FROM filled
ORDER BY timestamp_utc;

-- Opcional: substituir a tabela oficial pela versão preenchida
-- DROP TABLE iceberg.gold.producao_vs_consumo_hourly;
-- ALTER TABLE iceberg.gold.producao_vs_consumo_hourly_backfilled
-- RENAME TO iceberg.gold.producao_vs_consumo_hourly;

-- Check rápido de cobertura
SELECT
    COUNT(*) AS total_linhas,
    MIN(timestamp_utc) AS min_ts,
    MAX(timestamp_utc) AS max_ts,
    SUM(CASE WHEN flag_missing_source THEN 1 ELSE 0 END) AS linhas_imputadas
FROM iceberg.gold.producao_vs_consumo_hourly_backfilled;
