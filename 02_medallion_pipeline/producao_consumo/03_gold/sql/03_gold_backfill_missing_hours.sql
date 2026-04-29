-- =====================================================
-- BACKFILL GOLD (em etapas simples)
-- Objetivo:
--   Inserir na tabela principal apenas as horas em falta.
-- Tabela alvo:
--   iceberg.gold.producao_vs_consumo_hourly
-- =====================================================

-- ETAPA 1) Limpeza de tabelas temporárias
DROP TABLE IF EXISTS iceberg.gold.gold_hourly_calendar;
DROP TABLE IF EXISTS iceberg.gold.gold_hourly_base;
DROP TABLE IF EXISTS iceberg.gold.gold_hourly_missing_filled;

-- ETAPA 2) Criar calendário horário completo (entre MIN e MAX)
CREATE TABLE iceberg.gold.gold_hourly_calendar AS
WITH limits AS (
    SELECT
        date_trunc('hour', MIN(timestamp_utc)) AS min_ts,
        date_trunc('hour', MAX(timestamp_utc)) AS max_ts
    FROM iceberg.gold.producao_vs_consumo_hourly
)
SELECT date_add('hour', h, d) AS timestamp_utc
FROM limits
CROSS JOIN UNNEST(sequence(date_trunc('day', min_ts), date_trunc('day', max_ts), INTERVAL '1' DAY)) AS t(d)
CROSS JOIN UNNEST(sequence(0, 23)) AS u(h)
WHERE date_add('hour', h, d) BETWEEN min_ts AND max_ts;

-- ETAPA 3) Juntar calendário com a Gold atual
CREATE TABLE iceberg.gold.gold_hourly_base AS
SELECT
    c.timestamp_utc,
    g.consumo_total_kwh,
    g.producao_total_kwh,
    g.producao_dgm_kwh,
    g.producao_pre_kwh,
    g.timestamp_utc IS NULL AS row_missing
FROM iceberg.gold.gold_hourly_calendar c
LEFT JOIN iceberg.gold.producao_vs_consumo_hourly g
  ON g.timestamp_utc = c.timestamp_utc;

-- ETAPA 4) Calcular valores imputados apenas para horas missing
CREATE TABLE iceberg.gold.gold_hourly_missing_filled AS
WITH neighbors AS (
    SELECT
        *,
        last_value(consumo_total_kwh) IGNORE NULLS OVER (
            ORDER BY timestamp_utc
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_consumo_total_kwh,
        first_value(consumo_total_kwh) IGNORE NULLS OVER (
            ORDER BY timestamp_utc
            ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
        ) AS next_consumo_total_kwh,

        last_value(producao_total_kwh) IGNORE NULLS OVER (
            ORDER BY timestamp_utc
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_producao_total_kwh,
        first_value(producao_total_kwh) IGNORE NULLS OVER (
            ORDER BY timestamp_utc
            ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
        ) AS next_producao_total_kwh,

        last_value(producao_dgm_kwh) IGNORE NULLS OVER (
            ORDER BY timestamp_utc
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_producao_dgm_kwh,
        first_value(producao_dgm_kwh) IGNORE NULLS OVER (
            ORDER BY timestamp_utc
            ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
        ) AS next_producao_dgm_kwh,

        last_value(producao_pre_kwh) IGNORE NULLS OVER (
            ORDER BY timestamp_utc
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_producao_pre_kwh,
        first_value(producao_pre_kwh) IGNORE NULLS OVER (
            ORDER BY timestamp_utc
            ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
        ) AS next_producao_pre_kwh
    FROM iceberg.gold.gold_hourly_base
),
filled AS (
    SELECT
        timestamp_utc,
        COALESCE(
            consumo_total_kwh,
            CASE
                WHEN prev_consumo_total_kwh IS NOT NULL AND next_consumo_total_kwh IS NOT NULL
                    THEN (prev_consumo_total_kwh + next_consumo_total_kwh) / 2
                ELSE COALESCE(prev_consumo_total_kwh, next_consumo_total_kwh)
            END
        ) AS consumo_total_kwh,
        COALESCE(
            producao_total_kwh,
            CASE
                WHEN prev_producao_total_kwh IS NOT NULL AND next_producao_total_kwh IS NOT NULL
                    THEN (prev_producao_total_kwh + next_producao_total_kwh) / 2
                ELSE COALESCE(prev_producao_total_kwh, next_producao_total_kwh)
            END
        ) AS producao_total_kwh,
        COALESCE(
            producao_dgm_kwh,
            CASE
                WHEN prev_producao_dgm_kwh IS NOT NULL AND next_producao_dgm_kwh IS NOT NULL
                    THEN (prev_producao_dgm_kwh + next_producao_dgm_kwh) / 2
                ELSE COALESCE(prev_producao_dgm_kwh, next_producao_dgm_kwh)
            END
        ) AS producao_dgm_kwh,
        COALESCE(
            producao_pre_kwh,
            CASE
                WHEN prev_producao_pre_kwh IS NOT NULL AND next_producao_pre_kwh IS NOT NULL
                    THEN (prev_producao_pre_kwh + next_producao_pre_kwh) / 2
                ELSE COALESCE(prev_producao_pre_kwh, next_producao_pre_kwh)
            END
        ) AS producao_pre_kwh
    FROM neighbors
    WHERE row_missing
)
SELECT *
FROM filled;

-- ETAPA 5) Inserir na tabela principal apenas as horas missing imputadas
INSERT INTO iceberg.gold.producao_vs_consumo_hourly
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
    true AS flag_missing_source
FROM iceberg.gold.gold_hourly_missing_filled;

-- ETAPA 6) Check rápido: quantas horas continuam em falta
SELECT COUNT(*) AS horas_em_falta
FROM iceberg.gold.gold_hourly_calendar c
LEFT JOIN iceberg.gold.producao_vs_consumo_hourly g
  ON g.timestamp_utc = c.timestamp_utc
WHERE g.timestamp_utc IS NULL;

-- ETAPA 7) Limpeza final (opcional)
DROP TABLE IF EXISTS iceberg.gold.gold_hourly_base;
DROP TABLE IF EXISTS iceberg.gold.gold_hourly_missing_filled;
DROP TABLE IF EXISTS iceberg.gold.gold_hourly_calendar;
