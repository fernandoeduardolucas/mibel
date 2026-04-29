-- ============================================
-- BACKFILL GOLD - preencher horas em falta
-- Tabela alvo:
--   iceberg.gold.producao_vs_consumo_hourly
-- Estratégia:
--   1) Gera calendário horário contínuo entre MIN e MAX timestamp_utc da Gold
--   2) Junta com a Gold existente
--   3) Para horas sem registo, imputa por média entre vizinho anterior e seguinte
--      (usando vizinho não-nulo mais próximo); se só existir um lado, usa esse valor
--   4) Recalcula saldo, ratio e flags
-- ============================================

DROP TABLE IF EXISTS iceberg.gold.producao_vs_consumo_hourly_backfilled;

CREATE TABLE iceberg.gold.producao_vs_consumo_hourly_backfilled
WITH (format = 'PARQUET') AS
WITH limits AS (
    SELECT
        date_trunc('hour', MIN(timestamp_utc)) AS min_ts,
        date_trunc('hour', MAX(timestamp_utc)) AS max_ts
    FROM iceberg.gold.producao_vs_consumo_hourly
),
hourly_calendar AS (
    SELECT date_add('hour', h, d) AS timestamp_utc
    FROM limits
    CROSS JOIN UNNEST(sequence(date_trunc('day', min_ts), date_trunc('day', max_ts), INTERVAL '1' DAY)) AS t(d)
    CROSS JOIN UNNEST(sequence(0, 23)) AS u(h)
    WHERE date_add('hour', h, d) BETWEEN min_ts AND max_ts
),
base AS (
    SELECT
        cal.timestamp_utc,
        g.consumo_total_kwh,
        g.producao_total_kwh,
        g.producao_dgm_kwh,
        g.producao_pre_kwh,
        CASE WHEN g.timestamp_utc IS NULL THEN true ELSE false END AS row_was_missing
    FROM hourly_calendar cal
    LEFT JOIN iceberg.gold.producao_vs_consumo_hourly g
      ON cal.timestamp_utc = g.timestamp_utc
),
enriched AS (
    SELECT
        *,
        max_by(consumo_total_kwh, timestamp_utc) FILTER (WHERE consumo_total_kwh IS NOT NULL)
            OVER (ORDER BY timestamp_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS prev_consumo_total_kwh,
        min_by(consumo_total_kwh, timestamp_utc) FILTER (WHERE consumo_total_kwh IS NOT NULL)
            OVER (ORDER BY timestamp_utc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS next_consumo_total_kwh,

        max_by(producao_total_kwh, timestamp_utc) FILTER (WHERE producao_total_kwh IS NOT NULL)
            OVER (ORDER BY timestamp_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS prev_producao_total_kwh,
        min_by(producao_total_kwh, timestamp_utc) FILTER (WHERE producao_total_kwh IS NOT NULL)
            OVER (ORDER BY timestamp_utc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS next_producao_total_kwh,

        max_by(producao_dgm_kwh, timestamp_utc) FILTER (WHERE producao_dgm_kwh IS NOT NULL)
            OVER (ORDER BY timestamp_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS prev_producao_dgm_kwh,
        min_by(producao_dgm_kwh, timestamp_utc) FILTER (WHERE producao_dgm_kwh IS NOT NULL)
            OVER (ORDER BY timestamp_utc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS next_producao_dgm_kwh,

        max_by(producao_pre_kwh, timestamp_utc) FILTER (WHERE producao_pre_kwh IS NOT NULL)
            OVER (ORDER BY timestamp_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS prev_producao_pre_kwh,
        min_by(producao_pre_kwh, timestamp_utc) FILTER (WHERE producao_pre_kwh IS NOT NULL)
            OVER (ORDER BY timestamp_utc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS next_producao_pre_kwh
    FROM base
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
        ) AS producao_pre_kwh,
        row_was_missing
    FROM enriched
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
FROM filled;

-- Check rápido de cobertura
SELECT
    COUNT(*) AS total_linhas,
    MIN(timestamp_utc) AS min_ts,
    MAX(timestamp_utc) AS max_ts,
    SUM(CASE WHEN flag_missing_source THEN 1 ELSE 0 END) AS linhas_missing_source
FROM iceberg.gold.producao_vs_consumo_hourly_backfilled;
