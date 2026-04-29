-- ============================================
-- BACKFILL GOLD - preencher horas em falta
-- Tabela alvo:
--   iceberg.gold.producao_vs_consumo_hourly
-- Estratégia:
--   1) Gera calendário horário contínuo entre MIN timestamp_utc e hora atual
--   2) Junta com a Gold existente
--   3) Para horas sem registo, imputa por média entre vizinho anterior e seguinte
--      (usando vizinho não-nulo mais próximo); se só existir um lado, usa esse valor
--   4) Recalcula saldo, ratio e flags
-- ============================================

CREATE TABLE iceberg.gold.producao_vs_consumo_hourly_backfilled
WITH (format = 'PARQUET') AS
WITH consumo_hourly AS (
    SELECT
        date_trunc('hour', MIN(timestamp_utc)) AS min_ts,
        greatest(
            date_trunc('hour', MAX(timestamp_utc)),
            date_trunc('hour', current_timestamp)
        ) AS max_ts
    FROM iceberg.gold.producao_vs_consumo_hourly
),
hourly_calendar AS (
    SELECT date_add('hour', h, d) AS timestamp_utc
    FROM limits
    CROSS JOIN UNNEST(sequence(date_trunc('day', min_ts), date_trunc('day', max_ts), INTERVAL '1' DAY)) AS t(d)
    CROSS JOIN UNNEST(sequence(0, 23)) AS u(h)
    WHERE date_add('hour', h, d) BETWEEN min_ts AND max_ts
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
