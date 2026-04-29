-- =====================================================
-- BACKFILL GOLD (simples)
-- Objetivo:
--   Inserir na tabela principal apenas as horas em falta.
-- Tabela alvo:
--   iceberg.gold.producao_vs_consumo_hourly
-- Lógica de imputação (por coluna):
--   1) Se existir valor anterior e seguinte -> média dos dois
--   2) Se existir apenas um lado -> usa esse lado
--   3) Se não existir nenhum lado -> mantém NULL
-- =====================================================

INSERT INTO iceberg.gold.producao_vs_consumo_hourly
WITH limits AS (
    SELECT
        date_trunc('hour', MIN(timestamp_utc)) AS min_ts,
        date_trunc('hour', MAX(timestamp_utc)) AS max_ts
    FROM iceberg.gold.producao_vs_consumo_hourly
),
calendar AS (
    SELECT date_add('hour', h, d) AS timestamp_utc
    FROM limits
    CROSS JOIN UNNEST(sequence(date_trunc('day', min_ts), date_trunc('day', max_ts), INTERVAL '1' DAY)) AS t(d)
    CROSS JOIN UNNEST(sequence(0, 23)) AS u(h)
    WHERE date_add('hour', h, d) BETWEEN min_ts AND max_ts
),
missing_hours AS (
    SELECT c.timestamp_utc
    FROM calendar c
    LEFT JOIN iceberg.gold.producao_vs_consumo_hourly g
      ON g.timestamp_utc = c.timestamp_utc
    WHERE g.timestamp_utc IS NULL
),
filled_missing AS (
    SELECT
        m.timestamp_utc,

        CASE
            WHEN prev_c IS NOT NULL AND next_c IS NOT NULL THEN (prev_c + next_c) / 2
            ELSE COALESCE(prev_c, next_c)
        END AS consumo_total_kwh,

        CASE
            WHEN prev_pt IS NOT NULL AND next_pt IS NOT NULL THEN (prev_pt + next_pt) / 2
            ELSE COALESCE(prev_pt, next_pt)
        END AS producao_total_kwh,

        CASE
            WHEN prev_dgm IS NOT NULL AND next_dgm IS NOT NULL THEN (prev_dgm + next_dgm) / 2
            ELSE COALESCE(prev_dgm, next_dgm)
        END AS producao_dgm_kwh,

        CASE
            WHEN prev_pre IS NOT NULL AND next_pre IS NOT NULL THEN (prev_pre + next_pre) / 2
            ELSE COALESCE(prev_pre, next_pre)
        END AS producao_pre_kwh
    FROM (
        SELECT
            m.timestamp_utc,

            (SELECT g.consumo_total_kwh
             FROM iceberg.gold.producao_vs_consumo_hourly g
             WHERE g.timestamp_utc < m.timestamp_utc AND g.consumo_total_kwh IS NOT NULL
             ORDER BY g.timestamp_utc DESC
             LIMIT 1) AS prev_c,
            (SELECT g.consumo_total_kwh
             FROM iceberg.gold.producao_vs_consumo_hourly g
             WHERE g.timestamp_utc > m.timestamp_utc AND g.consumo_total_kwh IS NOT NULL
             ORDER BY g.timestamp_utc ASC
             LIMIT 1) AS next_c,

            (SELECT g.producao_total_kwh
             FROM iceberg.gold.producao_vs_consumo_hourly g
             WHERE g.timestamp_utc < m.timestamp_utc AND g.producao_total_kwh IS NOT NULL
             ORDER BY g.timestamp_utc DESC
             LIMIT 1) AS prev_pt,
            (SELECT g.producao_total_kwh
             FROM iceberg.gold.producao_vs_consumo_hourly g
             WHERE g.timestamp_utc > m.timestamp_utc AND g.producao_total_kwh IS NOT NULL
             ORDER BY g.timestamp_utc ASC
             LIMIT 1) AS next_pt,

            (SELECT g.producao_dgm_kwh
             FROM iceberg.gold.producao_vs_consumo_hourly g
             WHERE g.timestamp_utc < m.timestamp_utc AND g.producao_dgm_kwh IS NOT NULL
             ORDER BY g.timestamp_utc DESC
             LIMIT 1) AS prev_dgm,
            (SELECT g.producao_dgm_kwh
             FROM iceberg.gold.producao_vs_consumo_hourly g
             WHERE g.timestamp_utc > m.timestamp_utc AND g.producao_dgm_kwh IS NOT NULL
             ORDER BY g.timestamp_utc ASC
             LIMIT 1) AS next_dgm,

            (SELECT g.producao_pre_kwh
             FROM iceberg.gold.producao_vs_consumo_hourly g
             WHERE g.timestamp_utc < m.timestamp_utc AND g.producao_pre_kwh IS NOT NULL
             ORDER BY g.timestamp_utc DESC
             LIMIT 1) AS prev_pre,
            (SELECT g.producao_pre_kwh
             FROM iceberg.gold.producao_vs_consumo_hourly g
             WHERE g.timestamp_utc > m.timestamp_utc AND g.producao_pre_kwh IS NOT NULL
             ORDER BY g.timestamp_utc ASC
             LIMIT 1) AS next_pre
        FROM missing_hours m
    ) m
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
    true AS flag_missing_source
FROM filled_missing;

-- Check rápido do que falta após backfill
SELECT
    COUNT(*) AS horas_em_falta
FROM (
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
) cal
LEFT JOIN iceberg.gold.producao_vs_consumo_hourly g
  ON g.timestamp_utc = cal.timestamp_utc
WHERE g.timestamp_utc IS NULL;
