-- ============================================
-- PROJETO GOLD — producao_consumo
-- Entrada:
--   iceberg.silver.consumo_total_nacional_15min
--   iceberg.silver.energia_produzida_total_nacional_15min
-- Saída:
--   iceberg.gold.dp_energia_balance_hourly
-- ============================================

CREATE SCHEMA IF NOT EXISTS iceberg.gold
WITH (location = 's3a://warehouse/gold/');

-- ============================================
-- TABELA: dp_energia_balance_hourly
-- Grão: 1 linha por hora UTC
-- Chave de negócio: timestamp_utc
-- ============================================
DROP TABLE IF EXISTS iceberg.gold.dp_energia_balance_hourly;

CREATE TABLE iceberg.gold.dp_energia_balance_hourly (
    timestamp_utc          TIMESTAMP NOT NULL,
    consumo_total_kwh      DOUBLE,
    producao_total_kwh     DOUBLE,
    producao_dgm_kwh       DOUBLE,
    producao_pre_kwh       DOUBLE,
    saldo_kwh              DOUBLE,
    ratio_producao_consumo DOUBLE,
    flag_defice            BOOLEAN,
    flag_excedente         BOOLEAN,
    flag_missing_source    BOOLEAN,
    ano                    INTEGER,
    mes                    INTEGER
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['ano', 'mes'],
    location = 's3a://warehouse/gold/dp_energia_balance_hourly/'
);

ALTER TABLE iceberg.gold.dp_energia_balance_hourly
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true;

COMMENT ON TABLE iceberg.gold.dp_energia_balance_hourly IS
'DP-01: Saldo horário entre produção e consumo elétrico nacional (REN/ERSE). Agrega dados a 15 minutos para granularidade horária UTC. Consumidores: dashboard operacional, API HTTP, pipeline ML de classificação de défice.';

COMMENT ON COLUMN iceberg.gold.dp_energia_balance_hourly.timestamp_utc IS 'Chave de negócio: timestamp UTC canónico da hora (início da hora).';
COMMENT ON COLUMN iceberg.gold.dp_energia_balance_hourly.consumo_total_kwh IS 'Consumo elétrico nacional horário em kWh (soma de 4 intervalos de 15 min).';
COMMENT ON COLUMN iceberg.gold.dp_energia_balance_hourly.producao_total_kwh IS 'Produção elétrica total nacional horária em kWh (DGM + PRE).';
COMMENT ON COLUMN iceberg.gold.dp_energia_balance_hourly.producao_dgm_kwh IS 'Produção DGM (Despacho Global do Mercado) horária em kWh.';
COMMENT ON COLUMN iceberg.gold.dp_energia_balance_hourly.producao_pre_kwh IS 'Produção PRE (Produção em Regime Especial) horária em kWh.';
COMMENT ON COLUMN iceberg.gold.dp_energia_balance_hourly.saldo_kwh IS 'Saldo horário = producao_total_kwh - consumo_total_kwh (positivo = excedente).';
COMMENT ON COLUMN iceberg.gold.dp_energia_balance_hourly.ratio_producao_consumo IS 'Rácio de cobertura = producao_total_kwh / consumo_total_kwh. NULL se consumo = 0.';
COMMENT ON COLUMN iceberg.gold.dp_energia_balance_hourly.flag_defice IS 'True se producao_total_kwh < consumo_total_kwh (ambas as fontes presentes).';
COMMENT ON COLUMN iceberg.gold.dp_energia_balance_hourly.flag_excedente IS 'True se producao_total_kwh > consumo_total_kwh (ambas as fontes presentes).';
COMMENT ON COLUMN iceberg.gold.dp_energia_balance_hourly.flag_missing_source IS 'True se consumo ou produção está ausente para esta hora.';
COMMENT ON COLUMN iceberg.gold.dp_energia_balance_hourly.ano IS 'Ano derivado de timestamp_utc — coluna de partição.';
COMMENT ON COLUMN iceberg.gold.dp_energia_balance_hourly.mes IS 'Mês derivado de timestamp_utc — coluna de partição.';

-- ============================================
-- INSERT: Popular dp_energia_balance_hourly
-- ============================================
INSERT INTO iceberg.gold.dp_energia_balance_hourly
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
    END AS flag_missing_source,
    YEAR(COALESCE(c.timestamp_utc, p.timestamp_utc))  AS ano,
    MONTH(COALESCE(c.timestamp_utc, p.timestamp_utc)) AS mes
FROM consumo_hourly c
FULL OUTER JOIN producao_hourly p
    ON c.timestamp_utc = p.timestamp_utc
ORDER BY 1;

-- ============================================
-- VALIDACAO
-- ============================================
SELECT COUNT(*) AS linhas_gold
FROM iceberg.gold.dp_energia_balance_hourly;

SELECT MIN(timestamp_utc) AS min_ts, MAX(timestamp_utc) AS max_ts
FROM iceberg.gold.dp_energia_balance_hourly;

SELECT
    SUM(CASE WHEN flag_defice THEN 1 ELSE 0 END) AS horas_com_defice,
    SUM(CASE WHEN flag_excedente THEN 1 ELSE 0 END) AS horas_com_excedente,
    SUM(CASE WHEN flag_missing_source THEN 1 ELSE 0 END) AS horas_com_fonte_em_falta
FROM iceberg.gold.dp_energia_balance_hourly;

SELECT *
FROM iceberg.gold.dp_energia_balance_hourly
ORDER BY timestamp_utc
LIMIT 24;
