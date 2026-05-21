-- =============================================================================
-- Gold DDL — meteo_producao
-- Produto: dp_meteo_producao_daily_features  (DP-03)
-- Entradas:
--   iceberg.silver.meteo_open_meteo_hourly       → variáveis meteorológicas
--   iceberg.gold.dp_energia_balance_hourly      → produção e consumo (DP-01)
--   iceberg.gold.dp_energy_market_hourly         → preço spot day-ahead (DP-02)
-- Saída:
--   iceberg.gold.dp_meteo_producao_daily_features
-- Consumidores: pipeline ML (meteo_producao_mlflow_flow.py), dashboards, API
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.gold
WITH (location = 's3a://warehouse/gold/');

-- -----------------------------------------------------------------------------
-- Tabela principal: dp_meteo_producao_daily_features
-- Grão: 1 linha por dia
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.gold.dp_meteo_producao_daily_features (
    -- Temporal key
    data_dia                    DATE,                     -- chave diária (UTC)
    year                        INTEGER,                  -- ano (partição)
    month                       INTEGER,                  -- mês (partição)
    dia_semana                  INTEGER,                  -- 0=Seg … 6=Dom
    is_weekend                  BOOLEAN,                  -- Sab/Dom
    estacao                     INTEGER,                  -- 1=Inv 2=Pri 3=Ver 4=Out
    -- Meteorology aggregates
    temperature_mean_c          DOUBLE,                   -- temperatura média diária (°C)
    temperature_min_c           DOUBLE,                   -- temperatura mínima diária (°C)
    temperature_max_c           DOUBLE,                   -- temperatura máxima diária (°C)
    precipitation_total_mm      DOUBLE,                   -- precipitação acumulada diária (mm)
    wind_speed_mean_ms          DOUBLE,                   -- velocidade vento média (m/s)
    wind_speed_max_ms           DOUBLE,                   -- velocidade vento máxima (m/s)
    radiation_mean_wm2          DOUBLE,                   -- radiação solar média (W/m²)
    radiation_total_kwh_m2      DOUBLE,                   -- radiação solar total diária (kWh/m²)
    cloud_cover_mean_pct        DOUBLE,                   -- nebulosidade média (%)
    -- Production & consumption aggregates
    producao_total_daily_mwh    DOUBLE,                   -- produção elétrica total diária (MWh)
    consumo_total_daily_mwh     DOUBLE,                   -- consumo elétrico total diário (MWh)
    saldo_daily_mwh             DOUBLE,                   -- saldo diário = produção - consumo (MWh)
    -- Price aggregates
    preco_spot_medio_eur_mwh    DOUBLE,                   -- preço spot médio diário (€/MWh) [TARGET ML]
    preco_spot_max_eur_mwh      DOUBLE,                   -- preço spot máximo diário (€/MWh)
    preco_spot_min_eur_mwh      DOUBLE,                   -- preço spot mínimo diário (€/MWh)
    -- Lag features (D-1)
    temp_lag_1d                 DOUBLE,                   -- temperatura média dia anterior
    wind_lag_1d                 DOUBLE,                   -- vento médio dia anterior
    radiation_lag_1d            DOUBLE,                   -- radiação média dia anterior
    producao_lag_1d             DOUBLE,                   -- produção total dia anterior
    preco_lag_1d                DOUBLE,                   -- preço médio dia anterior
    -- Rolling averages (7-day window)
    temp_rolling_7d_avg         DOUBLE,                   -- média móvel temp 7 dias
    wind_rolling_7d_avg         DOUBLE,                   -- média móvel vento 7 dias
    radiation_rolling_7d_avg    DOUBLE,                   -- média móvel radiação 7 dias
    producao_rolling_7d_avg     DOUBLE,                   -- média móvel produção 7 dias
    -- Metadata
    _updated_at                 TIMESTAMP                 -- timestamp de geração do registo
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month'],
    format_version = 2,
    object_store_layout_enabled = true,
    extra_properties = MAP(
        ARRAY['layer', 'data_product', 'schema_version', 'product_version', 'deprecated', 'domain', 'grain'],
        ARRAY['gold', 'dp_meteo_producao_daily_features', '1', 'v1', 'false', 'meteo_producao', 'daily']
    ),
    location = 's3a://warehouse/gold/dp_meteo_producao_daily_features/'
);

COMMENT ON TABLE iceberg.gold.dp_meteo_producao_daily_features IS
'DP-03: Feature table diária que cruza variáveis meteorológicas (temperatura, precipitação, vento, radiação solar) com produção elétrica nacional e preço spot day-ahead MIBEL. Usada pelo pipeline ML meteo_producao_mlflow_flow.py.';

COMMENT ON COLUMN iceberg.gold.dp_meteo_producao_daily_features.data_dia IS 'Chave diária UTC — grão primário do produto.';
COMMENT ON COLUMN iceberg.gold.dp_meteo_producao_daily_features.preco_spot_medio_eur_mwh IS 'TARGET: preço spot médio diário em €/MWh (média aritmética dos preços horários day-ahead MIBEL PT).';
COMMENT ON COLUMN iceberg.gold.dp_meteo_producao_daily_features.producao_total_daily_mwh IS 'Produção elétrica total diária: soma de producao_total_kwh / 1000 da tabela dp_energia_balance_hourly.';
COMMENT ON COLUMN iceberg.gold.dp_meteo_producao_daily_features.estacao IS '1=Inverno (Dez-Fev), 2=Primavera (Mar-Mai), 3=Verão (Jun-Ago), 4=Outono (Set-Nov).';


-- -----------------------------------------------------------------------------
-- INSERT: Populate dp_meteo_producao_daily_features
-- -----------------------------------------------------------------------------
INSERT INTO iceberg.gold.dp_meteo_producao_daily_features
WITH meteo_daily AS (
    SELECT
        CAST(ts_utc AS DATE)                   AS data_dia,
        YEAR(ts_utc)                           AS year,
        MONTH(ts_utc)                          AS month,
        AVG(temperature_2m)                    AS temperature_mean_c,
        MIN(temperature_2m)                    AS temperature_min_c,
        MAX(temperature_2m)                    AS temperature_max_c,
        SUM(precipitation)                     AS precipitation_total_mm,
        AVG(wind_speed_10m)                    AS wind_speed_mean_ms,
        MAX(wind_speed_10m)                    AS wind_speed_max_ms,
        AVG(shortwave_radiation)               AS radiation_mean_wm2,
        SUM(shortwave_radiation) / 1000.0      AS radiation_total_kwh_m2,
        AVG(cloud_cover)                       AS cloud_cover_mean_pct
    FROM iceberg.silver.meteo_open_meteo_hourly
    WHERE _quality_flag = 'ok'
    GROUP BY 1, 2, 3
),
producao_daily AS (
    SELECT
        CAST(timestamp_utc AS DATE)              AS data_dia,
        SUM(COALESCE(producao_total_kwh, 0)) / 1000.0 AS producao_total_daily_mwh,
        SUM(COALESCE(consumo_total_kwh, 0))  / 1000.0 AS consumo_total_daily_mwh,
        SUM(COALESCE(saldo_kwh, 0))          / 1000.0 AS saldo_daily_mwh
    FROM iceberg.gold.dp_energia_balance_hourly
    GROUP BY 1
),
preco_daily AS (
    SELECT
        CAST(ts_utc AS DATE)                   AS data_dia,
        AVG(market_price_pt)                   AS preco_spot_medio_eur_mwh,
        MAX(market_price_pt)                   AS preco_spot_max_eur_mwh,
        MIN(market_price_pt)                   AS preco_spot_min_eur_mwh
    FROM iceberg.gold.dp_energy_market_hourly
    GROUP BY 1
),
joined AS (
    SELECT
        m.data_dia,
        m.year,
        m.month,
        -- Temporal features
        DAY_OF_WEEK(m.data_dia) - 1                                    AS dia_semana,
        DAY_OF_WEEK(m.data_dia) IN (6, 7)                              AS is_weekend,
        CASE
            WHEN MONTH(m.data_dia) IN (12, 1, 2)  THEN 1
            WHEN MONTH(m.data_dia) IN (3, 4, 5)   THEN 2
            WHEN MONTH(m.data_dia) IN (6, 7, 8)   THEN 3
            ELSE 4
        END                                                             AS estacao,
        -- Meteorology
        m.temperature_mean_c,
        m.temperature_min_c,
        m.temperature_max_c,
        m.precipitation_total_mm,
        m.wind_speed_mean_ms,
        m.wind_speed_max_ms,
        m.radiation_mean_wm2,
        m.radiation_total_kwh_m2,
        m.cloud_cover_mean_pct,
        -- Production
        p.producao_total_daily_mwh,
        p.consumo_total_daily_mwh,
        p.saldo_daily_mwh,
        -- Price
        pr.preco_spot_medio_eur_mwh,
        pr.preco_spot_max_eur_mwh,
        pr.preco_spot_min_eur_mwh
    FROM meteo_daily m
    LEFT JOIN producao_daily p
        ON m.data_dia = p.data_dia
    LEFT JOIN preco_daily pr
        ON m.data_dia = pr.data_dia
),
with_lags AS (
    SELECT
        j.*,
        -- Lag D-1
        LAG(j.temperature_mean_c, 1)       OVER (ORDER BY j.data_dia) AS temp_lag_1d,
        LAG(j.wind_speed_mean_ms, 1)       OVER (ORDER BY j.data_dia) AS wind_lag_1d,
        LAG(j.radiation_mean_wm2, 1)       OVER (ORDER BY j.data_dia) AS radiation_lag_1d,
        LAG(j.producao_total_daily_mwh, 1) OVER (ORDER BY j.data_dia) AS producao_lag_1d,
        LAG(j.preco_spot_medio_eur_mwh, 1) OVER (ORDER BY j.data_dia) AS preco_lag_1d,
        -- Rolling 7-day averages
        AVG(j.temperature_mean_c)          OVER (ORDER BY j.data_dia ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS temp_rolling_7d_avg,
        AVG(j.wind_speed_mean_ms)          OVER (ORDER BY j.data_dia ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS wind_rolling_7d_avg,
        AVG(j.radiation_mean_wm2)          OVER (ORDER BY j.data_dia ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS radiation_rolling_7d_avg,
        AVG(j.producao_total_daily_mwh)    OVER (ORDER BY j.data_dia ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS producao_rolling_7d_avg
    FROM joined j
)
SELECT
    data_dia,
    year,
    month,
    dia_semana,
    is_weekend,
    estacao,
    temperature_mean_c,
    temperature_min_c,
    temperature_max_c,
    precipitation_total_mm,
    wind_speed_mean_ms,
    wind_speed_max_ms,
    radiation_mean_wm2,
    radiation_total_kwh_m2,
    cloud_cover_mean_pct,
    producao_total_daily_mwh,
    consumo_total_daily_mwh,
    saldo_daily_mwh,
    preco_spot_medio_eur_mwh,
    preco_spot_max_eur_mwh,
    preco_spot_min_eur_mwh,
    temp_lag_1d,
    wind_lag_1d,
    radiation_lag_1d,
    producao_lag_1d,
    preco_lag_1d,
    temp_rolling_7d_avg,
    wind_rolling_7d_avg,
    radiation_rolling_7d_avg,
    producao_rolling_7d_avg,
    CURRENT_TIMESTAMP AS _updated_at
FROM with_lags
ORDER BY data_dia;


-- =============================================================================
-- VALIDAÇÃO
-- =============================================================================

SELECT COUNT(*) AS linhas_gold
FROM iceberg.gold.dp_meteo_producao_daily_features;

SELECT
    MIN(data_dia) AS min_dia,
    MAX(data_dia) AS max_dia
FROM iceberg.gold.dp_meteo_producao_daily_features;

SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN preco_spot_medio_eur_mwh IS NULL THEN 1 ELSE 0 END) AS sem_preco,
    SUM(CASE WHEN producao_total_daily_mwh IS NULL THEN 1 ELSE 0 END) AS sem_producao
FROM iceberg.gold.dp_meteo_producao_daily_features;

SELECT *
FROM iceberg.gold.dp_meteo_producao_daily_features
ORDER BY data_dia
LIMIT 10;
