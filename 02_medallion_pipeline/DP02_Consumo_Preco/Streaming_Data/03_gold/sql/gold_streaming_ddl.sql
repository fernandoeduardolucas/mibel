-- =============================================================================
-- Gold DDL — Streaming_Data (DP-02 API pipeline)
-- Tabelas com sufixo _api para coexistir com o pipeline estático.
-- Schema idêntico ao pipeline estático — permite comparação direta entre fontes.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.gold;

-- -----------------------------------------------------------------------------
-- Tabela 1: dp_energy_market_api_hourly
-- Produto analítico principal — join consumo × preço + features calendário + lags.
-- Upstream: iceberg.silver.consumo_api_hourly + iceberg.silver.preco_api_hourly
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.gold.dp_energy_market_api_hourly (
    ts_utc                  TIMESTAMP(6) WITH TIME ZONE,  -- chave temporal primária
    consumo_total           DOUBLE,                        -- carga horária (MWh)
    market_price_pt         DOUBLE,                        -- preço day-ahead PT (€/MWh)
    hora                    INTEGER,                       -- hora do dia (0–23)
    dia_semana              INTEGER,                       -- dia da semana (0=Segunda … 6=Domingo)
    is_weekend              BOOLEAN,                       -- Sábado ou Domingo
    consumo_lag_1h          DOUBLE,                        -- consumo 1h antes (nullable)
    consumo_lag_24h         DOUBLE,                        -- consumo 24h antes (nullable)
    price_lag_1h            DOUBLE,                        -- preço 1h antes (nullable)
    rolling_avg_consumo_24h DOUBLE,                        -- média móvel 24h consumo (nullable)
    rolling_avg_price_24h   DOUBLE,                        -- média móvel 24h preço (nullable)
    process_date            DATE,                          -- data lógica de execução
    year                    INTEGER,                       -- ano (partição)
    month                   INTEGER                        -- mês (partição)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month'],
    location = 's3a://warehouse/gold/dp_energy_market_api_hourly/'
);

ALTER TABLE iceberg.gold.dp_energy_market_api_hourly
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true;

COMMENT ON TABLE iceberg.gold.dp_energy_market_api_hourly IS
'Produto analítico Gold com consumo e preços de energia horários + features de calendário e lags. Origem: Energy-Charts API. Schema idêntico a dp_energy_market_hourly (fonte estática).';

COMMENT ON COLUMN iceberg.gold.dp_energy_market_api_hourly.ts_utc IS 'Timestamp UTC horário. Chave natural do produto.';
COMMENT ON COLUMN iceberg.gold.dp_energy_market_api_hourly.consumo_total IS 'Carga elétrica nacional em MWh (ENTSO-E via Energy-Charts).';
COMMENT ON COLUMN iceberg.gold.dp_energy_market_api_hourly.market_price_pt IS 'Preço day-ahead Portugal em €/MWh (OMIE via Energy-Charts).';
COMMENT ON COLUMN iceberg.gold.dp_energy_market_api_hourly.consumo_lag_1h IS 'Consumo na hora anterior. NULL para a primeira hora da série.';
COMMENT ON COLUMN iceberg.gold.dp_energy_market_api_hourly.consumo_lag_24h IS 'Consumo 24 horas antes. NULL para as primeiras 24 horas.';
COMMENT ON COLUMN iceberg.gold.dp_energy_market_api_hourly.rolling_avg_consumo_24h IS 'Média móvel de consumo das últimas 24 horas (janela de 24 registos).';

-- -----------------------------------------------------------------------------
-- Tabela 2: feat_load_forecasting_api_hourly
-- Feature table para ML — Gold + target consumo_next_hour.
-- Upstream: iceberg.gold.dp_energy_market_api_hourly
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.gold.feat_load_forecasting_api_hourly (
    ts_utc                  TIMESTAMP(6) WITH TIME ZONE,
    consumo_total           DOUBLE,
    market_price_pt         DOUBLE,
    hora                    INTEGER,
    dia_semana              INTEGER,
    is_weekend              BOOLEAN,
    consumo_lag_1h          DOUBLE,
    consumo_lag_24h         DOUBLE,
    price_lag_1h            DOUBLE,
    rolling_avg_consumo_24h DOUBLE,
    rolling_avg_price_24h   DOUBLE,
    consumo_next_hour       DOUBLE,                        -- variável alvo ML (LEAD 1h)
    process_date            DATE,
    year                    INTEGER,
    month                   INTEGER
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month'],
    location = 's3a://warehouse/gold/feat_load_forecasting_api_hourly/'
);

ALTER TABLE iceberg.gold.feat_load_forecasting_api_hourly
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true;

COMMENT ON TABLE iceberg.gold.feat_load_forecasting_api_hourly IS
'Feature table Gold para treino de modelos ML de previsão de carga. Inclui variável alvo consumo_next_hour (LEAD 1h). Origem: Energy-Charts API.';

COMMENT ON COLUMN iceberg.gold.feat_load_forecasting_api_hourly.consumo_next_hour IS 'Consumo da hora seguinte (variável alvo para load forecasting). Última linha da série excluída.';
