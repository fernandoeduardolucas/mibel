-- =============================================================================
-- Bronze DDL — REN DataHub (segunda fonte para cross-validation)
-- preco_ren_raw:    24 preços horários/dia PT+ES (hora local portuguesa 1-24)
-- consumo_ren_daily: consumo diário em GWh (só agregado diário disponível na API)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.bronze;

-- -----------------------------------------------------------------------------
-- Tabela 1: preco_ren_raw
-- Origem: https://servicebus.ren.pt/datahubapi/electricity/ElectricityMarketPricesDaily
-- Granularidade: horária (horas 1-24 em hora local portuguesa WET/WEST)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.bronze.preco_ren_raw (
    data_local          DATE,     -- data em hora local portuguesa
    hora_local          INTEGER,  -- hora do dia 1-24 (hora local WET/WEST)
    price_pt_eur_mwh    DOUBLE,   -- preço day-ahead PT (€/MWh)
    price_es_eur_mwh    DOUBLE,   -- preço day-ahead ES (€/MWh)
    source_url          VARCHAR,
    fetch_date          DATE,
    process_date        DATE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['process_date'],
    location = 's3a://warehouse/bronze/preco_ren_raw/'
);

ALTER TABLE iceberg.bronze.preco_ren_raw
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true;

COMMENT ON TABLE iceberg.bronze.preco_ren_raw IS
'Preços day-ahead horários MIBEL (PT+ES) obtidos via REN DataHub. Horas em hora local portuguesa (WET/WEST). Fonte secundária para cross-validation com Energy-Charts.';

-- -----------------------------------------------------------------------------
-- Tabela 2: consumo_ren_daily
-- Origem: https://servicebus.ren.pt/datahubapi/electricity/ElectricityConsumptionSupplyDaily
-- Granularidade: diária (API REN só disponibiliza totais diários para consumo)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.bronze.consumo_ren_daily (
    data_local      DATE,    -- data em hora local portuguesa
    consumo_gwh     DOUBLE,  -- consumo nacional (GWh)
    source_url      VARCHAR,
    fetch_date      DATE,
    process_date    DATE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['process_date'],
    location = 's3a://warehouse/bronze/consumo_ren_daily/'
);

ALTER TABLE iceberg.bronze.consumo_ren_daily
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true;

COMMENT ON TABLE iceberg.bronze.consumo_ren_daily IS
'Consumo nacional diário (GWh) obtido via REN DataHub. Granularidade diária — sem detalhe horário na API. Usado para validar totais diários do pipeline Energy-Charts.';
