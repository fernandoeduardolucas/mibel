-- =============================================================================
-- Bronze DDL — consumo_preco
-- Tabelas Iceberg nativas carregadas via workflow Flyte (INSERT via Trino).
-- Particionamento por process_date para idempotência e backfill diário.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Schema
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS iceberg.bronze;

-- -----------------------------------------------------------------------------
-- Tabela 1: consumo_raw
-- Origem: consumo-total-nacional.csv (granularidade 15 minutos)
-- Preserva todas as colunas da fonte + metadados de ingestão.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.bronze.consumo_raw (
    datahora          TIMESTAMP(6) WITH TIME ZONE,  -- timestamp original da fonte (UTC)
    dia               INTEGER,                       -- dia do mês (campo redundante da fonte)
    mes               INTEGER,                       -- mês (campo redundante da fonte)
    ano               INTEGER,                       -- ano (campo redundante da fonte)
    date_raw          VARCHAR,                       -- campo date original da fonte
    time_raw          VARCHAR,                       -- campo time original da fonte
    bt                DOUBLE,                        -- consumo BT (kW)
    mt                DOUBLE,                        -- consumo MT (kW)
    at                DOUBLE,                        -- consumo AT (kW)
    mat               DOUBLE,                        -- consumo MAT (kW)
    total             DOUBLE,                        -- consumo total nacional (kW)
    process_date      DATE                           -- data lógica de ingestão (partição)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['process_date'],
    location = 's3a://warehouse/bronze/consumo_raw/'
);

ALTER TABLE iceberg.bronze.consumo_raw
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true

COMMENT ON TABLE iceberg.bronze.consumo_raw IS
'Tabela Bronze com consumo elétrico nacional a 15 minutos, ingerida do CSV REN. Preserva todas as colunas da fonte sem transformação.';

COMMENT ON COLUMN iceberg.bronze.consumo_raw.datahora IS 'Timestamp original da fonte em UTC para o registo de 15 minutos.';
COMMENT ON COLUMN iceberg.bronze.consumo_raw.total IS 'Consumo total nacional no intervalo de 15 minutos em kW.';
COMMENT ON COLUMN iceberg.bronze.consumo_raw.process_date IS 'Data lógica de ingestão usada para idempotência e backfill.';

-- -----------------------------------------------------------------------------
-- Tabela 2: preco_raw
-- Origem: Day-ahead Market Prices_*.csv (granularidade horária, horas 1-25)
-- Preserva a numeração original das horas (1-25) sem interpretação UTC.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.bronze.preco_raw (
    date_raw              VARCHAR,   -- data original da fonte (string)
    hour                  INTEGER,   -- hora original OMIE (1-24, ou 25 em mudança DST)
    price_portugal_raw    DOUBLE,    -- preço Portugal (€/MWh)
    price_spain_raw       DOUBLE,    -- preço Espanha (€/MWh)
    process_date          DATE       -- data lógica de ingestão (partição)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['process_date'],
    location = 's3a://warehouse/bronze/preco_raw/'
);

ALTER TABLE iceberg.bronze.preco_raw
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true

COMMENT ON TABLE iceberg.bronze.preco_raw IS
'Tabela Bronze com preços day-ahead OMIE/MIBEL. Preserva numeração original das horas (1-25) sem interpretação UTC.';

COMMENT ON COLUMN iceberg.bronze.preco_raw.date_raw IS 'Data original da linha em formato string (fonte OMIE).';
COMMENT ON COLUMN iceberg.bronze.preco_raw.hour IS 'Hora original OMIE (1-24 normal; 25 em dias com mudança DST de outono).';
COMMENT ON COLUMN iceberg.bronze.preco_raw.price_portugal_raw IS 'Preço day-ahead de Portugal em €/MWh.';
COMMENT ON COLUMN iceberg.bronze.preco_raw.price_spain_raw IS 'Preço day-ahead de Espanha em €/MWh (preservado para referência futura).';
COMMENT ON COLUMN iceberg.bronze.preco_raw.process_date IS 'Data lógica de ingestão usada para idempotência e backfill.';
