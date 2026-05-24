-- =============================================================================
-- Bronze DDL — consumo_preco
-- Tabelas Iceberg nativas carregadas via workflow Flyte (INSERT via Trino).
-- Particionamento por process_date para idempotência e backfill diário.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Landing zone
-- Schema Hive externo que aponta para os CSVs raw em MinIO.
-- Os CSVs devem ser copiados para warehouse/landing/ antes de executar DDL.
-- Alternativa ao boto3 direto: permite query SQL dos ficheiros raw via Trino.
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS hive.landing
WITH (location = 's3a://warehouse/landing/');

CREATE TABLE IF NOT EXISTS hive.landing.consumo_raw_csv (
    datahora   VARCHAR,
    dia        VARCHAR,
    mes        VARCHAR,
    ano        VARCHAR,
    date_raw   VARCHAR,
    time_raw   VARCHAR,
    bt         VARCHAR,
    mt         VARCHAR,
    at         VARCHAR,
    mat        VARCHAR,
    total      VARCHAR
)
WITH (
    format = 'CSV',
    external_location = 's3a://warehouse/landing/consumo/',
    skip_header_line_count = 1,
    csv_separator = ','
);

CREATE TABLE IF NOT EXISTS hive.landing.preco_raw_csv (
    date_raw            VARCHAR,
    hour                VARCHAR,
    price_portugal_raw  VARCHAR,
    price_spain_raw     VARCHAR
)
WITH (
    format = 'CSV',
    external_location = 's3a://warehouse/landing/preco/',
    skip_header_line_count = 3,
    csv_separator = ';'
);

-- -----------------------------------------------------------------------------
-- Schema Bronze (Iceberg)
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS iceberg.bronze
WITH (location = 's3a://warehouse/bronze/');

-- -----------------------------------------------------------------------------
-- Tabela 1: consumo_raw
-- Origem: consumo-total-nacional.csv (granularidade 15 minutos)
-- Preserva todas as colunas da fonte + metadados de ingestão.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.bronze.consumo_raw (
    datahora          TIMESTAMP(6) WITH TIME ZONE,
    dia               INTEGER,
    mes               INTEGER,
    ano               INTEGER,
    date_raw          VARCHAR,
    time_raw          VARCHAR,
    bt                DOUBLE,
    mt                DOUBLE,
    at                DOUBLE,
    mat               DOUBLE,
    total             DOUBLE,
    process_date      DATE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['process_date'],
    location = 's3a://warehouse/bronze/consumo_raw/'
);

ALTER TABLE iceberg.bronze.consumo_raw
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true;

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
    date_raw              VARCHAR,
    hour                  INTEGER,
    price_portugal_raw    DOUBLE,
    price_spain_raw       DOUBLE,
    process_date          DATE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['process_date'],
    location = 's3a://warehouse/bronze/preco_raw/'
);

ALTER TABLE iceberg.bronze.preco_raw
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true;

COMMENT ON TABLE iceberg.bronze.preco_raw IS
'Tabela Bronze com preços day-ahead OMIE/MIBEL. Preserva numeração original das horas (1-25) sem interpretação UTC.';

COMMENT ON COLUMN iceberg.bronze.preco_raw.date_raw IS 'Data original da linha em formato string (fonte OMIE).';
COMMENT ON COLUMN iceberg.bronze.preco_raw.hour IS 'Hora original OMIE (1-24 normal; 25 em dias com mudança DST de outono).';
COMMENT ON COLUMN iceberg.bronze.preco_raw.price_portugal_raw IS 'Preço day-ahead de Portugal em €/MWh.';
COMMENT ON COLUMN iceberg.bronze.preco_raw.price_spain_raw IS 'Preço day-ahead de Espanha em €/MWh (preservado para referência futura).';
COMMENT ON COLUMN iceberg.bronze.preco_raw.process_date IS 'Data lógica de ingestão usada para idempotência e backfill.';
