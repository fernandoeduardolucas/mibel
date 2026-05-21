-- =============================================================================
-- BRONZE DDL — meteo_producao
-- Tabelas Hive (external) sobre CSV e Parquet + tabela Iceberg gerida.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1) SCHEMAS
-- -----------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS hive.bronze_raw
WITH (location = 's3a://warehouse/bronze/raw/');

CREATE SCHEMA IF NOT EXISTS hive.bronze_stage
WITH (location = 's3a://warehouse/bronze/clean/');

CREATE SCHEMA IF NOT EXISTS iceberg.bronze
WITH (location = 's3a://warehouse/bronze/managed/');


-- -----------------------------------------------------------------------------
-- 2) RAW TABLE (CSV)
-- Todas as colunas em VARCHAR — tipagem real feita na Silver.
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS hive.bronze_raw.meteo_open_meteo_raw;

CREATE TABLE hive.bronze_raw.meteo_open_meteo_raw (
    ts_utc              VARCHAR,
    year                VARCHAR,
    month               VARCHAR,
    day                 VARCHAR,
    hour                VARCHAR,
    temperature_2m      VARCHAR,
    precipitation       VARCHAR,
    wind_speed_10m      VARCHAR,
    shortwave_radiation VARCHAR,
    cloud_cover         VARCHAR,
    latitude            VARCHAR,
    longitude           VARCHAR,
    elevation_m         VARCHAR,
    _source_file        VARCHAR,
    _ingested_at        VARCHAR
)
WITH (
    format = 'CSV',
    external_location = 's3a://warehouse/bronze/raw/meteo_open_meteo/',
    csv_separator = ',',
    skip_header_line_count = 1
);


-- -----------------------------------------------------------------------------
-- 3) STAGE TABLE (PARQUET LIMPO)
-- Aponta para o Parquet gerado pelo fetch_open_meteo.py.
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS hive.bronze_stage.meteo_open_meteo_clean;

CREATE TABLE hive.bronze_stage.meteo_open_meteo_clean (
    ts_utc              TIMESTAMP,
    year                INTEGER,
    month               INTEGER,
    day                 INTEGER,
    hour                INTEGER,
    temperature_2m      DOUBLE,
    precipitation       DOUBLE,
    wind_speed_10m      DOUBLE,
    shortwave_radiation DOUBLE,
    cloud_cover         DOUBLE,
    latitude            DOUBLE,
    longitude           DOUBLE,
    elevation_m         DOUBLE,
    _source_file        VARCHAR,
    _ingested_at        VARCHAR
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://warehouse/bronze/clean/meteo_open_meteo/'
);


-- -----------------------------------------------------------------------------
-- 4) BRONZE MANAGED TABLE (ICEBERG)
-- Materialização gerida — base para Silver.
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS iceberg.bronze.meteo_open_meteo_hourly;

CREATE TABLE iceberg.bronze.meteo_open_meteo_hourly (
    ts_utc              TIMESTAMP,
    year                INTEGER,
    month               INTEGER,
    day                 INTEGER,
    hour                INTEGER,
    temperature_2m      DOUBLE,   -- temperatura do ar a 2 m (°C)
    precipitation       DOUBLE,   -- precipitação acumulada na hora (mm)
    wind_speed_10m      DOUBLE,   -- velocidade do vento a 10 m (m/s)
    shortwave_radiation DOUBLE,   -- radiação solar de onda curta (W/m²)
    cloud_cover         DOUBLE,   -- nebulosidade total (%)
    latitude            DOUBLE,
    longitude           DOUBLE,
    elevation_m         DOUBLE,
    _source_file        VARCHAR,
    _ingested_at        VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month'],
    extra_properties = MAP(
        ARRAY['layer', 'domain', 'schema_version', 'retention_policy', 'source_system'],
        ARRAY['bronze', 'meteo_producao', '1', 'indefinite', 'open_meteo_api']
    ),
    location = 's3a://warehouse/bronze/meteo_producao/meteo_open_meteo_hourly/'
);

ALTER TABLE iceberg.bronze.meteo_open_meteo_hourly
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true,
    extra_properties = MAP(
        ARRAY['layer', 'domain', 'schema_version', 'retention_policy', 'source_system'],
        ARRAY['bronze', 'meteo_producao', '1', 'indefinite', 'open_meteo_api']
    );

COMMENT ON TABLE iceberg.bronze.meteo_open_meteo_hourly IS
'Tabela Bronze com dados meteorológicos horários para Portugal Continental, ingeridos da API Open-Meteo. Preserva todas as variáveis horárias sem transformação semântica.';

COMMENT ON COLUMN iceberg.bronze.meteo_open_meteo_hourly.ts_utc IS 'Timestamp horário UTC da observação meteorológica — chave de negócio Bronze.';
COMMENT ON COLUMN iceberg.bronze.meteo_open_meteo_hourly.temperature_2m IS 'Temperatura do ar a 2 metros de altitude (°C).';
COMMENT ON COLUMN iceberg.bronze.meteo_open_meteo_hourly.precipitation IS 'Precipitação acumulada na hora (mm).';
COMMENT ON COLUMN iceberg.bronze.meteo_open_meteo_hourly.wind_speed_10m IS 'Velocidade do vento a 10 metros (m/s).';
COMMENT ON COLUMN iceberg.bronze.meteo_open_meteo_hourly.shortwave_radiation IS 'Radiação solar de onda curta instantânea (W/m²).';
COMMENT ON COLUMN iceberg.bronze.meteo_open_meteo_hourly._source_file IS 'Nome do ficheiro/endpoint fonte para rastreabilidade da ingestão.';
COMMENT ON COLUMN iceberg.bronze.meteo_open_meteo_hourly.year IS 'Ano — coluna de partição.';
COMMENT ON COLUMN iceberg.bronze.meteo_open_meteo_hourly.month IS 'Mês — coluna de partição.';

