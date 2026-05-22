-- =============================================================================
-- Silver DDL — Streaming_Data (DP-02 API pipeline)
-- Tabelas com sufixo _api para coexistir com o pipeline estático.
-- Particionadas por year/month. Carregadas via flyte_bronze_to_silver.py.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.silver;

-- -----------------------------------------------------------------------------
-- Tabela 1: consumo_api_hourly
-- Upstream: iceberg.bronze.consumo_api_raw
-- Transformação: MW → MWh (total * 1.0, já é horário), filtra nulos.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.silver.consumo_api_hourly (
    ts_utc      TIMESTAMP(6) WITH TIME ZONE,  -- timestamp horário canónico em UTC
    total_mwh   DOUBLE,                        -- carga horária em MWh
    year        INTEGER,                       -- ano (partição)
    month       INTEGER                        -- mês (partição)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month'],
    extra_properties = MAP(
        ARRAY['layer', 'domain', 'schema_version', 'grain', 'upstream_table'],
        ARRAY['silver', 'consumo_preco_api', '1', 'hourly', 'bronze.consumo_api_raw']
    ),
    location = 's3a://warehouse/silver/consumo_api_hourly/'
);

ALTER TABLE iceberg.silver.consumo_api_hourly
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true,
    extra_properties = MAP(
        ARRAY['layer', 'domain', 'schema_version', 'grain', 'upstream_table'],
        ARRAY['silver', 'consumo_preco_api', '1', 'hourly', 'bronze.consumo_api_raw']
    );

COMMENT ON TABLE iceberg.silver.consumo_api_hourly IS
'Tabela Silver com carga elétrica nacional horária normalizada. Origem: Energy-Charts API (ENTSO-E). Equivalente a consumo_hourly mas com fonte dinâmica.';

COMMENT ON COLUMN iceberg.silver.consumo_api_hourly.ts_utc IS 'Timestamp UTC canónico do início da hora.';
COMMENT ON COLUMN iceberg.silver.consumo_api_hourly.total_mwh IS 'Carga horária em MWh (= MW × 1h da fonte, já horária).';
COMMENT ON COLUMN iceberg.silver.consumo_api_hourly.year IS 'Ano derivado de ts_utc. Usado para particionamento.';
COMMENT ON COLUMN iceberg.silver.consumo_api_hourly.month IS 'Mês derivado de ts_utc. Usado para particionamento.';

-- -----------------------------------------------------------------------------
-- Tabela 2: preco_api_hourly
-- Upstream: iceberg.bronze.preco_api_raw
-- Transformação: deduplicação por ts_utc (GROUP BY + AVG), filtra nulos.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.silver.preco_api_hourly (
    ts_utc                  TIMESTAMP(6) WITH TIME ZONE,  -- timestamp horário canónico em UTC
    price_portugal_eur_mwh  DOUBLE,                        -- preço PT em €/MWh
    price_spain_eur_mwh     DOUBLE,                        -- preço ES em €/MWh (NULL se não disponível)
    year                    INTEGER,                       -- ano (partição)
    month                   INTEGER                        -- mês (partição)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['year', 'month'],
    extra_properties = MAP(
        ARRAY['layer', 'domain', 'schema_version', 'grain', 'upstream_table'],
        ARRAY['silver', 'consumo_preco_api', '1', 'hourly', 'bronze.preco_api_raw']
    ),
    location = 's3a://warehouse/silver/preco_api_hourly/'
);

ALTER TABLE iceberg.silver.preco_api_hourly
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true,
    extra_properties = MAP(
        ARRAY['layer', 'domain', 'schema_version', 'grain', 'upstream_table'],
        ARRAY['silver', 'consumo_preco_api', '1', 'hourly', 'bronze.preco_api_raw']
    );

COMMENT ON TABLE iceberg.silver.preco_api_hourly IS
'Tabela Silver com preços day-ahead horários normalizados. Origem: Energy-Charts API (OMIE/MIBEL zona PT). Equivalente a preco_hourly mas com fonte dinâmica.';

COMMENT ON COLUMN iceberg.silver.preco_api_hourly.ts_utc IS 'Timestamp UTC canónico do início da hora.';
COMMENT ON COLUMN iceberg.silver.preco_api_hourly.price_portugal_eur_mwh IS 'Preço day-ahead de Portugal em €/MWh.';
COMMENT ON COLUMN iceberg.silver.preco_api_hourly.price_spain_eur_mwh IS 'Preço day-ahead de Espanha em €/MWh. NULL quando o endpoint não disponibiliza.';
COMMENT ON COLUMN iceberg.silver.preco_api_hourly.year IS 'Ano derivado de ts_utc. Usado para particionamento.';
COMMENT ON COLUMN iceberg.silver.preco_api_hourly.month IS 'Mês derivado de ts_utc. Usado para particionamento.';
