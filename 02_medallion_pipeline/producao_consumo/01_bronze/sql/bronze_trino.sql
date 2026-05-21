-- =========================================================
-- BRONZE PIPELINE - TRINO SQL
-- =========================================================
-- Estrutura:
--   1) Schemas
--   2) Tabelas RAW em Hive sobre CSV
--   3) Tabelas STAGE em Hive sobre Parquet limpo
--   4) Tabelas BRONZE geridas em Iceberg
--   5) Queries de validação
-- =========================================================


-- =========================================================
-- 1) SCHEMAS
-- =========================================================

CREATE SCHEMA IF NOT EXISTS hive.bronze_raw
WITH (location = 's3a://warehouse/bronze/raw/');

CREATE SCHEMA IF NOT EXISTS hive.bronze_stage
WITH (location = 's3a://warehouse/bronze/clean/');

CREATE SCHEMA IF NOT EXISTS iceberg.bronze
WITH (location = 's3a://warehouse/bronze/managed/');


-- =========================================================
-- 2) RAW TABLES (CSV)
-- =========================================================
-- No Hive com CSV, as colunas devem ficar em VARCHAR.
-- A tipagem real é feita depois.
-- =========================================================

DROP TABLE IF EXISTS hive.bronze_raw.consumo_total_nacional_raw;

CREATE TABLE hive.bronze_raw.consumo_total_nacional_raw (
    datahora VARCHAR,
    dia VARCHAR,
    mes VARCHAR,
    ano VARCHAR,
    "date" VARCHAR,
    "time" VARCHAR,
    bt VARCHAR,
    mt VARCHAR,
    at VARCHAR,
    mat VARCHAR,
    total VARCHAR
)
WITH (
    format = 'CSV',
    external_location = 's3a://warehouse/bronze/raw/consumo_total_nacional/',
    csv_separator = ',',
    skip_header_line_count = 1
);

DROP TABLE IF EXISTS hive.bronze_raw.energia_produzida_total_nacional_raw;

CREATE TABLE hive.bronze_raw.energia_produzida_total_nacional_raw (
    datahora VARCHAR,
    dia VARCHAR,
    mes VARCHAR,
    ano VARCHAR,
    "date" VARCHAR,
    "time" VARCHAR,
    dgm VARCHAR,
    pre VARCHAR,
    total VARCHAR
)
WITH (
    format = 'CSV',
    external_location = 's3a://warehouse/bronze/raw/energia_produzida_total_nacional/',
    csv_separator = ',',
    skip_header_line_count = 1
);


-- =========================================================
-- 3) STAGE TABLES (PARQUET LIMPO)
-- =========================================================
-- Estas tabelas apontam para os Parquet gerados em Python.
-- Foi pedido TIMESTAMP.
-- =========================================================

DROP TABLE IF EXISTS hive.bronze_stage.consumo_total_nacional_clean;

CREATE TABLE hive.bronze_stage.consumo_total_nacional_clean (
    datahora_raw VARCHAR,
    timestamp_utc TIMESTAMP,
    dia INTEGER,
    mes INTEGER,
    ano INTEGER,
    data_local DATE,
    hora_local_raw VARCHAR,
    consumo_bt_kwh DOUBLE,
    consumo_mt_kwh DOUBLE,
    consumo_at_kwh DOUBLE,
    consumo_mat_kwh DOUBLE,
    consumo_total_kwh DOUBLE,
    duplicate_count INTEGER,
    duplicate_rank INTEGER,
    flag_duplicate_timestamp BOOLEAN,
    flag_bad_timestamp BOOLEAN,
    flag_bad_date BOOLEAN,
    flag_bad_total BOOLEAN,
    flag_zero_row BOOLEAN,
    ingest_ts_utc TIMESTAMP
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://warehouse/bronze/clean/consumo_total_nacional/'
);

DROP TABLE IF EXISTS hive.bronze_stage.energia_produzida_total_nacional_clean;

CREATE TABLE hive.bronze_stage.energia_produzida_total_nacional_clean (
    datahora_raw VARCHAR,
    timestamp_utc TIMESTAMP,
    dia INTEGER,
    mes INTEGER,
    ano INTEGER,
    data_local DATE,
    hora_local_raw VARCHAR,
    producao_dgm_kwh DOUBLE,
    producao_pre_kwh DOUBLE,
    producao_total_kwh DOUBLE,
    duplicate_count INTEGER,
    duplicate_rank INTEGER,
    flag_duplicate_timestamp BOOLEAN,
    flag_bad_timestamp BOOLEAN,
    flag_bad_date BOOLEAN,
    flag_bad_total BOOLEAN,
    flag_zero_row BOOLEAN,
    ingest_ts_utc TIMESTAMP
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://warehouse/bronze/clean/energia_produzida_total_nacional/'
);


-- =========================================================
-- 4) BRONZE MANAGED TABLES EM ICEBERG
-- =========================================================
-- Materialização gerida para servir de base aos produtos analíticos.
-- =========================================================

DROP TABLE IF EXISTS iceberg.bronze.consumo_total_nacional;

CREATE TABLE iceberg.bronze.consumo_total_nacional (
    datahora_raw VARCHAR,
    timestamp_utc TIMESTAMP,
    dia INTEGER,
    mes INTEGER,
    ano INTEGER,
    data_local DATE,
    hora_local_raw VARCHAR,
    consumo_bt_kwh DOUBLE,
    consumo_mt_kwh DOUBLE,
    consumo_at_kwh DOUBLE,
    consumo_mat_kwh DOUBLE,
    consumo_total_kwh DOUBLE,
    duplicate_count INTEGER,
    duplicate_rank INTEGER,
    flag_duplicate_timestamp BOOLEAN,
    flag_bad_timestamp BOOLEAN,
    flag_bad_date BOOLEAN,
    flag_bad_total BOOLEAN,
    flag_zero_row BOOLEAN,
    ingest_ts_utc TIMESTAMP
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['ano', 'mes'],
    location = 's3a://warehouse/bronze/producao_consumo/consumo_total_nacional/'
);

ALTER TABLE iceberg.bronze.consumo_total_nacional
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true

COMMENT ON TABLE iceberg.bronze.consumo_total_nacional IS
'Tabela Bronze com consumo elétrico nacional a 15 minutos, ingerida do CSV REN (ERSE). Preserva todas as colunas da fonte com flags de qualidade sem transformação semântica.';

COMMENT ON COLUMN iceberg.bronze.consumo_total_nacional.timestamp_utc IS 'Timestamp original da fonte interpretado em UTC para o registo de 15 minutos.';
COMMENT ON COLUMN iceberg.bronze.consumo_total_nacional.consumo_total_kwh IS 'Consumo total nacional no intervalo de 15 minutos em kW.';
COMMENT ON COLUMN iceberg.bronze.consumo_total_nacional.flag_duplicate_timestamp IS 'True se este timestamp_utc tem mais do que um registo na fonte.';
COMMENT ON COLUMN iceberg.bronze.consumo_total_nacional.flag_bad_timestamp IS 'True se o timestamp não foi possível converter correctamente.';
COMMENT ON COLUMN iceberg.bronze.consumo_total_nacional.flag_bad_total IS 'True se consumo_total_kwh é negativo ou nulo de forma anómala.';
COMMENT ON COLUMN iceberg.bronze.consumo_total_nacional.ano IS 'Ano — coluna de partição.';
COMMENT ON COLUMN iceberg.bronze.consumo_total_nacional.mes IS 'Mês — coluna de partição.';

INSERT INTO iceberg.bronze.consumo_total_nacional
SELECT
    datahora_raw,
    timestamp_utc,
    dia,
    mes,
    ano,
    data_local,
    hora_local_raw,
    consumo_bt_kwh,
    consumo_mt_kwh,
    consumo_at_kwh,
    consumo_mat_kwh,
    consumo_total_kwh,
    duplicate_count,
    duplicate_rank,
    flag_duplicate_timestamp,
    flag_bad_timestamp,
    flag_bad_date,
    flag_bad_total,
    flag_zero_row,
    ingest_ts_utc
FROM hive.bronze_stage.consumo_total_nacional_clean;


DROP TABLE IF EXISTS iceberg.bronze.energia_produzida_total_nacional;

CREATE TABLE iceberg.bronze.energia_produzida_total_nacional (
    datahora_raw VARCHAR,
    timestamp_utc TIMESTAMP,
    dia INTEGER,
    mes INTEGER,
    ano INTEGER,
    data_local DATE,
    hora_local_raw VARCHAR,
    producao_dgm_kwh DOUBLE,
    producao_pre_kwh DOUBLE,
    producao_total_kwh DOUBLE,
    duplicate_count INTEGER,
    duplicate_rank INTEGER,
    flag_duplicate_timestamp BOOLEAN,
    flag_bad_timestamp BOOLEAN,
    flag_bad_date BOOLEAN,
    flag_bad_total BOOLEAN,
    flag_zero_row BOOLEAN,
    ingest_ts_utc TIMESTAMP
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['ano', 'mes'],
    location = 's3a://warehouse/bronze/producao_consumo/energia_produzida_total_nacional/'
);

ALTER TABLE iceberg.bronze.energia_produzida_total_nacional
SET PROPERTIES
    format_version = 2,
    object_store_layout_enabled = true

COMMENT ON TABLE iceberg.bronze.energia_produzida_total_nacional IS
'Tabela Bronze com energia produzida total nacional a 15 minutos, ingerida do CSV REN. Preserva colunas DGM e PRE com flags de qualidade sem transformação semântica.';

COMMENT ON COLUMN iceberg.bronze.energia_produzida_total_nacional.timestamp_utc IS 'Timestamp original da fonte interpretado em UTC para o registo de 15 minutos.';
COMMENT ON COLUMN iceberg.bronze.energia_produzida_total_nacional.producao_total_kwh IS 'Produção total nacional (DGM + PRE) no intervalo de 15 minutos em kW.';
COMMENT ON COLUMN iceberg.bronze.energia_produzida_total_nacional.producao_dgm_kwh IS 'Produção DGM (Despacho Global do Mercado) no intervalo de 15 minutos em kW.';
COMMENT ON COLUMN iceberg.bronze.energia_produzida_total_nacional.producao_pre_kwh IS 'Produção PRE (Produção em Regime Especial) no intervalo de 15 minutos em kW.';
COMMENT ON COLUMN iceberg.bronze.energia_produzida_total_nacional.flag_duplicate_timestamp IS 'True se este timestamp_utc tem mais do que um registo na fonte.';
COMMENT ON COLUMN iceberg.bronze.energia_produzida_total_nacional.ano IS 'Ano — coluna de partição.';
COMMENT ON COLUMN iceberg.bronze.energia_produzida_total_nacional.mes IS 'Mês — coluna de partição.';

INSERT INTO iceberg.bronze.energia_produzida_total_nacional
SELECT
    datahora_raw,
    timestamp_utc,
    dia,
    mes,
    ano,
    data_local,
    hora_local_raw,
    producao_dgm_kwh,
    producao_pre_kwh,
    producao_total_kwh,
    duplicate_count,
    duplicate_rank,
    flag_duplicate_timestamp,
    flag_bad_timestamp,
    flag_bad_date,
    flag_bad_total,
    flag_zero_row,
    ingest_ts_utc
FROM hive.bronze_stage.energia_produzida_total_nacional_clean;


-- =========================================================
-- 5) VALIDAÇÃO
-- =========================================================

SELECT *
FROM hive.bronze_raw.consumo_total_nacional_raw
LIMIT 10;

SELECT *
FROM hive.bronze_raw.energia_produzida_total_nacional_raw
LIMIT 10;

SELECT *
FROM hive.bronze_stage.consumo_total_nacional_clean
LIMIT 10;

SELECT *
FROM hive.bronze_stage.energia_produzida_total_nacional_clean
LIMIT 10;

SELECT COUNT(*) AS total_linhas
FROM iceberg.bronze.consumo_total_nacional;

SELECT COUNT(*) AS total_linhas
FROM iceberg.bronze.energia_produzida_total_nacional;

SELECT
    COUNT(*) AS total_linhas,
    SUM(CASE WHEN flag_duplicate_timestamp THEN 1 ELSE 0 END) AS duplicados,
    SUM(CASE WHEN flag_bad_timestamp THEN 1 ELSE 0 END) AS timestamps_invalidos,
    SUM(CASE WHEN flag_bad_total THEN 1 ELSE 0 END) AS totais_invalidos,
    SUM(CASE WHEN flag_zero_row THEN 1 ELSE 0 END) AS linhas_zero
FROM iceberg.bronze.consumo_total_nacional;

SELECT
    COUNT(*) AS total_linhas,
    SUM(CASE WHEN flag_duplicate_timestamp THEN 1 ELSE 0 END) AS duplicados,
    SUM(CASE WHEN flag_bad_timestamp THEN 1 ELSE 0 END) AS timestamps_invalidos,
    SUM(CASE WHEN flag_bad_total THEN 1 ELSE 0 END) AS totais_invalidos,
    SUM(CASE WHEN flag_zero_row THEN 1 ELSE 0 END) AS linhas_zero
FROM iceberg.bronze.energia_produzida_total_nacional;

SELECT timestamp_utc, duplicate_count, duplicate_rank, consumo_total_kwh
FROM iceberg.bronze.consumo_total_nacional
WHERE flag_duplicate_timestamp
ORDER BY timestamp_utc, duplicate_rank
LIMIT 20;

SELECT timestamp_utc, duplicate_count, duplicate_rank, producao_total_kwh
FROM iceberg.bronze.energia_produzida_total_nacional
WHERE flag_duplicate_timestamp
ORDER BY timestamp_utc, duplicate_rank
LIMIT 20;