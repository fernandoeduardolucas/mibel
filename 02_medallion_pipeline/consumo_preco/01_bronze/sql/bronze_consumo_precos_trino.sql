-- bronze_consumo_precos_trino.sql
-- Bronze externa em Hive sobre os Parquet gerados pela pipeline Python.
-- Silver/Gold deverão usar Iceberg.

CREATE SCHEMA IF NOT EXISTS hive.bronze;

DROP TABLE IF EXISTS hive.bronze.consumo_total_nacional_clean;
CREATE TABLE hive.bronze.consumo_total_nacional_clean (
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
    flag_date_mismatch BOOLEAN,
    source_file_name VARCHAR,
    run_id VARCHAR,
    ingest_ts_utc TIMESTAMP
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://warehouse/bronze/clean/consumo_total_nacional/'
);

DROP TABLE IF EXISTS hive.bronze.energy_market_prices_pt_es_clean;
CREATE TABLE hive.bronze.energy_market_prices_pt_es_clean (
    date_raw VARCHAR,
    delivery_date_local DATE,
    market_hour INTEGER,
    timestamp_utc TIMESTAMP,
    preco_pt_eur_mwh DOUBLE,
    preco_es_eur_mwh DOUBLE,
    duplicate_count INTEGER,
    duplicate_rank INTEGER,
    flag_duplicate_timestamp BOOLEAN,
    flag_bad_timestamp BOOLEAN,
    flag_bad_date BOOLEAN,
    flag_bad_hour BOOLEAN,
    flag_bad_preco_pt BOOLEAN,
    flag_bad_preco_es BOOLEAN,
    flag_zero_preco_pt BOOLEAN,
    market_timezone_assumed VARCHAR,
    source_file_name VARCHAR,
    run_id VARCHAR,
    ingest_ts_utc TIMESTAMP
)
WITH (
    format = 'PARQUET',
    external_location = 's3a://warehouse/bronze/clean/energy_market_prices/'
);

DROP VIEW IF EXISTS hive.bronze.vw_consumo_total_nacional_preferred;
CREATE VIEW hive.bronze.vw_consumo_total_nacional_preferred AS
SELECT *
FROM hive.bronze.consumo_total_nacional_clean
WHERE duplicate_rank = 1
  AND NOT flag_bad_timestamp;

DROP VIEW IF EXISTS hive.bronze.vw_energy_market_prices_pt_es_preferred;
CREATE VIEW hive.bronze.vw_energy_market_prices_pt_es_preferred AS
SELECT *
FROM hive.bronze.energy_market_prices_pt_es_clean
WHERE duplicate_rank = 1
  AND NOT flag_bad_timestamp;