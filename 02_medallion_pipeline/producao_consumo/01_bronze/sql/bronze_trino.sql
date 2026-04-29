CREATE SCHEMA IF NOT EXISTS hive.bronze_raw WITH (location = 's3a://warehouse/bronze/raw/');
CREATE SCHEMA IF NOT EXISTS hive.bronze_stage WITH (location = 's3a://warehouse/bronze/clean/');
CREATE SCHEMA IF NOT EXISTS iceberg.bronze WITH (location = 's3a://warehouse/bronze/managed/');

DROP TABLE IF EXISTS hive.bronze_raw.consumo_total_nacional_raw;
CREATE TABLE hive.bronze_raw.consumo_total_nacional_raw (
    datahora VARCHAR, dia VARCHAR, mes VARCHAR, ano VARCHAR, "date" VARCHAR, "time" VARCHAR,
    bt VARCHAR, mt VARCHAR, at VARCHAR, mat VARCHAR, total VARCHAR
)
WITH (format='CSV', external_location='s3a://warehouse/bronze/raw/consumo_total_nacional/', csv_separator=',', skip_header_line_count=1);

DROP TABLE IF EXISTS hive.bronze_raw.energia_produzida_total_nacional_raw;
CREATE TABLE hive.bronze_raw.energia_produzida_total_nacional_raw (
    datahora VARCHAR, dia VARCHAR, mes VARCHAR, ano VARCHAR, "date" VARCHAR, "time" VARCHAR,
    dgm VARCHAR, pre VARCHAR, total VARCHAR
)
WITH (format='CSV', external_location='s3a://warehouse/bronze/raw/energia_produzida_total_nacional/', csv_separator=',', skip_header_line_count=1);

DROP TABLE IF EXISTS hive.bronze_stage.consumo_total_nacional_raw_parquet;
CREATE TABLE hive.bronze_stage.consumo_total_nacional_raw_parquet (
    datahora VARCHAR, dia VARCHAR, mes VARCHAR, ano VARCHAR, "date" VARCHAR, "time" VARCHAR,
    bt VARCHAR, mt VARCHAR, at VARCHAR, mat VARCHAR, total VARCHAR,
    origem_ficheiro VARCHAR, ingest_ts_utc VARCHAR
)
WITH (format='PARQUET', external_location='s3a://warehouse/bronze/clean/consumo_total_nacional/');

DROP TABLE IF EXISTS hive.bronze_stage.energia_produzida_total_nacional_raw_parquet;
CREATE TABLE hive.bronze_stage.energia_produzida_total_nacional_raw_parquet (
    datahora VARCHAR, dia VARCHAR, mes VARCHAR, ano VARCHAR, "date" VARCHAR, "time" VARCHAR,
    dgm VARCHAR, pre VARCHAR, total VARCHAR,
    origem_ficheiro VARCHAR, ingest_ts_utc VARCHAR
)
WITH (format='PARQUET', external_location='s3a://warehouse/bronze/clean/energia_produzida_total_nacional/');

DROP TABLE IF EXISTS iceberg.bronze.consumo_total_nacional_raw;
CREATE TABLE iceberg.bronze.consumo_total_nacional_raw WITH (format='PARQUET') AS
SELECT * FROM hive.bronze_stage.consumo_total_nacional_raw_parquet;

DROP TABLE IF EXISTS iceberg.bronze.energia_produzida_total_nacional_raw;
CREATE TABLE iceberg.bronze.energia_produzida_total_nacional_raw WITH (format='PARQUET') AS
SELECT * FROM hive.bronze_stage.energia_produzida_total_nacional_raw_parquet;
