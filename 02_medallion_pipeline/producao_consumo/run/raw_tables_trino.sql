-- RAW apenas no catálogo Hive:
-- 1) tabelas externas CSV
-- 2) tabelas raw geridas em PARQUET
-- 3) insert das externas para as geridas

CREATE SCHEMA IF NOT EXISTS hive.bronze_raw
WITH (location = 's3a://__BUCKET__/bronze/raw/');

CREATE SCHEMA IF NOT EXISTS hive.bronze_raw_managed
WITH (location = 's3a://__BUCKET__/bronze/managed_raw/');

DROP TABLE IF EXISTS hive.bronze_raw.consumo_total_nacional_ext;
CREATE TABLE hive.bronze_raw.consumo_total_nacional_ext (
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
    external_location = 's3a://__BUCKET__/bronze/raw/consumo_total_nacional/',
    csv_separator = ',',
    skip_header_line_count = 1
);

DROP TABLE IF EXISTS hive.bronze_raw.energia_produzida_total_nacional_ext;
CREATE TABLE hive.bronze_raw.energia_produzida_total_nacional_ext (
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
    external_location = 's3a://__BUCKET__/bronze/raw/energia_produzida_total_nacional/',
    csv_separator = ',',
    skip_header_line_count = 1
);

DROP TABLE IF EXISTS hive.bronze_raw_managed.consumo_total_nacional_raw;
CREATE TABLE hive.bronze_raw_managed.consumo_total_nacional_raw (
    datahora VARCHAR,
    dia VARCHAR,
    mes VARCHAR,
    ano VARCHAR,
    date_str VARCHAR,
    time_str VARCHAR,
    bt VARCHAR,
    mt VARCHAR,
    at VARCHAR,
    mat VARCHAR,
    total VARCHAR
)
WITH (format = 'PARQUET');

INSERT INTO hive.bronze_raw_managed.consumo_total_nacional_raw
SELECT datahora, dia, mes, ano, "date", "time", bt, mt, at, mat, total
FROM hive.bronze_raw.consumo_total_nacional_ext;

DROP TABLE IF EXISTS hive.bronze_raw_managed.energia_produzida_total_nacional_raw;
CREATE TABLE hive.bronze_raw_managed.energia_produzida_total_nacional_raw (
    datahora VARCHAR,
    dia VARCHAR,
    mes VARCHAR,
    ano VARCHAR,
    date_str VARCHAR,
    time_str VARCHAR,
    dgm VARCHAR,
    pre VARCHAR,
    total VARCHAR
)
WITH (format = 'PARQUET');

INSERT INTO hive.bronze_raw_managed.energia_produzida_total_nacional_raw
SELECT datahora, dia, mes, ano, "date", "time", dgm, pre, total
FROM hive.bronze_raw.energia_produzida_total_nacional_ext;
