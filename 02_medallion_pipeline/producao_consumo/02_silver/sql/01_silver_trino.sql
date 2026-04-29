CREATE SCHEMA IF NOT EXISTS iceberg.silver WITH (location = 's3a://warehouse/silver/');

-- Consumo: normalização de colunas, conversões, tipos, nulos e deduplicação
DROP TABLE IF EXISTS iceberg.silver.consumo_total_nacional_15min;
CREATE TABLE iceberg.silver.consumo_total_nacional_15min WITH (format='PARQUET') AS
WITH base AS (
  SELECT
    try(date_parse(datahora, '%Y-%m-%d %H:%i:%s')) AS timestamp_utc,
    try_cast(dia AS INTEGER) AS dia,
    try_cast(mes AS INTEGER) AS mes,
    try_cast(ano AS INTEGER) AS ano,
    try_cast("date" AS DATE) AS data_local,
    "time" AS hora_local_raw,
    try_cast(bt AS DOUBLE) AS consumo_bt_kwh,
    try_cast(mt AS DOUBLE) AS consumo_mt_kwh,
    try_cast(at AS DOUBLE) AS consumo_at_kwh,
    try_cast(mat AS DOUBLE) AS consumo_mat_kwh,
    try_cast(total AS DOUBLE) AS consumo_total_kwh,
    datahora AS datahora_raw,
    origem_ficheiro,
    from_iso8601_timestamp(ingest_ts_utc) AS ingest_ts_utc
  FROM iceberg.bronze.consumo_total_nacional_raw
), ranked AS (
  SELECT *, row_number() OVER (PARTITION BY timestamp_utc ORDER BY ingest_ts_utc DESC) AS rn
  FROM base
  WHERE timestamp_utc IS NOT NULL
)
SELECT *,
  (COALESCE(consumo_bt_kwh,0)+COALESCE(consumo_mt_kwh,0)+COALESCE(consumo_at_kwh,0)+COALESCE(consumo_mat_kwh,0)) AS consumo_componentes_kwh,
  ABS(COALESCE(consumo_total_kwh,0)-(COALESCE(consumo_bt_kwh,0)+COALESCE(consumo_mt_kwh,0)+COALESCE(consumo_at_kwh,0)+COALESCE(consumo_mat_kwh,0))) AS diff_componentes_total_kwh
FROM ranked WHERE rn=1;

DROP TABLE IF EXISTS iceberg.silver.energia_produzida_total_nacional_15min;
CREATE TABLE iceberg.silver.energia_produzida_total_nacional_15min WITH (format='PARQUET') AS
WITH base AS (
  SELECT
    try(date_parse(datahora, '%Y-%m-%d %H:%i:%s')) AS timestamp_utc,
    try_cast(dia AS INTEGER) AS dia,
    try_cast(mes AS INTEGER) AS mes,
    try_cast(ano AS INTEGER) AS ano,
    try_cast("date" AS DATE) AS data_local,
    "time" AS hora_local_raw,
    try_cast(dgm AS DOUBLE) AS producao_dgm_kwh,
    try_cast(pre AS DOUBLE) AS producao_pre_kwh,
    try_cast(total AS DOUBLE) AS producao_total_kwh,
    datahora AS datahora_raw,
    origem_ficheiro,
    from_iso8601_timestamp(ingest_ts_utc) AS ingest_ts_utc
  FROM iceberg.bronze.energia_produzida_total_nacional_raw
), ranked AS (
  SELECT *, row_number() OVER (PARTITION BY timestamp_utc ORDER BY ingest_ts_utc DESC) AS rn
  FROM base
  WHERE timestamp_utc IS NOT NULL
)
SELECT *,
  (COALESCE(producao_dgm_kwh,0)+COALESCE(producao_pre_kwh,0)) AS producao_componentes_kwh,
  ABS(COALESCE(producao_total_kwh,0)-(COALESCE(producao_dgm_kwh,0)+COALESCE(producao_pre_kwh,0))) AS diff_componentes_total_kwh
FROM ranked WHERE rn=1;

-- Junta produção e consumo na Silver
DROP TABLE IF EXISTS iceberg.silver.producao_consumo_15min;
CREATE TABLE iceberg.silver.producao_consumo_15min WITH (format='PARQUET') AS
SELECT
  COALESCE(c.timestamp_utc, p.timestamp_utc) AS timestamp_utc,
  c.data_local AS data_local,
  c.consumo_total_kwh,
  p.producao_total_kwh,
  p.producao_dgm_kwh,
  p.producao_pre_kwh
FROM iceberg.silver.consumo_total_nacional_15min c
FULL OUTER JOIN iceberg.silver.energia_produzida_total_nacional_15min p
  ON c.timestamp_utc = p.timestamp_utc;
