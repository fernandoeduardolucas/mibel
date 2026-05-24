CREATE SCHEMA IF NOT EXISTS iceberg.bronze;
CREATE SCHEMA IF NOT EXISTS iceberg.gold;

CREATE TABLE IF NOT EXISTS iceberg.bronze.producao_consumo_streaming (
  event_id VARCHAR,
  event_ts TIMESTAMP(6),
  consumo_total_kwh DOUBLE,
  producao_total_kwh DOUBLE,
  saldo_kwh DOUBLE,
  origem VARCHAR,
  ingest_ts TIMESTAMP(6)
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['day(event_ts)']
);

CREATE TABLE IF NOT EXISTS iceberg.gold.dp_producao_consumo_streaming (
  event_id VARCHAR,
  event_ts TIMESTAMP(6),
  consumo_total_kwh DOUBLE,
  producao_total_kwh DOUBLE,
  saldo_kwh DOUBLE,
  ratio_producao_consumo DOUBLE,
  flag_defice BOOLEAN,
  flag_excedente BOOLEAN,
  origem VARCHAR,
  ingest_ts TIMESTAMP(6)
)
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['day(event_ts)']
);

-- 1) Kafka -> Bronze
INSERT INTO iceberg.bronze.producao_consumo_streaming
SELECT
  event_id,
  CAST(from_iso8601_timestamp(timestamp_utc) AT TIME ZONE 'UTC' AS TIMESTAMP(6)) AS event_ts,
  consumo_total_kwh,
  producao_total_kwh,
  saldo_kwh,
  origem,
  current_timestamp(6) AS ingest_ts
FROM kafka.default.producao_consumo_events;

-- 2) Bronze -> Gold (deduplicação por event_id + enriquecimento)
INSERT INTO iceberg.gold.dp_producao_consumo_streaming
SELECT
  event_id,
  event_ts,
  consumo_total_kwh,
  producao_total_kwh,
  saldo_kwh,
  CASE
    WHEN consumo_total_kwh IS NULL OR consumo_total_kwh = 0 THEN NULL
    ELSE producao_total_kwh / consumo_total_kwh
  END AS ratio_producao_consumo,
  saldo_kwh < 0 AS flag_defice,
  saldo_kwh > 0 AS flag_excedente,
  origem,
  ingest_ts
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_ts DESC) AS rn
  FROM iceberg.bronze.producao_consumo_streaming
)
WHERE rn = 1;
