CREATE SCHEMA IF NOT EXISTS iceberg.gold WITH (location = 's3a://warehouse/gold/');

-- Tabela base horária para análises
DROP TABLE IF EXISTS iceberg.gold.producao_vs_consumo_hourly;
CREATE TABLE iceberg.gold.producao_vs_consumo_hourly WITH (format='PARQUET') AS
SELECT
  date_trunc('hour', timestamp_utc) AS timestamp_utc,
  SUM(consumo_total_kwh) AS consumo_total_kwh,
  SUM(producao_total_kwh) AS producao_total_kwh,
  SUM(producao_dgm_kwh) AS producao_dgm_kwh,
  SUM(producao_pre_kwh) AS producao_pre_kwh,
  SUM(producao_total_kwh) - SUM(consumo_total_kwh) AS saldo_kwh
FROM iceberg.silver.producao_consumo_15min
GROUP BY 1;

-- Consumo diário (dataset atual é nacional; "região" marcada como NACIONAL)
DROP TABLE IF EXISTS iceberg.gold.consumo_diario_regiao;
CREATE TABLE iceberg.gold.consumo_diario_regiao WITH (format='PARQUET') AS
SELECT
  CAST(date_trunc('day', timestamp_utc) AS DATE) AS data,
  'NACIONAL' AS regiao,
  SUM(consumo_total_kwh) AS consumo_total_kwh
FROM iceberg.silver.producao_consumo_15min
GROUP BY 1,2;

-- Produção mensal por tecnologia
DROP TABLE IF EXISTS iceberg.gold.producao_mensal_tecnologia;
CREATE TABLE iceberg.gold.producao_mensal_tecnologia WITH (format='PARQUET') AS
SELECT date_trunc('month', timestamp_utc) AS mes,
       SUM(producao_dgm_kwh) AS producao_dgm_kwh,
       SUM(producao_pre_kwh) AS producao_pre_kwh,
       SUM(producao_total_kwh) AS producao_total_kwh
FROM iceberg.silver.producao_consumo_15min
GROUP BY 1;

-- Indicadores finais
DROP TABLE IF EXISTS iceberg.gold.indicadores_finais;
CREATE TABLE iceberg.gold.indicadores_finais WITH (format='PARQUET') AS
SELECT
  COUNT(*) AS total_registos_15min,
  SUM(consumo_total_kwh) AS consumo_total_kwh,
  SUM(producao_total_kwh) AS producao_total_kwh,
  AVG(producao_total_kwh - consumo_total_kwh) AS saldo_medio_kwh,
  SUM(CASE WHEN producao_total_kwh < consumo_total_kwh THEN 1 ELSE 0 END) AS periodos_defice
FROM iceberg.silver.producao_consumo_15min;

-- Tabelas para dashboard
DROP TABLE IF EXISTS iceberg.gold.dashboard_kpis;
CREATE TABLE iceberg.gold.dashboard_kpis WITH (format='PARQUET') AS
SELECT * FROM iceberg.gold.indicadores_finais;

DROP TABLE IF EXISTS iceberg.gold.dashboard_series_horarias;
CREATE TABLE iceberg.gold.dashboard_series_horarias WITH (format='PARQUET') AS
SELECT * FROM iceberg.gold.producao_vs_consumo_hourly;

-- Features para modelos
DROP TABLE IF EXISTS iceberg.gold.features_modelos;
CREATE TABLE iceberg.gold.features_modelos WITH (format='PARQUET') AS
SELECT
  timestamp_utc,
  consumo_total_kwh,
  producao_total_kwh,
  producao_dgm_kwh,
  producao_pre_kwh,
  saldo_kwh,
  hour(timestamp_utc) AS feature_hora,
  day_of_week(timestamp_utc) AS feature_dia_semana,
  month(timestamp_utc) AS feature_mes
FROM iceberg.gold.producao_vs_consumo_hourly;
