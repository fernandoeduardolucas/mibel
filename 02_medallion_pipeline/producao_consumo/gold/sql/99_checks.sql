-- Gold checks
SELECT 'gold.producao_vs_consumo_hourly row_count' AS check_name, COUNT(*) AS value
FROM iceberg.gold.producao_vs_consumo_hourly;

SELECT 'gold.producao_vs_consumo_hourly null_timestamp' AS check_name,
       COUNT_IF(timestamp_utc IS NULL) AS value
FROM iceberg.gold.producao_vs_consumo_hourly;
