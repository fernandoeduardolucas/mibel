-- Silver checks
SELECT 'silver.consumo_total_nacional_15min null_timestamp' AS check_name,
       COUNT_IF(timestamp_utc IS NULL) AS value
FROM iceberg.silver.consumo_total_nacional_15min;

SELECT 'silver.energia_produzida_total_nacional_15min null_timestamp' AS check_name,
       COUNT_IF(timestamp_utc IS NULL) AS value
FROM iceberg.silver.energia_produzida_total_nacional_15min;
