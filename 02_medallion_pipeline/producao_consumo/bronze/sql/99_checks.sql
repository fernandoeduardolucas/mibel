-- Bronze checks
SELECT 'bronze.consumo_total_nacional row_count' AS check_name, COUNT(*) AS value
FROM iceberg.bronze.consumo_total_nacional;

SELECT 'bronze.energia_produzida_total_nacional row_count' AS check_name, COUNT(*) AS value
FROM iceberg.bronze.energia_produzida_total_nacional;
