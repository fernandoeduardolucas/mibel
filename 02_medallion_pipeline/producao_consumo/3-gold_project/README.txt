PROJETO GOLD - TEAD

Objetivo
Criar o produto final Gold de comparação horária entre produção nacional e consumo nacional.

Entrada esperada
- iceberg.silver.consumo_total_nacional_15min
- iceberg.silver.energia_produzida_total_nacional_15min

Saída criada
- iceberg.gold.producao_vs_consumo_hourly

O que a Gold faz
1. Agrega os dados de 15 minutos para 1 hora.
2. Junta produção e consumo por timestamp_utc horário.
3. Calcula saldo_kwh e ratio_producao_consumo.
4. Cria flags de défice, excedente e origem em falta.

Como correr
1. Garantir que a Silver já foi criada.
2. Abrir o ficheiro sql/01_gold_trino.sql no DataGrip.
3. Executar o script completo.
4. Validar com as queries do fim do script.
