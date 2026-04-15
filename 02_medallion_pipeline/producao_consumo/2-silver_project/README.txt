PROJETO SILVER - TEAD

Objetivo
Criar a camada Silver a partir das tabelas Bronze em Iceberg.

Entrada esperada
- iceberg.bronze.consumo_total_nacional
- iceberg.bronze.energia_produzida_total_nacional

Saída criada
- iceberg.silver.consumo_total_nacional_15min
- iceberg.silver.energia_produzida_total_nacional_15min

O que a Silver faz
1. Remove registos inválidos com timestamp ou total inválido.
2. Resolve timestamps duplicados com uma regra determinística.
3. Mantém granularidade de 15 minutos.
4. Normaliza o conjunto final para análise.
5. Cria flags de qualidade e consistência.

Regra de deduplicação usada
- agrupar por timestamp_utc
- preferir linhas não zero
- depois preferir o maior total
- em empate, preferir o menor duplicate_rank da Bronze
- em último caso, preferir ingest_ts_utc mais recente

Como correr
1. Garantir que a Bronze já foi criada.
2. Abrir o ficheiro sql/01_silver_trino.sql no DataGrip.
3. Executar o script completo.
4. Validar com as queries do fim do script.
