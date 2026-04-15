# producao_consumo medallion pipeline

Estrutura aplicada:

- `bronze/` - ingestão, limpeza inicial e tabelas Bronze
- `silver/` - normalização/deduplicação e tabelas Silver
- `gold/` - produto analítico final
- `pipeline/run_all.sh` - execução sequencial SQL + checks
- `docs/` - documentação de apoio

Ordem recomendada:
1. Bronze (`bronze/sql/bronze_trino.sql`)
2. Silver (`silver/sql/01_silver_trino.sql`)
3. Gold (`gold/sql/01_gold_trino.sql`)
4. Checks (`*/sql/99_checks.sql`)
