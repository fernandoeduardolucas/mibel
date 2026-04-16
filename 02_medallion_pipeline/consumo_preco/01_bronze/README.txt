Bronze Pipeline Package — Consumo vs Preços

Objetivo
Este pacote implementa a camada Bronze/Standardized para o caso de uso consumo elétrico nacional vs preços day-ahead MIBEL.

Inputs suportados
- data/raw/consumo-total-nacional.csv
- data/raw/Day-ahead Market Prices_20230101_20260311.csv

Outputs gerados
- data/clean/consumo/consumo_total_nacional_clean.parquet
- data/clean/precos/energy_market_prices_pt_es_clean.parquet

Âmbito funcional
1. Leitura dos ficheiros raw CSV.
2. Normalização mínima e validação de schema.
3. Construção de timestamp_utc canónico.
4. Flags de qualidade e deteção de duplicados.
5. Escrita em Parquet.
6. Upload opcional para MinIO/S3.

Decisões de contrato
- Consumo:
  - o campo datahora é lido como UTC porque o ficheiro contém offset +00:00.
  - a granularidade esperada é 15 minutos.
- Preços day-ahead:
  - o ficheiro tem 2 linhas iniciais de metadados.
  - o separador é ';'.
  - as colunas relevantes são Date, Hour, Portugal e Spain.
  - Hour pode assumir 1..25 em dias de mudança de hora.
  - o timestamp_utc é calculado a partir de Date + Hour usando o timezone de mercado configurado.

Estrutura do pacote
- data/raw: fontes originais.
- data/clean: outputs Parquet limpos por dataset.
- docs: documentação operacional.
- scripts/python: script principal e dependências.
- sql: DDL para consulta em Trino.
- logs: reservado para registos de execução.
- metadata: reservado para manifestos e relatórios de qualidade.

Notas arquiteturais
Esta implementação já faz normalização semântica e geração de colunas canónicas. Em muitos modelos lakehouse, isto aproxima-se mais de Silver inicial do que de Bronze puro. O nome Bronze/Standardized foi mantido por pragmatismo e continuidade do projeto.
