# Sequência Bronze: CSV -> Python/Pandas -> MinIO -> Hive Stage -> Iceberg Bronze

## Ordem de execução

1. Meter os CSV brutos no MinIO
   - `s3://warehouse/bronze/raw/consumo_total_nacional/consumo-total-nacional.csv`
   - `s3://warehouse/bronze/raw/energia_produzida_total_nacional/energia-produzida-total-nacional.csv`

2. No DataGrip, correr `bronze_trino.sql` até à secção dos RAW CSV tables.

3. No teu ambiente Python, instalar dependências:
   - `pip install pandas pyarrow boto3`

4. Correr o script Python:
   - `python bronze_clean_upload.py --consumo consumo-total-nacional.csv --producao energia-produzida-total-nacional.csv --out-dir ./output --upload`

5. Verificar no MinIO se foram criados:
   - `s3://warehouse/bronze/clean/consumo_total_nacional/consumo_total_nacional_clean.parquet`
   - `s3://warehouse/bronze/clean/energia_produzida_total_nacional/energia_produzida_total_nacional_clean.parquet`

6. No DataGrip, correr o resto do `bronze_trino.sql`:
   - criar tabelas stage em Hive sobre os Parquet limpos
   - materializar as tabelas Bronze geridas em Iceberg
   - correr as queries de validação

## Política de limpeza aplicada
- preserva os CSV originais na `bronze_raw`
- converte tipos e datas no Python
- não apaga duplicados na Bronze
- marca:
  - `flag_duplicate_timestamp`
  - `duplicate_count`
  - `duplicate_rank`
  - `flag_bad_timestamp`
  - `flag_bad_total`
  - `flag_zero_row`

## O que já vi nestes ficheiros
- sem colunas vazias relevantes
- sem timestamps inválidos
- sem nulos nas colunas principais
- existem timestamps duplicados nas mudanças de hora
- por isso, apagar duplicados logo na Bronze não é boa ideia
