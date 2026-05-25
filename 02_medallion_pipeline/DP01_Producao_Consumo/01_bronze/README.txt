Bronze Pipeline Package

Estrutura:
- data/raw/
  CSV originais de consumo e produção
- data/clean/
  Pasta reservada para outputs Parquet limpos
- scripts/python/
  Script de limpeza/upload e requirements
- sql/
  Script SQL para criar tabelas no Trino
- docs/
  README do pipeline e documento do Produto Gold A

Ordem sugerida:
1. Fazer upload dos CSV raw para o MinIO
2. Correr o SQL de criação das tabelas RAW no Trino
3. Instalar dependências Python
4. Correr o script bronze_clean_upload.py
5. Correr o resto do SQL para stage/bronze gerida
