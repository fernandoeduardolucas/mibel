#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR"
COMPOSE_FILE="$REPO_ROOT/01_bootstrap/tead_2.0_v1.2/docker-compose.yml"
BRONZE_DIR="$REPO_ROOT/02_medallion_pipeline/producao_consumo/01_bronze"
SILVER_SQL="$REPO_ROOT/02_medallion_pipeline/producao_consumo/02_silver/sql/01_silver_trino.sql"
GOLD_SQL="$REPO_ROOT/02_medallion_pipeline/producao_consumo/03_gold/sql/01_gold_trino.sql"
BRONZE_SQL="$REPO_ROOT/02_medallion_pipeline/producao_consumo/01_bronze/sql/bronze_trino.sql"

log() {
  printf '\n[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Erro: comando obrigatório não encontrado: $1" >&2
    exit 1
  fi
}

run_trino_sql() {
  local sql_file="$1"
  local stage_name="$2"
  log "A executar SQL da camada ${stage_name} via Docker (Trino): ${sql_file}"
  docker compose -f "$COMPOSE_FILE" exec -T trino trino < "$sql_file"
}

require_cmd docker
require_cmd python3

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "Erro: docker-compose.yml não encontrado em: $COMPOSE_FILE" >&2
  exit 1
fi

log "A subir a stack Docker (build incluído)"
docker compose -f "$COMPOSE_FILE" up -d --build

log "A instalar dependências Python da Bronze"
python3 -m pip install -r "$BRONZE_DIR/scripts/python/requirements_bronze.txt"

log "A correr limpeza + upload Bronze"
(
  cd "$BRONZE_DIR"
  S3_ENDPOINT_URL="http://localhost:9000" \
  AWS_ACCESS_KEY_ID="minioadmin" \
  AWS_SECRET_ACCESS_KEY="minioadmin" \
  S3_BUCKET="warehouse" \
  S3_PREFIX="bronze/clean" \
  python3 scripts/python/bronze_clean_upload.py \
    --consumo data/raw/consumo-total-nacional.csv \
    --producao data/raw/energia-produzida-total-nacional.csv \
    --out-dir data/clean \
    --upload
)

run_trino_sql "$BRONZE_SQL" "Bronze"
run_trino_sql "$SILVER_SQL" "Silver"
run_trino_sql "$GOLD_SQL" "Gold"

log "Pipeline Medallion concluída com sucesso."
log "Validação rápida (Gold):"
docker compose -f "$COMPOSE_FILE" exec -T trino trino --execute "SELECT COUNT(*) AS linhas_gold FROM iceberg.gold.producao_vs_consumo_hourly;"
