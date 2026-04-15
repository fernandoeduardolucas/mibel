#!/usr/bin/env bash
set -euo pipefail

# Runner simples para executar SQL Medallion no Trino.
# Requisitos:
#   - Trino CLI instalado
#   - Variável TRINO_HOST definida (default: localhost)
#   - Catálogo/schema já configurados

TRINO_HOST="${TRINO_HOST:-localhost}"
TRINO_PORT="${TRINO_PORT:-8080}"
TRINO_USER="${TRINO_USER:-admin}"

run_sql() {
  local file="$1"
  echo "[RUN] $file"
  trino --server "http://${TRINO_HOST}:${TRINO_PORT}" --user "$TRINO_USER" --file "$file"
}

run_sql "$(dirname "$0")/../bronze/sql/bronze_trino.sql"
run_sql "$(dirname "$0")/../silver/sql/01_silver_trino.sql"
run_sql "$(dirname "$0")/../gold/sql/01_gold_trino.sql"

# Data quality checks por camada
run_sql "$(dirname "$0")/../bronze/sql/99_checks.sql"
run_sql "$(dirname "$0")/../silver/sql/99_checks.sql"
run_sql "$(dirname "$0")/../gold/sql/99_checks.sql"

echo "Pipeline medallion concluído com sucesso."
