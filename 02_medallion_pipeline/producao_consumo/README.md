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

## Execução rápida (Windows)

No Windows, o runner usa `pipeline/run_all.bat`.

Ele tenta nesta ordem:
1. `TRINO_CMD` / `TRINO_EXE` (quando definidos)
2. comando `trino` no `PATH`
3. fallback automático para Docker Compose (`docker compose exec trino trino ...`)

### Opção A — Trino CLI local

```bat
set TRINO_CMD=C:\tools\trino.exe
cmd.exe /c pipeline\run_all.bat
```

### Opção B — Docker Compose (recomendado quando não há CLI local)

```bat
set TRINO_COMPOSE_FILE=..\..\01_bootstrap\tead_2.0_v1.2\docker-compose.yml
docker compose -f %TRINO_COMPOSE_FILE% up -d
cmd.exe /c pipeline\run_all.bat
```

Também pode usar as variáveis opcionais:

- `TRINO_USER` (default: `admin`)
- `TRINO_HOST` (default: `localhost`, usado no modo CLI local)
- `TRINO_PORT` (default: `8080`, usado no modo CLI local)
- `TRINO_DOCKER_SERVICE` (default: `trino`, usado no modo Docker)
- `TRINO_DOCKER_SERVER` (default: `http://trino:8080`, usado no modo Docker)
