# Como Executar — DP-02 Streaming_Data

## 1. Pré-requisitos

### Stack Docker (Trino, MinIO, Hive Metastore, MLflow, Grafana)

```powershell
cd 01_docker_stack
docker compose up -d --build
docker compose ps   # verificar: todos os serviços Up
```

### Token ENTSO-E

```powershell
$env:ENTSOE_TOKEN = "<o-teu-token>"
```

Para obter um token gratuito: enviar email para `transparency@entsoe.eu`, assunto `"RESTful API access"`. Resposta em ~3 dias úteis.

---

## 2. Comando principal

```powershell
# Working directory: DP02_Consumo_Preco/Streaming_Data/
python run_streaming_pipeline.py --skip-docker --days 7
```

`--skip-docker` indica que a stack já está em execução.

---

## 3. Flags disponíveis

| Flag | Descrição |
|------|-----------|
| `--skip-docker` | Não verificar nem iniciar o Docker Compose |
| `--skip-ddl` | Não re-aplicar o DDL (tabelas já existem) |
| `--no-quality` | Não executar quality gates (útil em dev) |
| `--days N` | Ingerir os últimos N dias |
| `--start YYYY-MM-DD` | Data de início do intervalo |
| `--end YYYY-MM-DD` | Data de fim do intervalo |
| `--full` | Histórico completo (desde 2022-01-01 até hoje) |
| `--today` | Apenas o dia de hoje |

---

## 4. Exemplos de execução

```powershell
# Últimos 7 dias (uso típico — manter dados actualizados)
python run_streaming_pipeline.py --skip-docker --days 7

# Período específico
python run_streaming_pipeline.py --skip-docker --start 2024-01-01 --end 2024-12-31

# Histórico completo (~2022 até hoje) — execução inicial
python run_streaming_pipeline.py --skip-docker --full

# Apenas hoje, sem quality gates (debug rápido)
python run_streaming_pipeline.py --skip-docker --today --no-quality

# Re-run quando tabelas já existem e stack já está up
python run_streaming_pipeline.py --skip-docker --skip-ddl --days 7
```

---

## 5. Fluxo completo do pipeline

```
1. Verificar Docker Compose (skip com --skip-docker)
2. Criar/activar venv (.venv_streaming_dp02/)
3. Instalar dependências (entsoe-py, trino, flytekit, pandas)
4. Aplicar DDL — criar tabelas Bronze, Silver, Gold se não existirem (skip com --skip-ddl)
5. Executar workflow Flyte: fetch_bronze_api
   ├── fetch_consumo_api  →  bronze.consumo_api_raw
   └── fetch_preco_api    →  bronze.preco_api_raw
6. Quality gate Bronze (skip com --no-quality)
7. Executar workflow Flyte: bronze_to_silver_api_full
   ├── transform_consumo  →  silver.consumo_api_hourly
   └── transform_preco    →  silver.preco_api_hourly
8. Quality gate Silver (skip com --no-quality)
9. Executar workflow Flyte: silver_to_gold_api_full
   ├── build_dp_energy_market_api_full      →  gold.dp_energy_market_api_hourly
   └── build_feat_load_forecasting_api_full →  gold.feat_load_forecasting_api_hourly
10. Quality gate Gold (skip com --no-quality)
```

---

## 6. Scripts standalone (debug / ingestão pontual)

Permitem ingerir dados directamente sem correr o pipeline completo:

```powershell
$env:ENTSOE_TOKEN = "<o-teu-token>"

# Consumo — últimos 7 dias
python 01_bronze/scripts/python/fetch_consumo_entsoe.py --days 7

# Consumo — intervalo específico
python 01_bronze/scripts/python/fetch_consumo_entsoe.py --start 2024-01-01 --end 2024-01-31

# Preços — últimos 7 dias
python 01_bronze/scripts/python/fetch_preco_entsoe.py --days 7

# Preços — intervalo específico
python 01_bronze/scripts/python/fetch_preco_entsoe.py --start 2024-01-01 --end 2024-01-31
```

Dependências dos scripts standalone: `entsoe-py`, `pandas` (`pip install entsoe-py pandas`).

---

## 7. Verificação rápida via Trino

```sql
-- Contagem e cobertura temporal — Gold
SELECT
    COUNT(*)            AS total_horas,
    MIN(ts_utc)         AS primeira_hora,
    MAX(ts_utc)         AS ultima_hora
FROM iceberg.gold.dp_energy_market_api_hourly;

-- Feature table ML
SELECT
    COUNT(*)            AS total_exemplos,
    MIN(ts_utc)         AS inicio,
    MAX(ts_utc)         AS fim
FROM iceberg.gold.feat_load_forecasting_api_hourly;

-- Freshness
SELECT
    date_diff('hour', MAX(ts_utc), CURRENT_TIMESTAMP) AS horas_atraso
FROM iceberg.gold.dp_energy_market_api_hourly;
```

---

## 8. ML — Treino do modelo

Após o pipeline de ingestão estar completo:

```powershell
.venv\Scripts\activate   # venv com scikit-learn, mlflow, trino, pandas
python 03_ml_pipeline/preco_consumo_mlflow_flow.py
```

Resultados em `http://localhost:15000` — experiment `consumo-preco-load-forecast`.

---

## 9. Grafana — Dashboard

Dashboard provisionado automaticamente em `http://localhost:3300`.

Reload sem reiniciar a stack:

```powershell
curl -X POST http://localhost:3300/api/admin/provisioning/dashboards/reload -u admin:admin
```

Dashboard: **`consumo_preco_streaming_overview.json`**
