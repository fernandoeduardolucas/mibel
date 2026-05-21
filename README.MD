# MIBEL Energy Analytics Platform

Plataforma de análise do mercado energético ibérico (MIBEL — Portugal), construída sobre uma arquitetura Medallion (Bronze → Silver → Gold) com Trino + Iceberg + MinIO. Cobre três produtos de dados que correlacionam produção elétrica, consumo, preço spot e meteorologia, com pipelines ML (MLflow) e dashboards interativos.

---

## Arquitetura Geral

```
┌─────────────────────────────────────────────────────────────────────┐
│                        FONTES DE DADOS                              │
│  Open-Meteo API (meteo)  │  REN/ERSE CSVs (prod/cons)  │  OMIE CSVs │
└──────────────┬───────────────────────┬──────────────────────┬───────┘
               │                       │                      │
               ▼                       ▼                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE — ingestão raw (MinIO S3 Parquet + Hive External Tables)    │
│  iceberg.bronze.meteo_open_meteo_hourly                             │
│  hive.bronze_stage.producao_consumo_clean / consumo_preco_clean     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ Trino SQL (dedup + validação)
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER — dados limpos e validados                                  │
│  iceberg.silver.meteo_open_meteo_hourly  (_quality_flag: ok/bad)    │
│  iceberg.silver.producao_consumo_hourly                             │
│  iceberg.silver.energy_market_hourly                                │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ Trino SQL (JOIN + agregações + lags)
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  GOLD — produtos de dados                                           │
│  DP-01: iceberg.gold.producao_vs_consumo_hourly                     │
│  DP-02: iceberg.gold.dp_energy_market_hourly                        │
│  DP-03: iceberg.gold.dp_meteo_producao_daily_features               │
└──────────┬──────────────────┬──────────────────────────────────────-┘
           │                  │
           ▼                  ▼
   ┌───────────────┐  ┌───────────────────────────────────────────────┐
   │  ML (MLflow)  │  │  Aplicações (API + Frontend + Grafana)        │
   │ 4 modelos     │  │  3 backends HTTP  │  3 frontends React (CDN)  │
   │ localhost:    │  │  localhost:8081   │  localhost:3300 (Grafana)  │
   │ 15000         │  │  localhost:8000   │                           │
   └───────────────┘  │  localhost:8083   │                           │
                      └───────────────────────────────────────────────┘
```

---

## Produtos de Dados

| ID | Tabela Gold | Grão | Descrição |
|----|-------------|------|-----------|
| DP-01 | `producao_vs_consumo_hourly` | Horário | Produção vs consumo elétrico nacional (REN/ERSE) |
| DP-02 | `dp_energy_market_hourly` | Horário | Preço spot day-ahead MIBEL PT (OMIE) |
| DP-03 | `dp_meteo_producao_daily_features` | Diário | Meteorologia + produção + preço + features ML (lags, rolling, temporais) |

---

## Stack de Serviços

| Serviço | URL | Credenciais |
|---------|-----|-------------|
| **Trino** (query engine) | http://localhost:8080 | sem auth |
| **MinIO** S3 Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| **MinIO** S3 API | http://localhost:9000 | `minioadmin` / `minioadmin` |
| **MLflow** | http://localhost:15000 | sem auth |
| **Grafana** | http://localhost:3300 | `admin` / `admin` |
| **Hive Metastore** | `thrift://localhost:9083` | — |

### Aplicações (backends + frontends)

| Produto | Backend | Frontend |
|---------|---------|----------|
| DP-01 Produção vs Consumo | http://localhost:8081 | [index.html](04_application/producao_consumo/frontend/index.html) |
| DP-02 Consumo vs Preço | http://localhost:8000 | [index.html](04_application/frontend/consumo_preco/index.html) |
| DP-03 Meteo + Produção | http://localhost:8083 | [index.html](04_application/meteo_producao/frontend/index.html) |

---

## Passo a Passo: Arrancar Tudo

### 1. Iniciar o stack Docker

```bash
cd 01_docker_stack
docker compose up -d
```

Aguarda ~30 segundos e verifica:
```bash
docker compose ps
# Todos os serviços devem estar "Up" exceto "mc" (exit 0 é esperado)
```

Confirma Trino disponível:
```bash
docker compose exec trino trino --execute "SHOW CATALOGS;"
# Deve listar: iceberg, hive, system, tpcds, tpch
```

---

### 2. Correr os Pipelines Medallion

> Cada pipeline faz fetch dos dados, upload para MinIO e materializa Bronze → Silver → Gold no Trino.

**DP-01 — Produção vs Consumo** (requer CSVs da REN/ERSE em `02_medallion_pipeline/producao_consumo/01_bronze/data/raw/`):
```bash
python 02_medallion_pipeline/producao_consumo/run_medallion_pipeline.py --skip-docker
```

**DP-02 — Consumo vs Preço Spot** (requer CSVs do OMIE em `02_medallion_pipeline/consumo_preco/01_bronze/data/raw/`):
```bash
python 02_medallion_pipeline/consumo_preco/run_medallion_consumo_precos.py --skip-docker
```

**DP-03 — Meteo + Produção** (dados meteorológicos obtidos automaticamente via Open-Meteo API):
```bash
python 02_medallion_pipeline/meteo_producao/run_medallion_meteo_producao.py --skip-docker
```

Flags disponíveis em todos os runners:
- `--skip-docker` — não faz `docker compose up` (stack já a correr)
- `--skip-ddl` — não recria as tabelas (já existem)
- `--no-quality` — salta o quality gate final

---

### 3. Correr os Pipelines ML (MLflow)

```bash
# Activa um venv com as dependências ML
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # Linux/macOS

pip install pandas scikit-learn trino mlflow boto3

# Treina os 4 modelos (2 por pipeline)
python 03_ml_pipeline/meteo_producao_mlflow_flow.py
```

Resultados em http://localhost:15000 (MLflow UI).

Modelos registados:
- `meteo_producao_producao_forecaster` — RandomForest → previsão produção (Target: `producao_total_daily_mwh`)
- `meteo_producao_preco_impact` — GradientBoosting → impacto no preço spot (Target: `preco_spot_medio_eur_mwh`)

---

### 4. Arrancar os Backends API

Cada backend é um servidor HTTP Python puro (sem dependências externas além de `trino`).

```bash
pip install trino

# DP-01 — porta 8081
python 04_application/producao_consumo/backend/app/main.py

# DP-02 — porta 8000
python 04_application/backend/consumo_preco/server.py

# DP-03 — porta 8083
python 04_application/meteo_producao/backend/app/main.py
```

Verifica:
```bash
curl http://localhost:8081/api/v1/producao-consumo/latest
curl http://localhost:8083/api/v1/meteo-producao/latest
```

---

### 5. Abrir os Frontends

Os frontends são HTML estáticos com React via CDN — basta abrir no browser:

```bash
# Windows
start 04_application/producao_consumo/frontend/index.html
start 04_application/frontend/consumo_preco/index.html
start 04_application/meteo_producao/frontend/index.html
```

> Os frontends detectam automaticamente o backend em `localhost` na porta configurada. O backend correspondente deve estar a correr.

---

### 6. Abrir o Dashboard Grafana

O dashboard DP-03 está pré-configurado em `04_application/meteo_producao/grafana/dashboards/meteo_producao_overview.json`.

**Importar via API** (apenas necessário uma vez):

```powershell
# PowerShell
$headers = @{ "Content-Type" = "application/json"; "Authorization" = "Basic " + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("admin:admin")) }
$json = Get-Content "04_application\meteo_producao\grafana\dashboards\meteo_producao_overview.json" -Raw
$body = @{
    dashboard = ($json | ConvertFrom-Json)
    overwrite = $true
    folderId  = 0
    inputs    = @(@{ name="DS_TRINO"; type="datasource"; pluginId="trino-datasource"; value="trino-iceberg" })
} | ConvertTo-Json -Depth 20 -Compress
Invoke-RestMethod -Method Post -Uri "http://localhost:3300/api/dashboards/import" -Headers $headers -Body $body
```

Abre o dashboard: http://localhost:3300/d/meteo-producao-dp03

---

## Fontes de Dados

| Produto | Fonte | Como obter |
|---------|-------|------------|
| **DP-01** Produção/Consumo | REN DataHub (`datahub.ren.pt`) e ERSE (`erse.pt`) | Download manual CSV → colocar em `02_medallion_pipeline/producao_consumo/01_bronze/data/raw/` |
| **DP-02** Preço Spot | OMIE (`omie.es`) | Download manual CSV day-ahead → colocar em `02_medallion_pipeline/consumo_preco/01_bronze/data/raw/` |
| **DP-03** Meteorologia | Open-Meteo Archive API (gratuita, sem chave) | Obtido automaticamente pelo `fetch_open_meteo.py` (lat=38.72, lon=-9.14, 2023-01-01 → hoje) |

> Os dados de DP-01 e DP-02 cobrem até **2026-03-04**. Para estender o período, descarrega novos CSVs e re-corre os respetivos pipelines.

---

## Diagrama da Pipeline DP-03

Arquivo: `02_medallion_pipeline/meteo_producao/diagrama_meteo_producao.html`

```bash
start 02_medallion_pipeline/meteo_producao/diagrama_meteo_producao.html
```

---

## Estrutura do Repositório

```
01_docker_stack/                       ← Docker Compose stack (Trino, MinIO, MLflow, Grafana)
02_medallion_pipeline/
  producao_consumo/                 ← Pipeline DP-01
  consumo_preco/                    ← Pipeline DP-02
  meteo_producao/                   ← Pipeline DP-03
    01_bronze/                      ← fetch_open_meteo.py + DDL bronze
    02_silver/                      ← DDL silver (dedup + quality flag)
    03_gold/                        ← DDL gold (features diárias DP-03)
    run_medallion_meteo_producao.py ← orquestrador
    diagrama_meteo_producao.html    ← diagrama de arquitetura
03_ml_pipeline/
  meteo_producao_mlflow_flow.py     ← treino modelos A e B (MLflow)
04_application/
  producao_consumo/                 ← backend 8081 + frontend + grafana
  backend/consumo_preco/            ← backend 8000
  frontend/consumo_preco/           ← frontend DP-02
  meteo_producao/                   ← backend 8083 + frontend + grafana (DP-03)
05_relatorio/relatorio.md           ← especificação dos produtos de dados
```
