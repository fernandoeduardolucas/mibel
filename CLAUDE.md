# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Stack

| Component | Technology | Port |
|---|---|---|
| Query engine | Trino 468 | 8080 |
| Object storage | MinIO (S3-compatible) | 9000 / 9001 (console) |
| Table format | Apache Iceberg | — |
| Catalog | Hive Metastore | 9083 |
| ML tracking | MLflow + PostgreSQL | 15000 |
| Visualization | Grafana | 3300 |
| Orchestration | Flyte sandbox (K3s) | — |

Default MinIO credentials: `minioadmin` / `minioadmin`.

## Architecture — Medallion Layers

Every pipeline follows `01_bronze → 02_silver → 03_gold`:

| Layer | Storage | Grain | Transformation |
|---|---|---|---|
| Bronze | Iceberg on `s3a://warehouse/bronze/` | Raw (15 min or hourly) | CSV → S3, type-cast only, quality flags |
| Silver | Iceberg on `s3a://warehouse/silver/` | 15 min normalized | Dedup (ROW_NUMBER), UTC alignment, unit conversion |
| Gold | Iceberg on `s3a://warehouse/gold/` | Hourly or daily | Business joins, derived metrics, ML-ready features |

Iceberg tables use `format_version = 2`, `object_store_layout_enabled = true`, `PARQUET` format.

Meta-columns present in every silver table: `_source_file`, `_ingested_at`, `_quality_flag`.  
Gold tables add `_updated_at`. ML feature tables add lag columns named `{metric}_lag_{n}d` and rolling averages named `{metric}_rolling_{n}d_avg`.

All timestamps are UTC-only. Portugal local time (`Europe/Lisbon`) appears only in display/reporting columns.

## Three Data Products

| Product | Gold table | Grain | Description |
|---|---|---|---|
| DP-01 | `iceberg.gold.producao_vs_consumo_hourly` | Hourly | Production vs. consumption balance, deficit/surplus flags |
| DP-02 | `iceberg.gold.dp_energy_market_hourly` | Hourly | Consumption + day-ahead price, lag/rolling ML features |
| DP-03 | `iceberg.gold.dp_meteo_producao_daily_features` | Daily | Meteorology + production + spot price — ML feature table |

Full data contracts, SLA/SLO, and schema versioning: `05_relatorio/relatorio.md`.

## Running the Stack

```bash
# Start all services (from repo root)
cd 01_bootstrap/tead_2.0_v1.2
docker compose up -d

# UIs
# Trino:   http://localhost:8080
# MinIO:   http://localhost:9001  (minioadmin / minioadmin)
# MLflow:  http://localhost:15000
# Grafana: http://localhost:3300
```

## Running a Pipeline

```bash
# producao_consumo (Bronze → Silver → Gold)
python 02_medallion_pipeline/producao_consumo/run_medallion_pipeline.py

# consumo_preco — full run with Docker + DDL
python 02_medallion_pipeline/consumo_preco/run_medallion_consumo_precos.py

# consumo_preco — stack already running, skip DDL
python 02_medallion_pipeline/consumo_preco/run_medallion_consumo_precos.py --skip-docker --skip-ddl

# consumo_preco — specific month backfill
python 02_medallion_pipeline/consumo_preco/run_medallion_consumo_precos.py --year 2023 --month 6

# meteo_producao — full run (fetches Open-Meteo data + Bronze → Silver → Gold)
python 02_medallion_pipeline/meteo_producao/run_medallion_meteo_producao.py

# meteo_producao — skip Docker + DDL
python 02_medallion_pipeline/meteo_producao/run_medallion_meteo_producao.py --skip-docker --skip-ddl
```

Runner flags available in `consumo_preco` and `meteo_producao`: `--skip-docker`, `--skip-ddl`, `--build`, `--no-quality`.

## Running ML Pipelines

```bash
# Set up venv first
python3.11 -m venv .venv && source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate                                  # Windows
pip install pandas==2.2.3 scikit-learn==1.6.1 trino==0.336.0 mlflow==3.10.1 boto3

# Deficit prediction (RandomForestClassifier)
python 03_ml_pipeline/producao_consumo_mlflow_flow.py

# Load forecasting (GradientBoostingRegressor)
python 03_ml_pipeline/preco_consumo_mlflow_flow.py

# Meteorology → production + price impact (RandomForest + GradientBoosting)
python 03_ml_pipeline/meteo_producao_mlflow_flow.py

# Remote Flyte execution
pyflyte run --remote -p flytesnacks -d development 03_ml_pipeline/preco_consumo_mlflow_flow.py preco_consumo_training_wf
```

All ML scripts use a **temporal train/test split (80/20, no shuffle)** — never random split on time-series data.

## Starting Backend APIs

```bash
# producao_consumo backend (port 8081, connects to Trino)
python 04_application/producao_consumo/backend/app/main.py

# consumo_preco backend (port 8082, reads CSVs directly)
python 04_application/consumo_preco/server.py

# meteo_producao backend (port 8083, connects to Trino)
python 04_application/meteo_producao/backend/app/main.py
```

Each backend is a plain Python `ThreadingHTTPServer` with no framework. Dependency: `pip install trino`.

## Backend MVC Pattern

All Trino-connected backends follow the same pattern (see `04_application/producao_consumo/backend/`):

```
app/
├── config.py          # TRINO_HOST, PORT, BASE_QUERY, CACHE_TTL_SECONDS
├── db/trino_client.py # trino.dbapi.connect() wrapper with run_query()
├── models/            # @dataclass domain models
├── repositories/      # SQL → domain model translation
├── services/          # cache layer + business aggregations
├── controllers/       # HTTP routing (route() method)
└── main.py            # ThreadingHTTPServer entry point
```

## Frontend Pattern

All dashboards are React 18 (no build step) loaded via ESM import maps from `esm.sh`. Entry: `index.html` → `main.js` → `src/main.js` → `src/components/DashboardApp.js`. Use `React.createElement` throughout — no JSX.

## Raw Data Sources

| Dataset | Path |
|---|---|
| Consumption (15 min) | `02_medallion_pipeline/producao_consumo/01_bronze/data/raw/consumo-total-nacional.csv` |
| Production (15 min) | `02_medallion_pipeline/producao_consumo/01_bronze/data/raw/energia-produzida-total-nacional.csv` |
| Day-ahead prices | `02_medallion_pipeline/consumo_preco/01_bronze/data/raw/Day-ahead Market Prices_*.csv` |
| Meteorology (hourly) | `02_medallion_pipeline/meteo_producao/01_bronze/data/raw/open-meteo-portugal-hourly.csv` |
