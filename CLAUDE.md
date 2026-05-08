# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**MIBEL** is a data lakehouse project for the Iberian electricity market, correlating national energy consumption with day-ahead market prices. It implements a Medallion architecture (Bronze → Silver → Gold) orchestrated by Flyte, running on a fully local Docker stack.

## Environment Setup

```bash
# Create venv at repo root (Python 3.11 required)
python3.11 -m venv .venv
source .venv/bin/activate   # Linux/Mac
# .\.venv\Scripts\Activate.ps1  # Windows PowerShell

pip install -U pip setuptools wheel
pip install -U flytekit

# Bronze script extra dependencies
pip install -r 02_medallion_pipeline/consumo_preco/01_bronze/scripts/python/requirements_bronze.txt
# requirements: pandas, pyarrow, boto3
```

## Local Stack (Docker)

All services are defined in `01_bootstrap/tead_2.0_v1.2/docker-compose.yml`:

```bash
cd 01_bootstrap/tead_2.0_v1.2
docker compose up -d          # start stack
docker compose down           # stop stack
docker compose up -d --build  # rebuild images (e.g. after MLflow Dockerfile change)
```

| Service | Port | Credentials |
|---|---|---|
| MinIO S3 API | 9000 | minioadmin / minioadmin |
| MinIO Console | 9001 | minioadmin / minioadmin |
| Trino | 8080 | — |
| Hive Metastore | 9083 | — |
| MLflow | 15000 | — |
| Grafana | 3300 | admin / admin |

MinIO buckets `warehouse` (Iceberg) and `mlflow` (artifacts) are created automatically on first start.

## Running the Medallion Pipeline

The main entry point for the `consumo_preco` pipeline:

```bash
# Full run (boots Docker, applies DDL, runs all layers + quality gates)
python 02_medallion_pipeline/consumo_preco/run_medallion_consumo_precos.py

# Stack already running, tables already created
python 02_medallion_pipeline/consumo_preco/run_medallion_consumo_precos.py --skip-docker --skip-ddl

# Specific month only
python 02_medallion_pipeline/consumo_preco/run_medallion_consumo_precos.py --year 2023 --month 1

# Skip quality gates (useful in development)
python 02_medallion_pipeline/consumo_preco/run_medallion_consumo_precos.py --no-quality
```

Running via Flyte remote:

```bash
# Full workflow (both pipelines)
pyflyte run --remote -p flytesnacks -d development \
  02_medallion_pipeline/flyte_workflow.py medallion_full_wf

# consumo_preco pipeline only
pyflyte run --remote -p flytesnacks -d development \
  02_medallion_pipeline/flyte_workflow.py medallion_full_wf \
  --run_producao_consumo False --run_consumo_preco True
```

## Architecture

### Folder Structure

```
01_bootstrap/tead_2.0_v1.2/   # Docker Compose stack (MinIO, Hive, Trino, MLflow, Grafana)
02_medallion_pipeline/
  consumo_preco/               # Consumption vs. market prices pipeline (primary)
    01_bronze/                 # Raw CSV ingest → Iceberg
    02_silver/                 # Aggregation + UTC normalization
    03_gold/                   # Two data products (analytics + ML features)
    04_quality/                # SQL-driven quality checks
    docs/                      # Schema specs, transformations, data product definitions
    workflows/                 # Flyte task definitions (one file per layer)
    run_medallion_consumo_precos.py  # Local orchestrator
  producao_consumo/            # Production vs. consumption pipeline
  flyte_workflow.py            # Top-level Flyte workflow (wraps both pipelines)
03_ml_pipeline/                # MLflow training (load forecasting)
04_application/                # Trino-backed backend + frontend dashboards
```

### Data Flow: consumo_preco

**Sources (raw CSVs in `01_bronze/data/raw/`):**
- `consumo-total-nacional.csv` — 15-min national consumption (kW), 2023–2026, ~13 MB
- `Day-ahead Market Prices_*.csv` — Hourly MIBEL market prices, 2023–2026

**Bronze** (`workflows/flyte_ingest_bronze.py`):
- CSVs are pre-cleaned by `01_bronze/scripts/python/bronze_clean_upload_consumo_precos.py` and uploaded to MinIO
- Flyte tasks read from MinIO, insert into Iceberg via Trino
- INSERTs batched at 5 000 rows / ≤60 partitions to avoid Iceberg writer limits
- Partition key: `day` (date)

**Silver** (`workflows/flyte_bronze_to_silver.py`):
- Consumption: aggregates 15-min kW intervals → hourly MWh (`SUM(kW) / 1000`)
- Prices: normalizes local hour (1–25, handles DST) → UTC timestamp
- Idempotency: `DELETE` same-day partition before `INSERT` (day granularity)

**Gold** (`workflows/flyte_silver_to_gold.py`):
- Produces two Iceberg tables:
  - `dp_energy_market_hourly` — INNER JOIN consumption × prices on `ts_utc`; adds calendar features (`hour`, `day_of_week`, `is_weekend`), lag columns (`lag_1h`, `lag_24h`), and 24-hour rolling averages
  - `feat_load_forecasting_hourly` — ML feature table derived from above; target = `consumo_next_hour`
- Window functions query the **full Silver history** (no date filter) to compute correct lag values at month boundaries; the final INSERT filters by year/month
- Idempotency: DELETE + INSERT per year/month partition

**Quality** (`workflows/flyte_quality_checks.py`):
- SQL files in `04_quality/sql/` contain a UNION ALL summary block (executed) + detail queries (for manual inspection)
- Raises `FlyteRecoverableException` on FAIL severity; WARN is logged only

### Key Design Decisions

- **Iceberg via Trino**: all DDL and DML run through Trino pointing at the `hive` catalog backed by MinIO + Hive Metastore
- **host.docker.internal**: Flyte tasks (running inside Docker) reach the local Trino/MinIO via `host.docker.internal`; local scripts use `localhost`
- **Idempotency**: every layer uses DELETE-then-INSERT at its natural partition granularity (day for Bronze/Silver, year-month for Gold), making reruns safe
- **Bronze CSV cleaning is a separate step**: `bronze_clean_upload_consumo_precos.py` normalizes columns, handles BOM, flags duplicates, and converts to Parquet before the Flyte ingest task reads from MinIO
