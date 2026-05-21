

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

All services are defined in `01_docker_stack/docker-compose.yml`:

```bash
cd 01_docker_stack
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
01_docker_stack/   # Docker Compose stack (MinIO, Hive, Trino, MLflow, Grafana)
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

## Table Schemas

### consumo_preco Schemas

#### Bronze

| Table | Column | Type | Notes |
| --- | --- | --- | --- |
| `bronze.consumo_raw` | datahora | TIMESTAMP WITH TZ | original source timestamp |
| | dia, mes, ano | INT | redundant date components |
| | date_raw, time_raw | VARCHAR | raw string fields |
| | bt, mt, at, mat, total | DOUBLE | consumption by voltage level + total (kW) |
| | process_date | DATE | partition key |
| `bronze.preco_raw` | date_raw | VARCHAR | date string |
| | hour | INT | 1–24 (25 = DST extra hour) |
| | price_portugal_raw, price_spain_raw | DOUBLE | €/MWh |
| | process_date | DATE | partition key |

#### Silver

| Table | Column | Type | Notes |
| --- | --- | --- | --- |
| `silver.consumo_hourly` | ts_utc | TIMESTAMP WITH TZ | canonical hourly timestamp |
| | total_mwh | DOUBLE | SUM(total)/1000 |
| | year, month | INT | partition columns |
| `silver.preco_hourly` | ts_utc | TIMESTAMP WITH TZ | canonical hourly timestamp |
| | price_portugal_eur_mwh, price_spain_eur_mwh | DOUBLE | €/MWh |
| | year, month | INT | partition columns |

#### Gold

| Table | Column | Type | Notes |
| --- | --- | --- | --- |
| `gold.dp_energy_market_hourly` | ts_utc | TIMESTAMP WITH TZ | business key |
| | consumo_total | DOUBLE | MWh |
| | market_price_pt | DOUBLE | €/MWh |
| | hora | INT | 0–23 |
| | dia_semana | INT | 0=Mon … 6=Sun |
| | is_weekend | BOOLEAN | |
| | consumo_lag_1h, consumo_lag_24h | DOUBLE | lag features |
| | price_lag_1h | DOUBLE | lag feature |
| | rolling_avg_consumo_24h, rolling_avg_price_24h | DOUBLE | 24h window avg |
| | process_date | DATE | |
| | year, month | INT | partition columns |
| `gold.feat_load_forecasting_hourly` | (all above) + | | |
| | consumo_next_hour | DOUBLE | ML TARGET (LEAD 1h) |

### producao_consumo Schemas

Bronze tables `bronze.consumo_total_nacional` and `bronze.energia_produzida_total_nacional` contain raw CSV columns plus quality flags:

- `flag_duplicate_timestamp`, `flag_bad_timestamp`, `flag_bad_total`, `flag_zero_row`

Silver tables `silver.consumo_total_nacional_15min` and `silver.energia_produzida_total_nacional_15min` are 15-min granularity, deduplicated with priority: non-zero → max total → min duplicate_rank → most recent.

`gold.dp_energia_balance_hourly`:

| Column | Type | Notes |
| --- | --- | --- |
| timestamp_utc | TIMESTAMP WITH TZ | hourly key |
| consumo_total_kwh | DOUBLE | |
| producao_total_kwh, producao_dgm_kwh, producao_pre_kwh | DOUBLE | production components |
| saldo_kwh | DOUBLE | producao − consumo |
| ratio_producao_consumo | DOUBLE | |
| flag_defice | BOOLEAN | production < consumption |
| flag_excedente | BOOLEAN | production > consumption |
| flag_missing_source | BOOLEAN | either source missing |

---

## Pipeline: producao_consumo

Sources: `consumo-total-nacional.csv` (consumption 15-min) + `energia-produzida-total-nacional.csv` (production 15-min)

Orchestrator: `02_medallion_pipeline/producao_consumo/run_medallion_pipeline.py`

DDL files:

- `02_medallion_pipeline/producao_consumo/01_bronze/sql/bronze_trino.sql`
- `02_medallion_pipeline/producao_consumo/02_silver/sql/`
- `02_medallion_pipeline/producao_consumo/03_gold/sql/01_gold_trino.sql`

Gold quality checks: `02_medallion_pipeline/producao_consumo/03_gold/sql/02_quality_checks.sql`

---

## Quality Checks Reference

Quality check SQL files are in `02_medallion_pipeline/consumo_preco/04_quality/sql/`. Each file contains a UNION ALL summary block (executed by Flyte) + detail queries (for manual inspection).

Output columns: `check_name`, `status` (PASS/WARN/FAIL), `valor_pct`, `threshold_pct`, `detalhe`

Behavior:

- `FAIL` → raises `FlyteRecoverableException` (retries=2), blocks layer promotion
- `WARN` → logged only, data is promoted
- `PASS` → logged, no action

Bronze checks (`01_bronze_checks.sql`): null_rate on datahora/total/prices (FAIL), hour range 1–25 (FAIL), positive total (WARN), positive prices (WARN), duplicate (datahora, process_date) (WARN), duplicate (date_raw, hour, process_date) (FAIL), completeness ≥80 records/day for consumo (WARN), ≥23/day for precos (WARN)

Silver checks (`02_silver_checks.sql`): null_rate on all columns (FAIL), unique ts_utc in each table (FAIL), ts_utc on hour boundary minute=0 second=0 (FAIL), join coverage consumo ↔ preco ≥95% (WARN), completeness ≥23h/day (WARN)

Gold checks (`03_gold_checks.sql`): null_rate on ts_utc/consumo_total/market_price_pt (FAIL), hora 0–23 (FAIL), dia_semana 0–6 (FAIL), unique ts_utc in both Gold tables (FAIL), null consumo_next_hour in feat table (FAIL), null lags in feat table (FAIL), row_count_parity dp vs feat ≤48 rows difference (WARN), lag_consistency lag_1h matches prior hour within 0.01 tolerance (FAIL if >0.1% inconsistent)

---

## ML Pipeline

File: `03_ml_pipeline/producao_consumao_mlflow_flow.py`

Model: `RandomForestClassifier` (300 estimators, max_depth=8)

Target: `flag_defice` — binary classification (will next hour have production deficit?)

Features (~24 total): hora, dia_semana, month + consumo_total_kwh, producao_total_kwh, saldo_kwh, ratio_producao_consumo + lags at 1h, 2h, 3h, 6h, 12h, 24h for each base metric

MLflow tracking: accuracy, precision, recall, F1, ROC-AUC; registered model name: `producao_consumo_defice_classifier`

```bash
python 03_ml_pipeline/producao_consumo_mlflow_flow.py
```

---

## Application Layer

### App: consumo_preco

Backend (`04_application/consumo_preco/backend/server.py`): Python stdlib HTTPServer

Endpoints:

- `GET /health`
- `GET /api/overview`
- `GET /api/timeseries?group=day|month`
- `GET /api/debug`

Dual data loading modes: CSV or Trino. Frontend: `04_application/consumo_preco/frontend/` — static HTML/CSS/JS.

### App: producao_consumo

Backend (`04_application/producao_consumo/backend/`): Python stdlib HTTPServer, port 8081

MVC pattern: Controller → Service (in-memory cache 60s) → Repository (Trino `trino.dbapi`) → `iceberg.gold.dp_energia_balance_hourly`

Endpoints:

- `GET /health`
- `GET /api/v1/producao-consumo/{hourly,daily,monthly,analytics,db-connection}`
- `GET /api/v1/producao-consumo/predictions/next-hour` (loads model from MLflow registry)

Frontend: `04_application/producao_consumo/frontend/` — static HTML/CSS/JS (`src/components/`, `src/services/`, `src/models/`, `src/utils/`)

---

## Grafana

Dashboard files: `01_docker_stack/grafana/dashboards/`

- `consumo_preco_overview.json` — queries `gold.dp_energy_market_hourly` + `gold.feat_load_forecasting_hourly`
- `producao_consumo_overview.json` — queries `gold.dp_energia_balance_hourly`

Datasource provisioning: `01_docker_stack/grafana/provisioning/datasources/trino.yml`
uid: `trino-iceberg`, catalog: `iceberg`, schema: `gold`, user: `grafana`

### Grafana Known Issues

The `format` field in dashboard JSON must be an integer (plugin `trino-datasource` rejects strings):

- `"format": 1` = table
- `"format": 0` = time_series

Time filter macro — do NOT use `CAST('$__timeFrom()' AS TIMESTAMP)` (plugin strips quotes, breaking SQL):

```sql
-- correct
WHERE $__timeFilter(ts_utc)
```

Trino health check: Docker marks Trino as `unhealthy` but it is functional — verify with:

```bash
curl http://localhost:8080/v1/info
docker exec tead_20_v12-trino-1 trino --execute "SELECT 1"
```
