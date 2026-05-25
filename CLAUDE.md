# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MIBEL is a local-first energy analytics platform for Portugal's electricity market. It implements a **medallion data architecture** (Bronze → Silver → Gold) with three independent data products (DPs), ML pipelines, and Grafana dashboards for visualization. All infrastructure runs via Docker Compose.

## Infrastructure

> **Reference stack:** The teacher-provided guide is TEAD 2.0 v1.3 (`05_relatorio/docs/1. Geral/tead_2.0 v1.3/README.md`). The key addition in v1.3 is **Flyte Sandbox** for workflow orchestration.

### Prerequisites

- Docker Engine v20.10+, Docker Compose v2 (`docker compose`, not `docker-compose`)
- ~8 GB RAM available to Docker (Trino + Flyte are memory-intensive)
- ~10 GB free disk space for images and volumes

### Docker Compose Stack

Services: MinIO, Hive Metastore (MariaDB backend), Trino, MLflow (Postgres backend), Grafana, Redpanda (Kafka-compatible streaming).

MinIO buckets created on startup: `warehouse` (Trino/Hive data) and `mlflow` (artifact store).

Start the full stack:

```powershell
cd 01_docker_stack
docker compose up -d --build
# Verify (~30s startup): docker compose ps
# Test Trino: docker compose exec trino trino --execute "SHOW CATALOGS;"
```

Expected Trino catalogs: `iceberg`, `hive`, `kafka`, `system`, `tpcds`, `tpch`.

Service URLs:

- **Trino**: <http://localhost:8080>
- **MinIO Console**: <http://localhost:9001> (minioadmin/minioadmin)
- **MinIO S3 API**: <http://localhost:9000>
- **MLflow**: <http://localhost:15000>
- **Grafana**: <http://localhost:3300> (admin/admin)
- **Hive Metastore (Thrift)**: `thrift://localhost:9083`

### Flyte Sandbox (External Orchestration — v1.3)

Flyte runs as a **separate container outside Docker Compose**, with its own internal Kubernetes (K3s). It cannot join the Compose network directly.

Install and start a local Flyte cluster via `flytectl`:

```bash
# Follow: https://docs-legacy.flyte.org/en/v1.13.3/getting_started_with_workflow_development/running_a_workflow_locally.html
flytectl demo start   # waits several minutes for Helm + K3s startup
# UI available at http://localhost:30080
```

**Connecting Flyte tasks to Compose services** — Flyte task pods are isolated from Compose. Use `host.docker.internal` as the hostname:

| Service | URL from Flyte tasks |
| --- | --- |
| MinIO S3 API | `http://host.docker.internal:9000` |
| MLflow | `http://host.docker.internal:15000` |
| Trino | `http://host.docker.internal:8080` |
| Hive Metastore | `thrift://host.docker.internal:9083` |

Set these env vars in Flyte task environments:

```text
MLFLOW_S3_ENDPOINT_URL=http://host.docker.internal:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
```

> On Linux, start the Flyte sandbox with `--add-host host.docker.internal:host-gateway`. On Windows/macOS (Docker Desktop) this works out of the box.

Flyte config files: `01_docker_stack/flyte/Dockerfile` + `flyte-core-overrides.yaml`.

**Flyte execution model** — Two modes depending on the data product:

| DP | Flyte mode | Notes |
| --- | --- | --- |
| DP-01 | Local | Monolithic `flyte_workflow.py`; tasks run in local Python process |
| DP-02 Static | **Remote** | Modular `workflows/` (one file per layer); tasks run in K3s pods; sandbox managed by `run_medallion_consumo_precos.py` |
| DP-02 Streaming | Local | Tasks run in local Python process |
| DP-03 | None | Orchestrated directly by run script (no Flyte) |

**DP-02 Static remote execution** — The orchestrator auto-starts the sandbox if not running (builds `mibel-flyte:latest` from `01_docker_stack/flyte/Dockerfile`, then `docker run --privileged`). Tasks in K3s pods connect to Trino/MinIO via `host.docker.internal`. Fast registration (`--copy all`) uploads local code (including `04_quality/sql/`) to MinIO `flyte/` bucket. A `.flyteignore` at `Static_Data/` excludes raw CSVs from the upload. Flyte config: `workflows/flyte-config.yaml`.

## Data Pipeline Commands

Each data product has its own Python orchestrator. Always pass `--skip-docker` if the stack is already running:

```powershell
# DP-01: Production vs Consumption (no --skip-docker flag; manages Docker internally)
python 02_medallion_pipeline/DP01_Producao_Consumo/run_medallion_pipeline.py

# DP-02: Consumption vs Price (static CSV pipeline)
python 02_medallion_pipeline/DP02_Consumo_Preco/Static_Data/run_medallion_consumo_precos.py --skip-docker

# DP-02: Consumption vs Price (streaming API pipeline)
python 02_medallion_pipeline/DP02_Consumo_Preco/Streaming_Data/run_streaming_pipeline.py --skip-docker --days 7

# DP-03: Meteo + Production (fetches live data from Open-Meteo API)
python 02_medallion_pipeline/DP03_Meteo_Producao/run_medallion_meteo_producao.py --skip-docker
```

The orchestrators handle: Docker readiness checks, Python venv creation, DDL execution via Trino, Silver/Gold transformations, and quality gates. They are idempotent — safe to re-run.

DP-02 static pipeline supports additional granular flags:

- `--skip-docker` — skip compose up (stack already running)
- `--skip-ddl` — skip DDL re-application (tables already created)
- `--skip-upload` — skip re-uploading CSVs to MinIO
- `--skip-flyte` — skip Flyte sandbox check/start (sandbox already running externally)
- `--no-quality` — skip quality gates (useful in dev)
- `--year YYYY --month M` — run for a specific month only

Quick re-run when stack + sandbox are already running:

```powershell
python 02_medallion_pipeline/DP02_Consumo_Preco/Static_Data/run_medallion_consumo_precos.py --skip-docker --skip-ddl --skip-flyte
```

### Python Virtual Environments

Each orchestrator auto-creates and manages its own venv:

| Venv | Used by |
| --- | --- |
| `.venv_medallion/` | DP-01 pipeline |
| `.venv_medallion_consumo_preco/` | DP-02 static pipeline |
| `.venv_streaming_dp02/` | DP-02 streaming pipeline |
| `.venv_medallion_meteo_producao/` | DP-03 pipeline |
| `.venv/` | ML training scripts (`03_ml_pipeline/`) and general use |

Do not manually manage these pipeline venvs — the orchestrators recreate them as needed. For ML training, create `.venv/` manually (see ML Training section).

## ML Training

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install pandas scikit-learn trino mlflow boto3

python 03_ml_pipeline/producao_consumo_mlflow_flow.py        # DP-01: RF deficit classifier (target: flag_defice t+1h)
python 03_ml_pipeline/preco_consumo_mlflow_flow.py           # DP-02 Static: GB load forecasting (target: consumo_next_hour)
python 03_ml_pipeline/preco_consumo_streaming_mlflow_flow.py # DP-02 Streaming: GB load forecasting (API feature table)
python 03_ml_pipeline/meteo_producao_mlflow_flow.py          # DP-03: RF production forecast + GB price impact (two models)
```

Results visible at <http://localhost:15000>.

## Grafana Dashboards

All three data products are served exclusively via Grafana (auto-provisioned from `04_application/grafana/`):

| DP | Dashboard |
| --- | --- |
| DP-01 | `producao_consumo_overview.json` |
| DP-02 Static | `consumo_preco_overview.json` |
| DP-02 Streaming | `consumo_preco_streaming_overview.json` |
| DP-03 | `meteo_producao_overview.json` |

Reload dashboards without restarting the stack:

```powershell
curl -X POST http://localhost:3300/api/admin/provisioning/dashboards/reload -u admin:admin
```

## Architecture

### Medallion Layers

```text
Raw Sources (CSV upload / Open-Meteo API / Redpanda streaming)
  └─> BRONZE  — raw ingest; Hive external tables (preserves raw Parquet on MinIO, no ACID overhead)
  └─> SILVER  — deduplication, range validation, quality flags; Iceberg managed (ACID + schema evolution)
  └─> GOLD    — joins, aggregations, lag/rolling features; Iceberg managed; ML-ready and query-ready
  └─> Grafana dashboards + ML training (MLflow)
```

SQL DDL and transformation scripts live under each layer directory:

- `02_medallion_pipeline/<dp_name>/01_bronze/sql/`
- `02_medallion_pipeline/<dp_name>/02_silver/sql/`
- `02_medallion_pipeline/<dp_name>/03_gold/sql/`

Python fetch scripts (for APIs/CSV uploads) live in `01_bronze/scripts/python/`.

### Quality Gate Architecture

There are **no unit tests** — quality assurance is done entirely via SQL checks in `02_medallion_pipeline/<dp_name>/04_quality/sql/`. Each DP has a set of SQL files that query Bronze, Silver, and Gold tables and return `PASS / WARN / FAIL` statuses with percentage thresholds. The orchestrators run these after each layer transform and abort on failures. There are 110+ checks across all DPs.

Quality checks cover: null rates, duplicate detection, out-of-range values (Portugal-specific rules), daily completeness, deduplication verification, join integrity, and lag/rolling feature validity.

DP-02 Streaming also writes pipeline audit data to `iceberg.audit.pipeline_runs` and lineage to `iceberg.audit.dataset_lineage`. It retries failed tasks 3× with exponential backoff and runs Iceberg auto-compaction after Gold writes.

### Data Products

| # | Name | Source | Grain | Gold Table | Models |
| --- | --- | --- | --- | --- | --- |
| DP-01 | `producao_consumo` | REN/ERSE CSV | Hourly | `iceberg.gold.dp_energia_balance_hourly` | RF deficit classifier |
| DP-02 Static | `consumo_preco` | OMIE day-ahead CSV | Hourly | `iceberg.gold.dp_energy_market_hourly` + `feat_load_forecasting_hourly` | GB load forecast |
| DP-02 Stream | `consumo_preco` | Energy-Charts API / Redpanda | Hourly | `iceberg.gold.dp_energy_market_api_hourly` + `feat_load_forecasting_api_hourly` | GB load forecast |
| DP-03 | `meteo_producao` | Open-Meteo API + REN CSV | Daily | `iceberg.gold.dp_meteo_producao_daily_features` | RF production + GB price impact |

### Query Engine

All SQL runs on **Trino** with three catalogs:

- `hive` — external tables (Bronze layer raw Parquet/CSV on MinIO)
- `iceberg` — managed tables (Silver and Gold layers, plus `iceberg.audit.*`)
- `kafka` — live Redpanda topics (DP-02 Streaming ingestion)

Trino catalog configs: `01_docker_stack/trino/etc/catalog/`.

### Data Product Contracts

Schema contracts, SLAs/SLOs, quality rules, and versioning strategy for all three DPs are documented in [05_relatorio/relatorio.md](05_relatorio/relatorio.md).
