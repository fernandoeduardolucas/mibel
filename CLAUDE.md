# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MIBEL is a local-first energy analytics platform for Portugal's electricity market. It implements a **medallion data architecture** (Bronze → Silver → Gold) with three independent data products (DPs), ML pipelines, and HTTP API + static HTML frontend layers. All infrastructure runs via Docker Compose.

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

Expected Trino catalogs: `iceberg`, `hive`, `system`, `tpcds`, `tpch`.

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

**Flyte execution model** — Workflows always run **locally** (not submitted to the remote K3s cluster) via `python -m flytekit.clis.sdk_in_container.pyflyte run`. DP-01 has a single monolithic `flyte_workflow.py`; DP-02 has modular tasks under `workflows/` (one file per layer); DP-03 has no Flyte workflow — it is orchestrated directly by the run script.

## Data Pipeline Commands

Each data product has its own Python orchestrator. Always pass `--skip-docker` if the stack is already running:

```powershell
# DP-01: Production vs Consumption (no --skip-docker flag; manages Docker internally)
python 02_medallion_pipeline/producao_consumo/run_medallion_pipeline.py

# DP-02: Consumption vs Price
python 02_medallion_pipeline/consumo_preco/run_medallion_consumo_precos.py --skip-docker

# DP-03: Meteo + Production (fetches live data from Open-Meteo API)
python 02_medallion_pipeline/meteo_producao/run_medallion_meteo_producao.py --skip-docker
```

The orchestrators handle: Docker readiness checks, Python venv creation, DDL execution via Trino, Silver/Gold transformations, and quality gates. They are idempotent — safe to re-run.

### Python Virtual Environments

Each orchestrator auto-creates and manages its own venv:

| Venv | Used by |
| --- | --- |
| `.venv_medallion/` | DP-01 pipeline |
| `.venv_medallion_consumo_preco/` | DP-02 pipeline |
| `.venv_medallion_meteo_producao/` | DP-03 pipeline |
| `.venv/` | ML training scripts (`03_ml_pipeline/`) and general use |

Do not manually manage these pipeline venvs — the orchestrators recreate them as needed. For ML training, create `.venv/` manually (see ML Training section).

## ML Training

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install pandas scikit-learn trino mlflow boto3

python 03_ml_pipeline/producao_consumo_mlflow_flow.py   # DP-01: RF deficit classifier
python 03_ml_pipeline/preco_consumo_mlflow_flow.py      # DP-02: GB load forecasting
python 03_ml_pipeline/meteo_producao_mlflow_flow.py     # DP-03: RF production forecast + GB price impact
```

Results visible at <http://localhost:15000>.

## Application Backends

No framework — backends use raw Python `BaseHTTPRequestHandler`. Run from each backend's own directory:

```powershell
# DP-01 (port 8081) — has server.py wrapper at backend root
cd 04_application/producao_consumo/backend && python server.py

# DP-02 (port 8000)
cd 04_application/backend/consumo_preco && python server.py

# DP-03 (port 8083) — no server.py; use -m to resolve app imports
cd 04_application/meteo_producao/backend && python -m app.main
```

Open frontends directly in a browser (static HTML files under `04_application/*/frontend/index.html`).

### Backend Configuration

DP-02 (`consumo_preco`) reads all connection settings from environment variables with sensible defaults:

```text
TRINO_HOST=localhost       TRINO_PORT=8080
TRINO_USER=admin           TRINO_CATALOG=iceberg
TRINO_SCHEMA=gold          TRINO_TABLE=dp_energy_market_hourly
API_HOST=0.0.0.0           PORT=8000
CACHE_TTL_SECONDS=60
```

DP-01 and DP-03 backends use hardcoded `localhost:8080` Trino defaults in their `config.py` files.

## Architecture

### Medallion Layers

```text
Raw Sources (CSV upload / Open-Meteo API)
  └─> BRONZE  — raw ingest; Hive external tables + Iceberg managed; Parquet on MinIO
  └─> SILVER  — deduplication, range validation, quality flags (Portugal-specific rules)
  └─> GOLD    — joins, aggregations, lag/rolling features; ML-ready and query-ready
  └─> Applications (HTTP APIs) + Grafana dashboards + ML training
```

SQL DDL and transformation scripts live under each layer directory:

- `02_medallion_pipeline/<dp_name>/01_bronze/sql/`
- `02_medallion_pipeline/<dp_name>/02_silver/sql/`
- `02_medallion_pipeline/<dp_name>/03_gold/sql/`

Python fetch scripts (for APIs/CSV uploads) live in `01_bronze/scripts/python/`.

### Quality Gate Architecture

There are **no unit tests** — quality assurance is done entirely via SQL checks in `02_medallion_pipeline/<dp_name>/04_quality/sql/`. Each DP has a set of SQL files that query Bronze, Silver, and Gold tables and return `PASS / WARN / FAIL` statuses with percentage thresholds. The orchestrators run these after each layer transform and abort on failures.

Quality checks cover: null rates, duplicate detection, out-of-range values (Portugal-specific rules), daily completeness, deduplication verification, join integrity, and lag/rolling feature validity.

### Backend MVC Pattern

All three backends follow the same structure:

```text
HTTP Handler (route) → Controller → Service → Repository → Trino Client
```

Each backend directory contains `models/`, `controllers/`, `services/`, `repositories/` packages.

### Data Products

| # | Name | Source | Grain | Gold Table | Models |
| --- | --- | --- | --- | --- | --- |
| DP-01 | `producao_consumo` | REN/ERSE CSV | Hourly | `iceberg.gold.dp_energia_balance_hourly` | RF deficit classifier |
| DP-02 | `consumo_preco` | OMIE day-ahead prices | Hourly | `iceberg.gold.dp_energy_market_hourly` | GB load forecast |
| DP-03 | `meteo_producao` | Open-Meteo API + production | Daily | `iceberg.gold.dp_meteo_producao_daily_features` | RF production + GB price impact |

### Query Engine

All SQL runs on **Trino** with two catalogs:

- `hive` — external tables (raw Parquet/CSV on MinIO)
- `iceberg` — managed tables (Silver and Gold layers)

Trino catalog configs: `01_docker_stack/trino/etc/catalog/`.

### Data Product Contracts

Schema contracts, SLAs/SLOs, quality rules, and versioning strategy for all three DPs are documented in [05_relatorio/relatorio.md](05_relatorio/relatorio.md).
