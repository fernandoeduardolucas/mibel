# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MIBEL is a local-first energy analytics platform for Portugal's electricity market. It implements a **medallion data architecture** (Bronze → Silver → Gold) with three independent data products (DPs), ML pipelines, and HTTP API + React frontend layers. All infrastructure runs via Docker Compose.

## Infrastructure

> **Reference stack:** The teacher-provided guide is TEAD 2.0 v1.3 (`05_relatorio/docs/1. Geral/tead_2.0 v1.3/README.md`). The project currently runs v1.2 (`01_bootstrap/tead_2.0_v1.2/`). The key addition in v1.3 is **Flyte Sandbox** for workflow orchestration.

### Prerequisites

- Docker Engine v20.10+, Docker Compose v2 (`docker compose`, not `docker-compose`)
- ~8 GB RAM available to Docker (Trino + Flyte are memory-intensive)
- ~10 GB free disk space for images and volumes

### Docker Compose Stack

Services: MinIO, Hive Metastore (MariaDB backend), Trino, MLflow (Postgres backend), Grafana.

MinIO buckets created on startup: `warehouse` (Trino/Hive data) and `mlflow` (artifact store).

Start the full stack:

```powershell
cd 01_bootstrap/tead_2.0_v1.2
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
|---|---|
| MinIO S3 API | `http://host.docker.internal:9000` |
| MLflow | `http://host.docker.internal:15000` |
| Trino | `http://host.docker.internal:8080` |
| Hive Metastore | `thrift://host.docker.internal:9083` |

Set these env vars in Flyte task environments:

```
MLFLOW_S3_ENDPOINT_URL=http://host.docker.internal:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
```

> On Linux, start the Flyte sandbox with `--add-host host.docker.internal:host-gateway`. On Windows/macOS (Docker Desktop) this works out of the box.

Flyte config files: `01_bootstrap/tead_2.0_v1.2/flyte/Dockerfile` + `flyte-core-overrides.yaml`.

## Data Pipeline Commands

Each data product has its own Python orchestrator. Always pass `--skip-docker` if the stack is already running:

```powershell
# DP-01: Production vs Consumption
python 02_medallion_pipeline/producao_consumo/run_medallion_pipeline.py --skip-docker

# DP-02: Consumption vs Price
python 02_medallion_pipeline/consumo_preco/run_medallion_consumo_precos.py --skip-docker

# DP-03: Meteo + Production (fetches live data from Open-Meteo API)
python 02_medallion_pipeline/meteo_producao/run_medallion_meteo_producao.py --skip-docker
```

The orchestrators handle: Docker readiness checks, Python venv creation, DDL execution via Trino, Silver/Gold transformations, and quality gates. They are idempotent — safe to re-run.

## ML Training

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install pandas scikit-learn trino mlflow boto3

python 03_ml_pipeline/meteo_producao_mlflow_flow.py   # DP-03: RF production forecast + GB price impact
python 03_ml_pipeline/preco_consumo_mlflow_flow.py    # DP-02: GB load forecasting
python 03_ml_pipeline/producao_consumao_mlflow_flow.py  # DP-01: RF deficit classification
```

Results visible at http://localhost:15000.

## Application Backends

No framework — backends use raw Python `BaseHTTPRequestHandler`. Install the Trino client first:

```powershell
pip install trino
python 04_application/producao_consumo/backend/app/main.py   # port 8081
python 04_application/backend/consumo_preco/server.py        # port 8000
python 04_application/meteo_producao/backend/app/main.py     # port 8083
```

Open frontends directly in a browser (static HTML files under `04_application/*/frontend/index.html`).

## Architecture

### Medallion Layers

```
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

### Backend MVC Pattern

All three backends follow the same structure:

```
HTTP Handler (route) → Controller → Service → Repository → Trino Client
```

Each backend directory contains `models/`, `controllers/`, `services/`, `repositories/` packages.

### Data Products

| # | Name | Source | Grain | Models |
|---|------|--------|-------|--------|
| DP-01 | `producao_consumo` | REN/ERSE CSV | Hourly | RF deficit classifier |
| DP-02 | `consumo_preco` | OMIE day-ahead prices | Hourly | GB load forecast |
| DP-03 | `meteo_producao` | Open-Meteo API + production | Daily | RF production + GB price impact |

### Query Engine

All SQL runs on **Trino** with two catalogs:
- `hive` — external tables (raw Parquet/CSV on MinIO)
- `iceberg` — managed tables (Silver and Gold layers)

Trino catalog configs: `01_bootstrap/tead_2.0_v1.2/trino/etc/catalog/`.

### Data Product Contracts

Schema contracts, SLAs/SLOs, quality rules, and versioning strategy for all three DPs are documented in [05_relatorio/relatorio.md](05_relatorio/relatorio.md).
