# MIBEL — Energy Analytics Platform

Plataforma local de análise do mercado energético português (MIBEL), construída sobre uma arquitetura **Medallion** (Bronze → Silver → Gold) com **Trino + Iceberg + MinIO**. Integra três Data Products independentes, pipelines de Machine Learning com MLflow, APIs HTTP e frontends React, quality gates automáticos e dashboards Grafana.

> Projeto académico — MEI / TEAD 2.0 | ESTG-IPP

---

## Arquitetura Global

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Fontes de Dados                                 │
│   REN/ERSE CSVs         OMIE CSVs          Open-Meteo API               │
│   (producao/consumo)    (precos mercado)    (meteorologia PT)            │
└────────────┬────────────────┬──────────────────┬───────────────────────┘
             │                │                  │
             ▼                ▼                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  01_docker_stack — Infraestrutura Local (Docker Compose)                │
│                                                                         │
│  MinIO (S3)  ──►  Hive Metastore  ──►  Trino (query engine)            │
│                                         catalogs: iceberg | hive        │
│  MLflow (tracking + model registry)     Grafana (dashboards)            │
│  Redpanda (Kafka streaming)             Flyte Sandbox (orquestração)    │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ Pipelines (02_medallion_pipeline)
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Arquitetura Medallion                                                  │
│                                                                         │
│  BRONZE  →  Hive external tables (Parquet raw em MinIO)                 │
│  SILVER  →  Iceberg managed (dedup, range validation, quality_flag)     │
│  GOLD    →  Iceberg managed (joins, lags, features ML-ready)            │
│                              + Quality Gates (PASS/FAIL/WARN)           │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
      03_ml_pipeline   04_application   Grafana
      (MLflow + Flyte) (APIs + React)  (Dashboards)
```

---

## Data Products

| # | Nome | Fonte | Grão | Gold Table |
|---|---|---|---|---|
| DP-01 | `producao_consumo` | REN/ERSE CSV | Horário | `dp_energia_balance_hourly` |
| DP-02 | `consumo_preco` | OMIE CSV | Horário | `dp_energy_market_hourly` + `feat_load_forecasting_hourly` |
| DP-03 | `meteo_producao` | Open-Meteo API | Diário | `dp_meteo_producao_daily_features` |

---

## Estrutura do Repositório

```
mibel/
├── 01_docker_stack/        Infraestrutura Docker (MinIO, Trino, MLflow, Grafana)
├── 02_medallion_pipeline/  Pipelines Bronze→Silver→Gold (3 DPs)
├── 03_ml_pipeline/         Workflows ML com MLflow + Flyte
├── 04_application/         APIs HTTP + Frontends React (3 DPs)
├── 05_relatorio/           Relatório técnico (LaTeX) e especificações
└── README.md               Este ficheiro
```

---

## Critérios de Avaliação

### A) Especificação de Data Products — 20%

**Objetivo:** definir formalmente os três produtos de dados com contratos, SLAs e estratégia de versionamento.

**Implementado:**

- Três Data Products especificados em [`05_relatorio/relatorio.md`](05_relatorio/relatorio.md):
  - Perguntas analíticas, métricas e consumidores por DP
  - Grão e chaves de negócio
  - Schema v1 com tipos explícitos e regras de qualidade
  - SLAs (freshness, completude mínima) e SLOs (null rate, unicidade)
  - Estratégia de schema evolution: versão semântica (`v1`, `v2`), janela de coexistência 30 dias, `feature_set_version` obrigatório no DP-03

**Decisão chave:** o DP-03 inclui `feature_set_version` em todas as linhas da Gold, permitindo que cada treino ML referencie explicitamente a versão do conjunto de features utilizado — garante reprodutibilidade e auditabilidade.

---

### B) Implementação do Lakehouse — 20%

**Objetivo:** lakehouse operacional com separação de camadas Bronze/Silver/Gold.

**Implementado:**

- Stack Docker Compose com MinIO (S3), Hive Metastore (MariaDB), Trino 440, MLflow, Grafana, Redpanda
- 5 catálogos Trino: `iceberg`, `hive`, `kafka`, `tpcds`, `tpch`
- Bronze: Hive external tables sobre Parquet em `s3://warehouse/bronze/`
- Silver: Iceberg managed tables com deduplicação e `_quality_flag` (ok / null_values / out_of_range)
- Gold: Iceberg managed tables com joins cross-DP, lags, rolling windows e flags de negócio
- 12 tabelas Iceberg no total (bronze + silver + gold dos 3 DPs)

**Decisão chave:** Hive para Bronze (tabelas externas preservam o Parquet original sem overhead ACID) e Iceberg para Silver/Gold (ACID, schema evolution sem rewrite, time-travel para auditoria).

Ver [`01_docker_stack/README.md`](01_docker_stack/README.md) e [`02_medallion_pipeline/README.md`](02_medallion_pipeline/README.md).

---

### C) Pipelines com Orquestração — 20%

**Objetivo:** pipelines reprodutíveis, idempotentes e orquestradas.

**Implementado:**

- Três runners Python idempotentes (Bronze→Silver→Gold + Quality Gate em sequência):
  - [`02_medallion_pipeline/producao_consumo/run_medallion_pipeline.py`](02_medallion_pipeline/producao_consumo/run_medallion_pipeline.py)
  - [`02_medallion_pipeline/consumo_preco/run_medallion_consumo_precos.py`](02_medallion_pipeline/consumo_preco/run_medallion_consumo_precos.py)
  - [`02_medallion_pipeline/meteo_producao/run_medallion_meteo_producao.py`](02_medallion_pipeline/meteo_producao/run_medallion_meteo_producao.py)
- Flags: `--skip-docker`, `--skip-ddl`, `--build`, `--no-quality`, `--date-from`, `--date-to`
- Reprocessamento incremental Gold: DELETE + INSERT com filtro temporal (DP-01 e DP-03)
- Compatibilidade Flyte: todos os scripts ML decorados com `@task` + `@workflow` (stubs locais sem Flyte instalado)
- Flyte Sandbox: [`01_docker_stack/flyte/`](01_docker_stack/flyte/) com Dockerfile e Helm overrides

**Decisão chave:** os runners gerem automaticamente o venv local e as dependências Python — zero setup manual além do Docker; idempotência garantida por `CREATE TABLE IF NOT EXISTS` + DDL antes de cada ingest.

---

### D) Serving — 10%

**Objetivo:** expor os dados Gold a consumidores via API e interface web.

**Implementado:**

- Três backends HTTP Python (`BaseHTTPRequestHandler`, sem framework):
  - DP-01 em `:8081`, DP-02 em `:8000`, DP-03 em `:8083`
  - Padrão MVC: Route → Controller → Service → Repository → Trino Client
  - Queries parametrizadas sobre `iceberg.gold.*`
- Três frontends React 18 (ESM CDN, sem build step):
  - DP-01: KPIs saldo/ratio/flags + tabela horária + respostas analíticas
  - DP-02: KPIs preço/consumo/custo + série temporal + análise de correlação
  - DP-03: KPIs meteo + série temp/produção + scatter + importâncias de features

**Decisão chave:** React via ESM importmap elimina toda a toolchain de frontend (Node.js, bundler, transpiler) — o ficheiro `index.html` abre directamente no browser sem servidor.

Ver [`04_application/README.md`](04_application/README.md).

---

### E) Qualidade e Observabilidade — 15%

**Objetivo:** validação automática da qualidade dos dados com evidências e visibilidade em dashboard.

**Implementado:**

- **76+ checks SQL** distribuídos por 8 ficheiros (Silver + Gold dos 3 DPs):
  - [`02_medallion_pipeline/producao_consumo/04_quality/sql/`](02_medallion_pipeline/producao_consumo/04_quality/sql/) — 21 checks
  - [`02_medallion_pipeline/consumo_preco/04_quality/sql/`](02_medallion_pipeline/consumo_preco/04_quality/sql/) — ~30 checks
  - [`02_medallion_pipeline/meteo_producao/04_quality/sql/`](02_medallion_pipeline/meteo_producao/04_quality/sql/) — 25 checks
- Tipos de checks: null rates, range físico (Portugal continental), unicidade, alinhamento temporal, lag consistency, cobertura cross-DP, freshness
- **Quality gates bloqueantes** nos 3 runners: FAIL → `SystemExit`; WARN → continua com log
- **4 dashboards Grafana** provisionados automaticamente:
  - 3 dashboards operacionais por DP (overview KPIs + séries temporais)
  - 1 dashboard unificado de Qualidade & Observabilidade (26 painéis, todas as tabelas Gold e Silver)

**Decisão chave:** bloquear o pipeline em FAIL (e não apenas alertar) garante que dados com qualidade insuficiente nunca chegam à Gold — é mais barato reprovar na pipeline do que corrigir downstream em ML ou dashboards.

---

### F) Machine Learning — 15%

**Objetivo:** workflow de treino reprodutível com rastreabilidade completa de dados e artefactos.

**Implementado:**

- 4 modelos treinados a partir de feature tables Gold:

| Script | Modelo | Target | Métricas |
|---|---|---|---|
| `producao_consumo_mlflow_flow.py` | RandomForestClassifier | `flag_defice` (t+1h) | accuracy, precision, recall, F1, ROC-AUC |
| `preco_consumo_mlflow_flow.py` | GradientBoostingRegressor | `consumo_next_hour` | MAE, RMSE, R², MAPE |
| `meteo_producao_mlflow_flow.py` (A) | RandomForestRegressor | `producao_total_daily_mwh` | MAE, RMSE, R², MAPE |
| `meteo_producao_mlflow_flow.py` (B) | GradientBoostingRegressor | `preco_spot_medio_eur_mwh` | MAE, RMSE, R², MAPE |

- **Split temporal** (sem shuffle) em todos os modelos — evita data leakage em séries temporais
- **MLflow tracking completo** por run: params (hiperparâmetros + período de dados), métricas, tags (`dataset`, `target`, `model_type`), artefactos (`feature_importances.json`, `classification_report.json`, `feature_columns.json`, `model/`)
- **Model Registry**: 4 modelos registados com `registered_model_name`
- Compatibilidade **Flyte** (`@task` + `@workflow`) com stubs locais

**Decisão chave:** feature tables pré-computadas na Gold (lags, rolling windows) garantem que treino e inferência usam exactamente a mesma transformação — sem risco de training-serving skew.

Ver [`03_ml_pipeline/README.md`](03_ml_pipeline/README.md).

---

## Execução Rápida (Ordem Correcta)

```powershell
# 1. Arrancar infraestrutura
cd 01_docker_stack
docker compose up -d --build

# 2. Aguardar ~30s e verificar
docker compose exec trino trino --execute "SHOW CATALOGS;"

# 3. Correr pipelines de dados
python 02_medallion_pipeline/producao_consumo/run_medallion_pipeline.py
python 02_medallion_pipeline/consumo_preco/run_medallion_consumo_precos.py --skip-docker
python 02_medallion_pipeline/meteo_producao/run_medallion_meteo_producao.py --skip-docker

# 4. Treinar modelos ML
python -m venv .venv && .venv\Scripts\activate
pip install pandas==2.2.3 scikit-learn==1.6.1 trino==0.336.0 mlflow==3.10.1 boto3
python 03_ml_pipeline/producao_consumo_mlflow_flow.py
python 03_ml_pipeline/preco_consumo_mlflow_flow.py
python 03_ml_pipeline/meteo_producao_mlflow_flow.py

# 5. Arrancar backends
cd 04_application/producao_consumo/backend && python server.py        # :8081
cd 04_application/backend/consumo_preco   && python server.py        # :8000
cd 04_application/meteo_producao/backend  && python -m app.main      # :8083

# 6. Abrir frontends directamente no browser
#    04_application/producao_consumo/frontend/index.html
#    04_application/frontend/consumo_preco/index.html
#    04_application/meteo_producao/frontend/index.html
```

## Endpoints

| Serviço | URL | Credenciais |
|---|---|---|
| Trino UI | <http://localhost:8080> | — |
| MinIO Console | <http://localhost:9001> | minioadmin / minioadmin |
| MLflow | <http://localhost:15000> | — |
| Grafana | <http://localhost:3300> | admin / admin |
| API DP-01 | <http://localhost:8081> | — |
| API DP-02 | <http://localhost:8000> | — |
| API DP-03 | <http://localhost:8083> | — |
