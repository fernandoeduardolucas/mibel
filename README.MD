# MIBEL — Energy Analytics Platform

Plataforma local de análise do mercado energético português (MIBEL), construída sobre uma arquitetura **Medallion** (Bronze → Silver → Gold) com **Trino + Iceberg + MinIO**. Integra três Data Products independentes, pipelines de Machine Learning com MLflow, quality gates automáticos e dashboards Grafana.

> Projeto académico — MEI / TEAD 2.0 | ESTG-IPP

---

## Arquitetura Global

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Fontes de Dados                                 │
│   REN/ERSE CSVs         OMIE CSVs          ENTSO-E API / Energy-Charts  │
│   (producao/consumo)    (precos mercado)   (streaming consumo+preco)    │
│                                            Open-Meteo API (meteorologia) │
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
      03_ml_pipeline   04_application   iceberg.audit
      (MLflow + Flyte) (Grafana 4 DSBs) (pipeline_runs +
                                         dataset_lineage)
```

---

## Data Products

| # | Nome | Fonte | Grão | Gold Table |
|---|---|---|---|---|
| DP-01 | `producao_consumo` | REN/ERSE CSV | Horário | `dp_energia_balance_hourly` |
| DP-02 Static | `consumo_preco` | OMIE CSV | Horário | `dp_energy_market_hourly` + `feat_load_forecasting_hourly` |
| DP-02 Streaming | `consumo_preco` | ENTSO-E API | Horário | `dp_energy_market_api_hourly` + `feat_load_forecasting_api_hourly` |
| DP-03 | `meteo_producao` | Open-Meteo API | Diário | `dp_meteo_producao_daily_features` |

---

## Estrutura do Repositório

```
mibel/
├── 01_docker_stack/        Infraestrutura Docker (MinIO, Trino, MLflow, Grafana, Flyte)
├── 02_medallion_pipeline/  Pipelines Bronze→Silver→Gold (DP-01, DP-02 Static+Streaming, DP-03)
├── 03_ml_pipeline/         Workflows ML com MLflow (5 modelos)
├── 04_application/         Dashboards Grafana (4 dashboards auto-provisionados)
├── 05_relatorio/           Relatório técnico (LaTeX) e especificações
└── README.MD               Este ficheiro
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

**Decisão chave:** o DP-03 inclui `feature_set_version` em todas as linhas da Gold, permitindo que cada treino ML referencie explicitamente a versão do conjunto de features — garante reprodutibilidade e auditabilidade.

---

### B) Implementação do Lakehouse — 20%

**Objetivo:** lakehouse operacional com separação de camadas Bronze/Silver/Gold.

**Implementado:**

- Stack Docker Compose com MinIO (S3), Hive Metastore (MariaDB), Trino 440, MLflow, Grafana, Redpanda
- 5 catálogos Trino: `iceberg`, `hive`, `kafka`, `tpcds`, `tpch`
- Bronze: Hive external tables sobre Parquet em `s3://warehouse/bronze/`
- Silver: Iceberg managed tables com deduplicação e `_quality_flag` (ok / null_values / out_of_range)
- Gold: Iceberg managed tables com joins cross-DP, lags, rolling windows e flags de negócio
- Schema `iceberg.audit`: tabelas de rastreabilidade de execuções (DP-02 Streaming)

**Decisão chave:** Hive para Bronze (tabelas externas preservam o Parquet original sem overhead ACID) e Iceberg para Silver/Gold (ACID, schema evolution sem rewrite, time-travel para auditoria).

Ver [`01_docker_stack/README.md`](01_docker_stack/README.md) e [`02_medallion_pipeline/README.MD`](02_medallion_pipeline/README.MD).

---

### C) Pipelines com Orquestração — 20%

**Objetivo:** pipelines reprodutíveis, idempotentes e orquestradas.

**Implementado:**

- Quatro runners Python idempotentes (Bronze→Silver→Gold + Quality Gate):
  - [`02_medallion_pipeline/producao_consumo/run_medallion_pipeline.py`](02_medallion_pipeline/producao_consumo/run_medallion_pipeline.py)
  - [`02_medallion_pipeline/DP02_Consumo_Preco/Static_Data/run_medallion_consumo_precos.py`](02_medallion_pipeline/DP02_Consumo_Preco/Static_Data/run_medallion_consumo_precos.py)
  - [`02_medallion_pipeline/DP02_Consumo_Preco/Streaming_Data/run_streaming_pipeline.py`](02_medallion_pipeline/DP02_Consumo_Preco/Streaming_Data/run_streaming_pipeline.py)
  - [`02_medallion_pipeline/meteo_producao/run_medallion_meteo_producao.py`](02_medallion_pipeline/meteo_producao/run_medallion_meteo_producao.py)
- Reprocessamento incremental Gold: DELETE + INSERT com filtro temporal (DP-01, DP-02 Static, DP-03)
- DP-02 Streaming: retry com backoff exponencial (3×), compactação automática Iceberg, SLA operacional monitorizado, auditoria em `iceberg.audit`
- Compatibilidade Flyte: scripts ML decorados com `@task` + `@workflow` (stubs locais sem Flyte instalado)
- Flyte Sandbox: [`01_docker_stack/flyte/`](01_docker_stack/flyte/) com Dockerfile e Helm overrides

**Decisão chave:** os runners gerem automaticamente o venv local e as dependências Python — zero setup manual além do Docker; idempotência garantida por `CREATE TABLE IF NOT EXISTS` + DDL antes de cada ingest.

---

### D) Serving — 10%

**Objetivo:** expor os dados Gold a consumidores via interface de visualização.

**Implementado:**

- Quatro dashboards Grafana auto-provisionados como código (JSON em git):
  - DP-01: saldo líquido, ratio produção/consumo, série temporal horária
  - DP-02 Static: preço médio day-ahead, custo estimado, correlação preço-consumo
  - DP-02 Streaming: dados quase-em-tempo-real, rolling 24h, freshness, auditoria de execuções
  - DP-03: KPIs meteorológicos, série temperatura/produção, scatter
- Datasource Trino executa SQL direto sobre `iceberg.gold.*` — sem ETL adicional

**Decisão chave:** Grafana sobre Trino como única camada de serving elimina servidores intermediários, mantém a Gold como única fonte de verdade e permite dashboards versionados e reprodutíveis.

Ver [`04_application/README.MD`](04_application/README.MD).

---

### E) Qualidade e Observabilidade — 15%

**Objetivo:** validação automática da qualidade dos dados com evidências e visibilidade em dashboard.

**Implementado:**

- **110 checks SQL** distribuídos por 4 pipelines:
  - DP-01: 21 checks (Silver + Gold)
  - DP-02 Static: 38 checks (Bronze + Silver + Gold)
  - DP-02 Streaming: 26 checks (Bronze + Silver + Gold) — inclui freshness, unicidade `ts_utc`, completude DST
  - DP-03: 25 checks (Silver + Gold)
- Tipos de checks: null rates, range físico (Portugal continental), unicidade, alinhamento temporal, lag consistency, cobertura cross-DP, freshness
- **Quality gates bloqueantes** nos 4 runners: FAIL → `SystemExit`; WARN → continua com log
- **Auditoria DP-02 Streaming**: `iceberg.audit.pipeline_runs` (run_id, status, duração, contagens) + `iceberg.audit.dataset_lineage` (grafo upstream→downstream por execução)
- **4 dashboards Grafana** provisionados automaticamente, incluindo painel de auditoria do Streaming

**Decisão chave:** bloquear o pipeline em FAIL garante que dados com qualidade insuficiente nunca chegam à Gold; a tabela de auditoria fecha o ciclo de observabilidade sem infraestrutura adicional.

---

### F) Machine Learning — 15%

**Objetivo:** workflow de treino reprodutível com rastreabilidade completa de dados e artefactos.

**Implementado:**

- 5 modelos treinados a partir de feature tables Gold:

| Script | Modelo | Target | Feature Table |
|---|---|---|---|
| `producao_consumo_mlflow_flow.py` | RandomForestClassifier | `flag_defice` (t+1h) | `dp_energia_balance_hourly` |
| `preco_consumo_mlflow_flow.py` | GradientBoostingRegressor | `consumo_next_hour` | `feat_load_forecasting_hourly` |
| `preco_consumo_streaming_mlflow_flow.py` | GradientBoostingRegressor | `consumo_next_hour` | `feat_load_forecasting_api_hourly` |
| `meteo_producao_mlflow_flow.py` (A) | RandomForestRegressor | `producao_total_daily_mwh` | `dp_meteo_producao_daily_features` |
| `meteo_producao_mlflow_flow.py` (B) | GradientBoostingRegressor | `preco_spot_medio_eur_mwh` | `dp_meteo_producao_daily_features` |

- **Split temporal** (sem shuffle) em todos os modelos — evita data leakage em séries temporais
- **MLflow tracking completo** por run: params, métricas, tags (`dataset`, `target`, `model_type`), artefactos (`feature_importances.json`, `model/`)
- **Model Registry**: 5 modelos registados com `registered_model_name`

**Decisão chave:** feature tables pré-computadas na Gold (lags, rolling windows) garantem que treino e inferência usam exactamente a mesma transformação — sem risco de training-serving skew.

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
python 02_medallion_pipeline/DP02_Consumo_Preco/Static_Data/run_medallion_consumo_precos.py --skip-docker
python 02_medallion_pipeline/DP02_Consumo_Preco/Streaming_Data/run_streaming_pipeline.py --skip-docker
python 02_medallion_pipeline/meteo_producao/run_medallion_meteo_producao.py --skip-docker

# 4. Treinar modelos ML
python -m venv .venv && .venv\Scripts\activate
pip install pandas scikit-learn trino mlflow boto3
python 03_ml_pipeline/producao_consumo_mlflow_flow.py
python 03_ml_pipeline/preco_consumo_mlflow_flow.py
python 03_ml_pipeline/preco_consumo_streaming_mlflow_flow.py
python 03_ml_pipeline/meteo_producao_mlflow_flow.py

# 5. Abrir Grafana — dashboards já provisionados automaticamente
# http://localhost:3300  (admin / admin)
```

## Endpoints

| Serviço | URL | Credenciais |
|---|---|---|
| Trino UI | <http://localhost:8080> | — |
| MinIO Console | <http://localhost:9001> | minioadmin / minioadmin |
| MLflow | <http://localhost:15000> | — |
| Grafana | <http://localhost:3300> | admin / admin |
| Flyte UI | <http://localhost:30080> | — |
