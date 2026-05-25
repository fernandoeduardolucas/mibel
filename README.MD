# MIBEL — Energy Analytics Platform

> Plataforma local de análise do mercado energético português, construída sobre uma arquitetura **Medallion** (Bronze → Silver → Gold) com Trino + Apache Iceberg + MinIO. Integra três Data Products independentes, pipelines de Machine Learning rastreados por MLflow, 110+ quality gates automáticos e quatro dashboards Grafana.
>
> Projeto académico — MEI / TEAD 2.0 | ESTG-IPP

---

## O Que Esta Plataforma Faz

O MIBEL responde a três perguntas sobre o mercado elétrico português:

| Data Product | Pergunta Central | Fonte |
|---|---|---|
| **DP-01** — Produção vs Consumo | Portugal está a produzir mais do que consome? Quando ocorrem défices? | REN/ERSE CSV (histórico) |
| **DP-02** — Consumo vs Preço | Como se relacionam o preço day-ahead e a carga elétrica? Qual o custo estimado por hora? | OMIE CSV (estático) + ENTSO-E API (streaming) |
| **DP-03** — Meteo + Produção | Como é que a temperatura e a irradiação solar influenciam a produção renovável e o preço spot? | Open-Meteo API + REN CSV |

Cada DP percorre o mesmo ciclo: **ingestão → limpeza → enriquecimento → qualidade → ML → dashboard**.

---

## Visão Geral do Sistema

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                            FONTES DE DADOS EXTERNAS                            ║
╠══════════════╦═══════════════════════╦══════════════════╦════════════════════════╣
║  REN/ERSE    ║  OMIE CSV             ║  ENTSO-E /       ║  Open-Meteo API        ║
║  CSVs        ║  (preços day-ahead)   ║  Energy-Charts   ║  (meteorologia PT)     ║
║  (DP-01)     ║  (DP-02 Static)       ║  (DP-02 Stream)  ║  (DP-03)              ║
╚══════╤═══════╩══════════╤════════════╩═══════╤══════════╩══════════╤═════════════╝
       │                  │                    │                     │
       └──────────────────┴────────────────────┴─────────────────────┘
                                      │
                                      ▼
╔══════════════════════════════════════════════════════════════════════════════════╗
║              01_docker_stack — INFRAESTRUTURA LOCAL (Docker Compose)           ║
║                                                                                ║
║   ┌──────────┐   ┌──────────────────┐   ┌────────────────────────────────┐    ║
║   │  MinIO   │──▶│  Hive Metastore  │──▶│  Trino 440 (Query Engine)      │    ║
║   │  (S3)    │   │  (MariaDB)       │   │  catalogs: iceberg | hive |     │    ║
║   └──────────┘   └──────────────────┘   │            kafka | tpch | tpcds│    ║
║                                         └────────────────────────────────┘    ║
║   ┌──────────┐   ┌──────────────────┐   ┌────────────────────────────────┐    ║
║   │  MLflow  │   │  Redpanda        │   │  Flyte Sandbox (K3s)           │    ║
║   │ (track.) │   │  (Kafka compat.) │   │  Orquestração DP-02 Static     │    ║
║   └──────────┘   └──────────────────┘   └────────────────────────────────┘    ║
║                                                                                ║
║   ┌──────────┐                                                                 ║
║   │  Grafana │  ◀── 4 dashboards auto-provisionados                           ║
║   └──────────┘                                                                 ║
╚══════════════════════════════════════════════════════════════════════════════════╝
                                      │
                                      ▼
╔══════════════════════════════════════════════════════════════════════════════════╗
║                    02_medallion_pipeline — ARQUITETURA MEDALLION               ║
║                                                                                ║
║  ┌─────────────────────────────────────────────────────────────────────────┐   ║
║  │ BRONZE   Hive external tables  →  Parquet raw em s3://warehouse/bronze/ │   ║
║  │          Preserva dados originais; zero transformação                   │   ║
║  └─────────────────────────────┬───────────────────────────────────────────┘   ║
║                                │ INSERT INTO iceberg.bronze.*                   ║
║  ┌─────────────────────────────▼───────────────────────────────────────────┐   ║
║  │ SILVER   Iceberg managed  →  Dedup, range validation, _quality_flag     │   ║
║  │          (ok / null_values / out_of_range)                              │   ║
║  └─────────────────────────────┬───────────────────────────────────────────┘   ║
║                                │ Quality Gate Silver (PASS/WARN/FAIL)           ║
║  ┌─────────────────────────────▼───────────────────────────────────────────┐   ║
║  │ GOLD     Iceberg managed  →  Joins cross-DP, lags, rolling windows      │   ║
║  │          Feature tables ML-ready; particionadas e compactadas           │   ║
║  └─────────────────────────────┬───────────────────────────────────────────┘   ║
║                                │ Quality Gate Gold (PASS/WARN/FAIL)             ║
║         ┌──────────────────────┼──────────────────────┐                        ║
║         ▼                      ▼                      ▼                        ║
║   03_ml_pipeline         04_application         iceberg.audit                  ║
║   5 modelos MLflow        4 dashboards          pipeline_runs +                ║
║   RF + GB classif.        Grafana               dataset_lineage                ║
╚══════════════════════════════════════════════════════════════════════════════════╝
```

---

## Data Products

### DP-01 — Produção vs Consumo

**Pergunta:** Portugal está a importar energia? Qual a frequência e magnitude dos défices?

```
REN/ERSE CSVs (2018–2024)
  ├── consumo_nacional.csv     (15 min, MWh)
  ├── producao_dgm.csv         (15 min, MWh — produção total)
  └── producao_pre.csv         (15 min, MWh — regime especial)
         │
         ▼ Bronze (Hive external)
  hive.bronze_stage.consumo_nacional_15min
  hive.bronze_stage.producao_dgm_15min
  hive.bronze_stage.producao_pre_15min
         │
         ▼ Silver (Iceberg) — agrega 15 min → 1 hora, dedup, quality_flag
  iceberg.silver.energia_balance_hourly
         │
         ▼ Gold (Iceberg) — saldo, ratio, flags de negócio
  iceberg.gold.dp_energia_balance_hourly
    ├── saldo_kwh = producao_total − consumo_total
    ├── ratio_producao_consumo
    ├── flag_defice (bool) — saldo < 0
    └── flag_excedente (bool) — saldo > threshold

         ▼ ML
  RandomForestClassifier → target: flag_defice (t+1h)
  Feature importance: saldo_lag_1h, ratio, hora_do_dia, dia_semana

         ▼ Dashboard Grafana
  Saldo líquido horário · Ratio produção/consumo · Série temporal
```

**Quality Gate DP-01:** 21 checks — null rates, ranges físicos PT, unicidade, completude ≥ 90 reg/dia, saldo = produção − consumo.

**DP-01 Streaming (experimental):** Módulo Kafka/Redpanda autónomo que gera eventos sintéticos de produção/consumo a cada 30 s e materializa incrementalmente em `iceberg.gold.dp_producao_consumo_streaming`.

---

### DP-02 — Consumo vs Preço (dois modos)

**Pergunta:** Como evoluiu o preço day-ahead em Portugal? Quanto custou cada hora de consumo?

#### DP-02 Static — Histórico OMIE (CSV)

```
OMIE CSVs (2022–2024)
  └── precos_consumo_horario.csv  (€/MWh + MWh, hora a hora)
         │
         ▼ Bronze (Hive external)
  hive.bronze.consumo_preco_raw
         │
         ▼ Silver (Iceberg) — dedup, market_hour ∈ [1,25], range €/MWh
  iceberg.silver.consumo_preco_hourly
         │
         ▼ Gold (Iceberg) — join consumo × preço, custo estimado, lags
  iceberg.gold.dp_energy_market_hourly
    ├── preco_eur_mwh (day-ahead PT)
    ├── consumo_mwh
    ├── custo_estimado_eur = preco × consumo
    └── lag_1h, lag_24h, rolling_24h_consumo

  iceberg.gold.feat_load_forecasting_hourly
    └── consumo_next_hour (LEAD 1h) — target ML

  Orquestração: Flyte Sandbox (K3s) — tasks correm em pods isolados
```

**Quality Gate DP-02 Static:** 38 checks (Bronze + Silver + Gold) — validação `market_hour`, lag consistency, null rate do target.

#### DP-02 Streaming — API em Tempo Real (ENTSO-E / Energy-Charts)

```
ENTSO-E API / Energy-Charts API
  ├── Carga eléctrica PT (MW, horária)
  └── Preços day-ahead PT + ES (€/MWh)
         │
         ▼ Bronze (Iceberg, particionado por process_date)
  iceberg.bronze.consumo_api_raw
  iceberg.bronze.preco_api_raw
         │
         ▼ Silver (Iceberg) — MW→MWh, dedup ts_utc, alinhamento horário
  iceberg.silver.consumo_api_hourly
  iceberg.silver.preco_api_hourly
         │
         ▼ Gold (Iceberg) — join, calendário, lags 1h/24h, rolling 24h
  iceberg.gold.dp_energy_market_api_hourly
  iceberg.gold.feat_load_forecasting_api_hourly

         ▼ Audit (Iceberg)
  iceberg.audit.pipeline_runs    ← run_id, status, duração, rows
  iceberg.audit.dataset_lineage  ← grafo upstream→downstream por run

  Funcionalidades:
  ✓ Retry exponencial 3× (2s → 4s → 8s)
  ✓ Compactação automática Iceberg após cada run
  ✓ SLA: freshness ≤ 7 dias · runtime ≤ 45 min
  ✓ Logging estruturado com run_id
```

**Quality Gate DP-02 Streaming:** 26 checks — freshness ≤ 3 dias (consumo) / ≤ 2 dias (preço), unicidade `ts_utc`, completude ≥ 23 h/dia. Preços negativos → WARN (fenómeno real do MIBEL).

---

### DP-03 — Meteorologia + Produção

**Pergunta:** Como é que temperatura, irradiação solar e vento afetam a produção renovável e o preço spot?

```
Open-Meteo API (retrocast 2023–hoje)        REN/ERSE CSV (produção)
  ├── temperature_2m (°C)                     └── producao_total_daily_mwh
  ├── shortwave_radiation (W/m²)
  ├── wind_speed_10m (m/s)
  ├── precipitation (mm)
  ├── cloud_cover (%)
  ├── relative_humidity_2m (%)
  └── surface_pressure (hPa)
         │
         ▼ Bronze (Iceberg) — Parquet horário via fetch_open_meteo.py
  iceberg.bronze.meteo_open_meteo_hourly
         │
         ▼ Silver (Iceberg) — dedup hora, ranges físicos PT continental
  iceberg.silver.meteo_open_meteo_hourly
    └── quality_flag: ok | null_values | out_of_range
         │
         ▼ Gold (Iceberg) — agrega horário→diário, join DP-01+DP-02
  iceberg.gold.dp_meteo_producao_daily_features
    ├── temp_media_c, temp_max_c, temp_min_c
    ├── irradiacao_total_wm2, vento_medio_ms
    ├── producao_total_daily_mwh  (de DP-01)
    ├── preco_spot_medio_eur_mwh  (de DP-02)
    ├── lag_1d (temperatura, produção, preço)
    ├── rolling_7d (médias)
    └── feature_set_version  (reprodutibilidade ML)

         ▼ ML (dois modelos independentes)
  RandomForestRegressor     → target: producao_total_daily_mwh
  GradientBoostingRegressor → target: preco_spot_medio_eur_mwh

  Cross-DP join: DP-03 Gold consome dados de DP-01 e DP-02
  como features — feito uma vez na Gold e cacheado em Iceberg.
```

**Quality Gate DP-03:** 25 checks — null rates, ranges físicos (temp ∈ [-5, 50] °C, irradiação ≥ 0, etc.), lag consistency (tolerância 0.01 °C), cobertura cross-DP ≥ 90%.

---

## Fluxo de Dados — Diagrama Completo

```
                    INGESTÃO              LIMPEZA              FEATURES             SAÍDA
                    ─────────             ────────             ────────             ─────

DP-01           CSVs REN/ERSE
                (15 min)          ──▶  Bronze (Hive)  ──▶  Silver (Ice) ──▶  Gold (Ice)  ──▶  RF Classifier
                                                                               saldo/ratio      flag_defice (t+1h)
                                                                               flag_defice
                                                                                          ──▶  Grafana DP-01

DP-02 Static    OMIE CSV
                (hora a hora)     ──▶  Bronze (Hive)  ──▶  Silver (Ice) ──▶  Gold (Ice)  ──▶  GB Regressor
                                                                               custo_est.       consumo_next_h
                                                                               feat_table
                                                                                          ──▶  Grafana DP-02 Static

DP-02 Stream    ENTSO-E API
                (incremental)     ──▶  Bronze (Ice)   ──▶  Silver (Ice) ──▶  Gold (Ice)  ──▶  GB Regressor
                                  ▲                                            feat_table       consumo_next_h
                                  │  retry 3×                          │
                                  │  backoff exp.                      ▼
                                  │                             iceberg.audit    ──▶  Grafana DP-02 Stream
                                  │                          pipeline_runs
                                  │                          dataset_lineage

DP-03           Open-Meteo API
                (retrocast)       ──▶  Bronze (Ice)   ──▶  Silver (Ice) ──▶  Gold (Ice)  ──▶  RF Regressor
                                                                          ▲    cross-DP        producao_daily
                                                            DP-01 Gold ───┤    join        ──▶  GB Regressor
                                                            DP-02 Gold ───┘    lags/roll.       preco_spot
                                                                                          ──▶  Grafana DP-03
```

---

## Qualidade de Dados — 110+ Checks SQL

Não existem testes unitários — a qualidade é garantida inteiramente por **SQL bloqueante** executado após cada camada.

```
┌─────────────────────────────────────────────────────────────────────────┐
│  QUALITY GATE ARCHITECTURE                                              │
│                                                                         │
│  Cada check retorna: check_name | status | valor_pct | threshold | det  │
│                                                                         │
│  PASS  ──▶ continua pipeline                                            │
│  WARN  ──▶ regista aviso, continua (ex: preços negativos MIBEL)         │
│  FAIL  ──▶ SystemExit — pipeline bloqueada, dados não chegam à Gold     │
└─────────────────────────────────────────────────────────────────────────┘

  DP-01  │  21 checks   (Silver 11 + Gold 10)
  DP-02  │  38 checks   (Bronze 11 + Silver 13 + Gold 14)  Static
         │  26 checks   (Bronze + Silver + Gold)            Streaming
  DP-03  │  25 checks   (Silver 13 + Gold 12)
         │                                          ──────────────────
         │  TOTAL       110 checks SQL              ████████████████
```

**Tipos de verificações por camada:**

| Camada | Checks |
|---|---|
| Bronze | Null rates, completude por período, tipos de dados |
| Silver | Range físico PT, unicidade, deduplicação, alinhamento temporal |
| Gold | Lag consistency, cobertura cross-DP ≥ 90%, null rate targets ML, saldo = produção − consumo |

**Demo de qualidade (DP-03):** O script `corrupt_bronze.py` injeta propositadamente nulos, valores fora de range e duplicados na Bronze para demonstrar como a Silver os deteta e isola via `_quality_flag`.

---

## Machine Learning — 5 Modelos MLflow

Todos os modelos usam **feature tables pré-computadas na Gold** — sem risco de training-serving skew. Split temporal (sem shuffle) para evitar data leakage em séries temporais.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ML PIPELINE                                                                    │
│                                                                                 │
│  iceberg.gold.*  ──▶  Feature Table  ──▶  Train/Test Split  ──▶  MLflow Run    │
│                       (lags, rolling,      (temporal, sem        params         │
│                        flags, cal.)         shuffle)             métricas       │
│                                                                   artefactos    │
│                                                                   model/        │
│                                                   ──────────────────────────    │
│                                              MLflow Model Registry              │
│                                              5 modelos registados               │
└─────────────────────────────────────────────────────────────────────────────────┘

Script                              │ Modelo         │ Target                  │ Feature Table
─────────────────────────────────── │ ────────────── │ ──────────────────────  │ ──────────────────────────────
producao_consumo_mlflow_flow.py     │ RF Classifier  │ flag_defice (t+1h)      │ dp_energia_balance_hourly
preco_consumo_mlflow_flow.py        │ GB Regressor   │ consumo_next_hour       │ feat_load_forecasting_hourly
preco_consumo_streaming_mlflow.py   │ GB Regressor   │ consumo_next_hour       │ feat_load_forecasting_api_hourly
meteo_producao_mlflow_flow.py (A)   │ RF Regressor   │ producao_total_daily    │ dp_meteo_producao_daily_features
meteo_producao_mlflow_flow.py (B)   │ GB Regressor   │ preco_spot_medio        │ dp_meteo_producao_daily_features
```

---

## Infraestrutura

### Stack Docker Compose

```
01_docker_stack/
  ├── docker-compose.yml
  ├── minio/           Buckets: warehouse (Iceberg/Hive) + mlflow (artefactos)
  ├── trino/
  │   └── etc/catalog/
  │       ├── iceberg.properties   ──▶ tabelas Silver + Gold (ACID, time-travel)
  │       ├── hive.properties      ──▶ tabelas Bronze (Parquet externo, sem ACID)
  │       └── kafka.properties     ──▶ tópicos Redpanda (DP-02 Streaming, DP-01 exp.)
  ├── hive-metastore/  Gestão de schemas (backend MariaDB)
  ├── mlflow/          Tracking server + model registry (backend Postgres)
  ├── grafana/         Dashboards auto-provisionados via JSON em git
  ├── redpanda/        Broker Kafka-compatible (DP-02 Streaming + DP-01 exp.)
  └── flyte/           Dockerfile + Helm overrides para Flyte Sandbox (K3s)
```

**Porquê Hive na Bronze e Iceberg na Silver/Gold?**

- **Hive (Bronze):** tabelas externas preservam o Parquet original sem overhead ACID; ingestão mais rápida; auditoria sobre dados brutos.
- **Iceberg (Silver/Gold):** ACID para deduplicação concorrente; schema evolution sem rewrite; time-travel para reprocessamentos e auditoria; `DELETE + INSERT` incremental eficiente.

### Flyte Sandbox (Orquestração DP-02 Static)

O Flyte corre num container separado com Kubernetes interno (K3s). Os tasks executam em pods isolados e acedem ao stack Compose via `host.docker.internal`.

```text
run_medallion_consumo_precos.py
  ├── auto-inicia Flyte Sandbox se não estiver ativo
  ├── constrói imagem mibel-flyte:latest
  ├── regista workflows remotamente (fast registration via MinIO)
  └── executa tasks em pods K3s:
      ├── bronze_task  →  upload CSV + DDL Hive
      ├── silver_task  →  transformação + quality gate
      └── gold_task    →  features + quality gate
```

---

## Dashboards Grafana

Quatro dashboards auto-provisionados como código (JSON em git). Datasource: Trino sobre `iceberg.gold.*` — sem ETL adicional, Gold é a única fonte de verdade.

| Dashboard | Painéis Principais |
|---|---|
| **DP-01** Produção vs Consumo | Saldo líquido horário · Ratio produção/consumo · Flag défice |
| **DP-02 Static** Consumo vs Preço | Preço médio day-ahead · Custo estimado por hora · Correlação |
| **DP-02 Streaming** (quasi-realtime) | Rolling 24h · Freshness indicator · Auditoria de execuções |
| **DP-03** Meteo + Produção | KPIs meteorológicos · Série temperatura/produção · Scatter |

---

## Estrutura do Repositório

```text
mibel/
├── 01_docker_stack/
│   ├── docker-compose.yml
│   ├── trino/etc/catalog/        Conectores Iceberg, Hive, Kafka
│   ├── flyte/                    Dockerfile + Helm overrides Flyte Sandbox
│   └── grafana/                  Provisioning (datasources + dashboards)
│
├── 02_medallion_pipeline/
│   ├── DP01_Producao_Consumo/    Bronze/Silver/Gold + Quality + Streaming Kafka
│   ├── DP02_Consumo_Preco/
│   │   ├── Static_Data/          Pipeline OMIE CSV (Flyte remoto K3s)
│   │   ├── Streaming_Data/       Pipeline ENTSO-E API (Flyte local + audit)
│   │   └── docs/                 Contratos, schemas, decisões de design
│   └── DP03_Meteo_Producao/      Bronze/Silver/Gold + Quality + demo corrupção
│
├── 03_ml_pipeline/
│   ├── producao_consumo_mlflow_flow.py
│   ├── preco_consumo_mlflow_flow.py
│   ├── preco_consumo_streaming_mlflow_flow.py
│   └── meteo_producao_mlflow_flow.py
│
├── 04_application/
│   └── grafana/
│       ├── provisioning/
│       └── dashboards/           4 ficheiros JSON versionados
│
└── 05_relatorio/                 Relatório técnico + especificações DP
    └── relatorio.md              Contratos, SLAs, SLOs, schema evolution
```

---

## Arranque Rápido

### Pré-requisitos

- Docker Engine v20.10+ e Docker Compose v2
- ~8 GB RAM disponível para Docker (Trino + Flyte são intensivos)
- ~10 GB disco livre

### 1. Infraestrutura

```powershell
cd 01_docker_stack
docker compose up -d --build

# Verificar (~30 s de startup)
docker compose ps
docker compose exec trino trino --execute "SHOW CATALOGS;"
# Esperado: hive, iceberg, kafka, system, tpcds, tpch
```

### 2. Pipelines de Dados

```powershell
# DP-01: Produção vs Consumo
python 02_medallion_pipeline/DP01_Producao_Consumo/run_medallion_pipeline.py

# DP-02 Static: Consumo vs Preço (OMIE CSV)
python 02_medallion_pipeline/DP02_Consumo_Preco/Static_Data/run_medallion_consumo_precos.py --skip-docker

# DP-02 Streaming: Consumo vs Preço (ENTSO-E API, últimos 12 meses)
python 02_medallion_pipeline/DP02_Consumo_Preco/Streaming_Data/run_streaming_pipeline.py --skip-docker

# DP-03: Meteorologia + Produção (Open-Meteo API desde 2023)
python 02_medallion_pipeline/DP03_Meteo_Producao/run_medallion_meteo_producao.py --skip-docker
```

Os orchestrators são **idempotentes** — seguros para re-executar. Gerem automaticamente o venv local e as dependências Python.

### 3. ML Training

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install pandas scikit-learn trino mlflow boto3

python 03_ml_pipeline/producao_consumo_mlflow_flow.py
python 03_ml_pipeline/preco_consumo_mlflow_flow.py
python 03_ml_pipeline/preco_consumo_streaming_mlflow_flow.py
python 03_ml_pipeline/meteo_producao_mlflow_flow.py
```

### 4. Visualização

Dashboards já provisionados automaticamente. Abre o Grafana em [http://localhost:3300](http://localhost:3300) (admin / admin).

---

## Endpoints

| Serviço | URL | Credenciais |
|---|---|---|
| Grafana | [http://localhost:3300](http://localhost:3300) | admin / admin |
| Trino UI | [http://localhost:8080](http://localhost:8080) | — |
| MLflow | [http://localhost:15000](http://localhost:15000) | — |
| MinIO Console | [http://localhost:9001](http://localhost:9001) | minioadmin / minioadmin |
| MinIO S3 API | [http://localhost:9000](http://localhost:9000) | minioadmin / minioadmin |
| Flyte UI | [http://localhost:30080](http://localhost:30080) | — |

---

## Flags Comuns dos Orchestrators

| Flag | Efeito |
|---|---|
| `--skip-docker` | Não faz `compose up` (stack já a correr) |
| `--skip-ddl` | Não aplica DDL (tabelas já existem) |
| `--no-quality` | Salta quality gates (útil em dev) |
| `--date-from DATE` | Data de início do fetch/reprocessamento |
| `--date-to DATE` | Data de fim |
| `--skip-flyte` | Não verifica/inicia Flyte Sandbox (DP-02 Static) |
| `--source {energycharts\|entsoe}` | Fonte de dados (DP-02 Streaming) |
| `--clean` | Apaga e recria tabelas (DP-02 Streaming) |
| `--full` | Carrega histórico completo desde 2022 (DP-02 Streaming) |

---

## Documentação Técnica Detalhada

| Documento | Conteúdo |
|---|---|
| [01_docker_stack/README.md](01_docker_stack/README.md) | Setup do stack, catálogos Trino, troubleshooting |
| [02_medallion_pipeline/README.MD](02_medallion_pipeline/README.MD) | Detalhe completo de todos os pipelines e quality gates |
| [02_medallion_pipeline/DP02_Consumo_Preco/docs/](02_medallion_pipeline/DP02_Consumo_Preco/docs/) | Contratos DP-02, schemas, decisões de design, ML |
| [02_medallion_pipeline/DP03_Meteo_Producao/README.md](02_medallion_pipeline/DP03_Meteo_Producao/README.md) | DP-03 detalhado: schemas, quality gates, resultado execução |
| [05_relatorio/relatorio.md](05_relatorio/relatorio.md) | Especificação formal: SLAs, SLOs, schema evolution, contratos |
