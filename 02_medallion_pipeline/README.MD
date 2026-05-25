# 02 — Medallion Pipeline (Bronze → Silver → Gold)

Implementação das pipelines de dados para os três Data Products do projeto MIBEL. Cada DP segue a arquitetura medallion com três camadas Iceberg/Hive no Trino, quality gates bloqueantes e orquestração via scripts Python (compatíveis com Flyte).

---

## Diagrama de Arquitetura

```
Fontes Externas
       │
       ├── REN/ERSE CSV (DP01_Producao_Consumo)
       ├── OMIE CSV (DP02 Static)
       ├── ENTSO-E API / Energy-Charts (DP02 Streaming)
       └── Open-Meteo API (DP03_Meteo_Producao)
       │
       ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE — Raw Ingest                                                │
│  Hive external tables  →  Parquet files em MinIO s3://warehouse/    │
│  Sem transformação; preserva dados originais para auditoria         │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ INSERT INTO iceberg.bronze.*
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER — Limpeza e Validação                                       │
│  Iceberg managed tables  →  Deduplicação, range validation,         │
│  quality flags (_quality_flag: ok / null_values / out_of_range)     │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ INSERT INTO iceberg.silver.*
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  GOLD — Features e Agregações                                       │
│  Iceberg managed tables  →  Joins cross-DP, lags, rolling windows,  │
│  flags de negócio; ML-ready e query-ready                           │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                    ┌───────┴───────┐
                    ▼               ▼
             Quality Gate      Grafana /
             (PASS/FAIL/WARN)  ML Pipeline
             bloqueante
```

**Fluxo completo por runner:**
```
run_medallion_*.py
  1. Docker check / compose up
  2. Trino healthcheck (SELECT 1)
  3. Venv local + pip install deps
  4. DDL: CREATE TABLE IF NOT EXISTS (Bronze/Silver/Gold)
  5. Fetch/upload dados → MinIO Parquet
  6. Bronze Iceberg INSERT (stage → managed)
  7. Silver INSERT (dedup + validação)
  8. Gold INSERT (features + joins)
  9. Quality Gate Silver + Gold (bloqueante em FAIL)
```

---

## Estrutura de Ficheiros

```
02_medallion_pipeline/
│
├── DP01_Producao_Consumo/                         DP-01: produção vs consumo
│   ├── run_medallion_pipeline.py                 Orquestrador principal
│   ├── flyte_workflow.py                         Decoradores Flyte (execução local)
│   ├── relatorio.md                              Notas técnicas do DP-01
│   ├── 01_bronze/
│   │   ├── sql/bronze_trino.sql                 DDL Hive stage + Iceberg Bronze
│   │   ├── scripts/python/                      Upload de CSVs para MinIO
│   │   └── data/raw/                            CSVs locais (DGM, PRE, consumo)
│   ├── 02_silver/
│   │   └── sql/01_silver_trino.sql              Dedup, validação, quality_flag
│   ├── 03_gold/
│   │   └── sql/
│   │       ├── 01_gold_trino.sql                Gold DDL + INSERT principal
│   │       └── 03_gold_backfill_missing_hours.sql
│   ├── 04_quality/
│   │   └── sql/
│   │       ├── 01_silver_checks.sql             11 checks Silver (PASS/FAIL/WARN)
│   │       └── 02_gold_checks.sql               10 checks Gold (PASS/FAIL/WARN)
│   └── streaming/                               DP-01 Streaming: dados fictícios Kafka/Redpanda
│       ├── README.md                            Instruções de execução e troubleshooting
│       ├── RELATORIO_KAFKA_GOLD_EXPERIMENTAL.md Relatório do módulo experimental
│       ├── kafka_config/
│       │   └── producao_consumo_events.json     Definição do tópico Kafka para Trino
│       ├── scripts/python/
│       │   ├── fake_producao_consumo_producer.py Produtor sintético (30 s) + materialização
│       │   └── requirements_streaming.txt
│       └── sql/
│           └── 01_streaming_to_iceberg.sql      INSERT incremental Kafka → Bronze → Gold
│
├── DP02_Consumo_Preco/
│   │
│   ├── docs/                                     Documentação técnica do DP-02
│   │   ├── 01_overview.md                        Visão geral do produto
│   │   ├── 02_fontes_dados.md                    Fontes de dados (OMIE, ENTSO-E)
│   │   ├── 03_lakehouse_design.md                Decisões de design lakehouse
│   │   ├── 04_schemas.md                         Schemas das tabelas
│   │   ├── 05_transformacoes.md                  Transformações Silver/Gold
│   │   ├── 06_qualidade.md                       Quality gates e checks
│   │   ├── 07_ml_pipeline.md                     ML pipeline (GB load forecasting)
│   │   ├── contract.yaml                         Contrato do data product
│   │   └── product.yaml                          Manifesto do produto
│   │
│   ├── Static_Data/                              DP-02 Static: histórico OMIE CSV
│   │   ├── run_medallion_consumo_precos.py       Orquestrador principal (Flyte remoto K3s)
│   │   ├── Mibel_Consumo_Preco.md               Documentação técnica do pipeline estático
│   │   ├── .flyteignore                          Exclui CSVs do upload ao Flyte
│   │   ├── workflows/                            Tasks Flyte (um ficheiro por camada)
│   │   ├── 01_bronze/
│   │   │   └── bronze_consumo_precos_trino.sql  DDL Bronze (OMIE CSVs)
│   │   ├── 02_silver/
│   │   │   └── sql/silver_consumo_precos_trino.sql
│   │   ├── 03_gold/
│   │   │   └── sql/gold_consumo_precos_trino.sql  Gold + feat_load_forecasting
│   │   └── 04_quality/
│   │       └── sql/
│   │           ├── 01_bronze_checks.sql         11 checks Bronze
│   │           ├── 02_silver_checks.sql         13 checks Silver
│   │           └── 03_gold_checks.sql           14 checks Gold
│   │
│   └── Streaming_Data/                           DP-02 Streaming: ENTSO-E API
│       ├── run_streaming_pipeline.py             Orquestrador principal (Flyte local)
│       ├── DP02_Pipeline.md                      Documentação técnica do pipeline
│       ├── .env.example                          Variáveis de ambiente (ENTSOE_TOKEN, etc.)
│       ├── workflows/                            Tasks Flyte (Bronze, Silver, Gold)
│       ├── 01_bronze/
│       │   └── scripts/python/
│       │       ├── fetch_consumo_entsoe.py       Carga eléctrica PT (ENTSO-E)
│       │       └── fetch_preco_entsoe.py         Preços day-ahead PT+ES (ENTSO-E)
│       ├── 02_silver/
│       ├── 03_gold/
│       ├── 04_quality/sql/                       ~26 checks (Bronze + Silver + Gold)
│       └── 05_audit/sql/                         DDL audit.pipeline_runs + dataset_lineage
│
└── DP03_Meteo_Producao/                           DP-03: meteorologia + produção
    ├── run_medallion_meteo_producao.py           Orquestrador principal
    ├── README.md                                 Documentação detalhada do DP-03
    ├── diagrama_meteo_producao.html              Diagrama interactivo da arquitectura
    ├── 01_bronze/
    │   ├── sql/01_bronze_ddl.sql                DDL Bronze (Hive raw + stage + Iceberg)
    │   ├── scripts/python/fetch_open_meteo.py   Fetch API + Parquet + MinIO
    │   └── data_quality_demo/
    │       └── corrupt_bronze.py                Demo académica: injeta dados inválidos na Bronze
    ├── 02_silver/
    │   └── sql/01_silver_trino.sql
    ├── 03_gold/
    │   └── sql/01_gold_trino.sql
    └── 04_quality/sql/
        ├── 01_silver_checks.sql                 13 checks Silver
        └── 02_gold_checks.sql                   12 checks Gold
```

---

## O Que Foi Implementado

### DP-01 — `DP01_Producao_Consumo`

| Camada | Tabela | Descrição |
|---|---|---|
| Bronze | `hive.bronze_stage.consumo_nacional_15min` | Parquet externo — granularidade 15 min |
| Bronze | `hive.bronze_stage.producao_dgm_15min` | Produção DGM por 15 min |
| Bronze | `hive.bronze_stage.producao_pre_15min` | Produção PRE por 15 min |
| Silver | `iceberg.silver.energia_balance_hourly` | Agregação horária, dedup, quality_flag |
| Gold | `iceberg.gold.dp_energia_balance_hourly` | Saldo, ratio, flag_defice, flag_excedente |

**Quality Gate DP-01:** 21 checks SQL — null rates, range, unicidade, alinhamento temporal 15 min, completude ≥ 90 registos/dia, saldo_kwh = producao − consumo.

**DP-01 Streaming (experimental)** — módulo Kafka autónomo em `DP01_Producao_Consumo/streaming/`. Gera eventos sintéticos de produção/consumo a cada 30 s via `fake_producao_consumo_producer.py`, publica no Redpanda e materializa incrementalmente em:

| Tabela | Descrição |
|---|---|
| `iceberg.bronze.producao_consumo_streaming` | Bronze Iceberg particionado por evento |
| `iceberg.gold.dp_producao_consumo_streaming` | Gold com saldo e métricas calculadas |

O Grafana tem um dashboard dedicado `Producao Consumo Streaming (30s)` que lê da tabela Gold. Ver `streaming/README.md` para instruções de execução e troubleshooting completos.

### DP-02 — Documentação Técnica

A pasta `DP02_Consumo_Preco/docs/` contém documentação detalhada partilhada entre os dois sub-pipelines:

| Ficheiro | Conteúdo |
|---|---|
| `01_overview.md` | Visão geral e objectivos do DP-02 |
| `02_fontes_dados.md` | Descrição das fontes OMIE e ENTSO-E |
| `03_lakehouse_design.md` | Decisões de design Hive/Iceberg |
| `04_schemas.md` | Schemas completos de todas as tabelas |
| `05_transformacoes.md` | Lógica de transformação Silver e Gold |
| `06_qualidade.md` | Regras e thresholds dos quality gates |
| `07_ml_pipeline.md` | Feature engineering e treino GB load forecasting |
| `contract.yaml` | Contrato do data product (SLA, SLO, versioning) |
| `product.yaml` | Manifesto do produto (owner, consumers, tags) |

### DP-02 Static — `consumo_preco` (OMIE CSV)

| Camada | Tabela | Descrição |
|---|---|---|
| Bronze | `hive.bronze.consumo_preco_raw` | Parquet OMIE (consumo + preço day-ahead) |
| Silver | `iceberg.silver.consumo_preco_hourly` | Dedup, validação range, quality_flag |
| Gold | `iceberg.gold.dp_energy_market_hourly` | Join consumo × preço, custo estimado |
| Gold | `iceberg.gold.feat_load_forecasting_hourly` | Feature table ML: lags 1h/24h, rolling 24h |

**Quality Gate DP-02 Static:** 38 checks SQL (Bronze + Silver + Gold); inclui validação `market_hour ∈ [1,25]`, lag consistency e null rate do target `consumo_next_hour`.

### DP-02 Streaming — `consumo_preco` (ENTSO-E API)

| Camada | Tabela | Descrição |
|---|---|---|
| Bronze | `iceberg.bronze.consumo_api_raw` | Carga eléctrica PT em MW, horária, particionada por `process_date` |
| Bronze | `iceberg.bronze.preco_api_raw` | Preços day-ahead PT + ES, horários, particionados por `process_date` |
| Silver | `iceberg.silver.consumo_api_hourly` | Dedup, MW→MWh, particionado por `year/month` |
| Silver | `iceberg.silver.preco_api_hourly` | Dedup, alinhamento horário, particionado por `year/month` |
| Gold | `iceberg.gold.dp_energy_market_api_hourly` | Join consumo × preço, features calendário, lags, rolling 24h |
| Gold | `iceberg.gold.feat_load_forecasting_api_hourly` | Feature table ML: adiciona `consumo_next_hour` (LEAD) |
| Audit | `iceberg.audit.pipeline_runs` | Registo de execuções: run_id, status, duração, contagens de linhas |
| Audit | `iceberg.audit.dataset_lineage` | Grafo upstream→downstream materializado por execução |

**Quality Gate DP-02 Streaming:** 26 checks — freshness (≤ 3 dias consumo, ≤ 2 dias preço), unicidade `ts_utc`, completude ≥ 23 horas/dia, null rates e ranges. Preços negativos geram WARN (fenómeno real do MIBEL).

**Funcionalidades específicas do Streaming:**
- Retry com backoff exponencial (3 tentativas: 2s → 4s) sobre cada chamada Flyte
- Compactação automática `ALTER TABLE ... EXECUTE optimize` após cada run
- SLA operacional: freshness ≤ 7 dias e runtime ≤ 45 min (aviso se ultrapassado)
- Logging estruturado com `run_id` prefixado em cada linha

### DP-03 — `DP03_Meteo_Producao` (contexto)

| Camada | Tabela | Descrição |
|---|---|---|
| Bronze | `iceberg.bronze.meteo_open_meteo_hourly` | Parquet Open-Meteo (horário, 7 variáveis) |
| Silver | `iceberg.silver.meteo_open_meteo_hourly` | Dedup hora, range físico PT, quality_flag |
| Gold | `iceberg.gold.dp_meteo_producao_daily_features` | Agrega horário→diário, join DP-01+DP-02, lags 1d, rolling 7d |

**Quality Gate DP-03:** 25 checks; destaque para lag_consistency (tolerância 0.01 °C) e cobertura cross-DP ≥ 90%.

> Ver [DP03_Meteo_Producao/README.md](DP03_Meteo_Producao/README.md) para documentação detalhada do DP-03 (schemas, quality gates, flags, resultado da última execução).

**Demo de Qualidade de Dados** — `01_bronze/data_quality_demo/corrupt_bronze.py` injeta propositadamente dados inválidos na Bronze (nulos, fora de range, duplicados, timestamps desalinhados) e demonstra como a Silver os detecta e sinaliza via `_quality_flag`. Útil para demonstrações académicas do valor da camada Silver.

```powershell
# Corromper 5% das linhas e reconstruir Silver
python 02_medallion_pipeline/DP03_Meteo_Producao/01_bronze/data_quality_demo/corrupt_bronze.py --dp meteo --type all --pct 5
python 02_medallion_pipeline/DP03_Meteo_Producao/01_bronze/data_quality_demo/corrupt_bronze.py --rerun-silver --dp meteo
# Restaurar estado original
python 02_medallion_pipeline/DP03_Meteo_Producao/01_bronze/data_quality_demo/corrupt_bronze.py --restore --dp meteo
```

### Justificação das Decisões

| Decisão | Justificação |
|---|---|
| **Iceberg para Silver/Gold** | ACID, schema evolution sem rewrite, time-travel para auditoria de reprocessamentos |
| **Hive para Bronze** | Tabelas externas preservam o Parquet original; sem custo de gestão ACID para dados raw |
| **Quality gates bloqueantes** | Erros de qualidade na Gold propagam-se para ML e dashboards; bloquear na pipeline é mais barato do que corrigir downstream |
| **UNION ALL + TSV_HEADER** | Padrão uniforme de output; todos os checks retornam `check_name | status | valor_pct | threshold_pct | detalhe` |
| **Flags --date-from/--date-to** | Permite reprocessamento incremental de intervalos específicos; Gold usa DELETE + INSERT com filtro temporal |
| **Cross-DP join na Gold DP-03** | Feature table de ML requer produção (DP-01) e preço (DP-02) como targets; join feito uma vez na Gold e cacheado em Iceberg |
| **audit.pipeline_runs (Streaming)** | Rastreabilidade de execuções sem infraestrutura externa; lineage materializado por run_id |

---

## Passo a Passo de Execução

### Pré-requisito

Stack Docker a correr (ver `01_docker_stack/README.md`).

### DP-01 — Produção vs Consumo

```powershell
# Execução completa (inclui Docker check)
python 02_medallion_pipeline/DP01_Producao_Consumo/run_medallion_pipeline.py

# Stack já a correr
python 02_medallion_pipeline/DP01_Producao_Consumo/run_medallion_pipeline.py --skip-docker

# Reprocessamento incremental de intervalo
python 02_medallion_pipeline/DP01_Producao_Consumo/run_medallion_pipeline.py --skip-docker --date-from 2024-01-01 --date-to 2024-06-30

# Saltar DDL (tabelas já criadas) e quality gate
python 02_medallion_pipeline/DP01_Producao_Consumo/run_medallion_pipeline.py --skip-docker --skip-ddl --no-quality
```

### DP-01 Streaming — Produção/Consumo Fictício (Kafka/Redpanda)

```powershell
# Criar venv e iniciar produtor (publica + materializa automaticamente a cada 30 s)
cd 02_medallion_pipeline/DP01_Producao_Consumo/streaming/scripts/python
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements_streaming.txt
python fake_producao_consumo_producer.py --trino-host localhost --trino-port 8080 --interval-seconds 30
```

> Antes de executar, confirmar que o tópico `kafka.default.producao_consumo_events` existe no Trino (ver `streaming/README.md` → secção 0).

### DP-02 Static — Consumo vs Preço (OMIE CSV)

```powershell
python 02_medallion_pipeline/DP02_Consumo_Preco/Static_Data/run_medallion_consumo_precos.py --skip-docker
python 02_medallion_pipeline/DP02_Consumo_Preco/Static_Data/run_medallion_consumo_precos.py --skip-docker --skip-ddl
python 02_medallion_pipeline/DP02_Consumo_Preco/Static_Data/run_medallion_consumo_precos.py --skip-docker --skip-ddl --skip-flyte
```

### DP-02 Streaming — Consumo vs Preço (ENTSO-E API)

```powershell
# Padrão: últimos 12 meses, fonte Energy-Charts (sem token)
python 02_medallion_pipeline/DP02_Consumo_Preco/Streaming_Data/run_streaming_pipeline.py --skip-docker

# Período específico
python 02_medallion_pipeline/DP02_Consumo_Preco/Streaming_Data/run_streaming_pipeline.py --skip-docker --start 2024-01-01 --end 2024-12-31

# Com token ENTSO-E (acesso direto)
$env:ENTSOE_TOKEN = "<o-teu-token>"
python 02_medallion_pipeline/DP02_Consumo_Preco/Streaming_Data/run_streaming_pipeline.py --skip-docker --source entsoe

# Limpar e recarregar histórico completo
python 02_medallion_pipeline/DP02_Consumo_Preco/Streaming_Data/run_streaming_pipeline.py --skip-docker --clean --full

# Sem recriar tabelas
python 02_medallion_pipeline/DP02_Consumo_Preco/Streaming_Data/run_streaming_pipeline.py --skip-docker --skip-ddl
```

### DP-03 — Meteorologia + Produção

```powershell
# Fetch Open-Meteo desde 2023-01-01 até hoje (padrão)
python 02_medallion_pipeline/DP03_Meteo_Producao/run_medallion_meteo_producao.py --skip-docker

# Intervalo específico
python 02_medallion_pipeline/DP03_Meteo_Producao/run_medallion_meteo_producao.py --skip-docker --date-from 2024-01-01 --date-to 2024-12-31
```

### Flags Comuns

| Flag | Efeito |
|---|---|
| `--skip-docker` | Não faz `compose up` (stack já a correr) |
| `--skip-ddl` | Não aplica DDL (tabelas já existem) |
| `--no-quality` | Salta os quality gates finais |
| `--date-from DATE` | Data de início do fetch/reprocessamento |
| `--date-to DATE` | Data de fim do fetch/reprocessamento |
| `--skip-flyte` | Não verifica/inicia o Flyte Sandbox (DP-02 Static) |
| `--source {energycharts\|entsoe}` | Fonte de dados (DP-02 Streaming) |
| `--clean` | Apaga e recria tabelas antes de inserir (DP-02 Streaming) |
| `--full` | Carrega histórico completo desde 2022 (DP-02 Streaming) |

### Verificar Resultados no Trino

```sql
-- Registos Gold DP-01
SELECT COUNT(*), MIN(timestamp_utc), MAX(timestamp_utc)
FROM iceberg.gold.dp_energia_balance_hourly;

-- Registos Gold DP-02 Static
SELECT COUNT(*), MIN(ts_utc), MAX(ts_utc)
FROM iceberg.gold.dp_energy_market_hourly;

-- Registos Gold DP-02 Streaming
SELECT COUNT(*), MIN(ts_utc), MAX(ts_utc)
FROM iceberg.gold.dp_energy_market_api_hourly;

-- Últimas execuções do Streaming
SELECT run_id, status, duration_seconds, rows_gold, start_ts
FROM iceberg.audit.pipeline_runs
ORDER BY start_ts DESC LIMIT 10;

-- Registos Gold DP-03
SELECT COUNT(*), MIN(data_dia), MAX(data_dia)
FROM iceberg.gold.dp_meteo_producao_daily_features;
```
