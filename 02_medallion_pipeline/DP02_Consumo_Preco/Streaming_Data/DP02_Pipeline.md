# DP-02 Streaming_Data — Resumo do Pipeline Medallion

Pipeline de ingestão contínua de dados de consumo e preços de eletricidade para Portugal via
**ENTSO-E Transparency Platform** (transparency.entsoe.eu).

Coexiste com o pipeline estático (Static_Data): todas as tabelas têm sufixo `_api`.

---

## Arquitetura geral

```
ENTSO-E API
  ├── Actual Total Load PT  (query_load)
  └── Day-Ahead Prices PT+ES  (query_day_ahead_prices)
        │
        ▼
   BRONZE  — ingestão raw, 1 linha por hora
        │
        ▼  deduplicação · filtragem nulos · unidade MW→MWh
   SILVER  — dados limpos e normalizados
        │
        ▼  join · features calendário · lags · rolling averages
     GOLD  — produto analítico + feature table ML
```

Formato de armazenamento: **Apache Parquet** em MinIO (`s3a://warehouse/`), gerido por **Apache Iceberg** via **Trino**.

Orquestração: workflows **Flyte** em `workflows/`, executados localmente via `pyflyte run`.

---

## Bronze — Ingestão raw

### Fonte de dados

| Dado | Endpoint ENTSO-E | Unidade | Granularidade |
|------|-----------------|---------|---------------|
| Carga eléctrica nacional PT | `query_load('PT')` | MW | Horária |
| Preço day-ahead Portugal | `query_day_ahead_prices('PT')` | €/MWh | Horária |
| Preço day-ahead Espanha | `query_day_ahead_prices('ES')` | €/MWh | Horária |

**Autenticação:** variável de ambiente `ENTSOE_TOKEN` (token gratuito — email para transparency@entsoe.eu, assunto "RESTful API access", resposta em ~3 dias úteis).

### Tabelas criadas

#### `iceberg.bronze.consumo_api_raw`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ts_utc` | TIMESTAMP WITH TIME ZONE | Timestamp UTC do início da hora |
| `total` | DOUBLE | Carga total nacional em MW |
| `source_url` | VARCHAR | URL da chamada à API (rastreabilidade) |
| `fetch_date` | DATE | Data em que a chamada foi feita |
| `process_date` | DATE | Data lógica de ingestão (coluna de partição) |

Particionada por `process_date`.

#### `iceberg.bronze.preco_api_raw`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ts_utc` | TIMESTAMP WITH TIME ZONE | Timestamp UTC do início da hora |
| `price_portugal_eur_mwh` | DOUBLE | Preço day-ahead Portugal em €/MWh |
| `price_spain_eur_mwh` | DOUBLE | Preço day-ahead Espanha em €/MWh |
| `source_url` | VARCHAR | URL da chamada à API |
| `fetch_date` | DATE | Data da chamada |
| `process_date` | DATE | Data lógica de ingestão (coluna de partição) |

Particionada por `process_date`.

### Workflow Flyte

**Ficheiro:** `workflows/flyte_fetch_bronze_api.py`  
**Workflow:** `fetch_bronze_api`

As duas tarefas (`fetch_consumo_api` e `fetch_preco_api`) correm **em paralelo**.
Cada tarefa é **idempotente**: apaga as partições `process_date` do intervalo antes de inserir.
Para intervalos superiores a 180 dias, o orquestrador divide automaticamente em chunks anuais para evitar timeouts.

```
fetch_bronze_api
  ├── fetch_consumo_api  →  consumo_api_raw
  └── fetch_preco_api    →  preco_api_raw
```

### Quality gate Bronze (10 checks)

| Check | Critério |
|-------|---------|
| Nulos em `ts_utc` | 0 nulos → PASS |
| Nulos em `total` | 0 nulos → PASS |
| Nulos em `price_portugal_eur_mwh` | 0 nulos → PASS |
| Range `total > 0` | MW positivo → PASS; negativo → WARN |
| Preço PT não-negativo | Negativo é WARN (preços negativos possíveis no MIBEL) |
| Unicidade `ts_utc` consumo | Sem duplicados → PASS |
| Unicidade `ts_utc` preço | Sem duplicados → PASS |
| Freshness consumo | Máx. 3 dias de atraso |
| Freshness preço | Máx. 2 dias de atraso (day-ahead publica D-1) |
| Completude diária | Dias com < 23 horas → WARN |

---

## Silver — Normalização e limpeza

### Transformações aplicadas

#### Consumo (`bronze.consumo_api_raw` → `silver.consumo_api_hourly`)

1. **Filtragem de nulos** — exclui linhas com `ts_utc IS NULL`, `total IS NULL` ou `total <= 0`
2. **Deduplicação** — `GROUP BY ts_utc` com `AVG(total)` (resolve duplicados ocasionais da API)
3. **Conversão de unidade** — MW × 1h = MWh (dado que a granularidade já é horária, `total_mwh = ROUND(AVG(total), 3)`)
4. **Derivação de partição** — colunas `year` e `month` extraídas de `ts_utc`

#### Preços (`bronze.preco_api_raw` → `silver.preco_api_hourly`)

1. **Filtragem de nulos** — exclui linhas com `ts_utc IS NULL` ou `price_portugal_eur_mwh IS NULL`
2. **Alinhamento horário** — `DATE_TRUNC('hour', ts_utc)` (garante granularidade horária exacta)
3. **Deduplicação** — `GROUP BY DATE_TRUNC('hour', ts_utc)` com `ROUND(AVG(...), 2)` para PT e ES
4. **Derivação de partição** — `year` e `month`

### Tabelas criadas

#### `iceberg.silver.consumo_api_hourly`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ts_utc` | TIMESTAMP WITH TIME ZONE | Timestamp UTC canónico |
| `total_mwh` | DOUBLE | Carga horária em MWh (arredondado a 3 casas) |
| `year` | INTEGER | Ano — partição |
| `month` | INTEGER | Mês — partição |

#### `iceberg.silver.preco_api_hourly`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ts_utc` | TIMESTAMP WITH TIME ZONE | Timestamp UTC canónico |
| `price_portugal_eur_mwh` | DOUBLE | Preço day-ahead PT em €/MWh (arredondado a 2 casas) |
| `price_spain_eur_mwh` | DOUBLE | Preço day-ahead ES em €/MWh (arredondado a 2 casas) |
| `year` | INTEGER | Ano — partição |
| `month` | INTEGER | Mês — partição |

Ambas as tabelas particionadas por `year / month`.

### Workflow Flyte

**Ficheiro:** `workflows/flyte_bronze_to_silver.py`

| Workflow | Descrição |
|----------|-----------|
| `bronze_to_silver_api` | Transforma um `process_date` específico |
| `bronze_to_silver_api_full` | Materializa todo o histórico Bronze |

As duas tarefas (consumo + preço) correm em paralelo. Ambas são idempotentes (apagam o intervalo antes de inserir).

---

## Gold — Enriquecimento analítico e ML

### Transformações aplicadas

O Gold constrói dois produtos a partir de um **INNER JOIN** entre Silver consumo e Silver preço por `ts_utc`.

#### Produto analítico: `dp_energy_market_api_hourly`

1. **Join** — `silver.consumo_api_hourly INNER JOIN silver.preco_api_hourly ON ts_utc`
   (apenas horas com consumo **e** preço disponíveis são incluídas)
2. **Features de calendário:**
   - `hora` — hora do dia (0–23), extraída de `ts_utc`
   - `dia_semana` — dia da semana (0 = Segunda … 6 = Domingo)
   - `is_weekend` — `TRUE` para Sábado e Domingo
3. **Lags temporais** (window functions sobre a série ordenada por `ts_utc`):
   - `consumo_lag_1h` — consumo 1 hora antes
   - `consumo_lag_24h` — consumo 24 horas antes (mesmo período do dia anterior)
   - `price_lag_1h` — preço 1 hora antes
4. **Médias móveis** (janela de 24 linhas anteriores + linha atual):
   - `rolling_avg_consumo_24h` — média de consumo das últimas 24 horas
   - `rolling_avg_price_24h` — média de preço das últimas 24 horas

#### Feature table ML: `feat_load_forecasting_api_hourly`

Derivada de `dp_energy_market_api_hourly` com adição de:

5. **Variável alvo:** `consumo_next_hour = LEAD(consumo_total, 1)` — consumo da hora seguinte
6. **Filtragem de exemplos incompletos:**
   - Remove a última linha da série (sem target futuro)
   - Remove as primeiras 24 linhas (lags ainda nulos)

### Tabelas criadas

#### `iceberg.gold.dp_energy_market_api_hourly`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ts_utc` | TIMESTAMP WITH TIME ZONE | Chave temporal primária |
| `consumo_total` | DOUBLE | Carga horária em MWh |
| `market_price_pt` | DOUBLE | Preço day-ahead PT em €/MWh |
| `hora` | INTEGER | Hora do dia (0–23) |
| `dia_semana` | INTEGER | Dia da semana (0=Segunda … 6=Domingo) |
| `is_weekend` | BOOLEAN | Sábado ou Domingo |
| `consumo_lag_1h` | DOUBLE | Consumo 1h antes (NULL nas primeiras horas) |
| `consumo_lag_24h` | DOUBLE | Consumo 24h antes (NULL nas primeiras 24h) |
| `price_lag_1h` | DOUBLE | Preço 1h antes (NULL na primeira hora) |
| `rolling_avg_consumo_24h` | DOUBLE | Média móvel 24h de consumo |
| `rolling_avg_price_24h` | DOUBLE | Média móvel 24h de preço |
| `process_date` | DATE | Data de execução do pipeline |
| `year` | INTEGER | Ano — partição |
| `month` | INTEGER | Mês — partição |

#### `iceberg.gold.feat_load_forecasting_api_hourly`

Todas as colunas de `dp_energy_market_api_hourly`, mais:

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `consumo_next_hour` | DOUBLE | Consumo da hora seguinte — **variável alvo ML** |

Última linha e linhas com lags nulos são excluídas → dataset pronto para treino.

### Workflow Flyte

**Ficheiro:** `workflows/flyte_silver_to_gold.py`  
**Workflow:** `silver_to_gold_api_full`

As window functions operam sobre o **histórico completo** para garantir lags e médias móveis corretos nas fronteiras de data. O workflow materializa sempre o histórico total de forma idempotente.

```
silver_to_gold_api_full
  ├── build_dp_energy_market_api_full      →  dp_energy_market_api_hourly
  └── build_feat_load_forecasting_api_full →  feat_load_forecasting_api_hourly
        (depende do upstream — executa depois)
```

---

## Como executar

### Pré-requisito

```powershell
$env:ENTSOE_TOKEN = "<o-teu-token>"
```

### Comando completo (últimos 7 dias)

```powershell
python run_streaming_pipeline.py --skip-docker --days 7
```

### Outros exemplos

```powershell
# Período específico
python run_streaming_pipeline.py --skip-docker --start 2024-01-01 --end 2024-12-31

# Histórico completo (~2022 até hoje)
python run_streaming_pipeline.py --skip-docker --full

# Apenas hoje, sem quality gates
python run_streaming_pipeline.py --skip-docker --today --no-quality

# Sem recriar tabelas (DDL já aplicado) e sem quality gates
python run_streaming_pipeline.py --skip-docker --skip-ddl --no-quality --days 7
```

---

## Scripts standalone (debug / ingestão manual)

Os dois scripts abaixo permitem ingerir dados diretamente via ENTSO-E sem correr o pipeline completo. Úteis para verificar a API, testar o token ou repopular um intervalo pontual.

| Script | Descrição |
|--------|-----------|
| `01_bronze/scripts/python/fetch_consumo_entsoe.py` | Carga eléctrica nacional PT (Actual Total Load) |
| `01_bronze/scripts/python/fetch_preco_entsoe.py` | Preços day-ahead PT + ES (Day-Ahead Prices) |

```powershell
$env:ENTSOE_TOKEN = "<o-teu-token>"

# Consumo — últimos 7 dias
python 01_bronze/scripts/python/fetch_consumo_entsoe.py --days 7

# Preços — intervalo específico
python 01_bronze/scripts/python/fetch_preco_entsoe.py --start 2024-01-01 --end 2024-01-31
```

Dependências: `entsoe-py`, `pandas` (instalar via `pip install entsoe-py pandas`).

---

## Documentação técnica

| Ficheiro | Conteúdo |
| --- | --- |
| [`.env.example`](.env.example) | **Template de variáveis de ambiente** — copia para `.env` e preenche o `ENTSOE_TOKEN` |
| [`docs/product.yaml`](docs/product.yaml) | **Data Product** — entrega, produção, lineage, observabilidade, governança |
| [`docs/contract.yaml`](docs/contract.yaml) | **Data Contract** — schema formal, semântica, qualidade, SLAs/SLOs, exemplos SQL |
