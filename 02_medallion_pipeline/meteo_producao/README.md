# DP-03 — Meteo + Produção (`meteo_producao`)

Data Product que cruza dados meteorológicos horários (Open-Meteo) com produção eléctrica nacional (DP-01) e preço spot day-ahead (DP-02), produzindo uma **feature table diária** pronta para treino de modelos ML.

---

## Visão Geral

| Item | Detalhe |
|---|---|
| **Produto** | `iceberg.gold.dp_meteo_producao_daily_features` |
| **Grão** | 1 registo por dia UTC |
| **Chave** | `data_dia DATE` |
| **Fontes** | Open-Meteo API · DP-01 (produção/consumo) · DP-02 (preço spot) |
| **Consumidores** | ML pipeline (`meteo_producao_mlflow_flow.py`) · Dashboard Grafana |

---

## Estrutura de Pastas

```
meteo_producao/
├── run_medallion_meteo_producao.py   ← orquestrador principal
├── 01_bronze/
│   ├── sql/01_bronze_ddl.sql          ← DDL das tabelas Bronze (Hive + Iceberg)
│   ├── scripts/python/fetch_open_meteo.py  ← fetch da API Open-Meteo
│   ├── data/raw/                      ← CSV + Parquet gerados pelo fetch
│   └── data_quality_demo/
│       └── corrupt_bronze.py          ← ferramenta de demo de qualidade de dados
├── 02_silver/
│   └── sql/01_silver_trino.sql        ← deduplicação + validação + _quality_flag
├── 03_gold/
│   └── sql/01_gold_trino.sql          ← agregações diárias + lags + rolling 7d
└── 04_quality/
    ├── sql/01_silver_checks.sql       ← 13 checks Silver
    └── sql/02_gold_checks.sql         ← 12 checks Gold
```

---

## Como Executar

### Pipeline completo (primeira execução)

```powershell
python run_medallion_meteo_producao.py
```

### Stack já a correr (execuções seguintes)

```powershell
python run_medallion_meteo_producao.py --skip-docker
```

### Reprocessar Silver + Gold sem novo fetch

```powershell
python run_medallion_meteo_producao.py --skip-docker --skip-ddl
```

> `--skip-ddl` salta a aplicação do DDL mas **não** o fetch nem os INSERTs de Silver/Gold.

### Flags disponíveis

| Flag | Efeito |
|---|---|
| `--skip-docker` | Não faz `docker compose up` |
| `--skip-ddl` | Não aplica DDL Bronze/Silver/Gold |
| `--build` | Adiciona `--build` ao compose up |
| `--no-quality` | Salta o quality gate final |
| `--date-from YYYY-MM-DD` | Data de início do fetch (default: `2023-01-01`) |
| `--date-to YYYY-MM-DD` | Data de fim do fetch (default: hoje) |

---

## Arquitetura Medallion

```
Open-Meteo API (arquivo horário Portugal)
        │
        ▼ fetch_open_meteo.py
  [Bronze] hive.bronze_raw.meteo_open_meteo_raw        ← CSV (VARCHAR)
  [Bronze] hive.bronze_stage.meteo_open_meteo_clean    ← Parquet (tipos)
  [Bronze] iceberg.bronze.meteo_open_meteo_hourly      ← Iceberg gerido
        │
        ▼  01_silver_trino.sql
  [Silver] iceberg.silver.meteo_open_meteo_hourly
           · Deduplicação por ts_utc (ROW_NUMBER, mantém _ingested_at mais recente)
           · Validação de intervalos físicos para Portugal
           · Coluna _quality_flag: 'ok' | 'null_values' | 'out_of_range'
        │
        ▼  01_gold_trino.sql
  [Gold]   iceberg.gold.dp_meteo_producao_daily_features
           · Agregações diárias de meteo (AVG/MIN/MAX/SUM)
           · LEFT JOIN com DP-01 (produção/consumo) e DP-02 (preço spot)
           · Lag D-1 e rolling 7 dias para todas as features principais
           · Filtra WHERE _quality_flag = 'ok' (dados validados pela Silver)
```

---

## Tabela Gold — Colunas Principais

| Coluna | Tipo | Descrição |
|---|---|---|
| `data_dia` | DATE | Chave diária UTC |
| `temperature_mean/min/max_c` | DOUBLE | Temperatura do ar a 2 m (°C) |
| `precipitation_total_mm` | DOUBLE | Precipitação acumulada diária (mm) |
| `wind_speed_mean/max_ms` | DOUBLE | Velocidade do vento (m/s) |
| `radiation_mean_wm2` | DOUBLE | Radiação solar média (W/m²) |
| `cloud_cover_mean_pct` | DOUBLE | Nebulosidade média (%) |
| `producao_total_daily_mwh` | DOUBLE | **TARGET A** — produção eléctrica total (MWh) |
| `preco_spot_medio_eur_mwh` | DOUBLE | **TARGET B** — preço spot day-ahead médio (€/MWh) |
| `temp_lag_1d` … `preco_lag_1d` | DOUBLE | Features lag D-1 |
| `temp_rolling_7d_avg` … | DOUBLE | Médias móveis 7 dias |
| `dia_semana`, `is_weekend`, `estacao` | INT/BOOL/INT | Features temporais/sazonais |

---

## Quality Gates

O orquestrador corre automaticamente os quality gates após Silver e Gold.

### Silver — 13 checks

| ID | Check | Nível |
|---|---|---|
| S01 | `row_count > 0` | FAIL |
| S02 | `null_rate(ts_utc) = 0%` | FAIL |
| S03 | `null_rate(temperature_2m) = 0%` | FAIL |
| S04–S05 | `null_rate(radiation, wind)` | WARN |
| S06–S10 | Intervalos físicos: temp [-10,50]°C, precip [0,200]mm, vento [0,80]m/s, radiation≥0, cloud [0,100]% | FAIL |
| S11 | Unicidade `ts_utc` | FAIL |
| S12 | Alinhamento horário (`minute=0, second=0`) | FAIL |
| S13 | `_quality_flag = 'ok' >= 95%` | WARN |

### Gold — 12 checks

| ID | Check | Nível |
|---|---|---|
| G01 | `row_count > 0` | FAIL |
| G02–G05 | Nulos em `data_dia`, `temperature_mean_c`, `producao_total_daily_mwh`, `preco_spot` | FAIL/WARN |
| G06 | Unicidade `data_dia` | FAIL |
| G07–G10 | Intervalos: temp, produção≥0, estação{1-4}, dia_semana[0-6] | FAIL/WARN |
| G11 | Cobertura cross-DP (meteo+produção+preço) ≥ 90% | WARN |
| G12 | Freshness < 400 dias | WARN |

---

## Demo de Qualidade de Dados — `corrupt_bronze.py`

A pasta `01_bronze/data_quality_demo/` contém um script para **demonstração académica** do valor da camada Silver: injeta propositadamente dados inválidos na Bronze e mostra como o Silver os deteta e sinaliza.

### Fluxo da demo

```powershell
cd 01_bronze/data_quality_demo

# 1. Injetar dados sujos (5% das linhas, todos os tipos)
python corrupt_bronze.py --dp meteo --type all --pct 5

# 2. Reconstruir Silver a partir do Bronze corrompido
python corrupt_bronze.py --rerun-silver --dp meteo

# 3. Verificar quality flags no Trino
#    SELECT _quality_flag, COUNT(*)
#    FROM iceberg.silver.meteo_open_meteo_hourly GROUP BY 1;

# 4. Restaurar Bronze ao estado original
python corrupt_bronze.py --restore --dp meteo
```

### Tipos de corrupção disponíveis

| `--type` | O que injeta | Como Silver responde |
|---|---|---|
| `nulls` | `temperature_2m = NULL` | `_quality_flag = 'null_values'` |
| `outofrange` | `temp=-50°C, radiation=-200, cloud=150%` | `_quality_flag = 'out_of_range'` |
| `duplicates` | Cópias com `_ingested_at=1970` | Silver deduplica via `ROW_NUMBER` |
| `timestamps` | `ts_utc` com `minute=30` | Quality gate: FAIL em alinhamento temporal |
| `all` | Todos os tipos acima | Combinação dos efeitos |

> O script cria automaticamente um snapshot antes de corromper — `--restore` repõe o estado original a qualquer momento.

### Resultado esperado (5% de 26 000 linhas ≈ 1 300 linhas corrompidas)

```
_quality_flag   | linhas
----------------|--------
ok              | ~24700
null_values     | ~650
out_of_range    | ~650
```

---

## Resultado da Última Execução (2026-05-25)

```
Quality gate — Silver:  PASS: 13  WARN: 0  FAIL: 0  ✓
Quality gate — Gold:    PASS: 10  WARN: 2  FAIL: 0  ✓
  [WARN] 82 dias sem preço spot     (DP-02 não cobre todo o período)
  [WARN] 82 dias sem dados produção (DP-01 não cobre todo o período)
```
