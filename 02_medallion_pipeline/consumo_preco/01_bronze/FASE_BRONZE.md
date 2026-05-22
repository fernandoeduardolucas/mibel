# Fase Bronze — consumo_preco (DP-02)

## Objetivo

Ingestão fiel dos dados brutos de consumo elétrico nacional e preços day-ahead MIBEL para o lakehouse Iceberg. A camada Bronze preserva a fonte sem transformações de negócio, acrescentando apenas metadados de ingestão e particionamento por `process_date`.

---

## Fontes de Dados

### 1. Consumo Total Nacional — REN

| Atributo | Valor |
|---|---|
| **Origem** | Redes Energéticas Nacionais (REN) — download manual |
| **Ficheiro** | `data/raw/consumo-total-nacional.csv` |
| **Formato** | CSV, separador vírgula, encoding UTF-8 com BOM |
| **Granularidade** | 15 minutos |
| **Unidade** | kW (kilowatt) |
| **Período** | 2023-01-01 → 2026-03-11 |
| **Colunas originais** | `datahora, dia, mes, ano, date, time, bt, mt, at, mat, total` |

Desafios de qualidade identificados:
- Timestamps duplicados (mesma hora com valores distintos) → resolvidos por deduplicação na Silver
- Registos com `total = 0` em períodos de madrugada → sinalizados como `WARN`
- Valores nulos esporádicos em colunas de componentes (bt, mt, at, mat)

### 2. Preços Day-Ahead MIBEL — OMIE

| Atributo | Valor |
|---|---|
| **Origem** | OMIE — Operador del Mercado Ibérico de Energía |
| **Ficheiro** | `data/raw/Day-ahead Market Prices_20230101_20260311.csv` |
| **Formato** | CSV, separador ponto-e-vírgula (`;`), 2 linhas de cabeçalho |
| **Granularidade** | Horária (horas 1-25) |
| **Unidade** | €/MWh |
| **Período** | 2023-01-01 → 2026-03-11 |
| **Colunas originais** | `Date, Hour, Portugal, Spain` |

Desafios de qualidade identificados:
- Hora 25 nos dias de mudança DST de outono (clock back) → preservada no Bronze, filtrada na Silver
- Preços zero em primeiros dias de 2023 (dados em falta do OMIE) → sinalizados como `WARN`
- Preços negativos possíveis (mercado MIBEL aceita preços negativos) → sinalizados como `WARN`

---

## Tabelas Bronze (Iceberg)

### `iceberg.bronze.consumo_raw`

| Coluna | Tipo | Descrição |
|---|---|---|
| `datahora` | `TIMESTAMP(6) WITH TIME ZONE` | Timestamp original da fonte (UTC) |
| `dia` | `INTEGER` | Dia do mês (campo redundante da fonte) |
| `mes` | `INTEGER` | Mês (campo redundante da fonte) |
| `ano` | `INTEGER` | Ano (campo redundante da fonte) |
| `date_raw` | `VARCHAR` | Campo date original em string |
| `time_raw` | `VARCHAR` | Campo time original em string |
| `bt` | `DOUBLE` | Consumo BT em kW |
| `mt` | `DOUBLE` | Consumo MT em kW |
| `at` | `DOUBLE` | Consumo AT em kW |
| `mat` | `DOUBLE` | Consumo MAT em kW |
| `total` | `DOUBLE` | Consumo total nacional em kW |
| `process_date` | `DATE` | Data lógica de ingestão (**partição**) |

Particionamento: `['process_date']` — suporta idempotência e backfill diário.
Localização MinIO: `s3a://warehouse/bronze/consumo_raw/`

### `iceberg.bronze.preco_raw`

| Coluna | Tipo | Descrição |
|---|---|---|
| `date_raw` | `VARCHAR` | Data original da linha (string OMIE) |
| `hour` | `INTEGER` | Hora original OMIE (1-24 normal; 25 em DST outono) |
| `price_portugal_raw` | `DOUBLE` | Preço day-ahead Portugal em €/MWh |
| `price_spain_raw` | `DOUBLE` | Preço day-ahead Espanha em €/MWh |
| `process_date` | `DATE` | Data lógica de ingestão (**partição**) |

Particionamento: `['process_date']` — suporta idempotência e backfill diário.
Localização MinIO: `s3a://warehouse/bronze/preco_raw/`

---

## Ficheiros

| Ficheiro | Descrição |
|---|---|
| `bronze_consumo_precos_trino.sql` | DDL das duas tabelas Bronze (idempotente com `IF NOT EXISTS`) |
| `data/raw/consumo-total-nacional.csv` | CSV raw de consumo (REN) |
| `data/raw/Day-ahead Market Prices_20230101_20260311.csv` | CSV raw de preços (OMIE) |
| `scripts/python/bronze_clean_upload_consumo_precos.py` | Script standalone de limpeza/exploração e upload para MinIO |
| `scripts/python/requirements_bronze.txt` | Dependências do script standalone |

---

## Fluxo de Ingestão

```
CSVs raw (data/raw/)
    │
    ▼ [run script: upload_raw_csvs_to_minio()]
MinIO warehouse/raw/
    │
    ▼ [Flyte: flyte_ingest_bronze.py → ingest_bronze_full]
Leitura CSV do MinIO (boto3)
    │
    ├─ ingest_consumo_full()  → DELETE + INSERT em bronze.consumo_raw
    └─ ingest_preco_full()    → DELETE + INSERT em bronze.preco_raw
```

A ingestão é **idempotente**: `DELETE WHERE 1=1` antes de `INSERT` garante re-execuções seguras.

Os INSERTs são agrupados em batches de 5000 linhas e máx. 60 partições por statement para respeitar os limites do Trino/Iceberg.

---

## Critérios de Qualidade (Bronze)

Verificações executadas após ingestão (`04_quality/sql/01_bronze_checks.sql`):

| Check | Threshold | Ação |
|---|---|---|
| Nulos em `datahora` | 0% | FAIL |
| Nulos em `total` | 0% | FAIL |
| Nulos em `price_portugal_raw` | 0% | FAIL |
| `hour` entre 1 e 25 | 100% | FAIL |
| `total > 0` | 100% | WARN |
| `price_portugal_raw >= 0` | 100% | WARN |
| Unicidade `(datahora, process_date)` | 0 duplicados | WARN |
| Unicidade `(date_raw, hour, process_date)` | 0 duplicados | FAIL |
| Completude consumo ≥ 80 reg/dia | todos os dias | WARN |
| Completude preços ≥ 23 reg/dia | todos os dias | WARN |

---

## Decisões de Design

- **Preservação fiel**: o Bronze não altera unidades, não interpreta timestamps, não converte tipos — garante reprodutibilidade e auditabilidade da fonte.
- **Hora 25 preservada**: dias com mudança DST de outono têm 25 horas no CSV OMIE. O Bronze preserva esta hora extra; a Silver filtra-a para manter o modelo UTC.
- **Partição por `process_date`**: permite backfill eficiente e garante que re-ingestões de dias específicos não afetam outros períodos.
- **Fonte CSV vs API**: optou-se por CSV descarregado manualmente (REN e OMIE não disponibilizam API pública gratuita para dados históricos). O CSV OMIE cobre 2023-2026 com granularidade horária completa.
