# 1. Objetivo do Lakehouse

O lakehouse do projeto organiza os dados em três camadas — Bronze, Silver e Gold — com o objetivo de garantir rastreabilidade da origem, normalização progressiva dos dados e disponibilização de produtos de dados prontos a consumir.

A arquitetura segue uma abordagem medallion:
- **Bronze**: preservação da origem com transformação mínima
- **Silver**: limpeza, normalização e harmonização temporal
- **Gold**: data products prontos para análise, serving e machine learning

O projeto adota **UTC como tempo canónico** nas camadas Silver e Gold.

---

# 2. Schemas / Camadas

## Bronze
Schema destinado à ingestão dos dados de origem com preservação máxima da estrutura original.

### Tabelas
- `bronze.consumo_raw`
- `bronze.preco_raw`

---

## Silver
Schema destinado à limpeza, tipagem, harmonização temporal e preparação intermédia dos datasets.

### Tabelas
- `silver.consumo_hourly`
- `silver.preco_hourly`

---

## Gold
Schema destinado aos produtos finais prontos para consumo analítico e machine learning.

### Tabelas
- `gold.dp_energy_market_hourly`
- `gold.feat_load_forecasting_hourly`

---

# 3. Desenho físico por tabela

## 3.1 Bronze

### `bronze.consumo_raw`
**Origem:** `consumo-total-nacional.csv` (fonte REN)

**Função:**
Preservar os dados originais de consumo elétrico nacional com metadados mínimos de ingestão, sem aplicar regras de negócio ou limpeza semântica.

**Colunas principais:** `datahora`, `total`, `process_date` + campos redundantes da fonte (`dia`, `mes`, `ano`, `date_raw`, `time_raw`, `bt`, `mt`, `at`, `mat`)

**Observações:**
- Mantém colunas redundantes da fonte para rastreabilidade completa
- Não agrega 15 minutos para 1 hora nesta camada
- `datahora` é ingerido como TIMESTAMP WITH TIME ZONE (já interpretado como UTC)

---

### `bronze.preco_raw`
**Origem:** `Day-ahead Market Prices_*.csv` (fonte OMIE)

**Função:**
Preservar os dados originais de preços MIBEL. A interpretação semântica da coluna `hour` (numeração OMIE 1–25) só é feita em Silver.

**Colunas principais:** `date_raw`, `hour`, `price_portugal_raw`, `price_spain_raw`, `process_date`

**Observações:**
- `price_spain_raw` é preservada mesmo não sendo usada no data product final
- `hour = 25` (DST) é mantida em Bronze e descartada em Silver

---

## 3.2 Silver

### `silver.consumo_hourly`
**Origem upstream:** `bronze.consumo_raw`

**Colunas:** `ts_utc`, `total_mwh`, `year`, `month`

**Transformações principais:**
- Truncagem de `datahora` à hora → `ts_utc`
- `SUM(total) / 1000` → `total_mwh` (kW × 4 intervalos → MWh)

---

### `silver.preco_hourly`
**Origem upstream:** `bronze.preco_raw`

**Colunas:** `ts_utc`, `price_portugal_eur_mwh`, `price_spain_eur_mwh`, `year`, `month`

**Transformações principais:**
- `CAST(date_raw AS DATE) + INTERVAL (hour - 1) HOURS` → `ts_utc`
- Descarte de linhas com `hour = 25`

---

## 3.3 Gold

### `gold.dp_energy_market_hourly`
**Colunas:** `ts_utc`, `consumo_total`, `market_price_pt`, `hora`, `dia_semana`, `is_weekend`, `consumo_lag_1h`, `consumo_lag_24h`, `price_lag_1h`, `rolling_avg_consumo_24h`, `rolling_avg_price_24h`, `process_date`, `year`, `month`

**Consumidores:** dashboard, API, analistas, base para ML

---

### `gold.feat_load_forecasting_hourly`
**Colunas:** todas as do dp + `consumo_next_hour` (TARGET)

**Consumidores:** workflow de treino ML, MLflow

---

# 4. Estratégia de particionamento

## Bronze
- **Coluna:** `process_date` (DATE)
- **Justificação:** facilita rastreabilidade da ingestão diária, suporta backfill e reprocessamento idempotente por dia

## Silver
- **Colunas:** `year`, `month`
- **Justificação:** adequado para datasets horários com consultas por período temporal; mantém granularidade de partição razoável (~720 linhas/partição)

## Gold
- **Colunas:** `year`, `month`
- **Justificação:** consumidores analíticos e ML filtram naturalmente por intervalos temporais; evita excesso de ficheiros pequenos

---

# 5. Formato e compaction

## Formato de armazenamento
- **Formato:** Parquet (colunar, compressão eficiente para leituras analíticas)
- **Versão Iceberg:** format_version = 2 (suporte a row-level deletes, merge-on-read)
- **Layout:** `object_store_layout_enabled = true` (distribui ficheiros para evitar hot spots em object storage)

## Estratégia de compaction

O projeto usa DELETE + INSERT por partição (`year`/`month` em Silver/Gold, `process_date` em Bronze) como mecanismo de idempotência. Este padrão produz ficheiros Parquet bem dimensionados por ciclo de execução.

Em caso de acumulação de ficheiros pequenos (ex: reprocessamentos parciais frequentes), recomenda-se:

```sql
-- Compaction manual via Trino (Iceberg OPTIMIZE)
ALTER TABLE iceberg.gold.dp_energy_market_hourly
    EXECUTE optimize
    WHERE year = 2024 AND month = 1;
```

O comando `OPTIMIZE` reescreve os ficheiros da partição num número menor de ficheiros maiores, melhorando a performance de leitura.

**Política de compaction:**
- Não é necessária compaction automática no âmbito atual do projeto (volume reduzido)
- Em produção com ingestão incremental diária, recomenda-se compaction mensal após fecho de cada partição `year`/`month`

---

# 6. Catálogo e metadados (Iceberg table properties)

Todas as tabelas do projeto têm propriedades Iceberg (`extra_properties`) que funcionam como catálogo mínimo embutido. Estas propriedades são consultáveis via Trino:

```sql
SELECT * FROM iceberg."$properties"
WHERE table_name = 'dp_energy_market_hourly';
```

### Propriedades por camada

**Bronze:**

| Propriedade      | Descrição |
|------------------|-----------|
| `layer`          | `bronze` — identificação da camada |
| `domain`         | `consumo_preco` — domínio do projeto |
| `schema_version` | Versão do schema da tabela |
| `retention_policy` | Política de retenção (`indefinite`) |
| `source_system`  | Sistema de origem (`ren_csv`, `omie_csv`) |

**Silver:**

| Propriedade      | Descrição |
|------------------|-----------|
| `layer`          | `silver` |
| `domain`         | `consumo_preco` |
| `schema_version` | Versão do schema |
| `grain`          | Granularidade (`hourly`) |
| `upstream_table` | Tabela Bronze de origem |

**Gold:**

| Propriedade             | Descrição |
|-------------------------|-----------|
| `layer`                 | `gold` |
| `data_product`          | Nome do data product |
| `schema_version`        | Versão do schema da tabela |
| `product_version`       | Versão do produto (`v1`) |
| `deprecated`            | `false` — produto ativo |
| `domain`                | `consumo_preco` |
| `grain`                 | `hourly` |
| `feature_schema_version`| (apenas feat table) versão do conjunto de features |
| `upstream_table`        | (apenas feat table) tabela Gold de origem |

### Comentários de tabela e coluna

Todas as tabelas e colunas têm comentários (`COMMENT ON TABLE / COMMENT ON COLUMN`) que descrevem o seu propósito e origem. Estes comentários são visíveis no Trino:

```sql
SHOW CREATE TABLE iceberg.gold.dp_energy_market_hourly;
DESCRIBE iceberg.gold.dp_energy_market_hourly;
```

---

# 7. Convenções de naming

## Schemas
- `bronze`, `silver`, `gold` — nomes curtos e sem ambiguidade

## Tabelas
- `snake_case` em todas as tabelas
- Prefixo semântico: `dp_` (data product analítico), `feat_` (feature table ML), sem prefixo em Bronze/Silver
- Exemplos: `consumo_raw`, `preco_raw`, `consumo_hourly`, `preco_hourly`, `dp_energy_market_hourly`, `feat_load_forecasting_hourly`

## Colunas
- `snake_case` em todas as colunas
- Chave temporal canónica: `ts_utc`
- Partições: `year`, `month`, `process_date`
- Features temporais em português: `hora`, `dia_semana`, `is_weekend`
- Métricas com unidade no nome quando relevante: `total_mwh`, `price_portugal_eur_mwh`

## Localização MinIO
```
s3a://warehouse/
  bronze/consumo_raw/
  bronze/preco_raw/
  silver/consumo_hourly/
  silver/preco_hourly/
  gold/dp_energy_market_hourly/
  gold/feat_load_forecasting_hourly/
```

---

# 8. Princípios de desenho adotados

- Preservar a origem em Bronze sem transformações semânticas
- Limpar e harmonizar em Silver (UTC canónico, granularidade horária)
- Expor consumo analítico em Gold (features, lags, rolling averages)
- Separar claramente dados operacionais, intermédios e finais
- Usar Iceberg format_version=2 em todas as tabelas (suporte a evolução de schema e row-level deletes)
- Garantir idempotência em todos os workflows via DELETE + INSERT por partição
- Documentar intenção via Iceberg table properties e comentários de coluna

---

# 9. Resultado esperado do desenho

No final da implementação, o lakehouse permite:
- Reconstituir a origem dos dados (Bronze preserva raw)
- Integrar consumo e preço de forma temporalmente consistente (Silver UTC)
- Servir um produto analítico horário (Gold dp)
- Alimentar um workflow de machine learning reprodutível (Gold feat)
- Identificar a versão e estado de cada tabela via Iceberg table properties
