# 1. Objetivo

Este documento define as regras de transformação entre as camadas Bronze, Silver e Gold do lakehouse, detalhando os passos necessários para garantir consistência temporal, qualidade de dados e preparação para consumo analítico e machine learning.

Todos os nomes de colunas referenciados neste documento correspondem ao DDL implementado nas tabelas Iceberg.

---

# 2. Bronze → Silver (Consumo)

## Origem
`iceberg.bronze.consumo_raw`

## Destino
`iceberg.silver.consumo_hourly`

## Passos de transformação

### 1. Truncagem temporal
- Truncar `datahora` (TIMESTAMP WITH TIME ZONE) à hora: `DATE_TRUNC('hour', datahora)`
- O resultado torna-se `ts_utc` em Silver — chave horária canónica em UTC

### 2. Agregação temporal
- Agrupar por `ts_utc` (hora truncada)
- `total_mwh = SUM(total) / 1000`
  - `total` em Bronze está em kW (intervalos de 15 min)
  - Dividir por 1000 converte para MWh

### 3. Derivação de colunas de partição
- `year = YEAR(ts_utc)`
- `month = MONTH(ts_utc)`

### 4. Idempotência
- DELETE das linhas da partição `year`/`month` antes de INSERT
- Garante reprocessamento seguro

## Regras de qualidade pós-transformação
- Cada hora deve ter ~4 registos de origem (intervalo 15 min)
- `total_mwh >= 0`
- `ts_utc` único após agregação
- Sem nulos em `ts_utc` ou `total_mwh`

## Colunas descartadas
`dia`, `mes`, `ano`, `date_raw`, `time_raw`, `bt`, `mt`, `at`, `mat`, `process_date`

---

# 3. Bronze → Silver (Preço)

## Origem
`iceberg.bronze.preco_raw`

## Destino
`iceberg.silver.preco_hourly`

## Passos de transformação

### 1. Filtragem de horas especiais DST
- Excluir linhas com `hour = 25` (hora extra em mudança DST de outono — sem correspondência UTC válida)

### 2. Construção do timestamp UTC
- `ts_utc = CAST(date_raw AS DATE) + INTERVAL (hour - 1) HOURS`
- A numeração OMIE começa em 1 (hora 1 = 00:00 UTC), por isso subtrai-se 1

### 3. Seleção de métricas
- `price_portugal_eur_mwh = price_portugal_raw`
- `price_spain_eur_mwh = price_spain_raw` (mantido para análise comparativa)

### 4. Derivação de colunas de partição
- `year = YEAR(ts_utc)`
- `month = MONTH(ts_utc)`

### 5. Idempotência
- DELETE das linhas da partição `year`/`month` antes de INSERT

## Regras de qualidade pós-transformação
- `ts_utc` único e no limite da hora (minuto=0, segundo=0)
- `price_portugal_eur_mwh` NOT NULL
- Coerência entre `date_raw`/`hour` Bronze e `ts_utc` Silver

## Colunas descartadas
`process_date` (partição passa a ser por `year`/`month`)

---

# 4. Silver → Gold (Produto Analítico)

## Origem
- `iceberg.silver.consumo_hourly`
- `iceberg.silver.preco_hourly`

## Destino
`iceberg.gold.dp_energy_market_hourly`

## Passos de transformação

### 1. Join temporal
- `INNER JOIN silver.consumo_hourly c ON c.ts_utc = p.ts_utc`
- Apenas horas com dados em ambas as tabelas são incluídas

### 2. Derivação de colunas de calendário
- `hora = HOUR(ts_utc)`  (0–23)
- `dia_semana = DAY_OF_WEEK(ts_utc) - 1`  (0=Segunda … 6=Domingo)
- `is_weekend = (dia_semana >= 5)`

### 3. Cálculo de lags (window functions sobre Silver completo)
- As window functions são calculadas sobre o histórico Silver completo (sem filtro de data) para garantir valores corretos nas fronteiras de partição:
  ```sql
  LAG(consumo_total, 1)  OVER (ORDER BY ts_utc) AS consumo_lag_1h
  LAG(consumo_total, 24) OVER (ORDER BY ts_utc) AS consumo_lag_24h
  LAG(market_price_pt, 1) OVER (ORDER BY ts_utc) AS price_lag_1h
  ```

### 4. Cálculo de rolling averages (window functions sobre Silver completo)
  ```sql
  AVG(consumo_total)   OVER (ORDER BY ts_utc ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS rolling_avg_consumo_24h
  AVG(market_price_pt) OVER (ORDER BY ts_utc ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS rolling_avg_price_24h
  ```

### 5. Metadados técnicos
- `process_date = CURRENT_DATE`
- `year = YEAR(ts_utc)`, `month = MONTH(ts_utc)` (partição)

### 6. Idempotência
- DELETE por `year`/`month` antes de INSERT

## Renomeações Silver → Gold

| Silver                        | Gold               |
|-------------------------------|--------------------|
| `total_mwh`                   | `consumo_total`    |
| `price_portugal_eur_mwh`      | `market_price_pt`  |

## Regras de qualidade pós-transformação
- `ts_utc` NOT NULL e único
- `consumo_total` e `market_price_pt` NOT NULL
- `hora` em [0, 23], `dia_semana` em [0, 6]
- Nulos em lags/rolling são válidos nas primeiras observações da série

---

# 5. Gold → Gold (Feature Table ML)

## Origem
`iceberg.gold.dp_energy_market_hourly`

## Destino
`iceberg.gold.feat_load_forecasting_hourly`

## Passos de transformação

### 1. Derivação do target
```sql
LEAD(consumo_total, 1) OVER (ORDER BY ts_utc) AS consumo_next_hour
```

### 2. Filtragem de registos inválidos
- Remover linhas com `consumo_next_hour IS NULL` (última hora de cada partição)
- Remover linhas com nulos em qualquer feature de lag/rolling (primeiras 24h da série)

### 3. Seleção de colunas
- Todas as colunas do `dp_energy_market_hourly` mais `consumo_next_hour`
- Colunas excluídas: nenhuma (a feature table é um superset do dp com o target adicionado)

### 4. Ordenação
- Ordenar por `ts_utc` crescente

### 5. Idempotência
- DELETE por `year`/`month` antes de INSERT

## Regras de qualidade pós-transformação
- `consumo_next_hour` NOT NULL em todos os registos
- Ausência de nulos em todas as 11 features
- `ts_utc` único
- Paridade com `dp_energy_market_hourly` com diferença ≤ 48 linhas

---

# 6. Fluxo global

```
CSV REN (consumo 15min)  ──┐
                            ├─[bronze_clean_upload]──► bronze.consumo_raw
CSV OMIE (preços horários) ─┘                                │
                                                             │
bronze.preco_raw ◄──────────────────────────────────────────┘
        │
        ├─[flyte_bronze_to_silver]─► silver.consumo_hourly (ts_utc, total_mwh)
        │                             silver.preco_hourly  (ts_utc, price_portugal_eur_mwh, price_spain_eur_mwh)
        │
        ├─[quality_gate_silver]────► checks: null, unique, boundary, join coverage
        │
        ├─[flyte_silver_to_gold]──► gold.dp_energy_market_hourly
        │                             (ts_utc, consumo_total, market_price_pt, hora, dia_semana, ...)
        │                            gold.feat_load_forecasting_hourly
        │                             (+ consumo_next_hour)
        │
        └─[quality_gate_gold]─────► checks: null, unique, lag consistency, row parity
```

---

# 7. Princípios de transformação

- Transformar o mínimo necessário em cada camada
- Garantir consistência temporal (UTC canónico) antes de qualquer join
- Separar claramente limpeza (Silver), integração e feature engineering (Gold)
- Evitar lógica de negócio na camada Bronze
- Calcular window functions sobre histórico completo para evitar erros nas fronteiras de partição
- Garantir idempotência: DELETE + INSERT por partição natural de cada camada
- Manter reprodutibilidade: o mesmo `process_date` deve produzir o mesmo resultado
