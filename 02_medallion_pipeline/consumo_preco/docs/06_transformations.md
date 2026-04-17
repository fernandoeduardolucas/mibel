# 1. Objetivo

Este documento define as regras de transformação entre as camadas Bronze, Silver e Gold do lakehouse, detalhando os passos necessários para garantir consistência temporal, qualidade de dados e preparação para consumo analítico e machine learning.

---

# 2. Bronze → Silver (Consumo)

## Origem
- `bronze.consumo_raw`

## Destino
- `silver.consumo_hourly`

## Passos de transformação

### 1. Parsing do timestamp
- converter `datahora_raw` para tipo TIMESTAMP
- validar formato e consistência

### 2. Normalização temporal
- garantir que o timestamp está corretamente interpretado
- definir como `timestamp_utc`

### 3. Agregação temporal
- agrupar por hora (`timestamp_utc` truncado à hora)
- calcular:
  - `consumo_total = SUM(total)`

### 4. Derivação de colunas de calendário
- `year`, `month`, `day`, `hour`

### 5. Colunas de controlo
- `source_min_ts = MIN(timestamp_origem)`
- `source_max_ts = MAX(timestamp_origem)`
- `source_rows = COUNT(*)`

### 6. Metadados técnicos
- adicionar `process_date`

---

## Regras de qualidade

- cada hora deve ter aproximadamente 4 registos de origem (15 min)
- `consumo_total >= 0`
- ausência de timestamps duplicados após agregação
- identificação de horas incompletas

---

# 3. Bronze → Silver (Preço)

## Origem
- `bronze.preco_raw`

## Destino
- `silver.preco_hourly`

## Passos de transformação

### 1. Limpeza inicial
- ignorar linhas inválidas ou metadata não tabular
- garantir que `date_raw` e `hour_raw` são válidos

### 2. Parsing da data
- converter `date_raw` para DATE

### 3. Interpretação da hora
- `hour_raw` representa hora no intervalo:
  - normal: 1–24
  - casos especiais: 25 (mudança DST)

### 4. Construção do timestamp
- `timestamp_local = date_raw + (hour_raw - 1) horas`
- tratar casos especiais:
  - hora 25 → duplicação controlada ou ajuste
- converter para `timestamp_utc`

### 5. Seleção de métricas
- `market_price_pt = portugal_price`

### 6. Derivação de colunas de calendário
- `year`, `month`, `day`, `hour`

### 7. Metadados técnicos
- `process_date`

---

## Regras de qualidade

- `timestamp_utc` único
- ausência de valores nulos em `market_price_pt`
- valores dentro de intervalo plausível
- coerência entre data/hora original e timestamp final

---

# 4. Silver → Gold (Produto Analítico)

## Origem
- `silver.consumo_hourly`
- `silver.preco_hourly`

## Destino
- `gold.dp_energy_market_hourly`

## Passos de transformação

### 1. Join temporal
- join INNER por `timestamp_utc`

### 2. Validação do join
- garantir correspondência 1:1
- identificar e analisar perdas de registos

### 3. Derivação de colunas de calendário
- `date`
- `day_of_week`
- `is_weekend`

### 4. Cálculo de lags
- ordenar por `timestamp_utc`
- calcular:
  - `consumo_lag_1h`
  - `consumo_lag_24h`
  - `price_lag_1h`

### 5. Cálculo de rolling averages
- janelas móveis de 24h:
  - `rolling_avg_consumo_24h`
  - `rolling_avg_price_24h`

### 6. Metadados técnicos
- adicionar `process_date`

---

## Regras de qualidade

- ausência de duplicações após join
- correspondência temporal completa entre datasets
- validação de valores nulos nas métricas principais
- coerência dos valores derivados

---

# 5. Gold → Gold (Feature Table ML)

## Origem
- `gold.dp_energy_market_hourly`

## Destino
- `gold.feat_load_forecasting_hourly`

## Passos de transformação

### 1. Seleção de colunas
- selecionar apenas features relevantes

### 2. Criação do target
- `consumo_next_hour = consumo_total` deslocado -1h

### 3. Filtragem
- remover:
  - última linha (sem target)
  - linhas com nulos nas features

### 4. Ordenação
- ordenar por `timestamp_utc`

### 5. Metadados técnicos
- manter `process_date`

---

## Regras de qualidade

- ausência de nulos nas features finais
- target consistente com série temporal
- correspondência correta entre features e target
- dataset pronto para treino sem pré-processamento adicional

---

# 6. Fluxo global

## Sequência de execução

1. Ingestão → Bronze
2. Bronze → Silver (consumo)
3. Bronze → Silver (preço)
4. Silver → Gold (produto analítico)
5. Gold → Gold (feature table)

---

# 7. Princípios de transformação

- transformar o mínimo necessário em cada camada
- garantir consistência temporal antes de qualquer join
- separar claramente limpeza, integração e consumo
- evitar lógica de negócio na camada Bronze
- manter reprodutibilidade e determinismo das transformações