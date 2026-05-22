# 1. Objetivo

Este documento define o schema técnico real (alinhado com o DDL implementado) das tabelas Gold do projeto, correspondentes ao produto analítico principal e à feature table utilizada no workflow de machine learning.

A camada Gold disponibiliza dados prontos a consumir por dashboards, API, análise exploratória e treino de modelos, com granularidade horária e tempo canónico em UTC.

---

# 2. Schema Gold

## 2.1 `gold.dp_energy_market_hourly`

**Tabela Iceberg:** `iceberg.gold.dp_energy_market_hourly`  
**Localização MinIO:** `s3a://warehouse/gold/dp_energy_market_hourly/`  
**Origem upstream:** `silver.consumo_hourly` + `silver.preco_hourly` (INNER JOIN por `ts_utc`)  
**Granularidade:** horária  
**Formato:** Parquet (Iceberg format_version=2)  
**Particionamento:** `year`, `month`  
**Função:** Integrar consumo e preço numa única tabela analítica horária com features temporais e de lag.

### Colunas

| Coluna                  | Tipo                        | Obrigatória | Origem / Derivação                        | Descrição |
|-------------------------|-----------------------------|-------------|-------------------------------------------|-----------|
| ts_utc                  | TIMESTAMP(6) WITH TIME ZONE | Sim         | Join Silver por `ts_utc`                  | Chave de negócio: timestamp UTC canónico da hora |
| consumo_total           | DOUBLE                      | Sim         | `silver.consumo_hourly.total_mwh`         | Consumo elétrico nacional horário em MWh |
| market_price_pt         | DOUBLE                      | Sim         | `silver.preco_hourly.price_portugal_eur_mwh` | Preço day-ahead MIBEL Portugal em €/MWh |
| hora                    | INTEGER                     | Sim         | `HOUR(ts_utc)`                            | Hora do dia (0–23) |
| dia_semana              | INTEGER                     | Sim         | `DAY_OF_WEEK(ts_utc) - 1`                 | Dia da semana (0=Segunda … 6=Domingo) |
| is_weekend              | BOOLEAN                     | Sim         | `dia_semana >= 5`                         | True se Sábado ou Domingo |
| consumo_lag_1h          | DOUBLE                      | Não         | `LAG(consumo_total, 1) OVER (ORDER BY ts_utc)` | Consumo observado na hora anterior |
| consumo_lag_24h         | DOUBLE                      | Não         | `LAG(consumo_total, 24) OVER (ORDER BY ts_utc)` | Consumo observado 24 horas antes |
| price_lag_1h            | DOUBLE                      | Não         | `LAG(market_price_pt, 1) OVER (ORDER BY ts_utc)` | Preço observado na hora anterior |
| rolling_avg_consumo_24h | DOUBLE                      | Não         | `AVG(consumo_total) OVER (... ROWS 23 PRECEDING)` | Média móvel consumo últimas 24h |
| rolling_avg_price_24h   | DOUBLE                      | Não         | `AVG(market_price_pt) OVER (... ROWS 23 PRECEDING)` | Média móvel preço últimas 24h |
| process_date            | DATE                        | Sim         | Derivada                                  | Data lógica da execução do workflow Silver → Gold |
| year                    | INTEGER                     | Sim         | `YEAR(ts_utc)`                            | Ano — coluna de partição |
| month                   | INTEGER                     | Sim         | `MONTH(ts_utc)`                           | Mês — coluna de partição |

### Propriedades Iceberg (catálogo)

| Propriedade     | Valor                    |
|-----------------|--------------------------|
| layer           | gold                     |
| data_product    | dp_energy_market_hourly  |
| schema_version  | 1                        |
| product_version | v1                       |
| deprecated      | false                    |
| domain          | consumo_preco            |
| grain           | hourly                   |

### Regras de transformação
- INNER JOIN entre `silver.consumo_hourly` e `silver.preco_hourly` por `ts_utc`
- Window functions calculadas sobre o histórico Silver completo (sem filtro de data) para garantir lags corretos nas fronteiras de mês
- Idempotência: DELETE por `year`/`month` seguido de INSERT
- `hora` e `dia_semana` derivados de `ts_utc` (sem depender de colunas de calendário intermediárias)

### Regras de qualidade
- `ts_utc` NOT NULL e único
- `consumo_total` NOT NULL
- `market_price_pt` NOT NULL
- `hora` no intervalo [0, 23]
- `dia_semana` no intervalo [0, 6]
- Unicidade de `ts_utc` em toda a tabela
- Valores nulos em colunas de lag/rolling são esperados e válidos nas primeiras observações da série

### Observações
- Esta é a tabela principal de serving analítico
- Serve como base à API, Grafana, exploração analítica e construção da feature table ML
- Lags e rolling averages terão nulos nas primeiras 24 horas da série histórica — comportamento esperado

---

## 2.2 `gold.feat_load_forecasting_hourly`

**Tabela Iceberg:** `iceberg.gold.feat_load_forecasting_hourly`  
**Localização MinIO:** `s3a://warehouse/gold/feat_load_forecasting_hourly/`  
**Origem upstream:** `gold.dp_energy_market_hourly`  
**Granularidade:** horária  
**Formato:** Parquet (Iceberg format_version=2)  
**Particionamento:** `year`, `month`  
**Função:** Feature table pronta para treino de modelos de previsão de consumo horário (supervised learning).

### Colunas

| Coluna                  | Tipo                        | Obrigatória | Origem / Derivação                             | Descrição |
|-------------------------|-----------------------------|-------------|------------------------------------------------|-----------|
| ts_utc                  | TIMESTAMP(6) WITH TIME ZONE | Sim         | `gold.dp_energy_market_hourly.ts_utc`          | Chave temporal: timestamp UTC canónico da hora |
| consumo_total           | DOUBLE                      | Sim         | `gold.dp_energy_market_hourly`                 | Feature: consumo atual (MWh) |
| market_price_pt         | DOUBLE                      | Sim         | `gold.dp_energy_market_hourly`                 | Feature: preço day-ahead atual (€/MWh) |
| hora                    | INTEGER                     | Sim         | `gold.dp_energy_market_hourly`                 | Feature: hora do dia (0–23) |
| dia_semana              | INTEGER                     | Sim         | `gold.dp_energy_market_hourly`                 | Feature: dia da semana (0–6) |
| is_weekend              | BOOLEAN                     | Sim         | `gold.dp_energy_market_hourly`                 | Feature: indicador fim de semana |
| consumo_lag_1h          | DOUBLE                      | Sim         | `gold.dp_energy_market_hourly`                 | Feature: consumo na hora anterior |
| consumo_lag_24h         | DOUBLE                      | Sim         | `gold.dp_energy_market_hourly`                 | Feature: consumo 24 horas antes |
| price_lag_1h            | DOUBLE                      | Sim         | `gold.dp_energy_market_hourly`                 | Feature: preço na hora anterior |
| rolling_avg_consumo_24h | DOUBLE                      | Sim         | `gold.dp_energy_market_hourly`                 | Feature: média móvel consumo 24h |
| rolling_avg_price_24h   | DOUBLE                      | Sim         | `gold.dp_energy_market_hourly`                 | Feature: média móvel preço 24h |
| consumo_next_hour       | DOUBLE                      | Sim         | `LEAD(consumo_total, 1) OVER (ORDER BY ts_utc)` | **TARGET**: consumo da hora seguinte |
| process_date            | DATE                        | Sim         | `gold.dp_energy_market_hourly`                 | Data lógica da execução |
| year                    | INTEGER                     | Sim         | `gold.dp_energy_market_hourly`                 | Ano — coluna de partição |
| month                   | INTEGER                     | Sim         | `gold.dp_energy_market_hourly`                 | Mês — coluna de partição |

### Propriedades Iceberg (catálogo)

| Propriedade          | Valor                          |
|----------------------|--------------------------------|
| layer                | gold                           |
| data_product         | feat_load_forecasting_hourly   |
| schema_version       | 1                              |
| feature_schema_version | 1                            |
| product_version      | v1                             |
| deprecated           | false                          |
| upstream_table       | gold.dp_energy_market_hourly   |

### Regras de transformação
- `consumo_next_hour = LEAD(consumo_total, 1) OVER (ORDER BY ts_utc)` aplicado sobre `gold.dp_energy_market_hourly`
- Registos com `consumo_next_hour IS NULL` são excluídos (última hora de cada partição sem target)
- Registos com nulos nas features de lag são excluídos (primeiras 24h da série histórica)
- Todas as features são obrigatórias nesta tabela (sem nulos tolerados)

### Regras de qualidade
- `ts_utc` NOT NULL e único
- `consumo_next_hour` NOT NULL
- Ausência de nulos em todas as features
- Paridade de linhas com `dp_energy_market_hourly` com tolerância ≤ 48 linhas (primeiras/últimas horas excluídas)
- Consistência do lag: `consumo_lag_1h` em t deve ser igual a `consumo_total` em t-1 (tolerância 0.01 MWh)

### Observações
- Esta tabela é exclusivamente orientada a ML
- Não é uma tabela de serving para dashboards ou API
- Consumidores: workflow de treino ML, MLflow tracking

---

# 3. Relação entre as tabelas Gold

```
silver.consumo_hourly ─┐
                        ├─[INNER JOIN ts_utc]─► gold.dp_energy_market_hourly
silver.preco_hourly  ──┘                                    │
                                                             │ [LEAD + filtro nulos]
                                                             ▼
                                              gold.feat_load_forecasting_hourly
```

A relação é de **especialização**:
- `dp_energy_market_hourly` — produto analítico generalista orientado a exploração, serving e reutilização
- `feat_load_forecasting_hourly` — derivada da anterior, orientada exclusivamente a treino e avaliação de modelos ML

---

# 4. Campos preservados vs descartados

## Silver → Gold (`dp_energy_market_hourly`)

### Preservados e transformados
- `ts_utc` de ambas as Silver
- `total_mwh` → renomeado para `consumo_total`
- `price_portugal_eur_mwh` → renomeado para `market_price_pt`

### Derivados em Gold
- `hora`, `dia_semana`, `is_weekend` — calendário
- `consumo_lag_1h`, `consumo_lag_24h`, `price_lag_1h` — lags
- `rolling_avg_consumo_24h`, `rolling_avg_price_24h` — rolling averages
- `process_date`, `year`, `month` — metadados de partição

### Não incluídos em Gold
- `price_spain_eur_mwh` — mantido na Silver para referência, mas fora do data product principal

---

## `dp_energy_market_hourly` → `feat_load_forecasting_hourly`

### Preservados
- `ts_utc`, `consumo_total`, `market_price_pt`, `hora`, `dia_semana`, `is_weekend`
- `consumo_lag_1h`, `consumo_lag_24h`, `price_lag_1h`
- `rolling_avg_consumo_24h`, `rolling_avg_price_24h`
- `process_date`, `year`, `month`

### Adicionado
- `consumo_next_hour` — target derivado por LEAD

### Não incluídos na feature table
- Não são excluídas colunas do dp; a tabela inclui todas as features relevantes para treino

**Justificação:** manter a feature table estável e orientada a treino reprodutível. Qualquer coluna adicional a ser usada como feature requer incremento de `feature_schema_version`.

---

# 5. Resultado esperado após Gold

Após a camada Gold, o projeto disponibiliza:

- `gold.dp_energy_market_hourly` — produto analítico principal pronto para dashboard, API e exploração
- `gold.feat_load_forecasting_hourly` — feature table pronta para treino, avaliação e tracking em MLflow

Estas tabelas garantem:
- Integração temporal consistente (UTC canónico)
- Semântica estável para consumidores (colunas versionadas via Iceberg table properties)
- Reutilização do mesmo produto base por serving e ML
