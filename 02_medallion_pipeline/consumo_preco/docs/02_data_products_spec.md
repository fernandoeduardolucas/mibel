# 1. Data Product: dp.energy_market_hourly

## Descrição
Produto Gold horário que integra consumo elétrico nacional e preço horário MIBEL PT, normalizados para UTC, para suporte a análise descritiva e decisão operacional.

---

## Grão
1 linha por `ts_utc` ao nível horário (timestamp UTC, início da hora)

---

## Chave de Negócio
`ts_utc` — TIMESTAMP(6) WITH TIME ZONE, único e NOT NULL

---

## Tabela Iceberg
`iceberg.gold.dp_energy_market_hourly`

---

## Schema

| Coluna                  | Tipo                        | Obrigatória | Descrição |
|-------------------------|-----------------------------|-------------|-----------|
| ts_utc                  | TIMESTAMP WITH TIME ZONE    | Sim         | Chave de negócio: timestamp UTC horário canónico |
| consumo_total           | DOUBLE                      | Sim         | Consumo elétrico nacional horário em MWh |
| market_price_pt         | DOUBLE                      | Sim         | Preço day-ahead MIBEL Portugal em €/MWh |
| hora                    | INTEGER                     | Sim         | Hora do dia derivada de ts_utc (0–23) |
| dia_semana              | INTEGER                     | Sim         | Dia da semana (0=Segunda … 6=Domingo) |
| is_weekend              | BOOLEAN                     | Sim         | True se Sábado ou Domingo |
| consumo_lag_1h          | DOUBLE                      | Não         | Consumo na hora anterior (nulo nas primeiras obs.) |
| consumo_lag_24h         | DOUBLE                      | Não         | Consumo 24h antes (nulo nas primeiras 24 obs.) |
| price_lag_1h            | DOUBLE                      | Não         | Preço na hora anterior (nulo nas primeiras obs.) |
| rolling_avg_consumo_24h | DOUBLE                      | Não         | Média móvel consumo últimas 24h |
| rolling_avg_price_24h   | DOUBLE                      | Não         | Média móvel preço últimas 24h |
| process_date            | DATE                        | Sim         | Data lógica da execução do workflow |
| year                    | INTEGER                     | Sim         | Ano — coluna de partição |
| month                   | INTEGER                     | Sim         | Mês — coluna de partição |

---

## Perguntas Analíticas

- Como evolui o consumo elétrico nacional ao longo do tempo?
- Que relação existe entre consumo e preço horário MIBEL?
- Que padrões diários e semanais se observam no consumo e no preço?
- Em que períodos ocorrem picos de consumo e de preço?
- Como variam as médias móveis de consumo e preço ao longo do ano?

---

## Métricas Principais

| Métrica                 | Tipo   | Unidade  | Descrição |
|-------------------------|--------|----------|-----------|
| consumo_total           | DOUBLE | MWh      | Consumo horário nacional |
| market_price_pt         | DOUBLE | €/MWh    | Preço day-ahead Portugal |
| rolling_avg_consumo_24h | DOUBLE | MWh      | Tendência de consumo (24h) |
| rolling_avg_price_24h   | DOUBLE | €/MWh    | Tendência de preço (24h) |

---

## Consumidores

| Consumidor            | Tipo          | Detalhe |
|-----------------------|---------------|---------|
| Dashboard Grafana     | Visualização  | `consumo_preco_overview.json` — 11 painéis |
| API HTTP              | Serving       | `/api/overview`, `/api/timeseries?group=day\|month` |
| Analistas de negócio  | Exploração    | Query direta via Trino |
| Feature table ML      | Upstream      | `gold.feat_load_forecasting_hourly` deriva daqui |

---

## SLAs / SLOs

| Dimensão          | SLO / Limiar                              | Comportamento em falha |
|-------------------|-------------------------------------------|------------------------|
| Frescura          | Dados disponíveis com atraso máx. de 24h  | WARN no Grafana; alerta operacional |
| Null rate         | < 1% em `consumo_total` e `market_price_pt` | Quality check FAIL bloqueia promoção |
| Unicidade         | `ts_utc` único em toda a tabela           | Quality check FAIL bloqueia promoção |
| Cobertura horária | ≥ 23h/dia para o período processado       | Quality check WARN (registo apenas) |
| Consistência lags | consumo_lag_1h coerente com t-1 (tol. 0.01 MWh) | Quality check FAIL se > 0.1% inconsistente |
| Disponibilidade   | Tabela disponível para query ≥ 99% do tempo | Backfill suportado via Flyte |

---

## Dependências (Upstream)

- `silver.consumo_hourly`
- `silver.preco_hourly`

---

## Atualização

- **Frequência:** diária
- **Tipo:** incremental por `year`/`month` (DELETE + INSERT idempotente)
- **Orquestração:** Flyte — `flyte_silver_to_gold.py` → `build_dp_energy_market_full()`

---

## Versão

- **product_version:** v1
- **schema_version:** 1
- **deprecated:** false

---

---

# 2. Data Product: feat.load_forecasting_hourly

## Descrição
Feature table Gold para treino supervisionado de modelos de previsão de consumo elétrico horário. Derivada do produto analítico principal com adição do target `consumo_next_hour`.

---

## Grão
1 linha por `ts_utc` ao nível horário (sem nulos no target nem nas features)

---

## Chave de Negócio
`ts_utc` — TIMESTAMP(6) WITH TIME ZONE, único e NOT NULL

---

## Tabela Iceberg
`iceberg.gold.feat_load_forecasting_hourly`

---

## Schema

| Coluna                  | Tipo                     | Obrigatória | Descrição |
|-------------------------|--------------------------|-------------|-----------|
| ts_utc                  | TIMESTAMP WITH TIME ZONE | Sim         | Chave temporal: timestamp UTC canónico |
| consumo_total           | DOUBLE                   | Sim         | Feature: consumo atual (MWh) |
| market_price_pt         | DOUBLE                   | Sim         | Feature: preço day-ahead atual (€/MWh) |
| hora                    | INTEGER                  | Sim         | Feature: hora do dia (0–23) |
| dia_semana              | INTEGER                  | Sim         | Feature: dia da semana (0–6) |
| is_weekend              | BOOLEAN                  | Sim         | Feature: indicador fim de semana |
| consumo_lag_1h          | DOUBLE                   | Sim         | Feature: consumo na hora anterior |
| consumo_lag_24h         | DOUBLE                   | Sim         | Feature: consumo 24h antes |
| price_lag_1h            | DOUBLE                   | Sim         | Feature: preço na hora anterior |
| rolling_avg_consumo_24h | DOUBLE                   | Sim         | Feature: média móvel consumo 24h |
| rolling_avg_price_24h   | DOUBLE                   | Sim         | Feature: média móvel preço 24h |
| consumo_next_hour       | DOUBLE                   | Sim         | **TARGET**: consumo da hora seguinte (LEAD 1h) |
| process_date            | DATE                     | Sim         | Data lógica da execução |
| year                    | INTEGER                  | Sim         | Ano — coluna de partição |
| month                   | INTEGER                  | Sim         | Mês — coluna de partição |

---

## Perguntas Analíticas / Preditivas

- É possível prever o consumo da próxima hora com base no histórico e no calendário?
- Que variáveis temporais e lags mais contribuem para a previsão?
- Qual o erro de previsão (MAE, RMSE) de um modelo treinado nesta feature table?

---

## Target

| Coluna             | Tipo   | Descrição |
|--------------------|--------|-----------|
| consumo_next_hour  | DOUBLE | Consumo da hora seguinte em MWh — derivado por LEAD(consumo_total, 1) OVER (ORDER BY ts_utc) |

---

## Features (11 features)

| Categoria  | Features |
|------------|----------|
| Temporal   | `hora`, `dia_semana`, `is_weekend` |
| Métricas   | `consumo_total`, `market_price_pt` |
| Lags       | `consumo_lag_1h`, `consumo_lag_24h`, `price_lag_1h` |
| Rolling    | `rolling_avg_consumo_24h`, `rolling_avg_price_24h` |

---

## Consumidores

| Consumidor              | Tipo           | Detalhe |
|-------------------------|----------------|---------|
| Workflow de treino ML   | ML training    | `03_ml_pipeline/consumo_preco_mlflow_flow.py` |
| MLflow                  | Tracking       | Experiment `consumo-preco-load-forecast` |
| Dashboard Grafana       | Monitorização  | Painel "Feature Table ML" em `consumo_preco_overview.json` |

---

## SLAs / SLOs

| Dimensão              | SLO / Limiar                                              | Comportamento em falha |
|-----------------------|-----------------------------------------------------------|------------------------|
| Null target           | `consumo_next_hour` NOT NULL em todos os registos        | Quality check FAIL bloqueia promoção |
| Null features         | Ausência de nulos em todas as 11 features                | Quality check FAIL bloqueia promoção |
| Unicidade             | `ts_utc` único em toda a tabela                          | Quality check FAIL bloqueia promoção |
| Paridade com dp       | Diferença ≤ 48 linhas vs `dp_energy_market_hourly`       | Quality check WARN (registo apenas) |
| Consistência lag      | `consumo_lag_1h` coerente com t-1 (tol. 0.01 MWh)       | Quality check FAIL se > 0.1% inconsistente |
| Rastreabilidade ML    | Dados de treino identificáveis por `year`/`month`        | Garantido por particionamento Iceberg |

---

## Dependências (Upstream)

- `gold.dp_energy_market_hourly`

---

## Atualização

- **Frequência:** diária
- **Tipo:** incremental por `year`/`month` (DELETE + INSERT idempotente)
- **Orquestração:** Flyte — `flyte_silver_to_gold.py` → `build_feat_load_forecasting_full()`

---

## Versão

- **product_version:** v1
- **schema_version:** 1
- **feature_schema_version:** 1
- **deprecated:** false
