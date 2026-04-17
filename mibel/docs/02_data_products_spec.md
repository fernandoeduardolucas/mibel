# 1. Data Product: dp.energy_market_hourly

## Descrição
Produto Gold horário que integra consumo elétrico nacional e preço horário MIBEL PT, normalizados para UTC, para suporte a análise e decisão.

---

## Grão
- 1 linha por `timestamp_utc` (hora)

---

## Chave de Negócio
- `timestamp_utc`

---

## Schema

| Coluna                   | Tipo        | Descrição |
|--------------------------|------------|----------|
| timestamp_utc           | TIMESTAMP  | Timestamp horário normalizado em UTC |
| date                    | DATE       | Data derivada de `timestamp_utc` |
| year                    | INT        | Ano |
| month                   | INT        | Mês |
| day                     | INT        | Dia |
| hour                    | INT        | Hora (0–23) |
| day_of_week             | INT        | Dia da semana (1–7) |
| is_weekend              | BOOLEAN    | Indicador de fim de semana |
| consumo_total           | DOUBLE     | Valor agregado horário da coluna `total` do dataset de consumo |
| market_price_pt         | DOUBLE     | Preço horário de Portugal proveniente do dataset MIBEL |
| consumo_lag_1h          | DOUBLE     | Consumo na hora anterior |
| consumo_lag_24h         | DOUBLE     | Consumo na mesma hora do dia anterior |
| price_lag_1h            | DOUBLE     | Preço na hora anterior |
| rolling_avg_consumo_24h | DOUBLE     | Média móvel de consumo nas últimas 24h |
| rolling_avg_price_24h   | DOUBLE     | Média móvel de preço nas últimas 24h |

---

## Regras de Qualidade

- `timestamp_utc` NOT NULL e único
- no máximo 1 registo por hora
- cobertura temporal contínua dentro do período processado, exceto falhas identificadas na origem
- `consumo_total` >= 0
- `market_price_pt` dentro de intervalo plausível (ex: -500 a 500)
- percentagem de nulos nas métricas críticas inferior a 1%

---

## Transformações Principais

- Agregação de consumo de 15 minutos → 1 hora
- Parsing, normalização e validação dos timestamps para `timestamp_utc`
- Tratamento da coluna `Hour` no dataset de preços (incluindo casos especiais como hora 25)
- Join entre consumo e preço por `timestamp_utc`
- Criação de features temporais (hora, dia da semana, fim de semana)
- Criação de lags (1h, 24h)
- Cálculo de médias móveis (24h)

---

## Dependências (Upstream)

- `silver.consumo_hourly`
- `silver.preco_hourly`

---

## Atualização

- Frequência: diária
- Tipo: incremental por `process_date`

---

# 2. Data Product: feat.load_forecasting_hourly

## Descrição
Feature table Gold para treino de modelos de previsão de consumo horário.

---

## Grão
- 1 linha por `timestamp_utc`

---

## Chave de Negócio
- `timestamp_utc`

---

## Target

| Coluna             | Tipo    | Descrição |
|--------------------|--------|----------|
| consumo_next_hour  | DOUBLE | Consumo da hora seguinte (derivado por deslocamento temporal de `consumo_total`) |

---

## Features

| Coluna                | Tipo    | Descrição |
|-----------------------|--------|----------|
| consumo_total         | DOUBLE | Consumo atual |
| market_price_pt       | DOUBLE | Preço atual |
| hour                  | INT    | Hora |
| day_of_week           | INT    | Dia da semana |
| is_weekend            | BOOLEAN| Indicador de fim de semana |
| consumo_lag_1h        | DOUBLE | Lag de 1 hora |
| consumo_lag_24h       | DOUBLE | Lag de 24 horas |
| price_lag_1h          | DOUBLE | Lag do preço |

---

## Regras de Qualidade

- `timestamp_utc` NOT NULL e único
- `consumo_next_hour` NOT NULL
- ausência de valores nulos nas features principais
- consistência temporal (sem saltos ou duplicações de timestamp)
- dados ordenados por tempo

---

## Transformações Principais

- Derivação de `consumo_next_hour` através de shift (-1h) sobre `consumo_total`
- Reutilização de features do data product `gold.dp_energy_market_hourly`
- Filtragem de registos incompletos (ex: última hora sem target)

---

## Dependências (Upstream)

- `gold.dp_energy_market_hourly`

---

## Atualização

- Frequência: diária
- Tipo: incremental