# 1. Cenário de Negócio

A organização fictícia atua no setor energético em Portugal e pretende monitorizar a evolução do consumo elétrico nacional e a sua relação com o preço horário do mercado MIBEL, suportando análise operacional, reporting e previsão de consumo.

---

# 2. Datasets Usados

## Dataset 1 — Consumo elétrico nacional
- **Ficheiro:** `consumo-total-nacional.csv`
- **Granularidade original:** 15 minutos
- **Cobertura temporal:** 2023–2026
- **Nível de agregação:** nacional

## Dataset 2 — Preço horário MIBEL Portugal
- **Ficheiro:** `Day-ahead Market Prices_20230101_20260311.csv`
- **Granularidade original:** horária
- **Cobertura temporal:** 2023–2026
- **Nível de agregação:** mercado PT

## Principais desafios de qualidade
- granularidades temporais diferentes  
- necessidade de harmonização temporal  
- tratamento da coluna de hora no dataset de preços  
- possível existência de registos em falta ou inconsistências temporais  

---

# 3. Data Product 1

## Nome
`dp.energy_market_hourly`

## Objetivo
Produto analítico horário que integra consumo elétrico nacional e preço horário MIBEL PT para análise descritiva e apoio à decisão.

## Consumidores
- dashboard analítico  
- API simples  
- analistas de negócio  

## Perguntas analíticas
- Como evolui o consumo ao longo do tempo?  
- Que relação existe entre consumo e preço horário?  
- Que padrões diários e semanais se observam?  
- Em que períodos ocorrem picos de consumo e preço?  

## Grão
- 1 linha por `timestamp_utc` ao nível horário  

## Métricas iniciais
- `consumo_total_mwh`  
- `market_price_eur_mwh`  

## Chave de negócio
- `timestamp_utc`  

## Versão inicial
- v1  

---

# 4. Data Product 2

## Nome
`feat.load_forecasting_hourly`

## Objetivo
Feature table horária para treino de modelos de previsão de consumo.

## Consumidores
- workflow de ML  
- MLflow  

## Perguntas analíticas / preditivas
- É possível prever o consumo da próxima hora com base em histórico e calendário?  
- Que variáveis temporais e lags mais contribuem para a previsão?  

## Grão
- 1 linha por `timestamp_utc` horário  

## Target
- `consumo_next_hour`  

## Features previstas
- `consumo_total_mwh`  
- `market_price_eur_mwh`  
- `hour`  
- `day_of_week`  
- `is_weekend`  
- `consumo_lag_1h`  
- `consumo_lag_24h`  
- `price_lag_1h`  

## Versão inicial
- v1

# 5. Convenção Temporal

O projeto adota UTC como tempo canónico para integração e consumo analítico dos dados. Os timestamps originais são preservados na camada Bronze sempre que aplicável, sendo convertidos para `timestamp_utc` na camada Silver. A camada Gold utiliza exclusivamente timestamps normalizados em UTC.  