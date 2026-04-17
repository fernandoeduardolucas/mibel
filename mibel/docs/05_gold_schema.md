# 1. Objetivo

Este documento define o schema técnico das tabelas Gold do projeto, correspondentes ao produto analítico principal e à feature table utilizada no workflow de machine learning.

A camada Gold disponibiliza dados prontos a consumir por dashboards, API, análise exploratória e treino de modelos, com granularidade horária e tempo canónico em UTC.

---

# 2. Schema Gold

## 2.1 `gold.dp_energy_market_hourly`

**Origem upstream:**
- `silver.consumo_hourly`
- `silver.preco_hourly`

**Granularidade de saída:** horária  
**Função:** integrar consumo e preço numa única tabela analítica horária pronta para consumo.

### Colunas

| Coluna                   | Tipo       | Obrigatória | Origem / Derivação | Descrição |
|--------------------------|------------|-------------|--------------------|-----------|
| timestamp_utc            | TIMESTAMP  | Sim         | Join / Silver      | Timestamp horário normalizado em UTC |
| date                     | DATE       | Sim         | Derivada           | Data derivada de `timestamp_utc` |
| year                     | INT        | Sim         | Derivada           | Ano |
| month                    | INT        | Sim         | Derivada           | Mês |
| day                      | INT        | Sim         | Derivada           | Dia |
| hour                     | INT        | Sim         | Derivada           | Hora (0–23) |
| day_of_week              | INT        | Sim         | Derivada           | Dia da semana |
| is_weekend               | BOOLEAN    | Sim         | Derivada           | Indicador de fim de semana |
| consumo_total            | DOUBLE     | Sim         | `silver.consumo_hourly` | Consumo horário agregado |
| market_price_pt          | DOUBLE     | Sim         | `silver.preco_hourly`    | Preço horário PT |
| consumo_lag_1h           | DOUBLE     | Não         | Derivada           | Consumo da hora anterior |
| consumo_lag_24h          | DOUBLE     | Não         | Derivada           | Consumo da mesma hora do dia anterior |
| price_lag_1h             | DOUBLE     | Não         | Derivada           | Preço da hora anterior |
| rolling_avg_consumo_24h  | DOUBLE     | Não         | Derivada           | Média móvel de consumo das últimas 24 horas |
| rolling_avg_price_24h    | DOUBLE     | Não         | Derivada           | Média móvel de preço das últimas 24 horas |
| process_date             | DATE       | Sim         | Derivada           | Data lógica da execução |

### Regras de transformação
- join temporal 1:1 entre `silver.consumo_hourly` e `silver.preco_hourly` por `timestamp_utc`
- derivação de atributos de calendário a partir de `timestamp_utc`
- cálculo de lags usando ordenação temporal crescente
- cálculo de rolling averages com janela temporal horária
- preservação apenas das colunas relevantes para consumo analítico

### Regras de qualidade
- `timestamp_utc` NOT NULL e único
- `consumo_total` NOT NULL
- `market_price_pt` NOT NULL
- no máximo 1 registo por hora
- ausência de duplicações após o join
- coerência temporal das features derivadas
- valores nulos permitidos apenas nas colunas de lag/rolling no arranque da série

### Observações
- esta é a tabela principal de serving analítico
- serve como base à API, exploração analítica e construção da feature table de ML
- lags e rolling averages podem ter nulos nas primeiras observações, o que é esperado

---

## 2.2 `gold.feat_load_forecasting_hourly`

**Origem upstream:**
- `gold.dp_energy_market_hourly`

**Granularidade de saída:** horária  
**Função:** disponibilizar uma feature table pronta para treino de modelos de previsão de consumo.

### Colunas

| Coluna                   | Tipo       | Obrigatória | Origem / Derivação | Descrição |
|--------------------------|------------|-------------|--------------------|-----------|
| timestamp_utc            | TIMESTAMP  | Sim         | `gold.dp_energy_market_hourly` | Timestamp horário em UTC |
| consumo_total            | DOUBLE     | Sim         | `gold.dp_energy_market_hourly` | Consumo atual |
| market_price_pt          | DOUBLE     | Sim         | `gold.dp_energy_market_hourly` | Preço atual |
| hour                     | INT        | Sim         | `gold.dp_energy_market_hourly` | Hora |
| day_of_week              | INT        | Sim         | `gold.dp_energy_market_hourly` | Dia da semana |
| is_weekend               | BOOLEAN    | Sim         | `gold.dp_energy_market_hourly` | Indicador de fim de semana |
| consumo_lag_1h           | DOUBLE     | Sim         | `gold.dp_energy_market_hourly` | Lag de 1 hora |
| consumo_lag_24h          | DOUBLE     | Sim         | `gold.dp_energy_market_hourly` | Lag de 24 horas |
| price_lag_1h             | DOUBLE     | Sim         | `gold.dp_energy_market_hourly` | Lag do preço |
| rolling_avg_consumo_24h  | DOUBLE     | Sim         | `gold.dp_energy_market_hourly` | Média móvel de consumo |
| rolling_avg_price_24h    | DOUBLE     | Sim         | `gold.dp_energy_market_hourly` | Média móvel de preço |
| consumo_next_hour        | DOUBLE     | Sim         | Derivada           | Target: consumo da hora seguinte |
| process_date             | DATE       | Sim         | `gold.dp_energy_market_hourly` | Data lógica da execução |

### Regras de transformação
- seleção das features relevantes a partir de `gold.dp_energy_market_hourly`
- derivação de `consumo_next_hour` através de deslocamento temporal de `consumo_total`
- remoção de registos sem target disponível
- remoção de registos sem histórico mínimo necessário para features de lag e rolling

### Regras de qualidade
- `timestamp_utc` NOT NULL e único
- `consumo_next_hour` NOT NULL
- ausência de nulos nas features utilizadas no treino
- ordenação temporal consistente
- correspondência exata entre features e target no instante previsto

### Observações
- esta tabela é exclusivamente orientada a ML
- não é uma tabela de serving para dashboards ou API
- as primeiras observações da série podem ser excluídas devido à ausência de histórico suficiente para features derivadas

---

# 3. Relação entre as tabelas Gold

## `gold.dp_energy_market_hourly`
Tabela Gold de consumo analítico geral, orientada a exploração, serving e reutilização por múltiplos consumidores.

## `gold.feat_load_forecasting_hourly`
Tabela Gold especializada para supervised learning, derivada da tabela analítica principal.

A relação entre ambas é de especialização:
- a primeira é generalista e analítica
- a segunda é orientada a treino e avaliação de modelos

---

# 4. Campos preservados vs descartados

## `gold.dp_energy_market_hourly`
### Preservados
- timestamp horário
- métricas principais de consumo e preço
- colunas de calendário
- features derivadas úteis para análise

### Não incluídos
- colunas técnicas detalhadas da Silver (`source_min_ts`, `source_max_ts`, `source_rows`, `source_date_raw`, `source_hour_raw`)

**Justificação:** essas colunas são úteis para rastreabilidade e validação, mas não para serving analítico principal.

---

## `gold.feat_load_forecasting_hourly`
### Preservados
- apenas as colunas necessárias para treino de modelo

### Não incluídos
- colunas de calendário redundantes não usadas no treino
- colunas técnicas de auditoria não necessárias ao workflow ML

**Justificação:** manter a feature table enxuta, estável e orientada a treino reprodutível.

---

# 5. Resultado esperado após Gold

Após a camada Gold, o projeto deverá disponibilizar:

- `gold.dp_energy_market_hourly`  
  Produto analítico principal pronto para dashboard, API e exploração analítica

- `gold.feat_load_forecasting_hourly`  
  Feature table pronta para treino, avaliação e tracking em MLflow

Estas tabelas devem garantir:
- integração temporal consistente
- semântica estável para consumidores
- reutilização do mesmo produto base por serving e ML