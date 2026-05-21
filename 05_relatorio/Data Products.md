# A) Especificação de Data Products

> Esta secção responde ao entregável **A) Especificação de Data Products** do enunciado (perguntas analíticas, métricas/consumidores, grão/chaves, contrato de dados com SLAs/SLOs e estratégia de versionamento).
>
> Contexto do grupo: domínio de energia em Portugal, cruzando consumo, produção, preço de mercado day-ahead e meteorologia.

## Visão geral dos Data Products do grupo

| Data Product | Objetivo de negócio | Consumidor principal |
|---|---|---|
| `dp_energia_balance_hourly` | Monitorizar défice/excedente entre produção e consumo por hora | Dashboard operacional + API |
| `dp_energy_market_hourly` | Estimar custo horário de energia consumida com base no preço day-ahead | Dashboard financeiro + API |
| `dp_meteo_producao_daily_features` | Fornecer features diárias para treino/avaliação de modelos de previsão de produção | Equipa ML (workflow Flyte + MLflow) |

---

## DP-01 — `dp_energia_balance_hourly`

### 1) Perguntas analíticas, métricas e consumidores

**Perguntas analíticas**
- Em que horas existe **défice energético** (`produção < consumo`)?
- Qual o **saldo energético** horário e a sua tendência diária/mensal?
- Qual a **taxa de cobertura** (`produção/consumo`) por período?
- Qual o peso relativo de `DGM` e `PRE` na produção total?

**Métricas**
- `consumo_total_kwh`
- `producao_total_kwh`
- `producao_dgm_kwh`
- `producao_pre_kwh`
- `saldo_kwh = producao_total_kwh - consumo_total_kwh`
- `ratio_producao_consumo = producao_total_kwh / consumo_total_kwh`
- `flag_defice`
- `flag_excedente`
- `flag_missing_source`

**Consumidores**
- Dashboard de operação energética (`frontend/producao_consumo`).
- API HTTP para exploração por sistemas externos (`backend/producao_consumo`).

### 2) Grão e chaves

- **Grão**: 1 registo por hora UTC.
- **Chave primária de negócio**: `timestamp_utc`.
- **Chaves técnicas recomendadas**: (`timestamp_utc`, `source_system_version`) para auditoria de reprocessamentos.

### 3) Contrato de dados (schema + SLAs/SLOs)

**Schema v1 (gold)**
- `timestamp_utc TIMESTAMP NOT NULL`
- `consumo_total_kwh DOUBLE`
- `producao_total_kwh DOUBLE`
- `producao_dgm_kwh DOUBLE`
- `producao_pre_kwh DOUBLE`
- `saldo_kwh DOUBLE`
- `ratio_producao_consumo DOUBLE`
- `flag_defice BOOLEAN`
- `flag_excedente BOOLEAN`
- `flag_missing_source BOOLEAN`

**Regras de qualidade**
- Unicidade de `timestamp_utc`.
- `consumo_total_kwh >= 0` e `producao_total_kwh >= 0`.
- `flag_defice` e `flag_excedente` mutuamente exclusivas quando ambas as fontes existem.

**SLAs/SLOs**
- Atualização: até **T+30 min** após fecho da hora.
- Freshness máxima aceitável: **2 horas**.
- Completude mínima: **>= 99.0%** de horas no intervalo esperado.
- Falha de qualidade crítica: duplicados por `timestamp_utc` ou valores negativos.

### 4) Estratégia de schema evolution/versionamento

- Política de versão semântica: `v1`, `v2`, ...
- **Mudanças compatíveis** (minor): adicionar colunas nulas por omissão (ex.: `fonte_predominante`).
- **Mudanças incompatíveis** (major): alterar definição de métricas (ex.: mudança de unidade kWh→MWh) implica novo `vN`.
- Janela de coexistência mínima de duas versões: **30 dias**.

---

## DP-02 — `dp_energy_market_hourly` + `feat_load_forecasting_hourly`

### 1) Perguntas analíticas, métricas e consumidores

**Perguntas analíticas**
- Qual o **custo estimado horário** do consumo nacional face ao preço day-ahead?
- Em que períodos o preço day-ahead impacta mais o custo total?
- Qual a correlação entre preço spot e volume de consumo por período?

**Métricas (produto analítico — `dp_energy_market_hourly`)**
- `consumo_total` — consumo horário em MWh
- `market_price_pt` — preço day-ahead PT em €/MWh
- `consumo_lag_1h`, `consumo_lag_24h` — lags de consumo
- `price_lag_1h` — lag de preço
- `rolling_avg_consumo_24h`, `rolling_avg_price_24h` — médias móveis 24h
- `hora`, `dia_semana`, `is_weekend` — features temporais

**Feature table ML (`feat_load_forecasting_hourly`)**
- Mesmas features do produto analítico + `consumo_next_hour` (TARGET)
- Derivada de `dp_energy_market_hourly`; consumida exclusivamente pelo workflow de treino

**Consumidores**
- Dashboard de custos energéticos (`frontend/consumo_preco`).
- API HTTP para análise de custos (`backend/consumo_preco`).
- Equipa ML — workflow de load forecasting (MLflow + Flyte).

### 2) Grão e chaves

- **Grão**: 1 registo por hora UTC (resultado do join consumo↔preço OMIE day-ahead).
- **Chave primária de negócio**: `ts_utc`.
- **Particionamento**: `year`, `month` (Iceberg partition spec v2).

### 3) Contrato de dados (schema + SLAs/SLOs)

**Schema v1 — `dp_energy_market_hourly` (gold)**
- `ts_utc TIMESTAMP(6) WITH TIME ZONE NOT NULL`
- `consumo_total DOUBLE` — MWh
- `market_price_pt DOUBLE` — €/MWh
- `hora INTEGER` — 0-23
- `dia_semana INTEGER` — 0=Seg … 6=Dom
- `is_weekend BOOLEAN`
- `consumo_lag_1h DOUBLE`, `consumo_lag_24h DOUBLE`
- `price_lag_1h DOUBLE`
- `rolling_avg_consumo_24h DOUBLE`, `rolling_avg_price_24h DOUBLE`
- `process_date DATE`, `year INTEGER`, `month INTEGER`

**Schema v1 — `feat_load_forecasting_hourly` (feature table ML)**
- Todas as colunas acima, mais:
- `consumo_next_hour DOUBLE` — TARGET: consumo da hora seguinte (LEAD)

**Regras de qualidade**
- Unicidade de `ts_utc`.
- `consumo_total >= 0`; `market_price_pt >= 0`.
- Null rate de `consumo_next_hour` em `feat_load_forecasting_hourly`: **0%**.
- Consistência de lag: `consumo_lag_1h(t)` deve igualar `consumo_total(t-1)`.

**SLAs/SLOs**
- Atualização: até **T+45 min** após fecho da hora.
- Freshness máxima: **4 horas**.
- Taxa de junção consumo×preço: **>= 98.0%** das horas do período.
- Percentual de valores nulos em métricas core: **0%**.

### 4) Estratégia de schema evolution/versionamento

- Versão contratual nos metadados da tabela Iceberg (`schema_version`, `product_version`).
- Evolução de features compatível (minor): adicionar novas colunas de lag/rolling sem alterar as existentes.
- Mudança de fórmula de custo ou target: nova versão major + janela de coexistência de 30 dias.
- `feat_load_forecasting_hourly` versionada independentemente (`feature_schema_version`) para rastreabilidade de treino.

---

## DP-03 — `dp_meteo_producao_daily_features`

### 1) Perguntas analíticas, métricas e consumidores

**Perguntas analíticas**
- Como a meteorologia (temperatura, precipitação, vento, radiação) influencia a produção diária?
- Quais features meteorológicas aumentam o desempenho preditivo para produção do dia seguinte?
- Qual o impacto da meteorologia no preço spot day-ahead (D+1)?

**Métricas/features**
- `data_dia` — chave diária UTC
- `temperature_mean_c`, `temperature_min_c`, `temperature_max_c` — temperatura (°C)
- `precipitation_total_mm` — precipitação acumulada diária (mm)
- `wind_speed_mean_ms`, `wind_speed_max_ms` — velocidade do vento (m/s)
- `radiation_mean_wm2`, `radiation_total_kwh_m2` — radiação solar
- `cloud_cover_mean_pct` — nebulosidade média (%)
- `producao_total_daily_mwh` — produção elétrica total diária (MWh) — TARGET ML modelo A
- `consumo_total_daily_mwh`, `saldo_daily_mwh` — agregados diários de DP-01
- `preco_spot_medio_eur_mwh` — preço spot médio diário (€/MWh) — TARGET ML modelo B
- **Lag features (D-1):** `temp_lag_1d`, `wind_lag_1d`, `radiation_lag_1d`, `producao_lag_1d`, `preco_lag_1d`
- **Rolling 7 dias:** `temp_rolling_7d_avg`, `wind_rolling_7d_avg`, `radiation_rolling_7d_avg`, `producao_rolling_7d_avg`
- `dia_semana`, `is_weekend`, `estacao` — features temporais/sazonais

**Consumidores**
- Workflow de treino ML (MLflow + Flyte): modelos de previsão de produção e de preço spot.
- Dashboard meteo+produção (`frontend/meteo_producao`).
- API HTTP (`backend/meteo_producao`, porta 8083).

### 2) Grão e chaves

- **Grão**: 1 registo por dia UTC.
- **Chave primária de negócio**: `data_dia`.
- **Chave técnica recomendada**: (`data_dia`, `feature_set_version`) para rastreabilidade de treino.
- **Cross-DP join**: agrega DP-01 (produção/consumo) e DP-02 (preço spot) numa única tabela diária.

### 3) Contrato de dados (schema + SLAs/SLOs)

**Schema v1 (gold/feature table)**
- `data_dia DATE NOT NULL` — chave primária de negócio
- `temperature_mean_c DOUBLE`, `temperature_min_c DOUBLE`, `temperature_max_c DOUBLE`
- `precipitation_total_mm DOUBLE`
- `wind_speed_mean_ms DOUBLE`, `wind_speed_max_ms DOUBLE`
- `radiation_mean_wm2 DOUBLE`, `radiation_total_kwh_m2 DOUBLE`
- `cloud_cover_mean_pct DOUBLE`
- `producao_total_daily_mwh DOUBLE NOT NULL` — TARGET modelo A
- `consumo_total_daily_mwh DOUBLE`, `saldo_daily_mwh DOUBLE`
- `preco_spot_medio_eur_mwh DOUBLE` — TARGET modelo B
- `preco_spot_max_eur_mwh DOUBLE`, `preco_spot_min_eur_mwh DOUBLE`
- `temp_lag_1d DOUBLE`, `wind_lag_1d DOUBLE`, `radiation_lag_1d DOUBLE`, `producao_lag_1d DOUBLE`, `preco_lag_1d DOUBLE`
- `temp_rolling_7d_avg DOUBLE`, `wind_rolling_7d_avg DOUBLE`, `radiation_rolling_7d_avg DOUBLE`, `producao_rolling_7d_avg DOUBLE`
- `dia_semana INTEGER`, `is_weekend BOOLEAN`, `estacao INTEGER`
- `_updated_at TIMESTAMP` — timestamp de geração do registo

**Regras de qualidade**
- Unicidade de `data_dia`.
- Sem nulos em `producao_total_daily_mwh`.
- Limites físicos: `precipitation_total_mm >= 0`, `radiation_mean_wm2 >= 0`, `cloud_cover_mean_pct ∈ [0, 100]`.
- Consistência de lag: `temp_lag_1d(d)` deve igualar `temperature_mean_c(d-1)` com tolerância 0.01 °C.
- Cobertura cross-DP: >= 90% dos dias com dados completos de DP-01 e DP-02.

**SLAs/SLOs**
- Publicação diária até **07:00 UTC**.
- Freshness máxima: **1 dia**.
- Completude: **>= 99.5%** dos dias no intervalo de treino.
- Drift monitorizado (média e desvio padrão por feature na Gold).

### 4) Estratégia de schema evolution/versionamento

- `feature_set_version` obrigatório nos metadados Iceberg de cada versão da tabela.
- Adição de novas features: compatível (minor) se não remover/alterar as existentes.
- Alteração de transformação de feature (ex.: nova janela de rolling ou unidade) implica novo major.
- Treinos MLflow devem referenciar explicitamente `dataset = "iceberg.gold.dp_meteo_producao_daily_features"` e `feature_set_version` nos tags do run.

---

## Critérios de aceitação transversais (grupo)

- Cada Data Product é rastreável até tabelas silver/bronze de origem.
- Todo consumidor (dashboard/API/ML) referencia explicitamente o contrato consumido.
- Quebras de contrato bloqueiam promoção para produção até correção.
- Métricas de qualidade e freshness são verificáveis via SQL e registadas no relatório técnico.
