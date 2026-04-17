# A) Especificação de Data Products

> Esta secção responde ao entregável **A) Especificação de Data Products** do enunciado (perguntas analíticas, métricas/consumidores, grão/chaves, contrato de dados com SLAs/SLOs e estratégia de versionamento).
>
> Contexto do grupo: domínio de energia em Portugal, cruzando consumo, produção, preço de mercado day-ahead e meteorologia.

## Visão geral dos Data Products do grupo

| Data Product | Objetivo de negócio | Consumidor principal |
|---|---|---|
| `dp_energia_balance_hourly` | Monitorizar défice/excedente entre produção e consumo por hora | Dashboard operacional + API |
| `dp_consumo_custo_hourly` | Estimar custo horário de energia consumida com base no preço day-ahead | Dashboard financeiro + API |
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

## DP-02 — `dp_consumo_custo_hourly`

### 1) Perguntas analíticas, métricas e consumidores

**Perguntas analíticas**
- Qual o **custo estimado horário** do consumo nacional?
- Em que períodos o preço day-ahead impacta mais o custo total?
- Qual a diferença entre **preço médio simples** e **preço médio ponderado** pelo consumo?

**Métricas**
- `consumo_mwh`
- `preco_eur_mwh`
- `custo_estimado_eur = consumo_mwh * preco_eur_mwh`
- `preco_medio_simples_eur_mwh`
- `preco_medio_ponderado_eur_mwh`

**Consumidores**
- Dashboard de custos energéticos (`frontend/consumo_preco`).
- API HTTP para análise de custos (`backend/consumo_preco`).
- Equipa financeira/planeamento.

### 2) Grão e chaves

- **Grão**: 1 registo por hora UTC (resultado do join consumo↔preço).
- **Chave primária de negócio**: `timestamp_utc`.
- **Chaves auxiliares**: `delivery_date_local`, `market_hour` (para rastrear origem do preço).

### 3) Contrato de dados (schema + SLAs/SLOs)

**Schema v1 (gold)**
- `timestamp_utc TIMESTAMP NOT NULL`
- `consumo_mwh DOUBLE NOT NULL`
- `preco_eur_mwh DOUBLE NOT NULL`
- `custo_estimado_eur DOUBLE NOT NULL`
- `delivery_date_local DATE`
- `market_hour INTEGER`
- `price_parser_version VARCHAR` (ex.: `v1_hour_minus_1`)

**Regras de qualidade**
- Unicidade de `timestamp_utc`.
- `consumo_mwh >= 0`; `preco_eur_mwh >= 0`.
- Validação do domínio de `market_hour` em `[1, 25]`.
- Registo explícito da regra temporal usada para mapear `Hour -> timestamp_utc`.

**SLAs/SLOs**
- Atualização: até **T+45 min** após fecho da hora.
- Freshness máxima: **4 horas**.
- Taxa de junção consumo×preço: **>= 98.0%** das horas do período.
- Percentual de valores nulos em métricas core: **0%**.

### 4) Estratégia de schema evolution/versionamento

- Versão contratual no metadado da tabela (`contract_version`).
- Evolução de parser temporal tratada como mudança de contrato (`price_parser_version`).
- Nova versão major obrigatória se a fórmula de custo for alterada.

---

## DP-03 — `dp_meteo_producao_daily_features`

### 1) Perguntas analíticas, métricas e consumidores

**Perguntas analíticas**
- Como a meteorologia (temperatura, precipitação, vento, radiação) influencia a produção diária?
- Quais features meteorológicas aumentam o desempenho preditivo para produção do dia seguinte?
- Qual o erro esperado por horizonte de previsão (D+1, D+2)?

**Métricas/features**
- `date_utc`
- `temp_mean_c`, `temp_max_c`, `temp_min_c`
- `precip_total_mm`
- `wind_mean_kmh`, `wind_max_kmh`
- `shortwave_radiation_sum`
- `producao_total_mwh` (target)
- `producao_pre_mwh`, `producao_dgm_mwh` (targets auxiliares)

**Consumidores**
- Workflow de treino em Flyte.
- Tracking de experiências/artefactos em MLflow.
- Equipa de previsão energética.

### 2) Grão e chaves

- **Grão**: 1 registo por dia UTC.
- **Chave primária de negócio**: `date_utc`.
- **Chave técnica recomendada**: (`date_utc`, `feature_set_version`).

### 3) Contrato de dados (schema + SLAs/SLOs)

**Schema v1 (gold/feature table)**
- `date_utc DATE NOT NULL`
- `temp_mean_c DOUBLE`
- `temp_max_c DOUBLE`
- `temp_min_c DOUBLE`
- `precip_total_mm DOUBLE`
- `wind_mean_kmh DOUBLE`
- `wind_max_kmh DOUBLE`
- `shortwave_radiation_sum DOUBLE`
- `producao_total_mwh DOUBLE NOT NULL`
- `feature_set_version VARCHAR NOT NULL`

**Regras de qualidade**
- Unicidade de `date_utc`.
- Sem nulos em `producao_total_mwh`.
- Limites físicos razoáveis (ex.: precipitação >= 0, radiação >= 0).
- Cobertura meteorológica diária mínima de **24 observações horárias** antes da agregação.

**SLAs/SLOs**
- Publicação diária até **07:00 UTC**.
- Freshness máxima: **1 dia**.
- Completude: **>= 99.5%** dos dias no intervalo de treino.
- Drift básico monitorizado (média e desvio padrão por feature).

### 4) Estratégia de schema evolution/versionamento

- `feature_set_version` obrigatório em todas as linhas.
- Adição de novas features: compatível (minor) se não remover/alterar as existentes.
- Alteração de transformação de feature (ex.: nova agregação) implica novo major.
- Treinos devem referenciar explicitamente (`data_product`, `contract_version`, `feature_set_version`).

---

## Critérios de aceitação transversais (grupo)

- Cada Data Product é rastreável até tabelas silver/bronze de origem.
- Todo consumidor (dashboard/API/ML) referencia explicitamente o contrato consumido.
- Quebras de contrato bloqueiam promoção para produção até correção.
- Métricas de qualidade e freshness são verificáveis via SQL e registadas no relatório técnico.
