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

## DP-02 — `dp_energy_market_api_hourly` + `feat_load_forecasting_api_hourly`

> **Fontes de dados:** Energy-Charts API (Fraunhofer ISE) — fonte por defeito, sem autenticação; ENTSO-E Transparency Platform — alternativa, requer token gratuito.
> Pipeline implementado em `02_medallion_pipeline/DP02_Consumo_Preco/Streaming_Data/`. A pasta `Static_Data/` mantém um pipeline legado sobre CSVs históricos (REN + OMIE).

### 1) Perguntas analíticas, métricas e consumidores

**Pergunta central de negócio**
> *Em que intervalo horário diário é mais vantajoso vender energia renovável à rede?*

**Perguntas analíticas de suporte**
- Qual o **perfil horário do preço day-ahead** (0–23h) — que horas têm preço sistematicamente mais alto?
- Em que horas o **índice de oportunidade** (preço relativo alto + consumo relativo baixo) é máximo?
- Qual a **evolução mensal e anual** do preço day-ahead em Portugal?
- Qual a **correlação e divergência** entre preço PT e preço ES no mercado MIBEL?
- Em que horas e períodos ocorrem **preços negativos** (excesso de oferta renovável)?

**Métricas (produto analítico — `dp_energy_market_api_hourly`)**

- `market_price_pt` — preço day-ahead Portugal em €/MWh
- `consumo_total` — carga eléctrica nacional horária em MWh
- `consumo_lag_1h`, `consumo_lag_24h` — lags de consumo (window functions)
- `price_lag_1h` — lag de preço
- `rolling_avg_consumo_24h`, `rolling_avg_price_24h` — médias móveis 24h
- `hora` (0–23), `dia_semana` (0=Seg … 6=Dom), `is_weekend` — features temporais

**Feature table ML (`feat_load_forecasting_api_hourly`)**
- Todas as features acima + `consumo_next_hour` (TARGET: `LEAD(consumo_total, 1)`)
- Primeiras 24h e última linha excluídas (lags nulos / sem target futuro)
- Consumida exclusivamente pelo workflow de treino GradientBoostingRegressor (MLflow)

**Consumidores**
- Dashboard Grafana: `consumo_preco_streaming_overview` — foco no preço e janela ótima de venda.
- Equipa ML — workflow de load forecasting (MLflow + Flyte): GBR, R² ≈ 0.989, MAPE ≈ 1.30%.

### 2) Grão e chaves

- **Grão**: 1 registo por hora UTC (INNER JOIN `silver.consumo_api_hourly` × `silver.preco_api_hourly`).
- **Chave primária de negócio**: `ts_utc`.
- **Cobertura**: 2022-01-01 até hoje.
- **Particionamento**: `year`, `month` (Iceberg partition spec v2).

### 3) Contrato de dados (schema + SLAs/SLOs)

**Schema v1 — `iceberg.gold.dp_energy_market_api_hourly`**

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ts_utc` | `TIMESTAMP(6) WITH TIME ZONE NOT NULL` | Chave temporal primária (início da hora UTC) |
| `consumo_total` | `DOUBLE` | Carga horária nacional em MWh |
| `market_price_pt` | `DOUBLE` | Preço day-ahead Portugal em €/MWh |
| `hora` | `INTEGER` | Hora do dia 0–23 |
| `dia_semana` | `INTEGER` | 0=Segunda … 6=Domingo |
| `is_weekend` | `BOOLEAN` | Sábado ou Domingo |
| `consumo_lag_1h` | `DOUBLE` | Consumo 1h antes |
| `consumo_lag_24h` | `DOUBLE` | Consumo 24h antes |
| `price_lag_1h` | `DOUBLE` | Preço 1h antes |
| `rolling_avg_consumo_24h` | `DOUBLE` | Média móvel 24h de consumo |
| `rolling_avg_price_24h` | `DOUBLE` | Média móvel 24h de preço |
| `process_date` | `DATE` | Data de execução do pipeline |
| `year` | `INTEGER` | Ano (partição) |
| `month` | `INTEGER` | Mês (partição) |

**Schema v1 — `iceberg.gold.feat_load_forecasting_api_hourly`**

Todas as colunas acima, mais:

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `consumo_next_hour` | `DOUBLE` | **TARGET ML** — consumo da hora seguinte via `LEAD` |

#### Regras de qualidade — Bronze (12 checks)

| ID | Regra | Critério | Nível |
| -- | ----- | -------- | ----- |
| B01–B04 | Nulos em `ts_utc` e colunas core (consumo e preço) | 0 nulos | FAIL |
| B05 | `total > 0` (MW positivo) | Valores negativos possíveis na API | WARN |
| B06 | Preço PT não-negativo | Preços negativos MIBEL são válidos | WARN |
| B07 | Unicidade `ts_utc` — consumo | Sem duplicados | WARN |
| B08 | Unicidade `ts_utc` — preço | Sem duplicados | FAIL |
| B09 | Freshness consumo | Atraso máx. 3 dias | WARN |
| B10 | Freshness preço | Atraso máx. 2 dias (day-ahead publica D-1) | WARN |
| B11–B12 | Completude diária consumo e preço | Dias com < 23 horas | WARN |

#### Regras de qualidade — Silver (4 checks)

| ID | Regra | Critério | Nível |
| -- | ----- | -------- | ----- |
| S01 | Nulos em `ts_utc` e `total_mwh` | null rate = 0% | FAIL |
| S02 | Unicidade `ts_utc` após deduplicação (consumo) | Sem duplicados | FAIL |
| S03 | Nulos em `ts_utc` e `price_portugal_eur_mwh` | null rate = 0% | FAIL |
| S04 | Unicidade `ts_utc` após deduplicação (preço) | Sem duplicados | FAIL |

#### Regras de qualidade — Gold (9 checks + 3 feature table)

| ID | Regra | Critério | Nível |
| -- | ----- | -------- | ----- |
| G01 | Unicidade `ts_utc` | `COUNT(*) = COUNT(DISTINCT ts_utc)` | FAIL |
| G02 | Nulos em `consumo_total` | null rate = 0% | FAIL |
| G03 | Nulos em `market_price_pt` | null rate = 0% | FAIL |
| G04 | `consumo_total >= 0` | Consumo não pode ser negativo | FAIL |
| G05 | `hora` BETWEEN 0 AND 23 | Slot horário válido | FAIL |
| G06 | `dia_semana` BETWEEN 0 AND 6 | Dia da semana válido | FAIL |
| G07 | Consistência `consumo_lag_1h` | `ABS(lag - consumo(t-1)) <= 0.001 MWh` (máx. 0.1% inconsistências) | FAIL |
| G08 | Taxa de junção consumo × preço | `>= 98%` das horas disponíveis | FAIL |
| G09 | Preços negativos | Contabilizados e reportados — não bloqueiam | WARN |
| F01 | Nulos em `consumo_next_hour` | null rate = 0% | FAIL |
| F02 | Nulos em features lag e rolling | Todos NOT NULL | FAIL |
| F03 | Paridade feat table vs dp | `COUNT(feat) >= COUNT(dp) - 48` | WARN |

**SLAs/SLOs**
- Atualização: até **T+45 min** após fecho da hora.
- Freshness máxima aceitável: **4 horas**.
- Disponibilidade da tabela via Trino: **≥ 99.0%**.
- Taxa de junção consumo×preço: **≥ 98.0%** das horas do período.
- Null rate em métricas core (`consumo_total`, `market_price_pt`): **0%**.

### 4) Estratégia de schema evolution/versionamento

- Versão contratual nos metadados da tabela Iceberg (`schema_version`, `product_version`).
- Sufixo `_api` distingue as tabelas do pipeline streaming das do pipeline estático (Static_Data).
- **Minor** (compatível): adicionar novas colunas nullable (ex.: novos lags/rolling) — incrementa `schema_version`, sem impacto nos consumidores existentes.
- **Major** (breaking): remover coluna, alterar tipo, alterar grão ou semântica — cria `_v2`, marca v1 com `deprecated=true`, janela de coexistência mínima de **30 dias**.
- `feat_load_forecasting_api_hourly` versionada independentemente (`feature_schema_version`) — qualquer alteração ao conjunto ou fórmula de features obriga a re-treino; cada run MLflow deve registar `feature_schema_version` nos tags.

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

## Demonstração de Qualidade de Dados — DP-03

> Esta secção documenta a actividade académica de **injeção controlada de dados sujos** na camada Bronze do DP-03 para evidenciar o papel de limpeza e validação da camada Silver.

### Motivação

Os dados que entram na Bronze **não chegam necessariamente válidos**:
- Pipelines CSV (DP-01, DP-02 Static) já aplicam quality flags no Python antes de escrever na Bronze — os dados chegam "marcados mas não limpos".
- Pipelines API (DP-02 Streaming, DP-03) têm validação mínima na ingestão — nulos são filtrados, mas intervalos físicos e duplicados não são verificados.

Em todos os casos, a Bronze preserva os dados tal como chegam. **A Silver é a camada de limpeza** — filtra, deduplica e valida antes de expor dados para Gold e ML.

### Ferramenta

**`02_medallion_pipeline/meteo_producao/01_bronze/data_quality_demo/corrupt_bronze.py`**

Script independente que injeta padrões de sujidade directamente nas tabelas Iceberg Bronze via Trino, sem modificar os scripts de ingestão de produção. Após corrupção, a re-execução do Silver demonstra o mecanismo de detecção e sinalização.

### Padrões de Sujidade Implementados

| Tipo | O que injeta | Como Silver responde |
|---|---|---|
| `nulls` | `temperature_2m = NULL` nas primeiras N linhas | `_quality_flag = 'null_values'` |
| `outofrange` | `temp=-50°C`, `radiation=-200 W/m²`, `cloud=150%` | `_quality_flag = 'out_of_range'` |
| `duplicates` | Cópias de timestamps com `_ingested_at=1970` | Silver deduplica via `ROW_NUMBER OVER (PARTITION BY ts_utc ORDER BY _ingested_at DESC)` — mantém o mais recente |
| `timestamps` | Timestamps com `minute=30` (não-horários) | Quality check S12 reporta FAIL em "alinhamento temporal" |

### Resultados Esperados (5% de corrupção, ~26 000 linhas Silver)

**Antes da corrupção:**
```
_quality_flag   | linhas  | %
----------------|---------|------
ok              | ~26 000 | 100%
null_values     | 0       | 0%
out_of_range    | 0       | 0%
```

**Após corrupção com `--type all --pct 5`:**
```
_quality_flag   | linhas  | %
----------------|---------|------
ok              | ~24 700 | ~95%
null_values     | ~650    | ~2.5%
out_of_range    | ~650    | ~2.5%
```

O quality check S13 (`pct_ok >= 95%`) passa a WARN (limiar exactamente na fronteira), e S06–S10 (intervalos físicos) passam a FAIL, bloqueando a promoção para Gold até restauro.

### Fluxo da Demo

```powershell
cd 02_medallion_pipeline/meteo_producao/01_bronze/data_quality_demo

# Corromper Bronze
python corrupt_bronze.py --dp meteo --type all --pct 5

# Reconstruir Silver a partir do Bronze corrompido
python corrupt_bronze.py --rerun-silver --dp meteo

# Verificar impacto no Trino
# SELECT _quality_flag, COUNT(*) FROM iceberg.silver.meteo_open_meteo_hourly GROUP BY 1;

# Restaurar Bronze ao estado original
python corrupt_bronze.py --restore --dp meteo
```

### Conclusão

A demo confirma que a arquitectura Medallion cumpre o seu propósito: Bronze actua como "landing zone" fiel à fonte (sem rejeições), e Silver garante que apenas dados validados (`_quality_flag = 'ok'`) chegam à Gold e ao pipeline ML. A Gold filtra explicitamente `WHERE _quality_flag = 'ok'`, pelo que dados corrompidos nunca contaminam os targets de treino.

---

## Critérios de aceitação transversais (grupo)

- Cada Data Product é rastreável até tabelas silver/bronze de origem.
- Todo consumidor (dashboard/API/ML) referencia explicitamente o contrato consumido.
- Quebras de contrato bloqueiam promoção para produção até correção.
- Métricas de qualidade e freshness são verificáveis via SQL e registadas no relatório técnico.
