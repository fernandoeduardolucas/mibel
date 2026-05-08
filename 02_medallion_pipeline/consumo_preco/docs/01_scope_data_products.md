# 1. Cenário de Negócio

A organização fictícia atua no setor energético em Portugal e pretende monitorizar a evolução do consumo elétrico nacional e a sua relação com o preço horário do mercado MIBEL, suportando análise operacional, reporting e previsão de consumo.

---

# 2. Datasets Usados

## Dataset 1 — Consumo elétrico nacional
- **Ficheiro:** `consumo-total-nacional.csv`
- **Fonte:** REN (Redes Energéticas Nacionais)
- **Granularidade original:** 15 minutos
- **Cobertura temporal:** 2023–2026
- **Nível de agregação:** nacional

## Dataset 2 — Preço horário MIBEL Portugal
- **Ficheiro:** `Day-ahead Market Prices_20230101_20260311.csv`
- **Fonte:** OMIE (Operador do Mercado Ibérico de Energia)
- **Granularidade original:** horária (horas 1–25)
- **Cobertura temporal:** 2023–2026
- **Nível de agregação:** mercado PT e ES

## Principais desafios de qualidade
- Granularidades temporais diferentes (15 min vs hora) — resolvido em Silver por agregação
- Necessidade de harmonização temporal para UTC canónico
- Numeração de horas OMIE começa em 1 (não em 0) — convertida em Silver
- Hora 25 em dias de mudança DST de outono — descartada em Silver (sem correspondência UTC direta)
- Possível existência de registos em falta ou inconsistências temporais — verificado pelos quality checks

---

# 3. Data Product 1

## Nome
`dp.energy_market_hourly`

## Tabela Iceberg
`iceberg.gold.dp_energy_market_hourly`

## Objetivo
Produto analítico horário que integra consumo elétrico nacional e preço horário MIBEL PT para análise descritiva e apoio à decisão.

## Consumidores
- Dashboard analítico Grafana
- API HTTP simples
- Analistas de negócio (query Trino)
- Base para feature table ML

## Perguntas analíticas
- Como evolui o consumo ao longo do tempo?
- Que relação existe entre consumo e preço horário?
- Que padrões diários e semanais se observam?
- Em que períodos ocorrem picos de consumo e preço?

## Grão
1 linha por `ts_utc` ao nível horário

## Chave de negócio
`ts_utc` — TIMESTAMP(6) WITH TIME ZONE, único e NOT NULL

## Métricas principais
- `consumo_total` (MWh)
- `market_price_pt` (€/MWh)

## SLAs resumo
- Frescura: dados disponíveis com atraso máximo de 24h
- Null rate em métricas críticas: < 1%
- Unicidade de `ts_utc` garantida por quality check FAIL

## Versão
v1 (`product_version=v1`, `schema_version=1`)

---

# 4. Data Product 2

## Nome
`feat.load_forecasting_hourly`

## Tabela Iceberg
`iceberg.gold.feat_load_forecasting_hourly`

## Objetivo
Feature table horária para treino de modelos de previsão de consumo.

## Consumidores
- Workflow de treino ML (`consumo_preco_mlflow_flow.py`)
- MLflow (experiment tracking e model registry)
- Dashboard Grafana (painel de monitorização da feature table)

## Perguntas analíticas / preditivas
- É possível prever o consumo da próxima hora com base em histórico e calendário?
- Que variáveis temporais e lags mais contribuem para a previsão?

## Grão
1 linha por `ts_utc` horário (sem nulos no target nem nas features)

## Chave de negócio
`ts_utc` — TIMESTAMP(6) WITH TIME ZONE, único e NOT NULL

## Target
`consumo_next_hour` — consumo da hora seguinte (LEAD 1h)

## Features (11)
`consumo_total`, `market_price_pt`, `hora`, `dia_semana`, `is_weekend`, `consumo_lag_1h`, `consumo_lag_24h`, `price_lag_1h`, `rolling_avg_consumo_24h`, `rolling_avg_price_24h`

## SLAs resumo
- `consumo_next_hour` NOT NULL — quality check FAIL
- Ausência de nulos em todas as features — quality check FAIL
- Unicidade de `ts_utc` — quality check FAIL

## Versão
v1 (`product_version=v1`, `schema_version=1`, `feature_schema_version=1`)

---

# 5. Convenção Temporal

O projeto adota UTC como tempo canónico para integração e consumo analítico dos dados. Os timestamps originais são preservados na camada Bronze sempre que aplicável, sendo convertidos para `ts_utc` na camada Silver. A camada Gold utiliza exclusivamente timestamps normalizados em UTC.

---

# 6. Estratégia de Schema Evolution e Versionamento

## Princípios gerais

O versionamento é gerido através de Iceberg table properties (`schema_version`, `product_version`, `feature_schema_version`, `deprecated`) e segue as regras abaixo.

## Tipos de alteração

| Tipo | Definição | Ação |
|------|-----------|------|
| **Additive / minor** | Adicionar nova coluna nullable, renomear em novo alias | Incrementar `schema_version`; consumidores existentes não são afetados |
| **Breaking / major** | Remover coluna, alterar tipo, alterar grão, alterar chave | Criar novo `product_version` (ex: v2); manter v1 marcada como `deprecated=true` durante período de transição |
| **Deprecação** | Coluna ou produto marcado para remoção futura | Marcar `deprecated=true` em table properties; comunicar consumidores; remover após período mínimo de 2 semanas |

## Procedimento de breaking change

1. Criar nova tabela com sufixo de versão (ex: `dp_energy_market_hourly_v2`)
2. Atualizar Iceberg table property `product_version=v2` na nova tabela
3. Manter tabela v1 com `deprecated=true`
4. Migrar consumidores gradualmente (API, dashboard, ML)
5. Remover tabela v1 após confirmação de migração completa

## Compatibilidade retroativa garantida

- Colunas adicionadas são sempre `nullable` inicialmente
- Nunca se altera o tipo de uma coluna existente em produção
- Nunca se altera o grão ou a chave de negócio dentro da mesma versão major

## Estado atual

| Tabela                           | schema_version | product_version | deprecated |
|----------------------------------|----------------|-----------------|------------|
| gold.dp_energy_market_hourly     | 1              | v1              | false      |
| gold.feat_load_forecasting_hourly| 1              | v1              | false      |
| silver.consumo_hourly            | 1              | —               | —          |
| silver.preco_hourly              | 1              | —               | —          |
| bronze.consumo_raw               | 1              | —               | —          |
| bronze.preco_raw                 | 1              | —               | —          |
