# 1. Objetivo

Este documento define a estrutura de implementação do projeto e a ordem de execução dos componentes técnicos necessários à construção do lakehouse, data products, workflows e serving.

---

# 2. Estrutura do repositório

## Documentação
- `docs/`

## Infraestrutura
- `infrastructure/docker/`
- `infrastructure/trino/`

## SQL
- `sql/bronze/`
- `sql/silver/`
- `sql/gold/`
- `sql/quality/`
- `sql/serving/`

## Workflows
- `workflows/`

## Machine Learning
- `ml/`

## Aplicação / Serving
- `app/api/`
- `app/dashboard/`

## Dados de origem
- `data/raw/`

---

# 3. Ordem de implementação

## Fase 1 — Infraestrutura
1. preparar `docker-compose.yml`
2. subir serviços mínimos da stack
3. validar acesso ao catálogo e ao motor SQL

## Fase 2 — Bronze
1. criar schemas
2. criar tabelas Bronze
3. implementar ingestão dos ficheiros raw

## Fase 3 — Silver
1. implementar transformação de consumo para horário
2. implementar transformação de preço para horário
3. validar timestamps e unicidade

## Fase 4 — Gold
1. implementar `gold.dp_energy_market_hourly`
2. implementar `gold.feat_load_forecasting_hourly`
3. validar lags, rolling e target

## Fase 5 — Qualidade
1. criar queries de validação Bronze
2. criar queries de validação Silver
3. criar queries de validação Gold

## Fase 6 — Workflows
1. workflow ingestão Bronze
2. workflow Bronze → Silver
3. workflow Silver → Gold
4. workflow treino ML

## Fase 7 — Serving
1. adaptar API para ler Gold
2. expor queries finais
3. validar consistência com o data product

---

# 4. Princípios de implementação

- implementar por camadas e não por ficheiros isolados
- validar cada camada antes de avançar
- não misturar lógica de transformação com lógica de serving
- manter SQL, workflows e ML separados
- garantir que Gold só depende de Silver e nunca diretamente de Bronze

---

# 5. Critério de fecho desta fase

A fase de planeamento técnico considera-se concluída quando:
- a estrutura do repositório está criada
- as pastas principais existem
- a ordem de implementação está acordada
- o projeto está pronto para iniciar a implementação da infraestrutura