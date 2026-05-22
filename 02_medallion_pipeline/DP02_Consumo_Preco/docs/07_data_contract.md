# Data Contracts — consumo_preco

Este documento define os contratos de dados formais para os produtos Gold do sub-projeto `consumo_preco`. Um contrato de dados especifica o acordo entre o produtor (pipeline de dados) e os consumidores (dashboard, API, ML) relativamente a schema, semântica, qualidade e disponibilidade.

---

# 1. Contrato: dp_energy_market_hourly v1

## Identificação

| Campo            | Valor |
|------------------|-------|
| Nome             | `dp_energy_market_hourly` |
| Versão           | v1 |
| Tabela Iceberg   | `iceberg.gold.dp_energy_market_hourly` |
| Domínio          | `consumo_preco` |
| Owner            | Equipa de Engenharia de Dados — setor energético PT |
| Estado           | Ativo (`deprecated=false`) |
| Data de criação  | 2025 |

## Grão e Chave

| Campo         | Valor |
|---------------|-------|
| Grão          | 1 linha por hora UTC |
| Chave primária | `ts_utc` (TIMESTAMP WITH TIME ZONE) |
| Unicidade     | `ts_utc` único e NOT NULL garantido por quality check |
| Cobertura     | 2023-01-01 00:00 UTC a presente |

## Schema v1

| Coluna                  | Tipo                     | Nullable | Descrição |
|-------------------------|--------------------------|----------|-----------|
| ts_utc                  | TIMESTAMP WITH TIME ZONE | NÃO      | Chave de negócio — timestamp UTC horário canónico |
| consumo_total           | DOUBLE                   | NÃO      | Consumo elétrico nacional horário (MWh) |
| market_price_pt         | DOUBLE                   | NÃO      | Preço day-ahead MIBEL Portugal (€/MWh) |
| hora                    | INTEGER                  | NÃO      | Hora do dia (0–23) derivada de ts_utc |
| dia_semana              | INTEGER                  | NÃO      | Dia da semana (0=Segunda … 6=Domingo) derivado de ts_utc |
| is_weekend              | BOOLEAN                  | NÃO      | True se dia_semana >= 5 |
| consumo_lag_1h          | DOUBLE                   | SIM      | Consumo na hora anterior (nulo nas primeiras observações) |
| consumo_lag_24h         | DOUBLE                   | SIM      | Consumo 24h antes (nulo nas primeiras 24 obs.) |
| price_lag_1h            | DOUBLE                   | SIM      | Preço na hora anterior (nulo nas primeiras obs.) |
| rolling_avg_consumo_24h | DOUBLE                   | SIM      | Média móvel consumo últimas 24h |
| rolling_avg_price_24h   | DOUBLE                   | SIM      | Média móvel preço últimas 24h |
| process_date            | DATE                     | NÃO      | Data lógica da execução do workflow |
| year                    | INTEGER                  | NÃO      | Ano (coluna de partição) |
| month                   | INTEGER                  | NÃO      | Mês (coluna de partição) |

## SLOs (Service Level Objectives)

| Dimensão           | Objetivo                                                   | Verificação |
|--------------------|-------------------------------------------------------------|-------------|
| Frescura           | Dados disponíveis com atraso máximo de 24h após ingestão   | Monitorização operacional |
| Null rate          | < 1% em `consumo_total` e `market_price_pt`                | `03_gold_checks.sql` — FAIL bloqueia promoção |
| Unicidade ts_utc   | 0 duplicados em toda a tabela                              | `03_gold_checks.sql` — FAIL bloqueia promoção |
| Range hora         | `hora` in [0, 23]                                          | `03_gold_checks.sql` — FAIL bloqueia promoção |
| Range dia_semana   | `dia_semana` in [0, 6]                                     | `03_gold_checks.sql` — FAIL bloqueia promoção |
| Consistência lag   | `consumo_lag_1h` coerente com t-1 (tol. 0.01 MWh, max 0.1% inconsistente) | `03_gold_checks.sql` — FAIL |
| Cobertura horária  | >= 23h/dia para o período processado                       | `03_gold_checks.sql` — WARN (registo apenas) |

## SLAs (Service Level Agreements)

| Dimensão        | Acordo |
|-----------------|--------|
| Disponibilidade | Tabela disponível para query via Trino >= 99% do tempo em horário de operação |
| Backfill        | Reprocessamento de qualquer partição `year`/`month` suportado sem perda de dados |
| Idempotência    | Múltiplas execuções do mesmo `process_date` produzem o mesmo resultado |
| Recuperação     | Em caso de falha (FAIL nos quality checks), a partição não é promovida e a execução é retentada até 2 vezes (Flyte retries) |

## Consumidores registados

| Consumidor          | Tipo        | Acesso |
|---------------------|-------------|--------|
| Grafana Dashboard   | Visualização | Trino datasource `trino-iceberg` |
| API HTTP            | Serving      | `TrinoConsumoPrecoService` → `/api/overview`, `/api/timeseries` |
| Analistas           | Exploração   | Query direta via Trino |
| feat_load_forecasting_hourly | Upstream | workflow `flyte_silver_to_gold.py` |

## Upstream

| Tabela                  | Relação |
|-------------------------|---------|
| `silver.consumo_hourly` | INNER JOIN por `ts_utc` |
| `silver.preco_hourly`   | INNER JOIN por `ts_utc` |

## Changelog

| Versão | Data | Alteração |
|--------|------|-----------|
| v1     | 2025 | Versão inicial — schema com 14 colunas, particionamento year/month |

---

# 2. Contrato: feat_load_forecasting_hourly v1

## Identificação

| Campo            | Valor |
|------------------|-------|
| Nome             | `feat_load_forecasting_hourly` |
| Versão           | v1 |
| Tabela Iceberg   | `iceberg.gold.feat_load_forecasting_hourly` |
| Domínio          | `consumo_preco` |
| Owner            | Equipa de ML — previsão de consumo |
| Estado           | Ativo (`deprecated=false`) |
| Data de criação  | 2025 |

## Grão e Chave

| Campo         | Valor |
|---------------|-------|
| Grão          | 1 linha por hora UTC (sem nulos no target nem nas features) |
| Chave primária | `ts_utc` (TIMESTAMP WITH TIME ZONE) |
| Unicidade     | `ts_utc` único e NOT NULL garantido por quality check |

## Schema v1

| Coluna                  | Tipo                     | Nullable | Papel   | Descrição |
|-------------------------|--------------------------|----------|---------|-----------|
| ts_utc                  | TIMESTAMP WITH TIME ZONE | NÃO      | Chave   | Timestamp UTC canónico da hora |
| consumo_total           | DOUBLE                   | NÃO      | Feature | Consumo atual (MWh) |
| market_price_pt         | DOUBLE                   | NÃO      | Feature | Preço day-ahead atual (€/MWh) |
| hora                    | INTEGER                  | NÃO      | Feature | Hora do dia (0–23) |
| dia_semana              | INTEGER                  | NÃO      | Feature | Dia da semana (0–6) |
| is_weekend              | BOOLEAN                  | NÃO      | Feature | Indicador fim de semana |
| consumo_lag_1h          | DOUBLE                   | NÃO      | Feature | Consumo na hora anterior |
| consumo_lag_24h         | DOUBLE                   | NÃO      | Feature | Consumo 24h antes |
| price_lag_1h            | DOUBLE                   | NÃO      | Feature | Preço na hora anterior |
| rolling_avg_consumo_24h | DOUBLE                   | NÃO      | Feature | Média móvel consumo 24h |
| rolling_avg_price_24h   | DOUBLE                   | NÃO      | Feature | Média móvel preço 24h |
| consumo_next_hour       | DOUBLE                   | NÃO      | TARGET  | Consumo da hora seguinte — alvo de previsão |
| process_date            | DATE                     | NÃO      | Meta    | Data lógica da execução |
| year                    | INTEGER                  | NÃO      | Partição | Ano |
| month                   | INTEGER                  | NÃO      | Partição | Mês |

## SLOs

| Dimensão              | Objetivo                                                         | Verificação |
|-----------------------|-------------------------------------------------------------------|-------------|
| Null target           | `consumo_next_hour` NOT NULL em 100% dos registos                | `03_gold_checks.sql` — FAIL |
| Null features         | 0 nulos em todas as 11 features                                  | `03_gold_checks.sql` — FAIL |
| Unicidade ts_utc      | 0 duplicados em toda a tabela                                    | `03_gold_checks.sql` — FAIL |
| Paridade com dp       | Diferença <= 48 linhas vs `dp_energy_market_hourly`              | `03_gold_checks.sql` — WARN |
| Consistência lag      | `consumo_lag_1h` coerente com t-1 (tol. 0.01 MWh, max 0.1%)    | `03_gold_checks.sql` — FAIL |
| Rastreabilidade ML    | Partição `year`/`month` identificável nos artefactos MLflow      | Garantido por particionamento Iceberg |

## SLAs

| Dimensão        | Acordo |
|-----------------|--------|
| Reprodutibilidade | O mesmo subset de dados (`year`/`month`) deve produzir o mesmo conjunto de features e target |
| Rastreabilidade | Cada run de treino ML deve registar em MLflow os intervalos temporais de dados usados |
| Estabilidade    | O schema das features (nomes, tipos, ordem) só muda com incremento de `feature_schema_version` |
| Backfill        | Reprocessamento de qualquer partição suportado sem perda de dados |

## Consumidores registados

| Consumidor                    | Tipo       | Acesso |
|-------------------------------|------------|--------|
| `consumo_preco_mlflow_flow.py` | ML training | Trino DBAPI → `iceberg.gold.feat_load_forecasting_hourly` |
| MLflow                        | Tracking   | Registo de runs, métricas e artefactos |
| Grafana (painel ML)           | Monitoring | Painel "Feature Table ML" em `consumo_preco_overview.json` |

## Upstream

| Tabela                       | Relação |
|------------------------------|---------|
| `gold.dp_energy_market_hourly` | Origem direta — LEAD + filtro de nulos |

## Changelog

| Versão | feature_schema_version | Data | Alteração |
|--------|------------------------|------|-----------|
| v1     | 1                      | 2025 | Versão inicial — 11 features + 1 target, particionamento year/month |

---

# 3. Estratégia de Schema Evolution

## Princípio geral

Toda a evolução de schema é gerida através de Iceberg table properties (`schema_version`, `product_version`, `feature_schema_version`, `deprecated`) e documentada neste changelog. O Iceberg format_version=2 suporta nativamente a adição de colunas e evolução de tipos compatíveis.

## Regras por tipo de alteração

### Alteração additive (minor) — sem breaking change

**Exemplos:** adicionar nova coluna nullable, adicionar nova métrica derivada

**Procedimento:**
1. Adicionar coluna nullable à tabela Iceberg via `ALTER TABLE ... ADD COLUMN`
2. Incrementar `schema_version` nas table properties
3. Atualizar este documento e `05_gold_schema.md`
4. Consumidores existentes não são afetados (coluna nova é nullable)

### Alteração breaking (major) — incompatível com consumidores atuais

**Exemplos:** remover coluna, alterar tipo, alterar grão, alterar chave de negócio, alterar semântica de coluna existente

**Procedimento:**
1. Criar nova tabela com sufixo de versão (ex: `dp_energy_market_hourly_v2`)
2. Definir `product_version=v2` na nova tabela
3. Manter tabela v1 com `deprecated=true` em table properties
4. Comunicar todos os consumidores registados (ver secção "Consumidores registados")
5. Migrar consumidores de forma gradual e verificada
6. Remover tabela v1 apenas após confirmação de migração completa de todos os consumidores
7. Período mínimo de coexistência v1/v2: 2 semanas

### Deprecação de feature (feat table)

**Exemplos:** remover feature obsoleta, substituir feature por versão melhorada

**Procedimento:**
1. Incrementar `feature_schema_version`
2. Se a alteração for breaking para modelos treinados: criar nova tabela com sufixo de versão
3. Modelos treinados com feature_schema_version antiga devem ser re-treinados
4. Registar no MLflow a `feature_schema_version` usada em cada run de treino

## Compatibilidade retroativa garantida (dentro da mesma versão major)

- Colunas adicionadas são sempre `nullable` inicialmente
- Tipos de colunas existentes nunca são alterados em produção
- Grão e chave de negócio (`ts_utc`) são imutáveis dentro de v1
- Semântica das colunas existentes não é alterada

## Estado atual de todas as tabelas

| Tabela                            | schema_version | product_version | deprecated |
|-----------------------------------|----------------|-----------------|------------|
| `gold.dp_energy_market_hourly`    | 1              | v1              | false      |
| `gold.feat_load_forecasting_hourly` | 1            | v1              | false      |
| `silver.consumo_hourly`           | 1              | —               | —          |
| `silver.preco_hourly`             | 1              | —               | —          |
| `bronze.consumo_raw`              | 1              | —               | —          |
| `bronze.preco_raw`                | 1              | —               | —          |
