# Relatório do Medallion Pipeline — `producao_consumo`

## 1) Objetivo do pipeline

O pipeline Medallion em `02_medallion_pipeline/producao_consumo` foi desenhado para transformar dados brutos de **consumo** e **produção** energética em um produto analítico confiável para decisão operacional e de negócio.

Do ponto de vista de negócio, o objetivo final é disponibilizar uma visão horária da relação entre produção e consumo, incluindo:
- saldo energético por hora;
- razão produção/consumo;
- identificação de horas com défice (produção < consumo) e excedente (produção > consumo).

Este produto final é a tabela Gold `iceberg.gold.producao_vs_consumo_hourly`.

---

## 2) Contexto de negócio

A análise conjunta de consumo e produção permite responder perguntas críticas, por exemplo:
- Em que horas o sistema entra em défice energético?
- Qual a dependência da componente PRE face à DGM na produção?
- Há tendência de desbalanceamento entre geração e procura ao longo do tempo?

Com isso, o pipeline suporta:
- monitorização da suficiência de produção;
- apoio a planeamento operacional e energético;
- criação de base estável para dashboards e modelos preditivos.

---

## 3) Fontes de dados (datasets de entrada)

Os datasets de entrada ficam em `01_bronze/data/raw/`:

1. `consumo-total-nacional.csv`
   - granularidade de 15 minutos;
   - componentes de consumo: BT, MT, AT, MAT;
   - total agregado de consumo.

2. `energia-produzida-total-nacional.csv`
   - granularidade de 15 minutos;
   - componentes de produção: DGM e PRE;
   - total agregado de produção.

### 3.1) Cobertura observada nos ficheiros raw

Com validação direta dos CSV:
- Consumo: **111.085** linhas de dados (111.086 linhas no ficheiro contando cabeçalho), de **2023-01-01 00:00:00 UTC** a **2026-03-04 00:00:00 UTC**;
- Produção: **109.933** linhas de dados (109.934 linhas no ficheiro contando cabeçalho), de **2023-01-01 00:00:00 UTC** a **2026-02-19 00:00:00 UTC**.

Também foi observado nos dados raw:
- 24 timestamps duplicados em cada dataset (48 linhas marcadas como duplicadas);
- 12 linhas com total zero em cada dataset;
- sem totais inválidos no parsing básico inicial.

---

## 4) Arquitetura utilizada

A solução segue o padrão **Medallion** em três camadas e usa stack lakehouse com MinIO + Hive + Iceberg + Trino.

### 4.1) Componentes

- **Python/Pandas**: limpeza e normalização na Bronze;
- **MinIO (S3-compatible)**: armazenamento raw e clean em objetos;
- **Hive (external tables)**: leitura de CSV raw e Parquet clean;
- **Iceberg (managed tables)**: tabelas versionáveis Bronze/Silver/Gold;
- **Trino**: engine SQL para materialização e validações;
- **Runner único** (`run_medallion_pipeline.py`): orquestra pipeline end-to-end.

### 4.2) Fluxo macro

1. Ingestão raw em `s3://warehouse/bronze/raw/...`.
2. Limpeza com Python e escrita de Parquet clean.
3. Criação de tabelas raw/stage (Hive) e materialização Bronze (Iceberg).
4. Regras de qualidade + deduplicação determinística para Silver.
5. Agregação horária e cálculo de KPIs na Gold.

---

## 5) Camada Bronze (qualidade técnica + rastreabilidade)

A Bronze foi desenhada para manter auditabilidade, evitando perda precoce de informação.

### 5.1) Regras principais aplicadas no Python

No script `bronze_clean_upload.py`:
- normalização de cabeçalhos (incluindo remoção de BOM);
- parsing de `datahora` para `timestamp_utc`;
- casting numérico das métricas;
- criação de flags de qualidade:
  - `flag_bad_timestamp`
  - `flag_bad_date`
  - `flag_bad_total`
  - `flag_zero_row`
  - `flag_duplicate_timestamp`
- cálculo de `duplicate_count` e `duplicate_rank`.

### 5.2) Estratégia de duplicados

Na Bronze, duplicados **não são removidos**. Em vez disso, são marcados e ordenados (ranking estável), preservando histórico técnico para auditoria e posterior seleção na Silver.

### 5.3) Estrutura de armazenamento na Bronze

- `hive.bronze_raw.*` → CSV raw externo
- `hive.bronze_stage.*` → Parquet clean externo
- `iceberg.bronze.*` → tabelas Bronze geridas

Esta separação reduz acoplamento e melhora rastreabilidade da transformação.

---

## 6) Camada Silver (registo confiável por 15 minutos)

A Silver transforma o histórico técnico da Bronze em dados analíticos limpos e consistentes.

### 6.1) Regras de qualidade Silver

Aplicadas nas duas tabelas (`consumo_total_nacional_15min` e `energia_produzida_total_nacional_15min`):
- excluir linhas com `flag_bad_timestamp = true`;
- excluir linhas com `flag_bad_total = true`;
- deduplicar por `timestamp_utc` com `ROW_NUMBER()` e critério determinístico.

### 6.2) Critério de desempate (deduplicação)

Para timestamps duplicados, a Silver seleciona a melhor linha com ordenação por:
1. priorizar não-zero (`flag_zero_row`);
2. maior total (`consumo_total_kwh` / `producao_total_kwh`);
3. menor `duplicate_rank` da Bronze;
4. ingestão mais recente.

### 6.3) Consistência entre componentes e total

A Silver calcula:
- soma dos componentes (`*_componentes_kwh`);
- diferença para o total (`diff_componentes_total_kwh`);
- flag de mismatch com tolerância técnica (`> 0.001`).

Isto gera maior confiabilidade analítica sem perder colunas de auditoria herdadas da Bronze.

---

## 7) Camada Gold (produto de negócio final)

A Gold entrega a visão executiva de produção vs consumo por hora.

### 7.1) Produto final

Tabela: `iceberg.gold.producao_vs_consumo_hourly`

Granularidade: **1 linha por hora (UTC)**.

### 7.2) Transformações

- agregação horária (`date_trunc('hour', timestamp_utc)`) das tabelas Silver;
- soma das métricas por hora;
- `FULL OUTER JOIN` entre consumo e produção para preservar horas com ausência em uma das fontes;
- cálculo de indicadores finais.

### 7.3) KPIs e flags da Gold

- `consumo_total_kwh`
- `producao_total_kwh`
- `producao_dgm_kwh`
- `producao_pre_kwh`
- `saldo_kwh = producao_total_kwh - consumo_total_kwh`
- `ratio_producao_consumo`
- `flag_defice`
- `flag_excedente`
- `flag_missing_source`

Assim, a Gold converte dados técnicos em informação de negócio diretamente utilizável.

---

## 8) Orquestração e execução end-to-end

O ficheiro `run_medallion_pipeline.py` executa o pipeline completo:
1. sobe o `docker compose` da stack;
2. cria/usa virtualenv local da pipeline;
3. instala dependências da Bronze;
4. corre limpeza + upload dos datasets;
5. executa SQL Bronze, Silver e Gold no Trino;
6. realiza validação rápida (`COUNT(*)`) da Gold.

Benefícios:
- repetibilidade;
- menor erro manual;
- execução padronizada em diferentes ambientes.

---

## 9) Resultado final (visão consolidada)

Do ponto de vista funcional, o pipeline entrega:
- rastreabilidade total do raw até Gold;
- governança de qualidade por flags e regras explícitas;
- dataset final pronto para consumo analítico e reporting energético;
- base sólida para extensões futuras (forecasting, alerting, otimização de despacho, etc.).

Em resumo:
- **Bronze** preserva e audita;
- **Silver** qualifica e consolida;
- **Gold** traduz em KPI de negócio.

---

## 10) Recomendações de evolução

1. Definir SLA de atualização (ex.: batch horário ou intradiário).
2. Acrescentar testes automáticos de qualidade (dbt tests ou SQL checks versionados).
3. Publicar dicionário de dados da Gold para utilizadores de negócio.
4. Criar dashboard operacional com alertas de défice/excedente.
5. Evoluir para partições por data/hora em Silver/Gold para ganho de performance.

---

## 11) Conclusão

O pipeline Medallion de `producao_consumo` está estruturado com boas práticas de engenharia de dados: separação de responsabilidades por camada, regras explícitas de qualidade, deduplicação determinística e modelação final orientada a decisão de negócio.

A arquitetura escolhida (MinIO + Hive + Iceberg + Trino + Python) é coerente para um lakehouse analítico e suporta evolução incremental com governança e escalabilidade.
