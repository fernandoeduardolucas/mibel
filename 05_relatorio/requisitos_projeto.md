# Requisitos do Projeto Prático — TEAD 2025/2026

**UC:** Tecnologias Escaláveis para Análise de Dados  
**Curso:** Mestrado em Engenharia Informática  
**Peso na avaliação:** 60% da classificação final  
**Entrega:** 26 de maio de 2026, às 23:55 (Moodle)  
**Apresentação/Defesa:** 2 de junho de 2026  
**Grupos:** 3 elementos

---

## Estado de Implementação (validado em 2026-05-21)

### Stack Mínima Obrigatória

| Componente | Estado | Evidência |
| --- | --- | --- |
| MinIO + Iceberg (Parquet) | **IMPLEMENTADO** | `01_docker_stack/docker-compose.yml`, `trino/etc/catalog/iceberg.properties` |
| Trino | **IMPLEMENTADO** | `trino/etc/catalog/` (iceberg, hive, kafka, tpcds, tpch) |
| Flyte | **IMPLEMENTADO** | `flyte/Dockerfile`, `flyte-core-overrides.yaml`, `flyte-workflows/workflows_incremental.py` |
| MLflow | **IMPLEMENTADO** | `mlflow/Dockerfile`, `03_ml_pipeline/*_mlflow_flow.py` (3 pipelines) |
| Docker/containers | **IMPLEMENTADO** | `docker-compose.yml` (13 serviços), Dockerfiles |

### Tecnologias Adicionais (bonificação)

| Componente | Estado | Evidência |
| --- | --- | --- |
| Redpanda (Kafka-compatible) | **IMPLEMENTADO** | `docker-compose.yml`, `kafka.properties`, `kafka_config/` |
| Grafana (dashboards/BI) | **IMPLEMENTADO** | `grafana/dashboards/*.json` (3 dashboards), `grafana/provisioning/` |

### Entregáveis (A–F)

| Entregável | Estado | Evidência |
| --- | --- | --- |
| **A) Data Products** | **IMPLEMENTADO** | `relatorio.md`, `consumo_preco/docs/02_data_products_spec.md` — 3 DPs com grão, chaves, SLAs/SLOs, versionamento semântico |
| **B) Lakehouse** | **IMPLEMENTADO** | DDL SQL por camada em todos os 3 DPs; particionamento `['process_date']` (bronze), `['year','month']` (silver/gold) |
| **C) Pipelines** | **IMPLEMENTADO** | `flyte-workflows/workflows_incremental.py`, `producao_consumo/flyte_workflow.py`; parametrização temporal, idempotência |
| **D) Serving** | **IMPLEMENTADO** | Backends HTTP (porta 8081/8000/8083) + frontends React para os 3 DPs |
| **E) Qualidade** | **IMPLEMENTADO** | SQL checks por camada (`02_gold_quality_checks.sql`, `04_quality/sql/0{1,2,3}_*`); Grafana dashboards |
| **F) ML** | **IMPLEMENTADO** | 3 modelos treinados (RF, GB) com MLflow tracking; features da camada gold; integração Flyte |

### Bonificações de Pipelines (entregável C)

| Item | Estado |
| --- | --- |
| Parametrização temporal (`process_date`) | **SIM** — `workflows_incremental.py` |
| Idempotência | **SIM** — MERGE incremental implementado |
| Retries | **SIM** — configuração Flyte task |
| Backfill (intervalo de datas) | **SIM** — suporte por parâmetro de data |
| Otimizações (partitioning, materialização) | **SIM** — particionamento por data/ano/mês em todas as camadas |

---

## A. Destinatários

Estudantes inscritos na UC de TEAD que pretendam realizar parte da avaliação durante o período letivo.

---

## B. Objetivos

Simular o ciclo de vida de construção e operação de uma plataforma analítica escalável orientada a produtos de dados, cobrindo data engineering / analytics engineering / data platform e ML.

### Competências a desenvolver

- Analisar cenários e requisitos (volume/velocidade/variedade e não-funcionais como custo, fiabilidade, segurança e governança) e o seu impacto nas escolhas arquiteturais.
- Desenhar arquiteturas modernas (lake/warehouse/lakehouse, separação compute/storage) e selecionar tecnologias, justificando trade-offs (latência, custo, complexidade operacional, lock-in, compliance).
- Definir e aplicar **data contracts** (schemas, chaves, semântica, SLAs/SLOs), incluindo evolução/versionamento e desenho de camadas bronze/silver/gold orientadas ao consumo.
- Implementar **pipelines escaláveis** (batch e incremental, com noções de streaming quando relevante), aplicando estratégias de otimização (particionamento, redução de shuffle, mitigação de skew, materialização vs query-on-read) suportadas por evidência.
- **Orquestrar e operar** pipelines "production-grade" (ex.: Flyte), garantindo reprodutibilidade, idempotência, retries, backfills, parametrização temporal e rastreabilidade de execuções.
- Implementar um **ambiente containerizado** (Docker) e compreender princípios de deployment/configuração por ambientes, incluindo integração com práticas cloud/Kubernetes quando aplicável.
- Integrar práticas de **qualidade e observabilidade** (validação, testes, monitorização, linhagem e documentação), assegurando confiança e auditabilidade.
- **Servir e disponibilizar resultados** via SQL/marts/APIs e/ou dashboards, assegurando consistência semântica e governança de métricas.
- Integrar componentes de **ML/análise avançada** (opcional) no ciclo de vida da plataforma, garantindo reprodutibilidade, avaliação adequada e integração com o serving de outputs.

---

## C. Stack Tecnológica Mínima

| Componente | Tecnologia |
|---|---|
| Storage/Lakehouse | MinIO + Iceberg (Parquet) |
| Query/SQL | Trino |
| Orquestração | Flyte |
| ML Observability | MLflow |
| Execução | Docker/containers |

**ML Framework:** qualquer (Scikit-learn, H2O, TensorFlow, Keras), desde que integrável com a stack acima.

Alterações à stack são permitidas desde que **justificadas, reproduzíveis e previamente validadas** com o docente.

### Tecnologias adicionais (bonificação)

- Tecnologias de eventos e/ou streaming (ex.: Kafka, ZeroMQ)
- Tecnologias de visualização/dashboards/BI (ex.: Grafana, Metabase, Superset)

---

## D. Enunciado

### Escolha de datasets

- Mínimo de **2 a 3 conjuntos de dados**, independentes mas relacionados (mesmo domínio).
- Definir um **cenário** com uma organização hipotética cuja operação envolve os datasets escolhidos.
- Pelo menos **um desafio de qualidade** nos dados (valores em falta, duplicados, inválidos, …). Se os datasets não tiverem estes desafios, o grupo deve introduzi-los artificialmente.
- A escolha dos datasets e do tema deve ser **comunicada ao docente para validação** antes de iniciar o trabalho.

### Escolhas fundamentais

1. **Domínio** — definido pela escolha dos datasets.
2. **Perspetiva de apoio à decisão** — pelo menos uma (ex.: realizador vs. espetador no caso de filmes).

### Metodologia sugerida (CRISP-DM simplificada)

| Fase | Descrição |
|---|---|
| Business Understanding | Levantamento e análise de literatura; identificar fontes de dados relevantes e características do domínio |
| Data Understanding | Familiarização com os dados selecionados e fontes externas |
| Data Preparation (collect) | Aquisição de dados de fontes externas |
| Data Preparation (integrate) | Integração dos dados num dataset consolidado (se aplicável) |
| Data Processing and Analysis | Limpeza, conversões, feature engineering, visualizações descritivas |
| Modeling + Evaluation | Treino e avaliação de modelos de ML para pelo menos um problema preditivo |
| Reporting | Dashboard interativo + utilização em produção dos modelos desenvolvidos |

---

## Entregáveis

### A) Especificação de Data Products

Cada elemento do grupo define **no mínimo um data product** usando o template das aulas.

Por data product, definir obrigatoriamente:

- [ ] Perguntas analíticas, métricas e consumidores (dashboard, API, ML, …)
- [ ] Grão e chaves
- [ ] Contrato de dados (schema, SLAs/SLOs)
- [ ] Estratégia de schema evolution/versionamento (ex.: v1/v2 compatível)

**Avaliação:** utilidade para o negócio + clareza e adequação da especificação.

---

### B) Implementação do Lakehouse

Camadas bronze/silver/gold com **Iceberg**, armazenando os dados intermédios adequadamente.

Definir em conjunto:

- [ ] Formatos a utilizar (além do obrigatório Parquet) + políticas de particionamento e compaction
- [ ] Convenções de nomes e organização no armazenamento
- [ ] Catálogo e metadados essenciais (descrições e propriedades)

**Avaliação:** qualidade das políticas definidas + adesão a essas políticas na implementação.

---

### C) Pipelines de Processamento com Orquestração

Cada elemento do grupo implementa:

1. Queries em **Trino**, e/ou
2. Workflows em **Flyte**

**Avaliação:** compatibilidade com os data products definidos + complexidade adequada aos requisitos.

**Bonificações:**

- [ ] Otimizações: pruning/partitioning, reduzir shuffle, mitigar skew, materialização vs. query-on-read
- [ ] Parametrização temporal (ex.: `process_date`)
- [ ] Idempotência e retries
- [ ] Backfill (executar intervalo de datas)

---

### D) Serving

Sempre que os data products **não sejam visualmente verificáveis** (via dashboards ou MLflow), os resultados da camada gold devem ser expostos via:

- SQL (Trino) — tabela materializada ou query-on-read, de forma justificada, e/ou
- API simples desenvolvida pelo grupo

> Quando existem dashboards ou MLflow para visualizar/interagir com os data products, o serving adicional não é obrigatório.

---

### E) Qualidade e Observabilidade

Implementar verificações aplicáveis ao cenário:

- [ ] Schema checks (contrato)
- [ ] Null rates / ranges / uniqueness (onde aplicável)
- [ ] Evidências das validações (no mínimo via queries SQL)

**Bonificação:** métricas de qualidade/observabilidade apresentadas em dashboard tipo Grafana.

---

### F) Machine Learning

- [ ] Pelo menos um data product consumido por uma equipa de ML
- [ ] Pelo menos um **workflow de treino** de um modelo de ML
- [ ] Feature table consumida da camada **gold** do lakehouse
- [ ] Treino reprodutível
- [ ] Tracking de avaliação do modelo e artefactos produzidos (rastreabilidade de dados por versão de modelo)

---

## E. Relatório

- Máximo de **12 páginas**
- Conteúdo:
  - Contexto/cenário criado pelo grupo
  - Resumo de cada entregável
  - Foco nas decisões tomadas com impacto na qualidade/performance
  - Relevância (hipotética) para o negócio dos elementos desenvolvidos

---

## F. Realização

- Grupos de **3 elementos** (exceções a validar previamente com o docente)
- Constituição dos grupos comunicada ao docente **por e-mail** assim que definida
- Escolha do tema/datasets comunicada para **validação pelo docente** antes de iniciar
- Submissão por **um elemento** do grupo em nome do grupo
- **Entrega:** 26 de maio de 2026, 23:55 — Moodle
- **Apresentação:** 2 de junho de 2026 (presença obrigatória de todo o grupo)

---

## G. Critérios de Avaliação

```
NTP = (0.2·A + 0.2·B + 0.2·C + 0.1·D + 0.15·E + 0.15·F) × R × Ad
```

| Componente | Peso | Descrição |
|---|---|---|
| A | 20% | Especificação de Data Products |
| B | 20% | Implementação do Lakehouse |
| C | 20% | Pipelines de Processamento com Orquestração |
| D | 10% | Serving |
| E | 15% | Qualidade e Observabilidade |
| F | 15% | Machine Learning |
| R | multiplicador | Qualidade do relatório (1 = esperado; < 1 = deficiências; > 1 = acima do esperado) |
| Ad | multiplicador | Elementos adicionais de bonificação (1 = nenhum; > 1 = elementos com valor para o domínio) |
