# 1. Objetivo

Este documento define a infraestrutura mínima do projeto para suportar o lakehouse, os data products, os workflows e o tracking de machine learning, com base em containers Docker.

---

# 2. Stack mínima adotada

A infraestrutura do projeto assenta nos seguintes componentes:

- **MinIO**: armazenamento de objetos para o lakehouse
- **Iceberg Catalog**: catálogo para gestão das tabelas Iceberg
- **Trino**: motor SQL para criação e consulta das tabelas
- **MLflow**: tracking de experiências e artefactos de machine learning
- **PostgreSQL**: backend metadata store do MLflow
- **Flyte**: orquestração dos workflows
- **Docker Compose**: mecanismo de execução local e reprodutível da stack

---

# 3. Serviços a levantar

## 3.1 MinIO
**Função:** armazenar ficheiros e tabelas do lakehouse em formato Parquet / Iceberg.

**Responsabilidades:**
- armazenar dados Bronze, Silver e Gold
- servir como object storage compatível com S3
- suportar persistência do lakehouse

---

## 3.2 Iceberg Catalog
**Função:** gerir metadados das tabelas Iceberg.

**Responsabilidades:**
- registar tabelas Bronze, Silver e Gold
- permitir criação e leitura das tabelas a partir do Trino
- suportar evolução de schema e operações do formato Iceberg

---

## 3.3 Trino
**Função:** motor SQL principal do projeto.

**Responsabilidades:**
- criar schemas e tabelas
- transformar dados entre Bronze, Silver e Gold
- executar queries de qualidade
- servir queries analíticas e de suporte à API

---

## 3.4 PostgreSQL
**Função:** backend relacional de suporte ao MLflow.

**Responsabilidades:**
- armazenar metadata das runs
- guardar parâmetros, métricas e referências a artefactos

---

## 3.5 MLflow
**Função:** tracking do workflow de machine learning.

**Responsabilidades:**
- registar experiências
- armazenar métricas, parâmetros e artefactos
- suportar reprodutibilidade do treino

---

## 3.6 Flyte
**Função:** orquestrar workflows do projeto.

**Responsabilidades:**
- workflow de ingestão Bronze
- workflow Bronze → Silver
- workflow Silver → Gold
- workflow de treino ML

---

# 4. Ordem de ativação da infraestrutura

A infraestrutura deverá ser ativada por fases, pela seguinte ordem:

## Fase 1
- MinIO
- Iceberg Catalog
- Trino

**Objetivo:** validar o lakehouse e o acesso SQL.

## Fase 2
- PostgreSQL
- MLflow

**Objetivo:** preparar tracking de machine learning.

## Fase 3
- Flyte

**Objetivo:** orquestrar os workflows já estabilizados.

---

# 5. Princípios de configuração

- a infraestrutura deve ser reproduzível localmente via Docker Compose
- os serviços devem comunicar através de uma rede Docker dedicada
- os dados devem ser persistidos em volumes
- as credenciais e configurações variáveis devem ficar em `.env`
- o lakehouse deve utilizar MinIO como object storage
- o Trino deve aceder ao catálogo Iceberg configurado sobre o object storage

---

# 6. Estrutura esperada na pasta infrastructure

## `infrastructure/docker/`
- `docker-compose.yml`
- `.env`

## `infrastructure/trino/catalog/`
- ficheiros de configuração do catálogo Iceberg

---

# 7. Validação mínima da infraestrutura

A infraestrutura considera-se validada quando for possível:

1. aceder ao MinIO
2. aceder ao Trino
3. criar um schema no catálogo Iceberg
4. criar uma tabela de teste
5. consultar essa tabela a partir do Trino
6. aceder ao MLflow
7. registar uma run de teste
8. preparar o ambiente para execução de workflows Flyte

---

# 8. Estratégia de implementação

A implementação da infraestrutura será incremental:

- primeiro validar armazenamento e SQL
- depois validar tracking ML
- só depois integrar orquestração

Esta abordagem reduz risco técnico e permite validar cada componente antes de aumentar complexidade.