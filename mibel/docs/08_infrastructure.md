# 1. Objetivo

Este documento define a infraestrutura mínima do projeto para suportar o lakehouse, os data products, os workflows e o tracking de machine learning, com base em containers Docker.

---

# 2. Arquitetura Geral

A arquitetura do projeto está dividida em dois blocos principais:

## 2.1 Docker Compose Stack (Data Platform)

Responsável pela plataforma de dados:

- MinIO (object storage)
- Hive Metastore + base de dados
- Trino (motor SQL)
- MLflow + PostgreSQL

## 2.2 Flyte Sandbox (Orquestração)

Executado como container independente fora do Docker Compose:

- Flyte Sandbox com K3s interno
- Responsável pela execução de workflows
- Comunica com a stack via endpoints expostos no host

---

# 3. Stack adotada

## 3.1 Serviços no Docker Compose

- **MinIO**: armazenamento S3-compatible
- **MC (MinIO Client)**: criação automática de buckets
- **MariaDB**: base de dados do Hive Metastore
- **Hive Metastore**: catálogo de metadados
- **Trino**: motor SQL
- **PostgreSQL**: backend do MLflow
- **MLflow**: tracking de machine learning

---

## 3.2 Serviço externo

- **Flyte Sandbox**
  - execução de workflows
  - cluster Kubernetes interno (K3s)
  - corre fora do docker-compose

---

# 4. Serviços detalhados

## 4.1 MinIO
- armazenamento de dados Bronze, Silver e Gold
- buckets:
  - `warehouse`
  - `mlflow`

## 4.2 MC (bootstrap)
- cria automaticamente os buckets
- execução única (termina após sucesso)

## 4.3 Hive Metastore
- catálogo de tabelas Iceberg/Hive
- usa MariaDB como backend

## 4.4 Trino
- execução de queries SQL
- criação de tabelas Iceberg
- transformação entre camadas

## 4.5 PostgreSQL
- metadata store do MLflow

## 4.6 MLflow
- tracking de modelos ML
- armazenamento de artefactos no MinIO

## 4.7 Flyte Sandbox
- orquestração dos workflows
- execução isolada em K3s
- acesso à stack via host

---

# 5. Conectividade Flyte ↔ Stack

Os workflows executados em Flyte comunicam com os serviços através do host:

| Serviço | Endpoint |
|--------|--------|
| MinIO | http://host.docker.internal:9000 |
| MLflow | http://host.docker.internal:15000 |
| Trino | http://host.docker.internal:8080 |
| Hive Metastore | thrift://host.docker.internal:9083 |

Credenciais:
- Access Key: `minioadmin`
- Secret Key: `minioadmin`

Variáveis necessárias em Flyte: