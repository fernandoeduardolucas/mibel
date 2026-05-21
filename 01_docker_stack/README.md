# 01 — Docker Stack (Infraestrutura Local)

Infraestrutura completa do projeto MIBEL em Docker Compose. Fornece o lakehouse local (MinIO + Hive + Trino + Iceberg), o tracking de modelos ML (MLflow), a observabilidade (Grafana) e o streaming (Redpanda), com o Flyte Sandbox como orquestrador externo de workflows.

---

## Diagrama de Arquitetura

```
┌──────────────────────────────────────────────────────────────────────┐
│                       Docker Compose Stack                           │
│                                                                      │
│  ┌──────────┐    ┌──────────────────┐    ┌───────────────────────┐  │
│  │  MinIO   │───►│  Hive Metastore  │───►│        Trino          │  │
│  │  (S3)    │    │  (MariaDB backend│    │  (query engine)       │  │
│  │:9000/9001│    │    :9083)        │    │  catalogs: iceberg,   │  │
│  └────┬─────┘    └──────────────────┘    │  hive, kafka, tpcds   │  │
│       │                                  └───────────────────────┘  │
│       │          ┌──────────────────┐    ┌───────────────────────┐  │
│       └─────────►│     MLflow       │    │       Grafana         │  │
│                  │  (Postgres       │    │  (dashboards :3300)   │  │
│                  │  backend :15000) │    └───────────────────────┘  │
│                  └──────────────────┘                               │
│                                         ┌───────────────────────┐  │
│                                         │      Redpanda         │  │
│                                         │  (Kafka-compat. :9092)│  │
│                                         └───────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
              ▲                    ▲                    ▲
              │    host ports      │                    │
┌──────────────────────────────────────────────────────────────────────┐
│             Flyte Sandbox (container externo — K3s interno)          │
│  Acede aos serviços via host.docker.internal:<porta>                 │
└──────────────────────────────────────────────────────────────────────┘
```

**Fluxo de dados:**
```
Fontes externas (CSVs / Open-Meteo API)
        │
        ▼
   MinIO (s3://warehouse/)
        │
        ▼ Hive external tables (Bronze)
   Trino ──► Iceberg managed tables (Silver → Gold)
        │
        ├──► MLflow (artefactos de modelos ML)
        ├──► Grafana (dashboards de qualidade e operacionais)
        └──► HTTP APIs (backends DP-01/02/03)
```

---

## Estrutura de Ficheiros

```
01_docker_stack/
├── docker-compose.yml              # Orquestrador principal (7 serviços)
├── hive/
│   └── conf/
│       └── metastore-site.xml      # Configuração Hive Metastore → MinIO S3
├── trino/
│   └── etc/
│       ├── config.properties       # Trino: coordinator, memória, discovery
│       ├── jvm.config              # JVM heap e GC settings
│       ├── node.properties         # Identificação do nó
│       ├── log.properties          # Nível de log por pacote
│       └── catalog/
│           ├── iceberg.properties  # Catálogo Iceberg (tabelas geridas)
│           ├── hive.properties     # Catálogo Hive (tabelas externas)
│           ├── kafka.properties    # Catálogo Kafka/Redpanda
│           ├── tpcds.properties    # Benchmark TPC-DS
│           └── tpch.properties     # Benchmark TPC-H
├── mlflow/
│   └── Dockerfile                  # Imagem MLflow + psycopg2 (Postgres)
├── flyte/
│   ├── Dockerfile                  # Patch sandbox Flyte (timeout Helm 30 min)
│   └── flyte-core-overrides.yaml   # Helm overrides para flyte-core
└── grafana/
    ├── provisioning/
    │   ├── dashboards.yml          # Auto-load de dashboards JSON
    │   └── trino.yml               # Datasource Trino (UID: trino-iceberg)
    └── dashboards/
        ├── producao_consumo_overview.json
        ├── consumo_preco_overview.json
        ├── meteo_producao_overview.json
        └── quality_observability.json  # Dashboard unificado de qualidade
```

---

## O Que Foi Implementado

### Serviços Docker Compose

| Serviço | Imagem | Função | Porta(s) |
|---|---|---|---|
| `minio` | `minio/minio` | Object storage S3-compatível (Bronze + artefactos ML) | `9000`, `9001` |
| `mc` | `minio/mc` | Bootstrap one-shot: cria buckets `warehouse` e `mlflow` | — |
| `metastore-db` | `mariadb:10.6` | Backend persistente do Hive Metastore | interno |
| `hive-metastore` | `apache/hive:4.0.0` | Serviço Thrift de metadados para Trino | `9083` |
| `trino` | `trinodb/trino:440` | Motor de query SQL distribuído | `8080` |
| `mlflow-db` | `postgres:15` | Backend persistente do MLflow | interno |
| `mlflow` | build local | Tracking server + artifact store (MinIO) | `15000` |
| `grafana` | `grafana/grafana:10.4.2` | Dashboards operacionais e de qualidade | `3300` |
| `redpanda` | `redpandadata/redpanda` | Broker Kafka-compatível para streaming | `9092` |

### Catálogos Trino

| Catálogo | Tipo | Função |
|---|---|---|
| `iceberg` | Iceberg REST | Tabelas geridas Silver e Gold (ACID, time-travel) |
| `hive` | Hive Metastore | Tabelas externas Bronze (Parquet no MinIO) |
| `kafka` | Kafka | Leitura de tópicos Redpanda |
| `tpcds` | TPC-DS | Dados de benchmark para testes de performance |
| `tpch` | TPC-H | Dados de benchmark para testes de performance |

### Grafana — Dashboards Provisionados

Quatro dashboards auto-carregados via provisioning (sem configuração manual):

- **DP-01 Overview** — produção vs consumo, saldo, flag de défice
- **DP-02 Overview** — preço spot, consumo, custo estimado
- **DP-03 Overview** — meteorologia, produção, correlações
- **Qualidade & Observabilidade** — 26 painéis unificados com null rates, freshness, duplicados e cobertura cross-DP para todos os DPs

### Justificação das Escolhas

| Decisão | Justificação |
|---|---|
| **MinIO como S3** | Compatibilidade nativa com o ecossistema Hadoop/Iceberg sem custos de cloud; API idêntica à AWS S3 |
| **Trino como query engine** | Suporte nativo a Iceberg (ACID, time-travel), federação multi-catálogo, separação entre storage e compute |
| **Iceberg para Silver/Gold** | Schema evolution sem rewrite de dados, partição eficiente, garantias ACID; o formato de facto para lakehouses modernos |
| **Hive para Bronze** | Tabelas externas são suficientes para raw data sem gestão de ACID; mantém o Parquet original intacto |
| **MLflow com Postgres + MinIO** | Postgres para metadata (runs, params, metrics) e MinIO para artefactos binários; stack 100% local sem dependências externas |
| **Grafana com Trino datasource** | Dashboards executam SQL direto sobre Iceberg; sem ETL adicional — a fonte de verdade é a tabela Gold |
| **Flyte externo** | O Flyte Sandbox inclui K3s interno incompatível com o network do Compose; isolamento intencional com acesso via `host.docker.internal` |

---

## Passo a Passo de Execução

### Pré-requisitos

- Docker Engine ≥ 20.10 e Docker Compose v2 (`docker compose`, não `docker-compose`)
- ≥ 8 GB RAM disponível para Docker
- ≥ 10 GB espaço livre em disco

### 1. Arrancar o Stack

```powershell
cd 01_docker_stack
docker compose up -d --build
```

O flag `--build` é necessário na primeira execução para construir a imagem MLflow customizada (`psycopg2`). Execuções subsequentes podem omiti-lo.

### 2. Verificar o Estado

```powershell
docker compose ps
```

Todos os serviços (exceto `mc`, que termina após criar os buckets) devem mostrar estado `running`.

### 3. Validar Trino

```powershell
docker compose exec trino trino --execute "SHOW CATALOGS;"
```

Resultado esperado: `iceberg`, `hive`, `kafka`, `system`, `tpcds`, `tpch`.

### 4. Validar MinIO

Abrir http://localhost:9001 (minioadmin/minioadmin) e confirmar a existência dos buckets `warehouse` e `mlflow`.

### 5. Validar MLflow

Abrir http://localhost:15000 — UI do MLflow sem erros.

### 6. Validar Grafana

Abrir http://localhost:3300 (admin/admin) → secção Dashboards → confirmar 4 dashboards carregados automaticamente.

### 7. Parar o Stack

```powershell
# Preserva volumes (dados persistentes)
docker compose down

# Reset completo (apaga todos os dados)
docker compose down -v
```

---

## Endpoints de Serviço

| Serviço | URL | Credenciais |
|---|---|---|
| Trino UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| MinIO S3 API | http://localhost:9000 | minioadmin / minioadmin |
| MLflow | http://localhost:15000 | — |
| Grafana | http://localhost:3300 | admin / admin |
| Hive Metastore | thrift://localhost:9083 | — |
| Redpanda | localhost:9092 | — |

---

## Troubleshooting

**`mc` termina imediatamente** — comportamento esperado. É um bootstrapper one-shot. Verificar logs: `docker compose logs mc`.

**Trino sem catálogos** — Hive Metastore pode não ter terminado de arrancar. Reiniciar Trino: `docker compose restart trino`.

**Flyte não alcança os serviços** — usar `host.docker.internal` como hostname (funciona nativamente no Docker Desktop Windows/macOS; em Linux adicionar `--add-host host.docker.internal:host-gateway` ao arrancar o Flyte Sandbox).
