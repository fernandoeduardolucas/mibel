# Medallion Pipeline com Flyte

Este diretório já tinha dois runners batch independentes:

- `producao_consumo/run_medallion_pipeline.py`
- `consumo_preco/run_medallion_consumo_precos.py`

Agora também inclui uma camada de orquestração Flyte:

- `flyte_workflow.py`

## Estratégia de conversão para Flyte

A conversão foi feita por **encapsulamento progressivo** (sem reescrever tudo de uma vez):

1. Manter os runners atuais (compatibilidade e baixo risco).
2. Expor cada runner como `@task` Flyte.
3. Compor as tasks num `@workflow` único (`medallion_full_wf`).
4. Evoluir gradualmente para tasks nativas (Python + SQL) quando necessário.

## Pré-requisitos

- Docker Engine ativo.
- Stack em `01_bootstrap/tead_2.0_v1.2/docker-compose.yml` funcional.
- Python com Flyte instalado para execução com `pyflyte`.

## Execução local (sem Flyte remote)

```bash
python 02_medallion_pipeline/producao_consumo/flyte_workflow.py
```

## Execução com Flyte

```bash
pyflyte run --remote -p flytesnacks -d development \
  02_medallion_pipeline/producao_consumo/flyte_workflow.py medallion_full_wf
```

## Parâmetros do workflow

- `run_producao_consumo` (bool): corre pipeline produção vs consumo.
- `run_consumo_preco` (bool): corre pipeline consumo vs preço.
- `build_images` (bool): propaga `--build` para os runners.
- `producao_consumo_backfill_days` (int): reprocessamento simples (reruns idempotentes) da pipeline `producao_consumo`.

Exemplo para correr só `consumo_preco`:

```bash
pyflyte run --remote -p flytesnacks -d development \
  02_medallion_pipeline/producao_consumo/flyte_workflow.py medallion_full_wf \
  --run_producao_consumo False --run_consumo_preco True
```

Exemplo com backfill simples para `producao_consumo`:

```bash
pyflyte run --remote -p flytesnacks -d development \
  02_medallion_pipeline/producao_consumo/flyte_workflow.py medallion_full_wf \
  --run_producao_consumo True --run_consumo_preco False \
  --producao_consumo_backfill_days 3
```

## Estado das etapas (produção_consumo)

Checklist rápido para validação do projeto:

1. **Etapa 1 — Stack operacional:** Docker Compose + MinIO + Trino (catálogos hive/iceberg) + MLflow + Flyte.
2. **Etapa 2 — Definições fechadas:** cenário, perspetiva, datasets e data products.
3. **Etapa 3 — Bronze:** ingestão de ficheiros e tabelas raw em Iceberg.
4. **Etapa 4 — Silver:** limpeza, normalização temporal, harmonização geográfica e validações.
5. **Etapa 5 — Gold:** tabelas finais de negócio + feature table para ML.
6. **Etapa 6 — Flyte:** workflows parametrizados, retries e backfill simples.

Neste momento, a Etapa 6 inclui:
- Workflow parametrizado (`medallion_full_wf`).
- Retry automático na task de backfill (`@task(retries=2)`).
- Backfill simples para `producao_consumo` por reruns idempotentes (`producao_consumo_backfill_days`).

## Próximos passos recomendados

1. Separar Bronze/Silver/Gold em tasks Flyte individuais.
2. Publicar outputs intermédios como artefactos Flyte.
3. Adicionar schedule (`LaunchPlan`) para execução periódica.
4. Adicionar alertas para falhas e checks de qualidade.
