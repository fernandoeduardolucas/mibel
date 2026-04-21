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
python 02_medallion_pipeline/flyte_workflow.py
```

## Execução com Flyte

```bash
pyflyte run --remote -p flytesnacks -d development \
  02_medallion_pipeline/flyte_workflow.py medallion_full_wf
```

## Parâmetros do workflow

- `run_producao_consumo` (bool): corre pipeline produção vs consumo.
- `run_consumo_preco` (bool): corre pipeline consumo vs preço.
- `build_images` (bool): propaga `--build` para os runners.

Exemplo para correr só `consumo_preco`:

```bash
pyflyte run --remote -p flytesnacks -d development \
  02_medallion_pipeline/flyte_workflow.py medallion_full_wf \
  --run_producao_consumo False --run_consumo_preco True
```

## Próximos passos recomendados

1. Separar Bronze/Silver/Gold em tasks Flyte individuais.
2. Publicar outputs intermédios como artefactos Flyte.
3. Adicionar schedule (`LaunchPlan`) para execução periódica.
4. Adicionar alertas para falhas e checks de qualidade.
