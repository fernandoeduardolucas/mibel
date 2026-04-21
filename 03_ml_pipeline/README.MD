# ML Pipeline

Flow de MLflow para o domínio **producao_consumo**.

## O que este flow faz

- Lê dados de `iceberg.gold.producao_vs_consumo_hourly` via Trino.
- Cria features temporais e lags de consumo/produção/saldo/ratio.
- Treina um `RandomForestClassifier` para prever `flag_defice` na hora seguinte (`t+1`).
- Regista métricas, parâmetros e artefactos no MLflow.
- Regista o modelo em `producao_consumo_defice_classifier`.

## Ficheiro principal

- `03_ml_pipeline/producao_consumo_mlflow_flow.py`

## Execução local

```bash
python 03_ml_pipeline/producao_consumo_mlflow_flow.py
```

## Execução com Flyte (remoto)

```bash
pyflyte run --remote -p flytesnacks -d development \
  03_ml_pipeline/producao_consumo_mlflow_flow.py producao_consumo_training_wf
```

## Configuração padrão usada no script

- Trino: `http://host.docker.internal:8080`
- Tabela: `iceberg.gold.producao_vs_consumo_hourly`
- MLflow: `http://host.docker.internal:15000`
- Experiência: `producao-consumo-defice`

Se precisares, posso também criar um segundo flow para **re-treino agendado** + **promoção automática de versão no Model Registry**.
