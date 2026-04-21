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

Antes de correr localmente, instala as dependências (incluindo `mlflow`):

```bash
python -m pip install pandas==2.2.3 scikit-learn==1.6.1 trino==0.336.0 mlflow==3.10.1 boto3
```

```bash
python 03_ml_pipeline/producao_consumo_mlflow_flow.py
```

Se estiveres fora de Docker (a correr Python diretamente no host), podes precisar de configurar endpoints explicitamente:

```bash
export TRINO_HOST=localhost
export TRINO_PORT=8080
export MLFLOW_TRACKING_URI=http://localhost:15000
export MLFLOW_S3_ENDPOINT_URL=http://localhost:9000
```

## Execução com Flyte (remoto)

```bash
pyflyte run --remote -p flytesnacks -d development \
  03_ml_pipeline/producao_consumo_mlflow_flow.py producao_consumo_training_wf
```

## Configuração padrão usada no script

- Trino: `http://host.docker.internal:8080` (Flyte task pods)
- Tabela: `iceberg.gold.producao_vs_consumo_hourly`
- MLflow: `http://host.docker.internal:15000` (Flyte task pods)
- Experiência: `producao-consumo-defice`

### Endpoints a usar dentro de Flyte Sandbox tasks

Como as tasks Flyte correm num container/K3s isolado, usa `host.docker.internal` para aceder aos serviços publicados no host:

- MinIO S3 API: `http://host.docker.internal:9000`
- MLflow: `http://host.docker.internal:15000`
- Trino: `http://host.docker.internal:8080`
- Hive Metastore: `thrift://host.docker.internal:9083`

Se precisares, posso também criar um segundo flow para **re-treino agendado** + **promoção automática de versão no Model Registry**.
