# ML Pipeline

Flows de MLflow para treino e tracking de modelos de energia MIBEL.

---

## Flow: producao_consumo (Classificação de Défice)

Flow de MLflow para o domínio **producao_consumo**.

Lê dados de `iceberg.gold.dp_energia_balance_hourly` via Trino, cria features temporais e lags de consumo/produção/saldo/ratio, e treina um `RandomForestClassifier` para prever `flag_defice` na hora seguinte (`t+1`). Regista o modelo em `producao_consumo_defice_classifier`.

**Ficheiro:** `03_ml_pipeline/producao_consumo_mlflow_flow.py`

**Execução local:**

```bash
python -m pip install pandas==2.2.3 scikit-learn==1.6.1 trino==0.336.0 mlflow==3.10.1 boto3

export TRINO_HOST=localhost
export MLFLOW_TRACKING_URI=http://localhost:15000
export MLFLOW_S3_ENDPOINT_URL=http://localhost:9000
python 03_ml_pipeline/producao_consumo_mlflow_flow.py
```

**Execução via Flyte (remoto):**

```bash
pyflyte run --remote -p flytesnacks -d development \
  03_ml_pipeline/producao_consumo_mlflow_flow.py producao_consumo_training_wf
```

**Endpoints dentro de Flyte Sandbox tasks** (K3s isolado — usa `host.docker.internal`):

- MinIO S3: `http://host.docker.internal:9000`
- MLflow: `http://host.docker.internal:15000`
- Trino: `http://host.docker.internal:8080`
- Hive Metastore: `thrift://host.docker.internal:9083`

---

## Flow: preco_consumo (Load Forecasting)

Flow de MLflow para o domínio **preco_consumo**.

Lê dados de `iceberg.gold.feat_load_forecasting_hourly` via Trino, usa features de lag e médias móveis já pré-calculadas na camada gold, e treina um `GradientBoostingRegressor` para prever `consumo_next_hour` (MWh). Regista o modelo em `preco_consumo_load_forecaster`.

**Ficheiro:** `03_ml_pipeline/preco_consumo_mlflow_flow.py`

**Features utilizadas (pré-calculadas na gold):**

| Feature | Descrição |
| --- | --- |
| `consumo_total` | Consumo horário (MWh) |
| `market_price_pt` | Preço day-ahead PT (€/MWh) |
| `hora`, `dia_semana`, `is_weekend` | Features temporais |
| `consumo_lag_1h`, `consumo_lag_24h` | Lags de consumo |
| `price_lag_1h` | Lag de preço |
| `rolling_avg_consumo_24h`, `rolling_avg_price_24h` | Médias móveis 24h |

**Execução local:**

```bash
export TRINO_HOST=localhost
export MLFLOW_TRACKING_URI=http://localhost:15000
export MLFLOW_S3_ENDPOINT_URL=http://localhost:9000
python 03_ml_pipeline/preco_consumo_mlflow_flow.py
```

**Execução via Flyte (remoto):**

```bash
pyflyte run --remote -p flytesnacks -d development \
  03_ml_pipeline/preco_consumo_mlflow_flow.py \
  preco_consumo_training_wf
```

**Resultados obtidos (Run inicial):**

| Métrica | Valor |
| --- | --- |
| R² | 0.9829 |
| MAPE | 1.55% |
| MAE | 93.1 MWh |
| RMSE | 160.3 MWh |
