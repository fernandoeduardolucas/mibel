# 03 — ML Pipeline (Treino e Tracking de Modelos)

Workflows de Machine Learning para os três Data Products do projeto MIBEL. Cada script treina modelo(s) scikit-learn consumindo feature tables da camada Gold do lakehouse, e regista todos os parâmetros, métricas e artefactos no MLflow com rastreabilidade completa dos dados.

---

## Diagrama de Arquitetura

```
iceberg.gold.*  (feature tables — camada Gold do lakehouse)
       │
       │ Trino DBAPI (pandas.read_sql)
       ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Python ML Script  (@task Flyte / execução local)                  │
│                                                                     │
│  _load_gold_table()  →  _build_features()  →  temporal split       │
│        │                                           │                │
│        │                                  train / test (80/20)     │
│        │                                           │                │
│        └──── Pipeline sklearn ────────────────────►│                │
│               (Imputer + Scaler + Estimador)       │                │
│                                                    ▼                │
│                                          Métricas de avaliação      │
└────────────────────────────────────────────────────┬────────────────┘
                                                     │ mlflow.*
                                                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│  MLflow Tracking Server  (http://localhost:15000)                   │
│                                                                     │
│  Experimento → Run → params + metrics + tags + artefactos           │
│                       │                                             │
│                       ├── evaluation/classification_report.json     │
│                       ├── evaluation/feature_importances.json       │
│                       ├── metadata/feature_columns.json             │
│                       └── model/  (sklearn serializado)             │
│                                    │                                │
│                       Model Registry (registered_model_name)        │
└─────────────────────────────────────────────────────────────────────┘
                                                     │
                                                     ▼
                              Flyte Sandbox  (execução remota opcional)
                              @workflow → @task → container image
```

**Split temporal (sem shuffle):**
```
Dados ordenados por timestamp
│
├── 80% → Treino  (ts_train_start .. ts_train_end)
└── 20% → Teste   (ts_test_start  .. ts_test_end )
         logged como params no MLflow
```

---

## Estrutura de Ficheiros

```
03_ml_pipeline/
├── producao_consumo_mlflow_flow.py    DP-01: classificador de défice energético
├── preco_consumo_mlflow_flow.py       DP-02: load forecasting (consumo próxima hora)
└── meteo_producao_mlflow_flow.py      DP-03: previsão de produção + impacto no preço
```

---

## O Que Foi Implementado

### DP-01 — `producao_consumo_mlflow_flow.py`

**Problema:** prever se haverá défice energético na próxima hora (`flag_defice t+1`).

| Elemento | Detalhe |
|---|---|
| Feature table | `iceberg.gold.dp_energia_balance_hourly` |
| Target | `flag_defice` (t+1h) — binário |
| Estimador | `RandomForestClassifier` (300 árvores, max_depth=8, min_samples_leaf=3) |
| Features | `hora`, `dia_semana`, `mês` + lags 1h/2h/3h/6h/12h/24h de consumo, produção, saldo e ratio |
| Métricas | `accuracy`, `precision`, `recall`, `f1`, `roc_auc` |
| Artefactos | `classification_report.json`, `feature_columns.json`, `model/` |
| Experimento MLflow | `producao-consumo-defice` |
| Modelo registado | `producao_consumo_defice_classifier` |
| Workflow Flyte | `producao_consumo_training_wf` |

### DP-02 — `preco_consumo_mlflow_flow.py`

**Problema:** prever o consumo elétrico da próxima hora (load forecasting).

| Elemento | Detalhe |
|---|---|
| Feature table | `iceberg.gold.feat_load_forecasting_hourly` |
| Target | `consumo_next_hour` (regressão) |
| Estimador | `GradientBoostingRegressor` (300 iters, max_depth=5, lr=0.05, subsample=0.8) |
| Features | `consumo_total`, `market_price_pt`, `hora`, `dia_semana`, `is_weekend`, lags 1h/24h, rolling 24h |
| Métricas | `mae`, `rmse`, `r2`, `mape` |
| Artefactos | `feature_importances.json`, `feature_columns.json`, `model/` |
| Experimento MLflow | `preco-consumo-forecasting` |
| Modelo registado | `preco_consumo_load_forecaster` |
| Workflow Flyte | `preco_consumo_training_wf` |

### DP-03 — `meteo_producao_mlflow_flow.py`

**Problema:** dois modelos em sequência — produção a partir de meteorologia, e preço a partir de meteorologia + produção.

**Modelo A — Previsão de Produção:**

| Elemento | Detalhe |
|---|---|
| Feature table | `iceberg.gold.dp_meteo_producao_daily_features` |
| Target | `producao_total_daily_mwh` (regressão) |
| Estimador | `RandomForestRegressor` (300 árvores, max_depth=8, min_samples_leaf=3) |
| Features | 9 variáveis meteo + 3 temporais + 6 lags (temp, vento, radiação, produção, preço) |
| Métricas | `mae`, `rmse`, `r2`, `mape` |
| Experimento MLflow | `meteo-producao-producao-forecast` |
| Modelo registado | `meteo_producao_producao_forecaster` |

**Modelo B — Impacto no Preço Spot:**

| Elemento | Detalhe |
|---|---|
| Target | `preco_spot_medio_eur_mwh` (regressão) |
| Estimador | `GradientBoostingRegressor` (300 iters, max_depth=5, lr=0.05) |
| Features | 9 meteo + 3 temporais + 9 lags + `producao_total_daily_mwh` + `saldo_daily_mwh` |
| Experimento MLflow | `meteo-producao-preco-impact` |
| Modelo registado | `meteo_producao_preco_impact` |

Workflow Flyte (`meteo_producao_training_wf`) encadeia os dois modelos em sequência.

### Rastreabilidade de Dados por Versão de Modelo

Cada run MLflow regista:

```
Tags:
  domain      = "meteo_producao" | "energia" | ...
  dataset     = "iceberg.gold.<tabela>"   ← aponta para a feature table exata
  target      = "<coluna target>"
  model_type  = "RandomForestClassifier" | ...
  task        = "classification" | "regression"

Params:
  test_ratio, random_state, n_estimators, max_depth, ...
  n_train, n_test
  train_start, train_end    ← intervalo temporal do treino
  test_start,  test_end     ← intervalo temporal do teste
```

### Justificação das Decisões

| Decisão | Justificação |
|---|---|
| **Split temporal (sem shuffle)** | Séries temporais têm dependência temporal; shuffle introduz data leakage (o modelo veria o futuro durante o treino) |
| **Pipeline sklearn (Imputer + Scaler + Estimador)** | Garante que o mesmo pré-processamento é aplicado em treino e inferência; modelo serializado inclui os transformers |
| **RandomForest para classificação/regressão base** | Robusto a features correlacionadas, não requer normalização obrigatória, feature importances interpretáveis sem custo adicional |
| **GradientBoosting para séries temporais com lags** | Captura não-linearidades entre lags e target melhor do que RF em dados tabulares com estrutura temporal |
| **Feature tables na Gold** | Lags e rolling windows calculados uma vez em SQL e armazenados em Iceberg; evita recalcular em cada treino e garante consistência entre treino e inferência (serving) |
| **Stubs Flyte locais** | `try/except ModuleNotFoundError` permite executar localmente sem instalar o SDK Flyte; mesmo código funciona local e no cluster Flyte sem modificação |

---

## Passo a Passo de Execução

### Pré-requisitos

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install pandas==2.2.3 scikit-learn==1.6.1 trino==0.336.0 mlflow==3.10.1 boto3
```

Stack Docker a correr com dados nas tabelas Gold (ver `02_medallion_pipeline/README.md`).

### Treino Local

```powershell
# DP-01: classificador de défice
python 03_ml_pipeline/producao_consumo_mlflow_flow.py

# DP-02: load forecasting
python 03_ml_pipeline/preco_consumo_mlflow_flow.py

# DP-03: produção + preço (2 modelos em sequência)
python 03_ml_pipeline/meteo_producao_mlflow_flow.py
```

### Treino Remoto via Flyte

```powershell
# Instalar flytekit
pip install flytekit

# DP-01
pyflyte run --remote -p flytesnacks -d development \
  03_ml_pipeline/producao_consumo_mlflow_flow.py producao_consumo_training_wf

# DP-02
pyflyte run --remote -p flytesnacks -d development \
  03_ml_pipeline/preco_consumo_mlflow_flow.py preco_consumo_training_wf

# DP-03
pyflyte run --remote -p flytesnacks -d development \
  03_ml_pipeline/meteo_producao_mlflow_flow.py meteo_producao_training_wf
```

### Verificar Resultados

Abrir MLflow UI em <http://localhost:15000>:

- Experimentos registados: `producao-consumo-defice`, `preco-consumo-forecasting`, `meteo-producao-producao-forecast`, `meteo-producao-preco-impact`
- Model Registry: 4 modelos registados com versões

### Variáveis de Ambiente (se necessário)

```powershell
$env:TRINO_HOST = "localhost"           # default: host.docker.internal
$env:MLFLOW_TRACKING_URI = "http://localhost:15000"
$env:MLFLOW_S3_ENDPOINT_URL = "http://localhost:9000"
$env:AWS_ACCESS_KEY_ID = "minioadmin"
$env:AWS_SECRET_ACCESS_KEY = "minioadmin"
```
