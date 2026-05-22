# ML Pipeline — Previsão de Consumo Horário (consumo_preco)

## 1. Contexto

A feature table `iceberg.gold.feat_load_forecasting_hourly` contém dados históricos horários prontos para treino supervisionado. Este documento especifica o pipeline de machine learning que consome esta tabela para treinar e registar um modelo de previsão de consumo elétrico horário.

O script a implementar é: `03_ml_pipeline/consumo_preco_mlflow_flow.py`

---

## 2. Problema

**Tipo:** Regressão supervisionada  
**Target:** `consumo_next_hour` — consumo elétrico nacional na hora seguinte (MWh)  
**Pergunta de negócio:** Dado o consumo atual, o preço de mercado e o histórico recente, qual será o consumo na próxima hora?

---

## 3. Feature Table

**Tabela:** `iceberg.gold.feat_load_forecasting_hourly`  
**Acesso:** Trino DBAPI → `host.docker.internal:8080` (dentro de container) ou `localhost:8080` (local)

### Features (11)

| Feature                 | Tipo    | Categoria  | Descrição |
|-------------------------|---------|------------|-----------|
| hora                    | INTEGER | Temporal   | Hora do dia (0–23) |
| dia_semana              | INTEGER | Temporal   | Dia da semana (0–6) |
| is_weekend              | BOOLEAN | Temporal   | Indicador fim de semana |
| consumo_total           | DOUBLE  | Métrica    | Consumo atual (MWh) |
| market_price_pt         | DOUBLE  | Métrica    | Preço day-ahead atual (€/MWh) |
| consumo_lag_1h          | DOUBLE  | Lag        | Consumo 1h atrás |
| consumo_lag_24h         | DOUBLE  | Lag        | Consumo 24h atrás |
| price_lag_1h            | DOUBLE  | Lag        | Preço 1h atrás |
| rolling_avg_consumo_24h | DOUBLE  | Rolling    | Média móvel consumo 24h |
| rolling_avg_price_24h   | DOUBLE  | Rolling    | Média móvel preço 24h |

### Target (1)

| Coluna             | Tipo   | Descrição |
|--------------------|--------|-----------|
| consumo_next_hour  | DOUBLE | Consumo da hora seguinte (MWh) |

---

## 4. Split Temporal

O dataset é uma série temporal — o split deve respeitar a ordem temporal para evitar data leakage.

| Conjunto | Critério                        | Proporção aprox. |
|----------|---------------------------------|------------------|
| Treino   | Dados mais antigos              | 80%              |
| Teste    | Dados mais recentes             | 20%              |

**Implementação:**
```python
df_sorted = df.sort_values('ts_utc')
split_idx = int(len(df_sorted) * 0.8)
train = df_sorted.iloc[:split_idx]
test  = df_sorted.iloc[split_idx:]
```

**Sem shuffle** — baralhar uma série temporal introduz leakage futuro → passado.

---

## 5. Modelos Candidatos

| Modelo              | Framework    | Justificação |
|---------------------|--------------|--------------|
| RandomForestRegressor | scikit-learn | Baseline robusto, interpretável, sem normalização |
| GradientBoostingRegressor | scikit-learn | Bom para séries com padrões não-lineares |
| XGBRegressor        | XGBoost      | Alternativa eficiente ao Gradient Boosting |
| LinearRegression    | scikit-learn | Baseline simples para comparação |

**Recomendação inicial:** começar com `RandomForestRegressor` como baseline e comparar com `GradientBoostingRegressor`.

---

## 6. Métricas de Avaliação

| Métrica | Descrição | Unidade |
|---------|-----------|---------|
| MAE     | Mean Absolute Error — erro médio em valor absoluto | MWh |
| RMSE    | Root Mean Squared Error — penaliza erros grandes | MWh |
| R²      | Coeficiente de determinação — proporção de variância explicada | — |
| MAPE    | Mean Absolute Percentage Error — erro relativo | % |

---

## 7. MLflow Tracking

**Experiment:** `consumo-preco-load-forecast`  
**Registered model:** `consumo_preco_load_forecaster`  
**Tracking URI:** `http://localhost:15000` (local) ou `http://host.docker.internal:15000` (container)

### O que registar por run

```python
mlflow.set_experiment("consumo-preco-load-forecast")

with mlflow.start_run():
    # Parâmetros
    mlflow.log_param("model_type", "RandomForestRegressor")
    mlflow.log_param("n_estimators", 300)
    mlflow.log_param("max_depth", 8)
    mlflow.log_param("feature_schema_version", 1)
    mlflow.log_param("train_start", str(train['ts_utc'].min()))
    mlflow.log_param("train_end",   str(train['ts_utc'].max()))
    mlflow.log_param("test_start",  str(test['ts_utc'].min()))
    mlflow.log_param("test_end",    str(test['ts_utc'].max()))
    mlflow.log_param("n_train_rows", len(train))
    mlflow.log_param("n_test_rows",  len(test))

    # Métricas
    mlflow.log_metric("mae",  mae)
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2",   r2)
    mlflow.log_metric("mape", mape)

    # Modelo
    mlflow.sklearn.log_model(model, "model",
        registered_model_name="consumo_preco_load_forecaster")

    # Feature importances (artefacto)
    mlflow.log_dict(feature_importances, "feature_importances.json")
```

### Rastreabilidade de dados

Os parâmetros `train_start`, `train_end`, `test_start`, `test_end` e `feature_schema_version` garantem que é possível saber **que dados foram usados em cada versão do modelo**, satisfazendo o requisito do entregável F.

---

## 8. Estrutura do Script

```
03_ml_pipeline/consumo_preco_mlflow_flow.py
│
├── load_feature_table()       # Lê feat_load_forecasting_hourly via Trino
├── prepare_features()         # Separa X (features) e y (target), split temporal
├── train_model()              # Treina modelo scikit-learn
├── evaluate_model()           # Calcula MAE, RMSE, R², MAPE
├── log_to_mlflow()            # Regista parâmetros, métricas, modelo
└── consumo_preco_training_wf  # Função principal / Flyte workflow (opcional)
```

---

## 9. Execução

```bash
# Execução local (stack Docker em execução)
python 03_ml_pipeline/consumo_preco_mlflow_flow.py

# Execução via Flyte (opcional)
pyflyte run --remote -p flytesnacks -d development \
  03_ml_pipeline/consumo_preco_mlflow_flow.py consumo_preco_training_wf
```

**Pré-requisitos:**
- Stack Docker em execução (`docker compose up -d`)
- `gold.feat_load_forecasting_hourly` populada (pipeline Silver → Gold executado)
- MLflow acessível em `http://localhost:15000`
- Dependências: `scikit-learn`, `pandas`, `mlflow`, `trino` (ou `trino-python-client`)

---

## 10. Verificação no MLflow

Após execução, verificar em `http://localhost:15000`:
1. Experiment `consumo-preco-load-forecast` criado
2. Run com parâmetros de dados (`train_start`, `train_end`, etc.) registados
3. Métricas MAE / RMSE / R² visíveis
4. Modelo registado em `Models` → `consumo_preco_load_forecaster`
5. Artefacto `feature_importances.json` disponível

---

## 11. Relação com o Lakehouse

```
gold.feat_load_forecasting_hourly
        │
        ├─[consumo_preco_mlflow_flow.py]─► MLflow Run
        │                                    ├─ parâmetros (dados usados, feature_schema_version)
        │                                    ├─ métricas (MAE, RMSE, R², MAPE)
        │                                    └─ modelo registado (consumo_preco_load_forecaster)
        │
        └─[Grafana]─► Painel "Feature Table ML"
                        ├─ COUNT(*) total de exemplos
                        ├─ AVG(consumo_next_hour) target médio
                        └─ SUM(CASE WHEN consumo_next_hour IS NULL ...) nulos no target
```
