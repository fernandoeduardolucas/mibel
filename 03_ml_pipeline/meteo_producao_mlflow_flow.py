"""Flow de treino para meteorologia → produção e meteorologia + produção → preço spot.

Treina dois modelos em sequência:
  Modelo A: RandomForestRegressor — prevê produção elétrica diária a partir do tempo
  Modelo B: GradientBoostingRegressor — prevê preço spot a partir do tempo + produção

Execução local:
    python3.11 -m venv .venv
    source .venv/bin/activate  # Linux/macOS  |  .venv\\Scripts\\activate  # Windows
    pip install pandas==2.2.3 scikit-learn==1.6.1 trino==0.336.0 mlflow==3.10.1 boto3
    python 03_ml_pipeline/meteo_producao_mlflow_flow.py

Execução remota no Flyte:
    pyflyte run --remote -p flytesnacks -d development \\
      03_ml_pipeline/meteo_producao_mlflow_flow.py meteo_producao_training_wf
"""
from __future__ import annotations

import json
import os
import socket
import sys
import tempfile
from typing import List

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

try:
    from flytekit import ImageSpec, Resources, task, workflow
except ModuleNotFoundError:
    class ImageSpec:  # type: ignore[override]
        def __init__(self, *args, **kwargs):
            pass

    class Resources:  # type: ignore[override]
        def __init__(self, *args, **kwargs):
            pass

    def task(*args, **kwargs):  # type: ignore[override]
        def decorator(func):
            return func
        if args and callable(args[0]) and len(args) == 1 and not kwargs:
            return args[0]
        return decorator

    def workflow(func=None, **kwargs):  # type: ignore[override]
        if func is not None and callable(func):
            return func
        def decorator(f):
            return f
        return decorator

image_spec = ImageSpec(
    name="ml-meteo-producao-image",
    registry="localhost:30000",
    packages=[
        "pandas==2.2.3",
        "scikit-learn==1.6.1",
        "trino==0.336.0",
        "mlflow==3.10.1",
        "boto3",
    ],
)

TRINO_HOST    = os.getenv("TRINO_HOST",    "localhost")
TRINO_PORT    = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER    = os.getenv("TRINO_USER",    "tead")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA  = os.getenv("TRINO_SCHEMA",  "gold")
TRINO_TABLE   = os.getenv("TRINO_TABLE",   "dp_meteo_producao_daily_features")

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:15000")

os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL",  "http://localhost:9000")
os.environ.setdefault("AWS_ACCESS_KEY_ID",        "minioadmin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY",    "minioadmin")
os.environ.setdefault("AWS_DEFAULT_REGION",       "us-east-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

# Features shared by both models
WEATHER_FEATURES: List[str] = [
    "temperature_mean_c",
    "temperature_min_c",
    "temperature_max_c",
    "precipitation_total_mm",
    "wind_speed_mean_ms",
    "wind_speed_max_ms",
    "radiation_mean_wm2",
    "radiation_total_kwh_m2",
    "cloud_cover_mean_pct",
]
TEMPORAL_FEATURES: List[str] = [
    "dia_semana",
    "is_weekend",
    "estacao",
]
LAG_FEATURES: List[str] = [
    "temp_lag_1d",
    "wind_lag_1d",
    "radiation_lag_1d",
    "producao_lag_1d",
    "preco_lag_1d",
    "temp_rolling_7d_avg",
    "wind_rolling_7d_avg",
    "radiation_rolling_7d_avg",
    "producao_rolling_7d_avg",
]


def _resolved_host(host: str) -> str:
    try:
        socket.getaddrinfo(host, None)
        return host
    except socket.gaierror:
        if host == "host.docker.internal":
            return "localhost"
        raise


def _get_trino_connection():
    import trino
    return trino.dbapi.connect(
        host=_resolved_host(TRINO_HOST),
        port=TRINO_PORT,
        user=TRINO_USER,
        http_scheme="http",
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        session_properties={
            "query_max_run_time": "10m",
            "query_max_execution_time": "8m",
        },
    )


def _load_gold_table():
    import pandas as pd

    all_cols = (
        ["data_dia"]
        + WEATHER_FEATURES
        + TEMPORAL_FEATURES
        + LAG_FEATURES
        + ["producao_total_daily_mwh", "saldo_daily_mwh", "preco_spot_medio_eur_mwh"]
    )
    col_list = ", ".join(all_cols)
    sql = f"""
        SELECT {col_list}
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}
        WHERE data_dia IS NOT NULL
          AND preco_spot_medio_eur_mwh IS NOT NULL
          AND producao_total_daily_mwh IS NOT NULL
        ORDER BY data_dia
    """
    conn = _get_trino_connection()
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()

    if df.empty:
        raise ValueError("Tabela dp_meteo_producao_daily_features está vazia; sem dados para treino.")

    df.columns = [c.strip().lower() for c in df.columns]
    df["data_dia"] = pd.to_datetime(df["data_dia"])
    df = df.sort_values("data_dia").reset_index(drop=True)

    for col in WEATHER_FEATURES + LAG_FEATURES + ["producao_total_daily_mwh", "saldo_daily_mwh", "preco_spot_medio_eur_mwh"]:
        if col in df.columns:
            df[col] = df[col].astype("float64")

    return df


def _temporal_split(X, y, ts, test_ratio: float):
    split_index = int(len(X) * (1 - test_ratio))
    if split_index <= 0 or split_index >= len(X):
        raise ValueError("test_ratio gerou split inválido.")
    return (
        X.iloc[:split_index], X.iloc[split_index:],
        y.iloc[:split_index], y.iloc[split_index:],
        ts.iloc[:split_index], ts.iloc[split_index:],
    )


def _log_regression_metrics(y_test, preds) -> dict:
    import numpy as np
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    mae  = float(mean_absolute_error(y_test, preds))
    rmse = float(np.sqrt(mean_squared_error(y_test, preds)))
    r2   = float(r2_score(y_test, preds))
    mask = y_test != 0
    mape = float(np.mean(np.abs((y_test[mask] - preds[mask]) / y_test[mask])) * 100) if mask.any() else float("nan")
    return {"mae": mae, "rmse": rmse, "r2": r2, "mape": mape}


@task(container_image=image_spec)
def train_producao_from_weather(test_ratio: float = 0.2, random_state: int = 42) -> str:
    """Modelo A: previsão de produção elétrica a partir de variáveis meteorológicas."""
    try:
        import mlflow
        import mlflow.sklearn
    except ModuleNotFoundError as exc:
        if exc.name == "mlflow":
            raise ModuleNotFoundError("Dependência ausente: mlflow.") from exc
        raise

    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    experiment = "meteo-producao-producao-forecast"
    registered_name = "meteo_producao_producao_forecaster"
    target_col = "producao_total_daily_mwh"

    feature_cols: List[str] = WEATHER_FEATURES + TEMPORAL_FEATURES + LAG_FEATURES[:6]

    df = _load_gold_table()
    model_df = df[["data_dia", target_col] + feature_cols].dropna().copy()
    if model_df.empty:
        raise ValueError("Sem dados suficientes após remoção de nulos.")

    X = model_df[feature_cols]
    y = model_df[target_col].astype("float64")
    ts = model_df["data_dia"]

    X_train, X_test, y_train, y_test, ts_train, ts_test = _temporal_split(X, y, ts, test_ratio)

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", Pipeline([
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler",  StandardScaler()),
            ]), feature_cols)
        ]
    )
    model = Pipeline([
        ("preprocessor", preprocessor),
        ("regressor", RandomForestRegressor(
            n_estimators=300,
            max_depth=8,
            min_samples_leaf=3,
            random_state=random_state,
            n_jobs=-1,
        )),
    ])
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    metrics = _log_regression_metrics(y_test, preds)

    importances = dict(zip(
        feature_cols,
        model.named_steps["regressor"].feature_importances_.tolist(),
    ))

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(experiment)

    with mlflow.start_run(run_name="rf-producao-from-weather"):
        mlflow.set_tags({
            "domain": "meteo_producao",
            "dataset": f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}",
            "target": target_col,
            "model_type": "RandomForestRegressor",
            "task": "regression",
        })
        mlflow.log_params({
            "test_ratio": test_ratio,
            "random_state": random_state,
            "n_estimators": 300,
            "max_depth": 8,
            "min_samples_leaf": 3,
            "n_train": len(X_train),
            "n_test": len(X_test),
            "train_start": str(ts_train.min()),
            "train_end": str(ts_train.max()),
            "test_start": str(ts_test.min()),
            "test_end": str(ts_test.max()),
        })
        mlflow.log_metrics(metrics)

        with tempfile.TemporaryDirectory() as tmpdir:
            imp_path = os.path.join(tmpdir, "feature_importances.json")
            feat_path = os.path.join(tmpdir, "feature_columns.json")
            with open(imp_path,  "w", encoding="utf-8") as f:
                json.dump(importances, f, indent=2)
            with open(feat_path, "w", encoding="utf-8") as f:
                json.dump(feature_cols, f, indent=2)
            mlflow.log_artifact(imp_path,  artifact_path="evaluation")
            mlflow.log_artifact(feat_path, artifact_path="metadata")

        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            registered_model_name=registered_name,
        )

    summary = {
        "experiment": experiment,
        "registered_model": registered_name,
        "target": target_col,
        "metrics": metrics,
    }
    print(json.dumps(summary, indent=2))
    return json.dumps(summary, ensure_ascii=False)


@task(container_image=image_spec)
def train_preco_from_meteo_producao(test_ratio: float = 0.2, random_state: int = 42) -> str:
    """Modelo B: impacto do tempo + produção no preço spot day-ahead (€/MWh)."""
    try:
        import mlflow
        import mlflow.sklearn
    except ModuleNotFoundError as exc:
        if exc.name == "mlflow":
            raise ModuleNotFoundError("Dependência ausente: mlflow.") from exc
        raise

    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    experiment = "meteo-producao-preco-impact"
    registered_name = "meteo_producao_preco_impact"
    target_col = "preco_spot_medio_eur_mwh"

    feature_cols: List[str] = (
        WEATHER_FEATURES
        + TEMPORAL_FEATURES
        + LAG_FEATURES
        + ["producao_total_daily_mwh", "saldo_daily_mwh"]
    )

    df = _load_gold_table()
    model_df = df[["data_dia", target_col] + feature_cols].dropna().copy()
    if model_df.empty:
        raise ValueError("Sem dados suficientes após remoção de nulos.")

    X = model_df[feature_cols]
    y = model_df[target_col].astype("float64")
    ts = model_df["data_dia"]

    X_train, X_test, y_train, y_test, ts_train, ts_test = _temporal_split(X, y, ts, test_ratio)

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", Pipeline([
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler",  StandardScaler()),
            ]), feature_cols)
        ]
    )
    model = Pipeline([
        ("preprocessor", preprocessor),
        ("regressor", GradientBoostingRegressor(
            n_estimators=300,
            max_depth=5,
            learning_rate=0.05,
            subsample=0.8,
            min_samples_leaf=5,
            random_state=random_state,
        )),
    ])
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    metrics = _log_regression_metrics(y_test, preds)

    importances = dict(zip(
        feature_cols,
        model.named_steps["regressor"].feature_importances_.tolist(),
    ))

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(experiment)

    with mlflow.start_run(run_name="gbr-preco-from-meteo-producao"):
        mlflow.set_tags({
            "domain": "meteo_producao",
            "dataset": f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}",
            "target": target_col,
            "model_type": "GradientBoostingRegressor",
            "task": "regression",
        })
        mlflow.log_params({
            "test_ratio": test_ratio,
            "random_state": random_state,
            "n_estimators": 300,
            "max_depth": 5,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "min_samples_leaf": 5,
            "n_train": len(X_train),
            "n_test": len(X_test),
            "train_start": str(ts_train.min()),
            "train_end": str(ts_train.max()),
            "test_start": str(ts_test.min()),
            "test_end": str(ts_test.max()),
        })
        mlflow.log_metrics(metrics)

        with tempfile.TemporaryDirectory() as tmpdir:
            imp_path  = os.path.join(tmpdir, "feature_importances.json")
            feat_path = os.path.join(tmpdir, "feature_columns.json")
            with open(imp_path,  "w", encoding="utf-8") as f:
                json.dump(importances, f, indent=2)
            with open(feat_path, "w", encoding="utf-8") as f:
                json.dump(feature_cols, f, indent=2)
            mlflow.log_artifact(imp_path,  artifact_path="evaluation")
            mlflow.log_artifact(feat_path, artifact_path="metadata")

        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            registered_model_name=registered_name,
        )

    summary = {
        "experiment": experiment,
        "registered_model": registered_name,
        "target": target_col,
        "metrics": metrics,
    }
    print(json.dumps(summary, indent=2))
    return json.dumps(summary, ensure_ascii=False)


@workflow
def meteo_producao_training_wf(test_ratio: float = 0.2, random_state: int = 42) -> str:
    result_a = train_producao_from_weather(test_ratio=test_ratio, random_state=random_state)
    result_b = train_preco_from_meteo_producao(test_ratio=test_ratio, random_state=random_state)
    return result_b


if __name__ == "__main__":
    print("\n=== Modelo A: Produção a partir de Meteorologia ===")
    train_producao_from_weather(test_ratio=0.2, random_state=42)
    print("\n=== Modelo B: Preço a partir de Meteorologia + Produção ===")
    train_preco_from_meteo_producao(test_ratio=0.2, random_state=42)
