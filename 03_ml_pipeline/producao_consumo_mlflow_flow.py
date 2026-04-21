"""Flow de treino para produção vs consumo com tracking no MLflow.

Execução local:
    python 03_ml_pipeline/producao_consumo_mlflow_flow.py

Execução remota no Flyte:
    pyflyte run --remote -p flytesnacks -d development \
      03_ml_pipeline/producao_consumo_mlflow_flow.py producao_consumo_training_wf
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from typing import Dict, List, Tuple

try:
    from flytekit import ImageSpec, Resources, task, workflow
except ModuleNotFoundError:  # execução local sem Flyte instalado
    class ImageSpec:  # type: ignore[override]
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    class Resources:  # type: ignore[override]
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

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
    name="ml-producao-consumo-image",
    registry="localhost:30000",
    packages=[
        "pandas==2.2.3",
        "scikit-learn==1.6.1",
        "trino==0.336.0",
        "mlflow==3.10.1",
        "boto3",
    ],
)

# Ajusta estes valores conforme o teu ambiente.
TRINO_HOST = "host.docker.internal"
TRINO_PORT = 8080
TRINO_USER = "tead"
TRINO_HTTP_SCHEME = "http"
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "gold"
TRINO_TABLE = "producao_vs_consumo_hourly"

MLFLOW_TRACKING_URI = "http://host.docker.internal:15000"
MLFLOW_EXPERIMENT = "producao-consumo-defice"

os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", "http://host.docker.internal:9000")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")


@dataclass
class FeatureSpec:
    lags: Tuple[int, ...] = (1, 2, 3, 6, 12, 24)


def _get_trino_connection():
    import trino

    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        http_scheme=TRINO_HTTP_SCHEME,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        session_properties={
            "query_max_run_time": "10m",
            "query_max_execution_time": "8m",
        },
    )


def _load_gold_table():
    import pandas as pd

    sql = f"""
        SELECT
            timestamp_utc,
            consumo_total_kwh,
            producao_total_kwh,
            saldo_kwh,
            ratio_producao_consumo,
            flag_defice,
            flag_excedente
        FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}
        WHERE timestamp_utc IS NOT NULL
        ORDER BY timestamp_utc
    """

    conn = _get_trino_connection()
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()

    if df.empty:
        raise ValueError("Tabela gold está vazia; não há dados para treino.")

    df.columns = [c.strip().lower() for c in df.columns]
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df = df.sort_values("timestamp_utc").reset_index(drop=True)

    required = {
        "consumo_total_kwh",
        "producao_total_kwh",
        "saldo_kwh",
        "ratio_producao_consumo",
        "flag_defice",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Faltam colunas obrigatórias na tabela gold: {sorted(missing)}")

    return df


def _build_features(df, feature_spec: FeatureSpec):
    import pandas as pd

    data = df.copy()
    data["hour"] = data["timestamp_utc"].dt.hour
    data["day_of_week"] = data["timestamp_utc"].dt.dayofweek
    data["month"] = data["timestamp_utc"].dt.month

    base_cols = [
        "consumo_total_kwh",
        "producao_total_kwh",
        "saldo_kwh",
        "ratio_producao_consumo",
    ]

    for col in base_cols:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    # Target: prever se haverá défice na próxima hora.
    target_col = "target_defice_t_plus_1"
    data[target_col] = data["flag_defice"].shift(-1)

    feature_cols: List[str] = ["hour", "day_of_week", "month"]

    for col in base_cols:
        for lag in feature_spec.lags:
            lag_col = f"{col}_lag_{lag}"
            data[lag_col] = data[col].shift(lag)
            feature_cols.append(lag_col)

    model_df = data[["timestamp_utc", target_col] + feature_cols].dropna().copy()

    if model_df.empty:
        raise ValueError("Sem dados suficientes após criação das features (lags/dropna).")

    model_df[target_col] = model_df[target_col].astype(int)

    X = model_df[feature_cols]
    y = model_df[target_col]
    ts = model_df["timestamp_utc"]
    return X, y, ts, feature_cols


@task(container_image=image_spec)
def train_producao_consumo_model(test_ratio: float = 0.2, random_state: int = 42) -> str:
    try:
        import mlflow
        import mlflow.sklearn
    except ModuleNotFoundError as exc:
        if exc.name == "mlflow":
            raise ModuleNotFoundError(
                "Dependência ausente: mlflow. Instala com `python -m pip install mlflow==3.10.1` "
                "(ou usa o ambiente/container do Flyte com as packages do ImageSpec)."
            ) from exc
        raise
    import pandas as pd
    from sklearn.compose import ColumnTransformer
    from sklearn.impute import SimpleImputer
    from sklearn.metrics import (
        accuracy_score,
        classification_report,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )
    from sklearn.model_selection import train_test_split
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import RandomForestClassifier

    raw_df = _load_gold_table()
    X, y, ts, feature_cols = _build_features(raw_df, FeatureSpec())

    # Split temporal (sem embaralhar) para respeitar série temporal.
    split_index = int(len(X) * (1 - test_ratio))
    if split_index <= 0 or split_index >= len(X):
        raise ValueError("test_ratio gerou split inválido.")

    X_train, X_test = X.iloc[:split_index], X.iloc[split_index:]
    y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]
    ts_train, ts_test = ts.iloc[:split_index], ts.iloc[split_index:]

    numeric_features = feature_cols

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "num",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="median")),
                        ("scaler", StandardScaler()),
                    ]
                ),
                numeric_features,
            )
        ]
    )

    model = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "classifier",
                RandomForestClassifier(
                    n_estimators=300,
                    max_depth=8,
                    min_samples_leaf=3,
                    random_state=random_state,
                    n_jobs=-1,
                ),
            ),
        ]
    )

    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    probs = model.predict_proba(X_test)[:, 1]

    accuracy = float(accuracy_score(y_test, preds))
    precision = float(precision_score(y_test, preds, zero_division=0))
    recall = float(recall_score(y_test, preds, zero_division=0))
    f1 = float(f1_score(y_test, preds, zero_division=0))
    roc_auc = float(roc_auc_score(y_test, probs))

    report = classification_report(y_test, preds, output_dict=True, zero_division=0)

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    with mlflow.start_run(run_name="rf-defice-t-plus-1"):
        mlflow.set_tags(
            {
                "domain": "energia",
                "dataset": f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}",
                "target": "flag_defice(+1h)",
                "model_type": "RandomForestClassifier",
            }
        )

        mlflow.log_params(
            {
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
            }
        )

        mlflow.log_metrics(
            {
                "accuracy": accuracy,
                "precision": precision,
                "recall": recall,
                "f1": f1,
                "roc_auc": roc_auc,
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = os.path.join(tmpdir, "classification_report.json")
            features_path = os.path.join(tmpdir, "feature_columns.json")

            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2)

            with open(features_path, "w", encoding="utf-8") as f:
                json.dump(feature_cols, f, indent=2)

            mlflow.log_artifact(report_path, artifact_path="evaluation")
            mlflow.log_artifact(features_path, artifact_path="metadata")

        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            registered_model_name="producao_consumo_defice_classifier",
        )

        summary = {
            "experiment": MLFLOW_EXPERIMENT,
            "registered_model": "producao_consumo_defice_classifier",
            "metrics": {
                "accuracy": accuracy,
                "precision": precision,
                "recall": recall,
                "f1": f1,
                "roc_auc": roc_auc,
            },
        }
        return json.dumps(summary, ensure_ascii=False)


@workflow
def producao_consumo_training_wf(test_ratio: float = 0.2, random_state: int = 42) -> str:
    return train_producao_consumo_model(test_ratio=test_ratio, random_state=random_state)


if __name__ == "__main__":
    result = train_producao_consumo_model(test_ratio=0.2, random_state=42)
    print(result)
