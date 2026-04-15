#Executar com pyflyte run --remote -p flytesnacks -d development train_grid_search.py iris_parallel_tree_training_wf

from __future__ import annotations

import json
import os
from functools import partial
from typing import List, Tuple

import joblib
from flytekit import ImageSpec, Resources, current_context, map_task, task, workflow
from flytekit.types.file import FlyteFile

image_spec = ImageSpec(
    name="ml-demo-image",
    registry="localhost:30000",
    packages=[
        "pandas==2.2.3",
        "scikit-learn==1.6.1",
        "joblib==1.4.2",
        "trino==0.336.0",
        "mlflow==3.10.1",
        "boto3",
    ],
)

TRINO_HOST = "host.docker.internal"
TRINO_PORT = "8080"
TRINO_USER = "tead"
TRINO_HTTP_SCHEME = "http"

TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "tead"
TRINO_TABLE = "iris"

MLFLOW_TRACKING_URI = "http://host.docker.internal:15000"
MLFLOW_EXPERIMENT = "flyte-iris"

os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://host.docker.internal:9000"
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_EC2_METADATA_DISABLED"] = "true"


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


def _load_iris_from_trino():
    import pandas as pd

    sql = f"SELECT * FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}"

    conn = _get_trino_connection()
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()

    if df.empty:
        raise ValueError(f"Tabela {TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE} veio vazia.")

    normalized = {c: c.strip().lower().replace(" ", "_") for c in df.columns}
    df = df.rename(columns=normalized)

    candidate_targets = ["species", "target", "class", "label", "variety"]
    target_col = next((c for c in candidate_targets if c in df.columns), df.columns[-1])

    X = df.drop(columns=[target_col])
    y = df[target_col]

    if X.shape[1] == 0:
        raise ValueError("Não encontrei features na tabela.")

    return X, y


@task
def build_param_grid() -> List[str]:
    grid = [
        {"criterion": "gini", "max_depth": 2, "min_samples_split": 2},
        {"criterion": "gini", "max_depth": 3, "min_samples_split": 2},
        {"criterion": "gini", "max_depth": 4, "min_samples_split": 2},
        {"criterion": "entropy", "max_depth": 2, "min_samples_split": 2},
        {"criterion": "entropy", "max_depth": 3, "min_samples_split": 2},
        {"criterion": "entropy", "max_depth": 4, "min_samples_split": 2},
        {"criterion": "log_loss", "max_depth": 3, "min_samples_split": 2},
        {"criterion": "log_loss", "max_depth": 4, "min_samples_split": 4},
    ]
    return [json.dumps(cfg, sort_keys=True) for cfg in grid]


@task(
    cache=True,
    cache_version="3.0",
    requests=Resources(cpu="1", mem="1Gi"),
    limits=Resources(cpu="1", mem="1Gi"),
    container_image=image_spec,
)
def train_and_evaluate_tree(
    config_json: str,
    test_size: float = 0.2,
    random_state: int = 42,
) -> str:
    import tempfile

    import mlflow
    import mlflow.sklearn
    import pandas as pd
    from sklearn.metrics import (
        accuracy_score,
        balanced_accuracy_score,
        classification_report,
        confusion_matrix,
        f1_score,
        precision_score,
        recall_score,
    )
    from sklearn.model_selection import train_test_split
    from sklearn.tree import DecisionTreeClassifier

    cfg = json.loads(config_json)
    X, y = _load_iris_from_trino()

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    run_name = f"dt-{cfg['criterion']}-d{cfg['max_depth']}-mss{cfg['min_samples_split']}"

    with mlflow.start_run(run_name=run_name):
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=test_size,
            random_state=random_state,
            stratify=y,
        )

        model = DecisionTreeClassifier(
            criterion=cfg["criterion"],
            max_depth=cfg["max_depth"],
            min_samples_split=cfg["min_samples_split"],
            random_state=random_state,
        )
        model.fit(X_train, y_train)

        preds = model.predict(X_test)

        accuracy = float(accuracy_score(y_test, preds))
        balanced_accuracy = float(balanced_accuracy_score(y_test, preds))

        precision_macro = float(precision_score(y_test, preds, average="macro", zero_division=0))
        recall_macro = float(recall_score(y_test, preds, average="macro", zero_division=0))
        f1_macro = float(f1_score(y_test, preds, average="macro", zero_division=0))

        precision_weighted = float(precision_score(y_test, preds, average="weighted", zero_division=0))
        recall_weighted = float(recall_score(y_test, preds, average="weighted", zero_division=0))
        f1_weighted = float(f1_score(y_test, preds, average="weighted", zero_division=0))

        mlflow.log_params(
            {
                "criterion": cfg["criterion"],
                "max_depth": cfg["max_depth"],
                "min_samples_split": cfg["min_samples_split"],
                "test_size": test_size,
                "random_state": random_state,
                "source_table": f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}",
            }
        )

        mlflow.log_metrics(
            {
                "accuracy": accuracy,
                "balanced_accuracy": balanced_accuracy,
                "precision_macro": precision_macro,
                "recall_macro": recall_macro,
                "f1_macro": f1_macro,
                "precision_weighted": precision_weighted,
                "recall_weighted": recall_weighted,
                "f1_weighted": f1_weighted,
            }
        )

        class_labels = sorted(pd.Series(y_test).astype(str).unique().tolist())
        cm = confusion_matrix(y_test, preds, labels=class_labels)
        report = classification_report(y_test, preds, output_dict=True, zero_division=0)

        for label, metrics in report.items():
            if isinstance(metrics, dict):
                safe_label = str(label).replace(" ", "_")
                for metric_name, metric_value in metrics.items():
                    if isinstance(metric_value, (int, float)):
                        mlflow.log_metric(f"class_{safe_label}_{metric_name}", float(metric_value))

        with tempfile.TemporaryDirectory() as tmpdir:
            cm_path = os.path.join(tmpdir, "confusion_matrix.csv")
            report_path = os.path.join(tmpdir, "classification_report.json")

            pd.DataFrame(cm, index=class_labels, columns=class_labels).to_csv(cm_path)
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2)

            mlflow.log_artifact(cm_path, artifact_path="evaluation")
            mlflow.log_artifact(report_path, artifact_path="evaluation")

        mlflow.sklearn.log_model(model, artifact_path=MLFLOW_EXPERIMENT)

        result = {
            "criterion": cfg["criterion"],
            "max_depth": cfg["max_depth"],
            "min_samples_split": cfg["min_samples_split"],
            "accuracy": accuracy,
            "balanced_accuracy": balanced_accuracy,
            "precision_macro": precision_macro,
            "recall_macro": recall_macro,
            "f1_macro": f1_macro,
            "precision_weighted": precision_weighted,
            "recall_weighted": recall_weighted,
            "f1_weighted": f1_weighted,
        }
        return json.dumps(result, sort_keys=True)


@task
def select_best_model(results_json: List[str]) -> str:
    results = [json.loads(r) for r in results_json]
    best = sorted(
        results,
        key=lambda r: (-r["f1_macro"], -r["accuracy"], r["max_depth"], r["min_samples_split"]),
    )[0]
    return json.dumps(best, sort_keys=True)


@task(
    requests=Resources(cpu="1", mem="1Gi"),
    limits=Resources(cpu="1", mem="1Gi"),
    container_image=image_spec,
)
def train_best_model_on_full_data(best_result_json: str) -> FlyteFile:
    from sklearn.tree import DecisionTreeClassifier

    best = json.loads(best_result_json)
    X, y = _load_iris_from_trino()

    model = DecisionTreeClassifier(
        criterion=best["criterion"],
        max_depth=best["max_depth"],
        min_samples_split=best["min_samples_split"],
        random_state=42,
    )
    model.fit(X, y)

    workdir = current_context().working_directory
    output_path = os.path.join(workdir, "best_iris_decision_tree.joblib")

    payload = {
        "model": model,
        "metadata": best,
        "feature_names": list(X.columns),
        "source_table": f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}",
    }
    joblib.dump(payload, output_path)

    return FlyteFile(output_path)


@workflow
def iris_parallel_tree_training_wf() -> Tuple[str, FlyteFile]:
    configs = build_param_grid()

    partial_train = partial(
        train_and_evaluate_tree,
        test_size=0.2,
        random_state=42,
    )

    results = map_task(
        partial_train,
        concurrency=4,
    )(config_json=configs)

    best = select_best_model(results_json=results)
    model_file = train_best_model_on_full_data(best_result_json=best)
    return best, model_file