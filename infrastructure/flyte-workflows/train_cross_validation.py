from __future__ import annotations

import json
import os
import random
import uuid
from functools import partial
from typing import List, Tuple

import joblib
from flytekit import ImageSpec, Resources, current_context, map_task, task, workflow
from flytekit.types.file import FlyteFile

image_spec = ImageSpec(
    name="ml-heart-cv-image",
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
TRINO_PORT = 8080
TRINO_USER = "tead"
TRINO_HTTP_SCHEME = "http"

TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "tead"
TRINO_TABLE = "heart"

MLFLOW_TRACKING_URI = "http://host.docker.internal:15000"
MLFLOW_EXPERIMENT = "flyte-heart-cv"

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


def _load_heart_from_trino():
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

    target_col = "result"
    if target_col not in df.columns:
        raise ValueError(f"Não encontrei a coluna target '{target_col}'.")

    # Garantir numérico
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    if df.isnull().any().any():
        raise ValueError("Existem valores nulos após conversão para numérico.")

    X = df.drop(columns=[target_col])
    y = df[target_col].astype(int)

    if X.shape[1] == 0:
        raise ValueError("Não encontrei features na tabela.")

    return X, y


@task
def create_cv_run_id() -> str:
    return str(uuid.uuid4())


@task(
    requests=Resources(cpu="1", mem="1Gi"),
    limits=Resources(cpu="1", mem="1Gi"),
    container_image=image_spec,
)
def build_fold_specs(n_folds: int, random_state: int = 42) -> List[str]:
    from sklearn.model_selection import StratifiedKFold

    X, y = _load_heart_from_trino()

    skf = StratifiedKFold(
        n_splits=n_folds,
        shuffle=True,
        random_state=random_state,
    )

    fold_specs: List[str] = []
    for fold_idx, (train_idx, test_idx) in enumerate(skf.split(X, y)):
        spec = {
            "fold_idx": fold_idx,
            "train_idx": train_idx.tolist(),
            "test_idx": test_idx.tolist(),
        }
        fold_specs.append(json.dumps(spec))

    return fold_specs


@task(
    retries=4,
    cache=False,
    requests=Resources(cpu="1", mem="1Gi"),
    limits=Resources(cpu="1", mem="1Gi"),
    container_image=image_spec,
)
def train_and_evaluate_fold(
    fold_spec_json: str,
    cv_run_id: str,
    n_folds: int,
    random_state: int = 42,
    logreg_c: float = 1.0,
    max_iter: int = 1000,
) -> str:
    import tempfile

    import mlflow
    import pandas as pd
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import (
        accuracy_score,
        average_precision_score,
        balanced_accuracy_score,
        classification_report,
        confusion_matrix,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    spec = json.loads(fold_spec_json)
    fold_idx = spec["fold_idx"]
    train_idx = spec["train_idx"]
    test_idx = spec["test_idx"]

    X, y = _load_heart_from_trino()

    X_train = X.iloc[train_idx]
    X_test = X.iloc[test_idx]
    y_train = y.iloc[train_idx]
    y_test = y.iloc[test_idx]

    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "clf",
                LogisticRegression(
                    C=logreg_c,
                    max_iter=max_iter,
                    solver="liblinear",
                    random_state=random_state,
                ),
            ),
        ]
    )

    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    proba = model.predict_proba(X_test)[:, 1]

    accuracy = float(accuracy_score(y_test, preds))
    balanced_accuracy = float(balanced_accuracy_score(y_test, preds))
    precision = float(precision_score(y_test, preds, zero_division=0))
    recall = float(recall_score(y_test, preds, zero_division=0))
    f1 = float(f1_score(y_test, preds, zero_division=0))
    roc_auc = float(roc_auc_score(y_test, proba))
    average_precision = float(average_precision_score(y_test, proba))

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    with mlflow.start_run(run_name=f"heart-cv-fold-{fold_idx + 1}-of-{n_folds}"):
        mlflow.set_tags(
            {
                "cv_run_id": cv_run_id,
                "fold_idx": str(fold_idx),
                "n_folds": str(n_folds),
                "model_type": "logistic_regression",
                "source_table": f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}",
            }
        )

        mlflow.log_params(
            {
                "logreg_c": logreg_c,
                "max_iter": max_iter,
                "random_state": random_state,
                "n_folds": n_folds,
                "fold_idx": fold_idx,
            }
        )

        mlflow.log_metrics(
            {
                "accuracy": accuracy,
                "balanced_accuracy": balanced_accuracy,
                "precision": precision,
                "recall": recall,
                "f1": f1,
                "roc_auc": roc_auc,
                "average_precision": average_precision,
            }
        )

        report = classification_report(y_test, preds, output_dict=True, zero_division=0)
        cm = confusion_matrix(y_test, preds, labels=[0, 1])

        for label, metrics in report.items():
            if isinstance(metrics, dict):
                safe_label = str(label).replace(" ", "_")
                for metric_name, metric_value in metrics.items():
                    if isinstance(metric_value, (int, float)):
                        mlflow.log_metric(f"class_{safe_label}_{metric_name}", float(metric_value))

        with tempfile.TemporaryDirectory() as tmpdir:
            cm_path = os.path.join(tmpdir, f"fold_{fold_idx}_confusion_matrix.csv")
            report_path = os.path.join(tmpdir, f"fold_{fold_idx}_classification_report.json")
            model_path = os.path.join(tmpdir, f"fold_{fold_idx}_model.joblib")

            pd.DataFrame(cm, index=["true_0", "true_1"], columns=["pred_0", "pred_1"]).to_csv(cm_path)
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2)

            joblib.dump(model, model_path)

            mlflow.log_artifact(cm_path, artifact_path="evaluation")
            mlflow.log_artifact(report_path, artifact_path="evaluation")
            mlflow.log_artifact(model_path, artifact_path="models")

    result = {
        "fold_idx": fold_idx,
        "accuracy": accuracy,
        "balanced_accuracy": balanced_accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "roc_auc": roc_auc,
        "average_precision": average_precision,
    }
    return json.dumps(result, sort_keys=True)


@task(
    requests=Resources(cpu="1", mem="1Gi"),
    limits=Resources(cpu="1", mem="1Gi"),
    container_image=image_spec,
)
def summarize_cv_results(results_json: List[str], cv_run_id: str, n_folds: int) -> str:
    import tempfile

    import mlflow
    import pandas as pd

    rows = [json.loads(r) for r in results_json]
    df = pd.DataFrame(rows).sort_values("fold_idx").reset_index(drop=True)

    summary = {
        "n_folds": n_folds,
        "accuracy_mean": float(df["accuracy"].mean()),
        "accuracy_std": float(df["accuracy"].std(ddof=0)),
        "balanced_accuracy_mean": float(df["balanced_accuracy"].mean()),
        "balanced_accuracy_std": float(df["balanced_accuracy"].std(ddof=0)),
        "precision_mean": float(df["precision"].mean()),
        "precision_std": float(df["precision"].std(ddof=0)),
        "recall_mean": float(df["recall"].mean()),
        "recall_std": float(df["recall"].std(ddof=0)),
        "f1_mean": float(df["f1"].mean()),
        "f1_std": float(df["f1"].std(ddof=0)),
        "roc_auc_mean": float(df["roc_auc"].mean()),
        "roc_auc_std": float(df["roc_auc"].std(ddof=0)),
        "average_precision_mean": float(df["average_precision"].mean()),
        "average_precision_std": float(df["average_precision"].std(ddof=0)),
    }

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    with mlflow.start_run(run_name=f"heart-cv-summary-{cv_run_id}"):
        mlflow.set_tags(
            {
                "cv_run_id": cv_run_id,
                "n_folds": str(n_folds),
                "run_type": "cv_summary",
                "source_table": f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}",
            }
        )
        mlflow.log_metrics(summary)

        with tempfile.TemporaryDirectory() as tmpdir:
            folds_path = os.path.join(tmpdir, "fold_metrics.csv")
            summary_path = os.path.join(tmpdir, "cv_summary.json")

            df.to_csv(folds_path, index=False)
            with open(summary_path, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2)

            mlflow.log_artifact(folds_path, artifact_path="cv")
            mlflow.log_artifact(summary_path, artifact_path="cv")

    return json.dumps(summary, sort_keys=True)


@task(
    retries=100,
    requests=Resources(cpu="1", mem="1Gi"),
    limits=Resources(cpu="1", mem="1Gi"),
    container_image=image_spec,
)
def train_final_model(
    cv_summary_json: str,
    cv_run_id: str,
    random_state: int = 42,
    logreg_c: float = 1.0,
    max_iter: int = 1000,
) -> FlyteFile:
    import mlflow
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    summary = json.loads(cv_summary_json)
    X, y = _load_heart_from_trino()

    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "clf",
                LogisticRegression(
                    C=logreg_c,
                    max_iter=max_iter,
                    solver="liblinear",
                    random_state=random_state,
                ),
            ),
        ]
    )
    model.fit(X, y)

    workdir = current_context().working_directory
    output_path = os.path.join(workdir, "heart_logreg_final.joblib")
    joblib.dump(
        {
            "model": model,
            "cv_summary": summary,
            "feature_names": list(X.columns),
            "source_table": f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}",
        },
        output_path,
    )

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    with mlflow.start_run(run_name=f"heart-final-model-{cv_run_id}"):
        mlflow.set_tags(
            {
                "cv_run_id": cv_run_id,
                "run_type": "final_model",
                "model_type": "logistic_regression",
                "source_table": f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_TABLE}",
            }
        )
        mlflow.log_params(
            {
                "logreg_c": logreg_c,
                "max_iter": max_iter,
                "random_state": random_state,
            }
        )
        mlflow.log_metrics(summary)
        mlflow.log_artifact(output_path, artifact_path="final_model")

    return FlyteFile(output_path)


@workflow
def heart_cv_workflow(
    n_folds: int = 5,
    random_state: int = 42,
    logreg_c: float = 1.0,
    max_iter: int = 1000,
) -> Tuple[str, FlyteFile]:
    cv_run_id = create_cv_run_id()
    fold_specs = build_fold_specs(n_folds=n_folds, random_state=random_state)

    fold_results = map_task(
        partial(
            train_and_evaluate_fold,
            cv_run_id=cv_run_id,
            n_folds=n_folds,
            random_state=random_state,
            logreg_c=logreg_c,
            max_iter=max_iter,
        )
    )(fold_spec_json=fold_specs)

    summary = summarize_cv_results(
        results_json=fold_results,
        cv_run_id=cv_run_id,
        n_folds=n_folds,
    )

    final_model = train_final_model(
        cv_summary_json=summary,
        cv_run_id=cv_run_id,
        random_state=random_state,
        logreg_c=logreg_c,
        max_iter=max_iter,
    )

    return summary, final_model