#Executar com pyflyte run --remote -p flytesnacks -d development train_ml.py iris_parallel_tree_training_wf

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
    packages=["pandas", "scikit-learn", "joblib", "trino"],
)

TRINO_HOST = os.getenv("TRINO_HOST", "host.docker.internal")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "tead")
TRINO_HTTP_SCHEME = os.getenv("TRINO_HTTP_SCHEME", "http")

TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "tead"
TRINO_TABLE = "iris"


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

    normalized = {c: c.strip().lower().replace(' ', '_') for c in df.columns}
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
    cache_version="1.0",
    requests=Resources(cpu="1", mem="1Gi"),
    limits=Resources(cpu="1", mem="1Gi"),
    container_image=image_spec,
)
def train_and_evaluate_tree(config_json: str,test_size: float = 0.2,random_state: int = 42) -> str:
    from sklearn.metrics import accuracy_score
    from sklearn.model_selection import train_test_split
    from sklearn.tree import DecisionTreeClassifier

    cfg = json.loads(config_json)
    X, y = _load_iris_from_trino()

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

    result = {
        "criterion": cfg["criterion"],
        "max_depth": cfg["max_depth"],
        "min_samples_split": cfg["min_samples_split"],
        "accuracy": accuracy,
    }
    return json.dumps(result, sort_keys=True)


@task
def select_best_model(results_json: List[str]) -> str:
    results = [json.loads(r) for r in results_json]
    best = sorted(
        results,
        key=lambda r: (-r["accuracy"], r["max_depth"], r["min_samples_split"]),
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