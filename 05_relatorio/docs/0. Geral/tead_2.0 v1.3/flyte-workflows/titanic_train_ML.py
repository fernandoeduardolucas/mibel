from flytekit import task, workflow, ImageSpec
import sys
import os

training_image = ImageSpec(
    name="titanic_training",
    registry="localhost:30000",
    packages=["pandas==2.2.3",
        "scikit-learn==1.6.1",
        "joblib==1.4.2",
        "trino==0.336.0",
        "mlflow==3.10.1",
        "boto3"]
)

TRINO_HOST = "host.docker.internal"
TRINO_PORT = 8080
TRINO_USER = "tead"
TRINO_HTTP_SCHEME = "http"
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "gold"

MLFLOW_TRACKING_URI = "http://host.docker.internal:15000"
MLFLOW_EXPERIMENT = "titanic_survival"

os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://host.docker.internal:9000"
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_EC2_METADATA_DISABLED"] = "true"


@task(container_image=training_image)
def train_titanic_model() -> float:
    import trino
    import pandas as pd
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import accuracy_score
    from sklearn.preprocessing import LabelEncoder
    import mlflow

    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST, 
            port=TRINO_PORT, 
            user=TRINO_USER,
            catalog=TRINO_CATALOG, 
            schema=TRINO_SCHEMA
        )
        query = "SELECT * FROM titanic_ml_features"
        df = pd.read_sql_query(query, conn)


        le = LabelEncoder()
        df['sex'] = le.fit_transform(df['sex'])
        df['embarked'] = df['embarked'].fillna('S')
        df['embarked'] = le.fit_transform(df['embarked'])

        X = df.drop(['survived', '_feature_timestamp'], axis=1)
        y = df['survived']

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        N_ESTIMATORS = 50

        model = RandomForestClassifier(n_estimators=N_ESTIMATORS, random_state=42)
        model.fit(X_train, y_train)

        predictions = model.predict(X_test)
        accuracy = accuracy_score(y_test, predictions)

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment(MLFLOW_EXPERIMENT)

        mlflow.log_param("n_estimators", N_ESTIMATORS)
        mlflow.log_param("model_type", "RandomForest")
        mlflow.log_metric("accuracy", accuracy)        
        mlflow.sklearn.log_model(model, "model")
        
        return float(accuracy)

    except Exception as e:
        print(f"ERRO NO TREINO: {e}", file=sys.stderr)
        raise e

@workflow
def titanic_training_workflow() -> float:
    return train_titanic_model()