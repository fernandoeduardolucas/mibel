from flytekit import task, workflow, ImageSpec
import sys
import os

predict_image = ImageSpec(
    name="titanic_inference",
    registry="localhost:30000",
    packages=["trino", "pandas", "scikit-learn", "mlflow", "boto3", "protobuf<5.0.0,>=4.21.0"]
)

TRINO_HOST = "host.docker.internal"
TRINO_PORT = 8080
TRINO_USER = "tead"
TRINO_HTTP_SCHEME = "http"
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "gold"

MLFLOW_TRACKING_URI = "http://host.docker.internal:15000"
MLFLOW_EXPERIMENT = "titanic_survival"
MLFLOW_MODEL = "021ff984d613460f8b16b6f5694c13d2"

os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://host.docker.internal:9000"
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["AWS_EC2_METADATA_DISABLED"] = "true"

@task(container_image=predict_image)
def predict_and_save_to_gold():
    import trino
    import pandas as pd
    import mlflow.sklearn
    from sklearn.preprocessing import LabelEncoder
    

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    model_uri = "runs:/"+MLFLOW_MODEL+"/model"
    model = mlflow.sklearn.load_model(model_uri)

    try:
        conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog=TRINO_CATALOG, schema=TRINO_SCHEMA)
        df = pd.read_sql_query("SELECT * FROM titanic_clean", conn)

        passenger_ids = df['passenger_id']

        le = LabelEncoder()
        df_ml = df[['pclass', 'sex', 'age', 'sib_sp', 'parch', 'fare', 'embarked']].copy()
        df_ml['sex'] = le.fit_transform(df_ml['sex'])
        df_ml['embarked'] = df_ml['embarked'].fillna('S')
        df_ml['embarked'] = le.fit_transform(df_ml['embarked'])

        predictions = model.predict(df_ml)

        results = df.copy()
        results['predicted_survived'] = predictions
        
        cur = conn.cursor()
        cur.execute("DELETE FROM iceberg.gold.titanic_predictions")
        
        insert_sql = """
        INSERT INTO iceberg.gold.titanic_predictions 
        (passenger_id, pclass, sex, age, sib_sp, parch, fare, embarked, predicted_survived, prediction_timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now())
        """
        
        data_to_insert = [
            (row.passenger_id, row.pclass, row.sex, row.age, row.sib_sp, row.parch, row.fare, row.embarked, int(row.predicted_survived))
            for row in results.itertuples()
        ]
        
        cur.executemany(insert_sql, data_to_insert)
        print(f"Previsões concluídas para {len(results)} passageiros.")

    except Exception as e:
        print(f"Erro na Inferência: {e}", file=sys.stderr)
        raise e

@workflow
def titanic_prediction_workflow():
    predict_and_save_to_gold()