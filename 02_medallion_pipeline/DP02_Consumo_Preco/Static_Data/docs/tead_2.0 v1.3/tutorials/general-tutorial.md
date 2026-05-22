# General Tutorial

This tutorial is a general walkthrough to creating a data lake according to the Medallion architecture and implementing flye workflows that move the data and also train ML models. Specifically, it shows:
- How to create a Medallion architecture in a data lake
- How to create a landing zone and import data to the data lake
- How to include libraries in containerized Flyte tasks
- How to implement idempotency through MERGE
- How to move data between layers with workflows
- How to train a ML model using scikit-learn, and writing results (including the model) to MLFlow



## Creating the Medallion architecture

In Trino, run the following commands to create the three zones of our data lake.

```sql
CREATE SCHEMA IF NOT EXISTS iceberg.bronze WITH (location = 's3a://warehouse/bronze/');
CREATE SCHEMA IF NOT EXISTS iceberg.silver WITH (location = 's3a://warehouse/silver/');
CREATE SCHEMA IF NOT EXISTS iceberg.gold   WITH (location = 's3a://warehouse/gold/');
```

## Creating the landing zone
Now, let's create a landing zone for the datasets we will receive from external sources, such as CSV files.

```sql
CREATE SCHEMA IF NOT EXISTS hive.landing 
WITH (location = 's3a://warehouse/landing/');
````

This tutorial uses the well known `titanic.csv` dataset. In MinIO, create the path `warehouse/landing/titanic` and copy the dataset there. 

Next, run the script below to create a hive table that is filled with the content of the CSV. Note that if there are multiple files (with the same schema) in the folder, their content will be uploaded to the same table.

```sql
CREATE TABLE IF NOT EXISTS hive.landing.titanic_csv (
    PassengerId VARCHAR,
    Survived VARCHAR,
    Pclass VARCHAR,
    Name VARCHAR,
    Sex VARCHAR,
    Age VARCHAR,
    SibSp VARCHAR,
    Parch VARCHAR,
    Ticket VARCHAR,
    Fare VARCHAR,
    Cabin VARCHAR,
    Embarked VARCHAR
) 
WITH (
    format = 'CSV',
    external_location = 's3a://warehouse/landing/titanic/', 
    skip_header_line_count = 1,
    csv_quote = '"',
    csv_separator = ','
);
```

You can check that the data was properly inserted by running `select * from hive.landing.titanic_csv`.

## Bronze

Now, let's create the bronze table to hold the titanic dataset in parquet format. We are partitioning by two fields (just to show it is possible), although it does not really make much sense in this case. 

```sql
CREATE TABLE IF NOT EXISTS iceberg.bronze.titanic (
    passenger_id BIGINT,
    survived INTEGER,
    pclass INTEGER,
    name VARCHAR,
    sex VARCHAR,
    age DOUBLE,
    sib_sp INTEGER,
    parch INTEGER,
    ticket VARCHAR,
    fare DOUBLE,
    cabin VARCHAR,
    embarked VARCHAR,
    _ingested_at TIMESTAMP(6) WITH TIME ZONE
) 
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['sex', 'day(_ingested_at)'],
    location = 's3a://warehouse/bronze/titanic/'
);
```

Now we're ready to move the data from hive to iceberg. After this, we could eventually delete the file and table from the landing zone.

```sql
INSERT INTO iceberg.bronze.titanic
SELECT 
    CAST(PassengerId AS BIGINT),
    CAST(Survived AS INTEGER),
    CAST(Pclass AS INTEGER),
    Name,
    Sex,
    TRY_CAST(Age AS DOUBLE), 
    CAST(SibSp AS INTEGER),
    CAST(Parch AS INTEGER),
    Ticket,
    TRY_CAST(Fare AS DOUBLE),
    Cabin,
    Embarked,
    now() AS _ingested_at
FROM hive.landing.titanic_csv;
````

You can check the data is already in bronze by running

```sql
SELECT *
FROM iceberg.bronze.titanic
ORDER BY passenger_id 
LIMIT 10;
```

## Silver

As you can see if you analyze the data in Bronze, this dataset has some data quality issues. Namely, in many cases, the field `Age` has missing data. 

Let's create a workflow that reads data from bronze and filters it based on `Age`, inserting only the rows that have this field.

The workflow below is also used to highlight several relevant aspects of Flyte:
- It shows how ImageSpecs can be defined when a Flyte Task needs some import. For instance, in this case, we are creating an image that contains the trino library. This image is then used in the respective Task, in which we must also include the import. This is because, as we have seen in Class 5, Tasks in Flyte are containerized and each is shipped and runs in its own environment. 
- It shows how to specify the configuration of Trino so that Flyte can connect to it
- It shows how to use MERGE to avoid duplicate data in Silver when the workflow is run multiple times for the same data, effectively implementing the principle of Idempotency, as discussed in Class.

```python
import trino
from flytekit import task, workflow, ImageSpec
from datetime import datetime


image_spec = ImageSpec(
    name="titanic_clean",
    registry="localhost:30000",
    packages=[
        "trino"
    ]
)

TRINO_HOST = "host.docker.internal"
TRINO_PORT = 8080
TRINO_USER = "tead"
TRINO_HTTP_SCHEME = "http"
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "silver"


@task(container_image=image_spec)
def bronze_to_silver_cleaning():
    import trino

    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA
    )
    cur = conn.cursor()


    merge_sql = """
    MERGE INTO iceberg.silver.titanic AS target
    USING (
        SELECT * FROM iceberg.bronze.titanic
        WHERE age IS NOT NULL
    ) AS source
    ON target.passenger_id = source.passenger_id
    WHEN NOT MATCHED THEN
        INSERT (passenger_id, survived, pclass, name, sex, age, sib_sp, parch, ticket, fare, cabin, embarked, _updated_at)
        VALUES (source.passenger_id, source.survived, source.pclass, source.name, source.sex, source.age, 
                source.sib_sp, source.parch, source.ticket, source.fare, source.cabin, source.embarked, now())
    """
    
    cur.execute(merge_sql)

@workflow
def titanic_cleaning_workflow():
    bronze_to_silver_cleaning()
```

You can run this workflow with `pyflyte run --remote titanic_clean_data.py titanic_cleaning_workflow`, after which you should have your clean data in Silver.

## Gold

Now, we'll create a Data Product in Gold for a ML team. Specifically, we'll create a dataset that has only the relevant features for training ML models.

Let's start by creating the table in Gold:

```sql
CREATE TABLE IF NOT EXISTS iceberg.gold.titanic_ml_features (
    survived INTEGER,
    pclass INTEGER,
    sex VARCHAR,
    age DOUBLE,
    sib_sp INTEGER,
    parch INTEGER,
    fare DOUBLE,
    embarked VARCHAR,
    _feature_timestamp TIMESTAMP(6) WITH TIME ZONE
) 
WITH (
    format = 'PARQUET'
);
````

Next, create and run the following workflow, which populates the Gold Table we just created. Note that in this case, to avoid duplicate data, we are deleting the previous data in the table.

You can run the workflow with `pyflyte run --remote titanic_generate_ML_features.py titanic_generate_ML`, after which your Gold table will be populated.


```python
from flytekit import task, workflow, ImageSpec
import sys

image_spec = ImageSpec(
    name="titanic_gold_ml",
    registry="localhost:30000",
    packages=["trino"]
)

TRINO_HOST = "host.docker.internal"
TRINO_PORT = 8080
TRINO_USER = "tead"
TRINO_HTTP_SCHEME = "http"
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "gold"

@task(container_image=image_spec)
def silver_to_gold_features():
    import trino
    
    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA
        )
        cur = conn.cursor()

        cur.execute("DELETE FROM iceberg.gold.titanic_ml_features")

        insert_sql = """
        INSERT INTO iceberg.gold.titanic_ml_features
        SELECT 
            survived, 
            pclass, 
            sex, 
            age, 
            sib_sp, 
            parch, 
            fare, 
            embarked,
            now()
        FROM iceberg.silver.titanic
        """
        
        cur.execute(insert_sql)

    except Exception as e:
        print(f"ERRO NA CAMADA GOLD: {e}", file=sys.stderr)
        raise e

@workflow
def titanic_generate_ML():
    silver_to_gold_features()
```

## Using Gold data for ML

Finally, let's train a ML model and log it on MLFlow.

This can be done with the workflow below, which you can run with `pyflyte run --remote titanic_train_ML.py titanic_training_workflow`.

In this workflow, note that:
- We are now using a larger number of libraries in our containerized task
- We added some variables which are necessary to connect to MLFLow and serialize models

```python
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
```

Once you run this workflow, you can head over to MLFlow and check your experiment. If you run this workflow multiple times, you will see different models in the same experiment, which you can compare to pick the best one.

## ML Inference

**TODO: Apparently, there's some issue in writing / reading models to / from MinIO so this script is not working ATM.**

Now, let's use the trained model to do batch inference. Let's start by creating a Table that will hold the predictions of the model. It should have the same schema as the original dataset (input features), with a new feature for the predicted value (`predicted_survived`).

```sql
CREATE TABLE IF NOT EXISTS iceberg.gold.titanic_predictions (
    passenger_id BIGINT,
    pclass INTEGER,
    sex VARCHAR,
    age DOUBLE,
    sib_sp INTEGER,
    parch INTEGER,
    fare DOUBLE,
    embarked VARCHAR,
    predicted_survived INTEGER, 
    prediction_timestamp TIMESTAMP(6) WITH TIME ZONE
) 
WITH (
    format = 'PARQUET'
);
```

And now, let's run the workflow below to use a model to batch predict on our Silver data. Note that this is not correct as this is the same data that was used for training, since we did not split it.

```python
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
```
