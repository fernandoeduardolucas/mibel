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