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