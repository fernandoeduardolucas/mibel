from flytekit import task, workflow, ImageSpec
import trino

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
def incremental_bronze_to_silver():
    import trino

    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog=TRINO_CATALOG)
    cur = conn.cursor()

    # 1. Get the Watermark
    cur.execute("SELECT COALESCE(MAX(ingested_at), TIMESTAMP '1970-01-01 00:00:00') FROM silver.web_events_clean")
    watermark = cur.fetchone()[0]
    print(f"Current Watermark: {watermark}")

    # 2. Execute the Incremental MERGE
    # We use event_id to ensure deduplication
    merge_sql = f"""
    MERGE INTO silver.web_events_clean AS target
    USING (
        SELECT 
            event_id, event_type, customer_id, item_id, amount,
            from_iso8601_timestamp(event_time_raw) as event_time,
            ingestion_timestamp
        FROM bronze.web_events_raw
        WHERE ingestion_timestamp > TIMESTAMP '{watermark}'
    ) AS source
    ON target.event_id = source.event_id
    WHEN NOT MATCHED THEN
        INSERT (event_id, event_type, customer_id, item_id, amount, event_time, ingested_at)
        VALUES (source.event_id, source.event_type, source.customer_id, source.item_id, source.amount, source.event_time, source.ingestion_timestamp)
    """
    
    cur.execute(merge_sql)

@task(container_image=image_spec)
def silver_to_gold_batch():
    import trino

    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog=TRINO_CATALOG)
    cur = conn.cursor()

    # Batch Aggregation Logic
    # We re-compute the last few days to handle late-arriving data
    batch_sql = """
    INSERT INTO gold.daily_sales_summary
    SELECT 
        CAST(event_time AS DATE) as sale_date,
        item_id,
        SUM(amount) as total_revenue,
        COUNT(*) as order_count,
        COUNT(DISTINCT customer_id) as unique_customers,
        now() as updated_at
    FROM silver.web_events_clean
    WHERE event_time >= current_date - INTERVAL '7' DAY
    GROUP BY 1, 2
    """
    
    # Note: In a real production scenario, you would DELETE the window 
    # in Gold before INSERTING to avoid duplicates in the Batch layer.
    
    cur.execute(batch_sql)

@workflow
def shopnow_incremental_sync():
    # 1. Promote from Bronze to Silver (Incremental)
    incremental_bronze_to_silver()

@workflow
def shopnow_batch_sync():
    # 2. Create Gold from Silver (Batch)
    silver_to_gold_batch()
