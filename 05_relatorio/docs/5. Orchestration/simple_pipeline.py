import os
from dataclasses import dataclass
from datetime import timedelta
from typing import Tuple, List

from flytekit import task, workflow
from trino.dbapi import connect


# ---------- Trino helper ----------
@dataclass
class TrinoConfig:
    host: str = os.getenv("TRINO_HOST", "localhost")
    port: int = int(os.getenv("TRINO_PORT", "8080"))
    user: str = os.getenv("TRINO_USER", "tead")

def run_sql(sql: str, fetch: bool = False) -> List[Tuple]:
    cfg = TrinoConfig()
    conn = connect(host=cfg.host, port=cfg.port, user=cfg.user)
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall() if fetch else []


# ---------- Flyte tasks ----------
@task
def create_output_table(target_table: str) -> None:
    """
    Creates the output Iceberg table if it doesn't exist.
    target_table must be fully qualified: iceberg.schema.table
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
      day DATE,
      event_type VARCHAR,
      event_count BIGINT,
      produced_at TIMESTAMP
    )
    WITH (
      format = 'PARQUET',
      partitioning = ARRAY['day']
    )
    """
    run_sql(sql)

@task(retries=1, timeout=timedelta(minutes=10))
def build_daily_counts(source_table: str, target_table: str, logical_date: str) -> int:
    """
    Very simple, idempotent-ish approach:
    - delete the day from target
    - insert fresh counts for that day
    Returns number of rows written for that day (event types count).
    """
    delete_sql = f"""
    DELETE FROM {target_table}
    WHERE day = DATE '{logical_date}'
    """
    insert_sql = f"""
    INSERT INTO {target_table}
    SELECT
      CAST(date(event_time) AS DATE) AS day,
      event_type,
      count(*) AS event_count,
      current_timestamp AS produced_at
    FROM {source_table}
    WHERE CAST(date(event_time) AS DATE) = DATE '{logical_date}'
    GROUP BY 1, 2
    """
    count_sql = f"""
    SELECT count(*) FROM {target_table}
    WHERE day = DATE '{logical_date}'
    """

    run_sql(delete_sql)
    run_sql(insert_sql)
    n = run_sql(count_sql, fetch=True)[0][0]
    return int(n)


# ---------- Flyte workflow ----------
@workflow
def daily_event_counts(
    logical_date: str,
    source_table: str,
    target_table: str,
) -> int:
    """
    Minimal Flyte workflow:
    1) Ensure target table exists
    2) Build daily aggregation for logical_date
    """
    create_output_table(target_table=target_table)
    return build_daily_counts(
        source_table=source_table,
        target_table=target_table,
        logical_date=logical_date,
    )