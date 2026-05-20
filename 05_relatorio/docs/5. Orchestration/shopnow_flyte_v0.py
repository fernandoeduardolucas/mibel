import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Tuple

from flytekit import task, workflow
import trino
from trino.dbapi import connect


# -----------------------------
# Trino helper
# -----------------------------
@dataclass
class TrinoConfig:
    host: str = os.getenv("TRINO_HOST", "trino")
    port: int = int(os.getenv("TRINO_PORT", "8080"))
    user: str = os.getenv("TRINO_USER", "tead")
    http_scheme: str = os.getenv("TRINO_SCHEME", "http")
    catalog: str = os.getenv("TRINO_CATALOG", "iceberg")
    schema: str = os.getenv("TRINO_SCHEMA", "tead_flyte")


def _connect_trino(cfg: TrinoConfig):
    return connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        http_scheme=cfg.http_scheme,
        catalog=cfg.catalog,
        schema=cfg.schema,
    )


def run_sql(sql: str, fetch: bool = False) -> List[Tuple]:
    """
    Execute one statement. For multi-statement scripts call run_sql_script.
    """
    cfg = TrinoConfig()
    conn = _connect_trino(cfg)
    cur = conn.cursor()
    cur.execute(sql)
    if fetch:
        return cur.fetchall()
    return []


def run_sql_script(script: str) -> None:
    """
    Naive splitter: good enough for this lab (avoid semicolons inside strings).
    """
    statements = [s.strip() for s in script.split(";") if s.strip()]
    for stmt in statements:
        run_sql(stmt, fetch=False)


# -----------------------------
# Flyte tasks
# -----------------------------
@task
def compute_window(logical_date: str, lookback_days: int) -> Tuple[str, str]:
    """
    Returns ISO timestamps: window_start (inclusive), window_end (exclusive)
    Policy: each run repairs the last `lookback_days` days up to logical_date.
    """
    d = date.fromisoformat(logical_date)
    window_end = datetime(d.year, d.month, d.day) + timedelta(days=1)
    window_start = window_end - timedelta(days=lookback_days + 1)  # include logical day + lookback
    return window_start.isoformat(sep=" "), window_end.isoformat(sep=" ")


@task
def upsert_silver(window_start_ts: str, window_end_ts: str) -> None:
    """
    Deduplicate by event_id keeping latest ingest_time.
    Upsert into silver so reruns are safe.
    """
    sql = f"""
    MERGE INTO iceberg.tead_flyte.silver_web_events_clean t
    USING (
      SELECT
        event_id,
        event_type,
        customer_id,
        event_time,
        CAST(date(event_time) AS DATE) AS event_date,
        order_id,
        order_total_eur,
        source,
        ingest_time,
        batch_id
      FROM (
        SELECT
          *,
          row_number() OVER (PARTITION BY event_id ORDER BY ingest_time DESC) AS rn
        FROM iceberg.tead_flyte.bronze_web_events_raw
        WHERE event_time >= TIMESTAMP '{window_start_ts}'
          AND event_time <  TIMESTAMP '{window_end_ts}'
      )
      WHERE rn = 1
    ) s
    ON (t.event_id = s.event_id)
    WHEN MATCHED AND s.ingest_time > t.ingest_time THEN UPDATE SET
      event_type = s.event_type,
      customer_id = s.customer_id,
      event_time = s.event_time,
      event_date = s.event_date,
      order_id = s.order_id,
      order_total_eur = s.order_total_eur,
      source = s.source,
      ingest_time = s.ingest_time,
      batch_id = s.batch_id
    WHEN NOT MATCHED THEN INSERT (
      event_id, event_type, customer_id, event_time, event_date,
      order_id, order_total_eur, source, ingest_time, batch_id
    )
    VALUES (
      s.event_id, s.event_type, s.customer_id, s.event_time, s.event_date,
      s.order_id, s.order_total_eur, s.source, s.ingest_time, s.batch_id
    )
    """
    run_sql(sql)


@task(retries=1, timeout=timedelta(minutes=15))
def build_gold_append_then_fail(window_start_ts: str, window_end_ts: str, fail_after_write: bool) -> None:
    """
    INTENTIONALLY UNSAFE: appends into gold and may fail afterwards.
    If fail_after_write=True, Flyte retries this *task* and appends again => duplicates.
    """
    sql = f"""
    INSERT INTO iceberg.tead_flyte.gold_customer_daily_kpis
    SELECT
      event_date AS day,
      customer_id,
      sum(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_views,
      sum(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart,
      sum(CASE WHEN event_type = 'checkout_started' THEN 1 ELSE 0 END) AS checkout_started,
      sum(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
      count(DISTINCT CASE WHEN event_type = 'purchase' THEN order_id END) AS orders,
      CAST(sum(CASE WHEN event_type = 'purchase' THEN coalesce(order_total_eur, CAST(0 AS DECIMAL(10,2))) ELSE CAST(0 AS DECIMAL(10,2)) END) AS DECIMAL(18,2)) AS revenue_eur,
      true AS active,
      max(event_time) AS last_event_time,
      current_timestamp AS produced_at
    FROM iceberg.tead_flyte.silver_web_events_clean
    WHERE event_time >= TIMESTAMP '{window_start_ts}'
      AND event_time <  TIMESTAMP '{window_end_ts}'
      AND customer_id IS NOT NULL
    GROUP BY 1,2
    """
    run_sql(sql)

    if fail_after_write:
        raise RuntimeError("Intentional failure AFTER write to demonstrate duplicates on retry.")


@task
def validate_gold(window_start_ts: str, window_end_ts: str) -> str:
    """
    Minimal contract checks:
      - PK uniqueness (day, customer_id)
      - non-null customer_id
    """
    dup_sql = f"""
    SELECT count(*) AS n
    FROM (
      SELECT day, customer_id, count(*) AS c
      FROM iceberg.tead_flyte.gold_customer_daily_kpis
      WHERE day >= CAST(date(TIMESTAMP '{window_start_ts}') AS DATE)
        AND day <  CAST(date(TIMESTAMP '{window_end_ts}') AS DATE)
      GROUP BY 1,2
      HAVING count(*) > 1
    )
    """
    null_sql = f"""
    SELECT count(*) AS n
    FROM iceberg.tead_flyte.gold_customer_daily_kpis
    WHERE day >= CAST(date(TIMESTAMP '{window_start_ts}') AS DATE)
      AND day <  CAST(date(TIMESTAMP '{window_end_ts}') AS DATE)
      AND customer_id IS NULL
    """

    dup = run_sql(dup_sql, fetch=True)[0][0]
    nul = run_sql(null_sql, fetch=True)[0][0]

    if dup > 0:
        raise ValueError(f"Contract violation: duplicated (day, customer_id) rows = {dup}")
    if nul > 0:
        raise ValueError(f"Contract violation: null customer_id rows = {nul}")

    return f"OK: duplicates={dup}, null_customer_id={nul}"


# -----------------------------
# Flyte workflow
# -----------------------------
@workflow
def shopnow_kpis_v0(logical_date: str, lookback_days: int = 0, fail_after_write: bool = True) -> str:
    """
    v0 is intentionally unsafe to reproduce 'duplicates after retries'.
    Use lookback_days=0 to focus on one day.
    """
    window_start, window_end = compute_window(logical_date=logical_date, lookback_days=lookback_days)
    upsert_silver(window_start_ts=window_start, window_end_ts=window_end)
    build_gold_append_then_fail(window_start_ts=window_start, window_end_ts=window_end, fail_after_write=fail_after_write)
    return validate_gold(window_start_ts=window_start, window_end_ts=window_end)