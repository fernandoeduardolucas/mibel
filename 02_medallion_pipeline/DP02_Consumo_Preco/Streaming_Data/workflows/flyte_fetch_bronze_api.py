"""
Workflow Flyte: fetch das APIs para a camada Bronze — Streaming_Data (DP-02).

Obtém dados de consumo e preços via Energy-Charts API (Fraunhofer ISE):
  - Consumo horário PT: public_power?country=pt  (Load extraído de production_types)
  - Preços day-ahead PT: price?bzn=PT (OMIE/MIBEL)

Sem autenticação — APIs abertas ao público.

Execução:
    pyflyte run workflows/flyte_fetch_bronze_api.py fetch_bronze_api \\
        --start_date 2024-01-01 --end_date 2024-01-07
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

import requests
import trino
from flytekit import task, workflow

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))

CONSUMO_URL = "https://api.energy-charts.info/public_power"
PRECO_URL   = "https://api.energy-charts.info/price"

BATCH_SIZE = 2000


def _trino_conn(schema: str = "bronze") -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="admin",
        catalog="iceberg",
        schema=schema,
    )


def _exec(cur, sql: str) -> None:
    cur.execute(sql)
    cur.fetchall()


def _flush_batch(cur, table: str, cols: str, batch: list[str]) -> int:
    if not batch:
        return 0
    cur.execute(f"INSERT INTO {table} ({cols}) VALUES {', '.join(batch)}")
    cur.fetchall()
    n = len(batch)
    batch.clear()
    return n


# ---------------------------------------------------------------------------
# Task 1: fetch consumo horário via Energy-Charts total_power API
# ---------------------------------------------------------------------------
@task(retries=3)
def fetch_consumo_api(start_date: date, end_date: date) -> int:
    """
    Obtém carga elétrica nacional horária da Energy-Charts API (dados ENTSO-E).

    Endpoint: https://api.energy-charts.info/public_power?country=pt
    Resposta: {"unix_seconds": [...], "production_types": [{"name": "Load", "data": [...]}]}
    O campo "Load" vem dentro de production_types (total_power nao suporta PT).

    Idempotente: apaga process_dates do intervalo antes de inserir.
    """
    start_iso = f"{start_date.isoformat()}T00:00:00Z"
    end_iso   = f"{end_date.isoformat()}T23:59:59Z"
    url = f"{CONSUMO_URL}?country=pt&start={start_iso}&end={end_iso}"

    print(f"[fetch_consumo] GET {url}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    unix_seconds = data.get("unix_seconds", [])
    # public_power returns production_types list; extract "Load" entry
    load_values: list = []
    for pt in data.get("production_types", []):
        if pt.get("name") == "Load":
            load_values = pt.get("data", [])
            break

    if not unix_seconds or not load_values:
        print(f"[fetch_consumo] AVISO: sem dados para {start_date}–{end_date}.")
        return 0

    records: list[dict] = []
    fetch_dt = date.today()
    for i, unix_ts in enumerate(unix_seconds):
        if i >= len(load_values) or load_values[i] is None:
            continue
        ts = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
        records.append({
            "ts_utc": ts,
            "total": float(load_values[i]),
            "source_url": url,
            "fetch_date": fetch_dt,
            "process_date": ts.date(),
        })

    if not records:
        return 0

    conn = _trino_conn()
    cur  = conn.cursor()

    process_dates = sorted({r["process_date"] for r in records})
    for pd in process_dates:
        _exec(cur, f"DELETE FROM iceberg.bronze.consumo_api_raw WHERE process_date = DATE '{pd}'")
        print(f"[fetch_consumo] Limpa {pd}.")

    cols = "ts_utc, total, source_url, fetch_date, process_date"
    batch: list[str] = []
    total = 0

    for r in records:
        ts_str = r["ts_utc"].strftime("%Y-%m-%d %H:%M:%S.000000 UTC")
        batch.append(
            f"(TIMESTAMP '{ts_str}', {r['total']}, '{r['source_url']}', "
            f"DATE '{r['fetch_date']}', DATE '{r['process_date']}')"
        )
        if len(batch) >= BATCH_SIZE:
            total += _flush_batch(cur, "iceberg.bronze.consumo_api_raw", cols, batch)

    total += _flush_batch(cur, "iceberg.bronze.consumo_api_raw", cols, batch)
    conn.close()
    print(f"[fetch_consumo] Inseridos: {total} registos ({start_date}–{end_date}).")
    return total


# ---------------------------------------------------------------------------
# Task 2: fetch preços horários via Energy-Charts price API
# ---------------------------------------------------------------------------
@task(retries=3)
def fetch_preco_api(start_date: date, end_date: date) -> int:
    """
    Obtém preços day-ahead horários da Energy-Charts API (OMIE zona PT).

    Endpoint: https://api.energy-charts.info/price?bzn=PT
    Resposta: {"unix_seconds": [...], "price": [...]}.
    price_spain_eur_mwh fica NULL (não disponível neste endpoint).

    Idempotente: apaga process_dates do intervalo antes de inserir.
    """
    start_iso = f"{start_date.isoformat()}T00:00:00Z"
    end_iso   = f"{end_date.isoformat()}T23:59:59Z"
    url = f"{PRECO_URL}?bzn=PT&start={start_iso}&end={end_iso}"

    print(f"[fetch_preco] GET {url}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    unix_seconds = data.get("unix_seconds", [])
    prices       = data.get("price", [])

    if not unix_seconds or not prices:
        print(f"[fetch_preco] AVISO: sem dados para {start_date}–{end_date}.")
        return 0

    records: list[dict] = []
    fetch_dt = date.today()
    for i, unix_ts in enumerate(unix_seconds):
        if i >= len(prices):
            break
        ts = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
        price_val = prices[i]
        records.append({
            "ts_utc": ts,
            "price_pt": float(price_val) if price_val is not None else None,
            "source_url": url,
            "fetch_date": fetch_dt,
            "process_date": ts.date(),
        })

    if not records:
        return 0

    conn = _trino_conn()
    cur  = conn.cursor()

    process_dates = sorted({r["process_date"] for r in records})
    for pd in process_dates:
        _exec(cur, f"DELETE FROM iceberg.bronze.preco_api_raw WHERE process_date = DATE '{pd}'")
        print(f"[fetch_preco] Limpa {pd}.")

    cols  = "ts_utc, price_portugal_eur_mwh, price_spain_eur_mwh, source_url, fetch_date, process_date"
    batch: list[str] = []
    total = 0

    for r in records:
        ts_str   = r["ts_utc"].strftime("%Y-%m-%d %H:%M:%S.000000 UTC")
        pt_price = str(r["price_pt"]) if r["price_pt"] is not None else "NULL"
        batch.append(
            f"(TIMESTAMP '{ts_str}', {pt_price}, NULL, '{r['source_url']}', "
            f"DATE '{r['fetch_date']}', DATE '{r['process_date']}')"
        )
        if len(batch) >= BATCH_SIZE:
            total += _flush_batch(cur, "iceberg.bronze.preco_api_raw", cols, batch)

    total += _flush_batch(cur, "iceberg.bronze.preco_api_raw", cols, batch)
    conn.close()
    print(f"[fetch_preco] Inseridos: {total} registos ({start_date}–{end_date}).")
    return total


# ---------------------------------------------------------------------------
# Workflow: fetch ambas as fontes em paralelo
# ---------------------------------------------------------------------------
@workflow
def fetch_bronze_api(
    start_date: date = date.today() - timedelta(days=6),
    end_date:   date = date.today(),
) -> None:
    """
    Obtém dados das APIs Energy-Charts e insere na camada Bronze.

    As duas tarefas (consumo + preço) são independentes e correm em paralelo.

    Pré-requisito: tabelas iceberg.bronze.consumo_api_raw e preco_api_raw devem existir.

    Execução:
        pyflyte run workflows/flyte_fetch_bronze_api.py fetch_bronze_api \\
            --start_date 2024-01-01 --end_date 2024-01-07
    """
    fetch_consumo_api(start_date=start_date, end_date=end_date)
    fetch_preco_api(start_date=start_date, end_date=end_date)
