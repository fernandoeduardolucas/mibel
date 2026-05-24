"""
Workflow Flyte: fetch das APIs para a camada Bronze — Energy-Charts fallback (DP-02).

Fonte: Energy-Charts API (Fraunhofer ISE) — api.energy-charts.info
  - Consumo horário PT : GET /total_power?country=pt  (redistribui ENTSO-E)
  - Preços day-ahead PT: GET /price?bzn=PT             (OMIE/MIBEL zona PT)
  - Preços day-ahead ES: GET /price?bzn=ES             (OMIE/MIBEL zona ES)

Sem autenticação — alternativa ao flyte_fetch_bronze_api.py quando ENTSOE_TOKEN
não está disponível. As tabelas Bronze de destino são as mesmas (_api suffix),
pelo que Silver, Gold e quality checks não precisam de alterações.

Nota: Energy-Charts pode devolver consumo em granularidade 15-min para alguns
períodos. O Silver já agrega por DATE_TRUNC('hour') + AVG, pelo que é transparente.

Execução standalone:
    pyflyte run workflows/flyte_fetch_bronze_energycharts.py fetch_bronze_energycharts \\
        --start_date 2024-01-01 --end_date 2024-01-07

Via orquestrador:
    python run_streaming_pipeline.py --source energycharts --skip-docker --days 7
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

import requests
import trino
from flytekit import task, workflow

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))

BASE_URL          = "https://api.energy-charts.info"
LOAD_TYPE_NAME    = "Load"   # nome do production_type na resposta /public_power
BATCH_SIZE = 2000


# ---------------------------------------------------------------------------
# Helpers partilhados
# ---------------------------------------------------------------------------

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


def _ec_get(endpoint: str, params: dict) -> dict:
    url  = f"{BASE_URL}/{endpoint}"
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def _find_load_series(production_types: list[dict]) -> list | None:
    """Devolve a lista de valores de carga. Aceita variações de capitalização."""
    for pt in production_types:
        if pt.get("name", "").strip().lower() == LOAD_TYPE_NAME.lower():
            return pt.get("data")
    return None


def _aggregate_to_hourly(ts_value_pairs: list[tuple[datetime, float]]) -> dict[datetime, float]:
    """Agrega pares (ts, valor) de granularidade sub-horária para horária via média."""
    buckets: dict[datetime, list[float]] = {}
    for ts, val in ts_value_pairs:
        hour_ts = ts.replace(minute=0, second=0, microsecond=0)
        buckets.setdefault(hour_ts, []).append(val)
    return {ts: sum(vals) / len(vals) for ts, vals in buckets.items()}


# ---------------------------------------------------------------------------
# Task 1: consumo horário PT — Energy-Charts /total_power
# ---------------------------------------------------------------------------

@task(retries=3)
def fetch_consumo_ec(start_date: date, end_date: date) -> int:
    """
    Obtém carga eléctrica nacional horária via Energy-Charts /total_power (PT).

    Sem autenticação. Idempotente: apaga process_dates do intervalo antes de inserir.
    Destino: iceberg.bronze.consumo_api_raw — schema idêntico ao pipeline ENTSO-E.
    """
    source_url = f"{BASE_URL}/public_power?country=pt&start={start_date}&end={end_date}"
    print(f"[fetch_consumo EC] GET {source_url}")

    data = _ec_get(
        "public_power",
        {"country": "pt", "start": start_date.isoformat(), "end": end_date.isoformat()},
    )

    unix_seconds = data.get("unix_seconds", [])
    load_data    = _find_load_series(data.get("production_types", []))

    if not load_data or not unix_seconds:
        print(f"[fetch_consumo EC] Sem dados de carga para {start_date}--{end_date}.")
        return 0

    # Energy-Charts pode devolver sub-horário (15-min); agregar para horário
    raw_pairs = [
        (datetime.fromtimestamp(ts, tz=timezone.utc), float(v))
        for ts, v in zip(unix_seconds, load_data)
        if v is not None
    ]
    hourly_map = _aggregate_to_hourly(raw_pairs)

    fetch_dt = date.today()
    records: list[dict] = []

    for ts_utc, value in sorted(hourly_map.items()):
        if ts_utc.date() < start_date or ts_utc.date() > end_date:
            continue
        records.append({
            "ts_utc":       ts_utc,
            "total":        round(value, 3),
            "source_url":   source_url,
            "fetch_date":   fetch_dt,
            "process_date": ts_utc.date(),
        })

    if not records:
        print(f"[fetch_consumo EC] Serie vazia para {start_date}--{end_date}.")
        return 0

    conn = _trino_conn()
    cur  = conn.cursor()

    process_dates = sorted({r["process_date"] for r in records})
    for pd_date in process_dates:
        _exec(
            cur,
            f"DELETE FROM iceberg.bronze.consumo_api_raw "
            f"WHERE process_date = DATE '{pd_date}'",
        )
        print(f"[fetch_consumo EC] Limpa {pd_date}.")

    cols  = "ts_utc, total, source_url, fetch_date, process_date"
    batch: list[str] = []
    total = 0

    for r in records:
        ts_str = r["ts_utc"].strftime("%Y-%m-%d %H:%M:%S.000000 UTC")
        safe_url = r["source_url"].replace("'", "''")
        batch.append(
            f"(TIMESTAMP '{ts_str}', {r['total']}, '{safe_url}', "
            f"DATE '{r['fetch_date']}', DATE '{r['process_date']}')"
        )
        if len(batch) >= BATCH_SIZE:
            total += _flush_batch(cur, "iceberg.bronze.consumo_api_raw", cols, batch)

    total += _flush_batch(cur, "iceberg.bronze.consumo_api_raw", cols, batch)
    conn.close()
    print(f"[fetch_consumo EC] Inseridos: {total} registos ({start_date}--{end_date}).")
    return total


# ---------------------------------------------------------------------------
# Task 2: preços day-ahead PT + ES — Energy-Charts /price
# ---------------------------------------------------------------------------

@task(retries=3)
def fetch_preco_ec(start_date: date, end_date: date) -> int:
    """
    Obtém preços day-ahead PT e ES via Energy-Charts /price.

    Sem autenticação. Idempotente: apaga process_dates do intervalo antes de inserir.
    Destino: iceberg.bronze.preco_api_raw — schema idêntico ao pipeline ENTSO-E.
    """
    print(f"[fetch_preco EC] GET precos PT+ES: {start_date} -> {end_date}")

    source_url = f"{BASE_URL}/price?bzn=PT&start={start_date}&end={end_date}"

    # --- PT
    try:
        pt_data   = _ec_get("price", {"bzn": "PT", "start": start_date.isoformat(), "end": end_date.isoformat()})
        pt_unix   = pt_data.get("unix_seconds", [])
        pt_prices = pt_data.get("price", [])
    except Exception as exc:
        print(f"[fetch_preco EC] Erro PT: {exc}")
        pt_unix, pt_prices = [], []

    # --- ES
    try:
        es_data   = _ec_get("price", {"bzn": "ES", "start": start_date.isoformat(), "end": end_date.isoformat()})
        es_unix   = es_data.get("unix_seconds", [])
        es_prices = es_data.get("price", [])
    except Exception as exc:
        print(f"[fetch_preco EC] Erro ES: {exc}")
        es_unix, es_prices = [], []

    # Agregar de sub-horário (15-min) para horário via média
    pt_map = _aggregate_to_hourly([
        (datetime.fromtimestamp(ts, tz=timezone.utc), float(p))
        for ts, p in zip(pt_unix, pt_prices) if p is not None
    ])
    es_map = _aggregate_to_hourly([
        (datetime.fromtimestamp(ts, tz=timezone.utc), float(p))
        for ts, p in zip(es_unix, es_prices) if p is not None
    ])

    all_ts = sorted(set(pt_map) | set(es_map))
    if not all_ts:
        print(f"[fetch_preco EC] Sem dados para {start_date}--{end_date}.")
        return 0

    fetch_dt = date.today()
    records: list[dict] = []

    for ts_utc in all_ts:
        if ts_utc.date() < start_date or ts_utc.date() > end_date:
            continue
        records.append({
            "ts_utc":                 ts_utc,
            "price_portugal_eur_mwh": pt_map.get(ts_utc),
            "price_spain_eur_mwh":    es_map.get(ts_utc),
            "source_url":             source_url,
            "fetch_date":             fetch_dt,
            "process_date":           ts_utc.date(),
        })

    if not records:
        print(f"[fetch_preco EC] Sem registos no intervalo {start_date}--{end_date}.")
        return 0

    conn = _trino_conn()
    cur  = conn.cursor()

    process_dates = sorted({r["process_date"] for r in records})
    for pd_date in process_dates:
        _exec(
            cur,
            f"DELETE FROM iceberg.bronze.preco_api_raw "
            f"WHERE process_date = DATE '{pd_date}'",
        )
        print(f"[fetch_preco EC] Limpa {pd_date}.")

    cols  = "ts_utc, price_portugal_eur_mwh, price_spain_eur_mwh, source_url, fetch_date, process_date"
    batch: list[str] = []
    total = 0

    for r in records:
        ts_str  = r["ts_utc"].strftime("%Y-%m-%d %H:%M:%S.000000 UTC")
        pt_val  = str(r["price_portugal_eur_mwh"]) if r["price_portugal_eur_mwh"] is not None else "NULL"
        es_val  = str(r["price_spain_eur_mwh"])    if r["price_spain_eur_mwh"]    is not None else "NULL"
        safe_url = r["source_url"].replace("'", "''")
        batch.append(
            f"(TIMESTAMP '{ts_str}', {pt_val}, {es_val}, '{safe_url}', "
            f"DATE '{r['fetch_date']}', DATE '{r['process_date']}')"
        )
        if len(batch) >= BATCH_SIZE:
            total += _flush_batch(cur, "iceberg.bronze.preco_api_raw", cols, batch)

    total += _flush_batch(cur, "iceberg.bronze.preco_api_raw", cols, batch)
    conn.close()
    print(f"[fetch_preco EC] Inseridos: {total} registos ({start_date}--{end_date}).")
    return total


# ---------------------------------------------------------------------------
# Workflow: consumo + preço em paralelo
# ---------------------------------------------------------------------------

@workflow
def fetch_bronze_energycharts(
    start_date: date = date.today() - timedelta(days=6),
    end_date:   date = date.today(),
) -> None:
    """
    Obtém dados Energy-Charts (consumo + preço PT+ES) e insere na camada Bronze.
    Sem autenticação — alternativa ao workflow ENTSO-E.
    As duas tarefas correm em paralelo (sem dependências entre si).

    Execução:
        pyflyte run workflows/flyte_fetch_bronze_energycharts.py fetch_bronze_energycharts \\
            --start_date 2024-01-01 --end_date 2024-01-07
    """
    fetch_consumo_ec(start_date=start_date, end_date=end_date)
    fetch_preco_ec(start_date=start_date, end_date=end_date)
