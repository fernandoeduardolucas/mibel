"""
Fetch de preços horários e consumo diário via REN DataHub API.

Fonte oficial portuguesa (REN — Redes Energéticas Nacionais):
  - Preços MIBEL day-ahead (PT+ES): ElectricityMarketPricesDaily
  - Consumo nacional diário (GWh):  ElectricityConsumptionSupplyDaily

Notas:
  - Preços: 24 valores horários por dia, hora local portuguesa (WET/WEST = UTC±0/+1).
  - Consumo: apenas total diário — sem detalhe horário neste endpoint.
  - Sem autenticação.

Uso standalone:
    python fetch_ren_datahub.py --start 2022-01-01 --end 2022-12-31
    python fetch_ren_datahub.py --days 30
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, timedelta

import requests

BASE_URL     = "https://servicebus.ren.pt/datahubapi/electricity"
PRECO_EP     = f"{BASE_URL}/ElectricityMarketPricesDaily"
CONSUMO_EP   = f"{BASE_URL}/ElectricityConsumptionSupplyDaily"

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))

BATCH_SIZE    = 500
REQUEST_DELAY = 0.3  # segundos entre chamadas à API (cortesia)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _get(url: str, params: dict) -> dict | None:
    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        return resp.json()
    except requests.HTTPError as e:
        print(f"  [AVISO] HTTP {e.response.status_code} para {url} params={params}")
        return None
    except Exception as e:
        print(f"  [AVISO] Erro ao chamar {url}: {e}")
        return None


def fetch_precos_dia(d: date) -> list[dict]:
    """Devolve lista de 24 registos horários de preço para o dia d."""
    url    = PRECO_EP
    params = {"culture": "en-US", "date": d.isoformat()}
    data   = _get(url, params)
    if not data:
        return []

    series = data.get("series", [])
    pt_data = next((s.get("data", []) for s in series if s.get("name") == "PT"), [])
    es_data = next((s.get("data", []) for s in series if s.get("name") == "ES"), [])

    if not pt_data:
        return []

    source = f"{url}?culture=en-US&date={d.isoformat()}"
    records = []
    for hora_idx, pt_price in enumerate(pt_data):
        hora_local = hora_idx + 1  # horas 1-24
        es_price   = es_data[hora_idx] if hora_idx < len(es_data) else None
        records.append({
            "data_local":       d,
            "hora_local":       hora_local,
            "price_pt_eur_mwh": float(pt_price) if pt_price is not None else None,
            "price_es_eur_mwh": float(es_price) if es_price is not None else None,
            "source_url":       source,
            "fetch_date":       date.today(),
            "process_date":     d,
        })
    return records


def fetch_consumo_dia(d: date) -> dict | None:
    """Devolve o consumo nacional diário (GWh) para o dia d."""
    url    = CONSUMO_EP
    params = {"culture": "en-US", "date": d.isoformat()}
    data   = _get(url, params)
    if not data:
        return None

    # Procura CONSUMPTION no array de tipos
    consumo_gwh = None
    for item in data if isinstance(data, list) else []:
        if item.get("type") == "CONSUMPTION":
            consumo_gwh = item.get("daily_Accumulation")
            break

    if consumo_gwh is None:
        return None

    return {
        "data_local":   d,
        "consumo_gwh":  float(consumo_gwh),
        "source_url":   f"{url}?culture=en-US&date={d.isoformat()}",
        "fetch_date":   date.today(),
        "process_date": d,
    }


# ---------------------------------------------------------------------------
# Trino helpers
# ---------------------------------------------------------------------------

def _trino_conn():
    import trino
    return trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT,
        user="admin", catalog="iceberg", schema="bronze",
    )


def _exec(cur, sql: str) -> None:
    cur.execute(sql)
    cur.fetchall()


def _flush(cur, table: str, cols: str, batch: list[str]) -> int:
    if not batch:
        return 0
    cur.execute(f"INSERT INTO {table} ({cols}) VALUES {', '.join(batch)}")
    cur.fetchall()
    n = len(batch)
    batch.clear()
    return n


def insert_precos(records: list[dict], dates: list[date]) -> int:
    if not records:
        return 0
    conn = _trino_conn()
    cur  = conn.cursor()
    for d in dates:
        _exec(cur, f"DELETE FROM iceberg.bronze.preco_ren_raw WHERE process_date = DATE '{d}'")
    cols  = "data_local, hora_local, price_pt_eur_mwh, price_es_eur_mwh, source_url, fetch_date, process_date"
    batch: list[str] = []
    total = 0
    for r in records:
        pt = str(r["price_pt_eur_mwh"]) if r["price_pt_eur_mwh"] is not None else "NULL"
        es = str(r["price_es_eur_mwh"]) if r["price_es_eur_mwh"] is not None else "NULL"
        batch.append(
            f"(DATE '{r['data_local']}', {r['hora_local']}, {pt}, {es}, "
            f"'{r['source_url']}', DATE '{r['fetch_date']}', DATE '{r['process_date']}')"
        )
        if len(batch) >= BATCH_SIZE:
            total += _flush(cur, "iceberg.bronze.preco_ren_raw", cols, batch)
    total += _flush(cur, "iceberg.bronze.preco_ren_raw", cols, batch)
    conn.close()
    return total


def insert_consumo(records: list[dict], dates: list[date]) -> int:
    if not records:
        return 0
    conn = _trino_conn()
    cur  = conn.cursor()
    for d in dates:
        _exec(cur, f"DELETE FROM iceberg.bronze.consumo_ren_daily WHERE process_date = DATE '{d}'")
    cols  = "data_local, consumo_gwh, source_url, fetch_date, process_date"
    batch: list[str] = []
    total = 0
    for r in records:
        batch.append(
            f"(DATE '{r['data_local']}', {r['consumo_gwh']}, "
            f"'{r['source_url']}', DATE '{r['fetch_date']}', DATE '{r['process_date']}')"
        )
        if len(batch) >= BATCH_SIZE:
            total += _flush(cur, "iceberg.bronze.consumo_ren_daily", cols, batch)
    total += _flush(cur, "iceberg.bronze.consumo_ren_daily", cols, batch)
    conn.close()
    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch REN DataHub — preços + consumo")
    parser.add_argument("--start", type=str)
    parser.add_argument("--end",   type=str)
    parser.add_argument("--days",  type=int, default=30)
    args = parser.parse_args()

    if args.start and args.end:
        start = date.fromisoformat(args.start)
        end   = date.fromisoformat(args.end)
    else:
        end   = date.today()
        start = end - timedelta(days=args.days - 1)

    print(f"[ren_datahub] Período: {start} -> {end}")

    try:
        import trino  # noqa: F401
    except ImportError:
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "trino", "requests", "-q"], check=True)
        import trino  # noqa: F401

    preco_records:   list[dict] = []
    consumo_records: list[dict] = []
    dates: list[date] = []

    current = start
    while current <= end:
        dates.append(current)
        recs = fetch_precos_dia(current)
        preco_records.extend(recs)
        consumo = fetch_consumo_dia(current)
        if consumo:
            consumo_records.append(consumo)
        if current.day % 30 == 0 or current == end:
            print(f"  [ren_datahub] {current} — precos: {len(recs)} horas, consumo: {'OK' if consumo else 'N/A'}")
        current += timedelta(days=1)
        time.sleep(REQUEST_DELAY)

    print(f"\n[ren_datahub] Inserindo {len(preco_records)} registos de precos...")
    n_preco = insert_precos(preco_records, dates)
    print(f"[ren_datahub] Inseridos: {n_preco} registos de precos.")

    print(f"[ren_datahub] Inserindo {len(consumo_records)} registos de consumo diario...")
    n_consumo = insert_consumo(consumo_records, dates)
    print(f"[ren_datahub] Inseridos: {n_consumo} registos de consumo diario.")


if __name__ == "__main__":
    main()
