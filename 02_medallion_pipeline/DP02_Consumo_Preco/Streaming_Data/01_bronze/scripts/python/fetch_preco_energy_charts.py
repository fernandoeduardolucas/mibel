"""
Fetch de preços day-ahead horários via Energy-Charts API (Fraunhofer ISE).

Fonte: https://api.energy-charts.info/price?bzn=PT
Dados: Preços OMIE/MIBEL day-ahead para Portugal, granularidade horária, €/MWh.
Autenticação: nenhuma.

Nota: O endpoint price?bzn=PT devolve apenas Portugal (sem Espanha separado).
      price_spain_eur_mwh fica NULL no Bronze.

Uso standalone:
    python fetch_preco_energy_charts.py --start 2024-01-01 --end 2024-01-07
    python fetch_preco_energy_charts.py --days 7
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone

import requests

BASE_URL = "https://api.energy-charts.info/price"
BZN = "PT"

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))

BATCH_SIZE = 2000


def _build_url(start: date, end: date) -> str:
    start_iso = f"{start.isoformat()}T00:00:00Z"
    end_iso = f"{end.isoformat()}T23:59:59Z"
    return f"{BASE_URL}?bzn={BZN}&start={start_iso}&end={end_iso}"


def fetch_price_data(start: date, end: date) -> list[dict]:
    """
    Chama a Energy-Charts price API e devolve lista de dicts com ts_utc e preço PT.

    Resposta esperada:
        {
          "unix_seconds": [1704067200, ...],
          "price": [45.23, ...]
        }
    """
    url = _build_url(start, end)
    print(f"[fetch_preco] GET {url}")

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    unix_seconds = data.get("unix_seconds", [])
    prices = data.get("price", [])

    if not unix_seconds or not prices:
        print(f"[fetch_preco] AVISO: resposta sem dados para {start}–{end}.")
        return []

    records = []
    fetch_ts = date.today()
    for i, unix_ts in enumerate(unix_seconds):
        if i >= len(prices):
            break
        price_val = prices[i]
        ts_utc = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
        process_dt = ts_utc.date()
        records.append({
            "ts_utc": ts_utc,
            "price_portugal_eur_mwh": float(price_val) if price_val is not None else None,
            "price_spain_eur_mwh": None,
            "source_url": url,
            "fetch_date": fetch_ts,
            "process_date": process_dt,
        })

    print(f"[fetch_preco] {len(records)} registos obtidos para {start}–{end}.")
    return records


def _trino_conn():
    import trino
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="admin",
        catalog="iceberg",
        schema="bronze",
    )


def insert_to_bronze(records: list[dict], process_dates: list[date]) -> int:
    """
    Insere registos em iceberg.bronze.preco_api_raw.
    Idempotente: apaga as partições process_date antes de inserir.
    """
    if not records:
        print("[fetch_preco] Nenhum registo para inserir.")
        return 0

    conn = _trino_conn()
    cur = conn.cursor()

    for pd in process_dates:
        cur.execute(
            f"DELETE FROM iceberg.bronze.preco_api_raw WHERE process_date = DATE '{pd}'"
        )
        cur.fetchall()
        print(f"[fetch_preco] Limpa process_date={pd}.")

    cols = "ts_utc, price_portugal_eur_mwh, price_spain_eur_mwh, source_url, fetch_date, process_date"
    inserted = 0
    batch: list[str] = []

    def flush():
        nonlocal inserted
        if not batch:
            return
        cur.execute(
            f"INSERT INTO iceberg.bronze.preco_api_raw ({cols}) VALUES {', '.join(batch)}"
        )
        cur.fetchall()
        inserted += len(batch)
        batch.clear()

    for r in records:
        ts_str = r["ts_utc"].strftime("%Y-%m-%d %H:%M:%S.000000 UTC")
        pt_price = str(r["price_portugal_eur_mwh"]) if r["price_portugal_eur_mwh"] is not None else "NULL"
        es_price = "NULL"
        batch.append(
            f"(TIMESTAMP '{ts_str}', "
            f"{pt_price}, "
            f"{es_price}, "
            f"'{r['source_url']}', "
            f"DATE '{r['fetch_date']}', "
            f"DATE '{r['process_date']}')"
        )
        if len(batch) >= BATCH_SIZE:
            flush()

    flush()
    conn.close()
    print(f"[fetch_preco] Total inserido: {inserted} registos.")
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch preços horários via Energy-Charts API")
    parser.add_argument("--start", type=str, help="Data início YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="Data fim YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=7, help="Últimos N dias (default: 7)")
    args = parser.parse_args()

    if args.start and args.end:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
    else:
        end = date.today()
        start = end - timedelta(days=args.days - 1)

    print(f"[fetch_preco] Período: {start} → {end}")

    try:
        import trino  # noqa: F401
    except ImportError:
        print("[fetch_preco] A instalar dependências...")
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "trino", "requests", "-q"], check=True)
        import trino  # noqa: F401

    records = fetch_price_data(start, end)

    process_dates = sorted({r["process_date"] for r in records})
    insert_to_bronze(records, process_dates)


if __name__ == "__main__":
    main()
