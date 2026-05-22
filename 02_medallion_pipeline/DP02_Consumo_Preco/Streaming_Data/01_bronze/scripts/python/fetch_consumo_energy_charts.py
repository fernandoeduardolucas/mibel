"""
Fetch de carga elétrica nacional horária via Energy-Charts API (Fraunhofer ISE).

Fonte: https://api.energy-charts.info/total_power?country=pt
Dados: ENTSO-E load data para Portugal, granularidade horária, em MW.
Autenticação: nenhuma.

Uso standalone:
    python fetch_consumo_energy_charts.py --start 2024-01-01 --end 2024-01-07
    python fetch_consumo_energy_charts.py --days 7
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone

import requests

BASE_URL = "https://api.energy-charts.info/total_power"
COUNTRY = "pt"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))

BATCH_SIZE = 2000


def _build_url(start: date, end: date) -> str:
    start_iso = f"{start.isoformat()}T00:00:00Z"
    end_iso = f"{end.isoformat()}T23:59:59Z"
    return f"{BASE_URL}?country={COUNTRY}&start={start_iso}&end={end_iso}"


def fetch_load_data(start: date, end: date) -> list[dict]:
    """
    Chama a Energy-Charts API e devolve lista de dicts com ts_utc e total (MW).

    Resposta esperada:
        {
          "unix_seconds": [1704067200, ...],
          "Load": [5123.4, ...]    (ou "load" dependendo da versão)
        }
    """
    url = _build_url(start, end)
    print(f"[fetch_consumo] GET {url}")

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    unix_seconds = data.get("unix_seconds", [])

    load_values = (
        data.get("Load")
        or data.get("load")
        or data.get("load_actual")
    )

    if not unix_seconds or not load_values:
        print(f"[fetch_consumo] AVISO: resposta sem dados unix_seconds/load para {start}–{end}.")
        return []

    records = []
    fetch_ts = date.today()
    for i, unix_ts in enumerate(unix_seconds):
        if i >= len(load_values):
            break
        load_val = load_values[i]
        if load_val is None:
            continue
        ts_utc = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
        process_dt = ts_utc.date()
        records.append({
            "ts_utc": ts_utc,
            "total": float(load_val),
            "source_url": url,
            "fetch_date": fetch_ts,
            "process_date": process_dt,
        })

    print(f"[fetch_consumo] {len(records)} registos obtidos para {start}–{end}.")
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
    Insere registos em iceberg.bronze.consumo_api_raw.
    Idempotente: apaga as partições process_date antes de inserir.
    """
    if not records:
        print("[fetch_consumo] Nenhum registo para inserir.")
        return 0

    conn = _trino_conn()
    cur = conn.cursor()

    for pd in process_dates:
        cur.execute(
            f"DELETE FROM iceberg.bronze.consumo_api_raw WHERE process_date = DATE '{pd}'"
        )
        cur.fetchall()
        print(f"[fetch_consumo] Limpa process_date={pd}.")

    cols = "ts_utc, total, source_url, fetch_date, process_date"
    inserted = 0
    batch: list[str] = []

    def flush():
        nonlocal inserted
        if not batch:
            return
        cur.execute(
            f"INSERT INTO iceberg.bronze.consumo_api_raw ({cols}) VALUES {', '.join(batch)}"
        )
        cur.fetchall()
        inserted += len(batch)
        batch.clear()

    for r in records:
        ts_str = r["ts_utc"].strftime("%Y-%m-%d %H:%M:%S.000000 UTC")
        batch.append(
            f"(TIMESTAMP '{ts_str}', "
            f"{r['total']}, "
            f"'{r['source_url']}', "
            f"DATE '{r['fetch_date']}', "
            f"DATE '{r['process_date']}')"
        )
        if len(batch) >= BATCH_SIZE:
            flush()

    flush()
    conn.close()
    print(f"[fetch_consumo] Total inserido: {inserted} registos.")
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch consumo horário via Energy-Charts API")
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

    print(f"[fetch_consumo] Período: {start} → {end}")

    try:
        import trino  # noqa: F401
    except ImportError:
        print("[fetch_consumo] A instalar dependências...")
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "trino", "requests", "-q"], check=True)
        import trino  # noqa: F401

    records = fetch_load_data(start, end)

    process_dates = sorted({r["process_date"] for r in records})
    insert_to_bronze(records, process_dates)


if __name__ == "__main__":
    main()
