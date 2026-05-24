"""
Workflow Flyte: fetch das APIs para a camada Bronze -- Streaming_Data (DP-02).

Fonte: ENTSO-E Transparency Platform (transparency.entsoe.eu)
  - Consumo horario PT : Actual Total Load via query_load('PT')
  - Precos day-ahead PT: query_day_ahead_prices('PT') -- EUR/MWh
  - Precos day-ahead ES: query_day_ahead_prices('ES') -- EUR/MWh (agora preenchido)

Vantagens vs Energy-Charts (fonte anterior):
  - Dados primarios directo da ENTSO-E (sem intermediario)
  - price_spain_eur_mwh preenchido (ES separado disponivel)
  - Dados horarios exatos (sem 15-min a agregar)
  - Sem "deprecated" em nenhum endpoint

Autenticacao: token gratuito -- ver instrucoes em README ou fetch_consumo_entsoe.py.
Variavel de ambiente: ENTSOE_TOKEN

Execucao:
    ENTSOE_TOKEN=<token> pyflyte run workflows/flyte_fetch_bronze_api.py fetch_bronze_api \\
        --start_date 2024-01-01 --end_date 2024-01-07
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import trino
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError
from flytekit import task, workflow

TRINO_HOST    = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT    = int(os.getenv("TRINO_PORT", "8080"))
ENTSOE_TOKEN  = os.getenv("ENTSOE_TOKEN", "")

BATCH_SIZE = 2000


# ---------------------------------------------------------------------------
# Helpers partilhados
# ---------------------------------------------------------------------------

def _get_client() -> EntsoePandasClient:
    if not ENTSOE_TOKEN:
        raise EnvironmentError(
            "ENTSOE_TOKEN nao definido. "
            "Obtém token gratuito: envia email para transparency@entsoe.eu "
            "com assunto 'RESTful API access' (resposta em ~3 dias). "
            "Depois: $env:ENTSOE_TOKEN = '<o-teu-token>'  (PowerShell)"
        )
    return EntsoePandasClient(api_key=ENTSOE_TOKEN)


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


def _to_series(result) -> pd.Series:
    """Normaliza o retorno de entsoe-py para Series (pode vir DataFrame)."""
    if isinstance(result, pd.DataFrame):
        return result.iloc[:, 0]
    return result


def _ts_to_utc(ts_idx) -> datetime:
    """Converte indice pandas (Timestamp) para datetime UTC."""
    dt = ts_idx.to_pydatetime()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# ---------------------------------------------------------------------------
# Task 1: consumo horario PT -- ENTSO-E Actual Total Load
# ---------------------------------------------------------------------------
@task(retries=3)
def fetch_consumo_api(start_date: date, end_date: date) -> int:
    """
    Obtém carga eléctrica nacional horária via ENTSO-E (Actual Total Load PT).

    A ENTSO-E é a fonte primária dos dados que a Energy-Charts redistribuía.
    Dados em MW horários -- Silver converte para MWh (1 MW x 1h = 1 MWh).

    Idempotente: apaga process_dates do intervalo antes de inserir.
    """
    client   = _get_client()
    start_ts = pd.Timestamp(start_date.isoformat(), tz="UTC")
    end_ts   = pd.Timestamp((end_date + timedelta(days=1)).isoformat(), tz="UTC")

    print(f"[fetch_consumo ENTSOE] Actual Total Load PT: {start_date} -> {end_date}")

    try:
        raw = client.query_load("PT", start=start_ts, end=end_ts)
    except NoMatchingDataError:
        print(f"[fetch_consumo ENTSOE] Sem dados ENTSO-E para {start_date}--{end_date}.")
        return 0

    load_series = _to_series(raw)

    records: list[dict] = []
    fetch_dt   = date.today()
    source_url = (
        f"https://transparency.entsoe.eu/ "
        f"ENTSO-E Actual Total Load PT {start_date}/{end_date}"
    )

    for ts_idx, value in load_series.items():
        if pd.isna(value):
            continue
        ts_utc = _ts_to_utc(ts_idx)
        records.append({
            "ts_utc":       ts_utc,
            "total":        float(value),
            "source_url":   source_url,
            "fetch_date":   fetch_dt,
            "process_date": ts_utc.date(),
        })

    if not records:
        print(f"[fetch_consumo ENTSOE] Serie vazia para {start_date}--{end_date}.")
        return 0

    conn = _trino_conn()
    cur  = conn.cursor()

    process_dates = sorted({r["process_date"] for r in records})
    for pd_date in process_dates:
        _exec(
            cur,
            f"DELETE FROM iceberg.bronze.consumo_api_raw "
            f"WHERE process_date = DATE '{pd_date}'"
        )
        print(f"[fetch_consumo ENTSOE] Limpa {pd_date}.")

    cols  = "ts_utc, total, source_url, fetch_date, process_date"
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
    print(f"[fetch_consumo ENTSOE] Inseridos: {total} registos ({start_date}--{end_date}).")
    return total


# ---------------------------------------------------------------------------
# Task 2: precos day-ahead PT + ES -- ENTSO-E
# ---------------------------------------------------------------------------
@task(retries=3)
def fetch_preco_api(start_date: date, end_date: date) -> int:
    """
    Obtém preços day-ahead MIBEL PT e ES via ENTSO-E Transparency Platform.

    Melhoria vs Energy-Charts anterior:
      - price_spain_eur_mwh agora preenchido (ES disponivel separadamente)
      - Dados horarios exactos (24 valores/dia, sem 15-min a agregar)
      - Endpoint sem flag "deprecated"

    Idempotente: apaga process_dates do intervalo antes de inserir.
    """
    client   = _get_client()
    start_ts = pd.Timestamp(start_date.isoformat(), tz="UTC")
    end_ts   = pd.Timestamp((end_date + timedelta(days=1)).isoformat(), tz="UTC")

    print(f"[fetch_preco ENTSOE] Day-Ahead Prices PT+ES: {start_date} -> {end_date}")

    # -- PT prices
    try:
        pt_raw    = client.query_day_ahead_prices("PT", start=start_ts, end=end_ts)
        pt_series = _to_series(pt_raw).rename("pt")
    except NoMatchingDataError:
        print(f"[fetch_preco ENTSOE] Sem precos PT para {start_date}--{end_date}.")
        pt_series = pd.Series(dtype=float, name="pt")

    # -- ES prices
    try:
        es_raw    = client.query_day_ahead_prices("ES", start=start_ts, end=end_ts)
        es_series = _to_series(es_raw).rename("es")
    except NoMatchingDataError:
        print(f"[fetch_preco ENTSOE] Sem precos ES para {start_date}--{end_date}.")
        es_series = pd.Series(dtype=float, name="es")

    merged = pt_series.to_frame().join(es_series.to_frame(), how="outer")

    if merged.empty:
        print(f"[fetch_preco ENTSOE] Sem dados para {start_date}--{end_date}.")
        return 0

    records: list[dict] = []
    fetch_dt   = date.today()
    source_url = (
        f"https://transparency.entsoe.eu/ "
        f"ENTSO-E Day-Ahead Prices PT+ES {start_date}/{end_date}"
    )

    for ts_idx, row in merged.iterrows():
        ts_utc   = _ts_to_utc(ts_idx)
        pt_price = None if pd.isna(row.get("pt")) else float(row["pt"])
        es_price = None if pd.isna(row.get("es")) else float(row["es"])
        records.append({
            "ts_utc":                ts_utc,
            "price_portugal_eur_mwh": pt_price,
            "price_spain_eur_mwh":    es_price,
            "source_url":            source_url,
            "fetch_date":            fetch_dt,
            "process_date":          ts_utc.date(),
        })

    conn = _trino_conn()
    cur  = conn.cursor()

    process_dates = sorted({r["process_date"] for r in records})
    for pd_date in process_dates:
        _exec(
            cur,
            f"DELETE FROM iceberg.bronze.preco_api_raw "
            f"WHERE process_date = DATE '{pd_date}'"
        )
        print(f"[fetch_preco ENTSOE] Limpa {pd_date}.")

    cols  = "ts_utc, price_portugal_eur_mwh, price_spain_eur_mwh, source_url, fetch_date, process_date"
    batch: list[str] = []
    total = 0

    for r in records:
        ts_str   = r["ts_utc"].strftime("%Y-%m-%d %H:%M:%S.000000 UTC")
        pt_val   = str(r["price_portugal_eur_mwh"]) if r["price_portugal_eur_mwh"] is not None else "NULL"
        es_val   = str(r["price_spain_eur_mwh"])    if r["price_spain_eur_mwh"]    is not None else "NULL"
        batch.append(
            f"(TIMESTAMP '{ts_str}', {pt_val}, {es_val}, '{r['source_url']}', "
            f"DATE '{r['fetch_date']}', DATE '{r['process_date']}')"
        )
        if len(batch) >= BATCH_SIZE:
            total += _flush_batch(cur, "iceberg.bronze.preco_api_raw", cols, batch)

    total += _flush_batch(cur, "iceberg.bronze.preco_api_raw", cols, batch)
    conn.close()
    print(f"[fetch_preco ENTSOE] Inseridos: {total} registos ({start_date}--{end_date}).")
    return total


# ---------------------------------------------------------------------------
# Workflow: fetch consumo + preco em paralelo
# ---------------------------------------------------------------------------
@workflow
def fetch_bronze_api(
    start_date: date = date.today() - timedelta(days=6),
    end_date:   date = date.today(),
) -> None:
    """
    Obtém dados ENTSO-E (consumo + preco PT+ES) e insere na camada Bronze.

    As duas tarefas correm em paralelo (independentes).
    Requer: variavel de ambiente ENTSOE_TOKEN.

    Execucao:
        ENTSOE_TOKEN=<token> pyflyte run workflows/flyte_fetch_bronze_api.py fetch_bronze_api \\
            --start_date 2024-01-01 --end_date 2024-01-07
    """
    fetch_consumo_api(start_date=start_date, end_date=end_date)
    fetch_preco_api(start_date=start_date, end_date=end_date)
