#!/usr/bin/env python3
"""Gera dados fictícios e materializa automaticamente Kafka -> Bronze -> Gold."""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

try:
    from kafka import KafkaProducer
except ModuleNotFoundError:
    print(
        "Dependência em falta: kafka-python.\n"
        "Instala com:\n"
        "  python -m pip install -r "
        "02_medallion_pipeline/DP01_Producao_Consumo/streaming/scripts/python/requirements_streaming.txt",
        file=sys.stderr,
    )
    raise SystemExit(1)

try:
    import trino
except ModuleNotFoundError:
    print(
        "Dependência em falta: trino.\n"
        "Instala com:\n"
        "  python -m pip install -r "
        "02_medallion_pipeline/DP01_Producao_Consumo/streaming/scripts/python/requirements_streaming.txt",
        file=sys.stderr,
    )
    raise SystemExit(1)

BROKER = "localhost:19092"
TOPIC = "producao_consumo_events"
INTERVAL_SECONDS = 30
TRINO_HOST = "localhost"
TRINO_PORT = 8080
TRINO_USER = "trino"
SQL_FILE = "02_medallion_pipeline/DP01_Producao_Consumo/streaming/sql/01_streaming_to_iceberg.sql"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker", default=BROKER)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--interval-seconds", type=int, default=INTERVAL_SECONDS)
    parser.add_argument("--trino-host", default=TRINO_HOST)
    parser.add_argument("--trino-port", type=int, default=TRINO_PORT)
    parser.add_argument("--trino-user", default=TRINO_USER)
    parser.add_argument("--sql-file", default=SQL_FILE)
    return parser.parse_args()


def build_event() -> dict:
    consumo = round(random.uniform(22000.0, 44000.0), 2)
    producao = round(consumo * random.uniform(0.85, 1.15), 2)
    saldo = round(producao - consumo, 2)
    return {
        "event_id": str(uuid4()),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "consumo_total_kwh": consumo,
        "producao_total_kwh": producao,
        "saldo_kwh": saldo,
        "origem": "simulador_ficticio_30s",
    }


def split_statements(sql_text: str) -> list[str]:
    return [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]


def materialize(statements: list[str], host: str, port: int, user: str) -> None:
    conn = trino.dbapi.connect(host=host, port=port, user=user)
    cur = conn.cursor()
    for stmt in statements:
        cur.execute(stmt)
        if cur.description:
            _ = cur.fetchall()


def main() -> None:
    args = parse_args()

    sql_path = Path(args.sql_file)
    if not sql_path.exists():
        print(f"SQL file não encontrado: {sql_path}", file=sys.stderr)
        raise SystemExit(1)
    statements = split_statements(sql_path.read_text(encoding="utf-8"))

    producer = KafkaProducer(
        bootstrap_servers=[args.broker],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )

    print(
        f"Produzindo no tópico '{args.topic}' a cada {args.interval_seconds}s em {args.broker}..."
    )
    print(
        "Materialização automática ativa para "
        f"iceberg.gold.dp_producao_consumo_streaming via Trino {args.trino_host}:{args.trino_port}."
    )

    while True:
        event = build_event()
        producer.send(args.topic, key=event["event_id"], value=event)
        producer.flush()
        print(f"Publicado: {event}")

        try:
            materialize(
                statements=statements,
                host=args.trino_host,
                port=args.trino_port,
                user=args.trino_user,
            )
            print("[OK] Kafka -> Bronze -> Gold materializado automaticamente.")
        except Exception as exc:  # noqa: BLE001
            print(f"[ERRO] materialização automática falhou: {exc}", file=sys.stderr)

        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
