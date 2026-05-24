#!/usr/bin/env python3
"""Gera dados fictícios de produção/consumo a cada 30 segundos para Kafka/Redpanda."""

from __future__ import annotations

import json
import random
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4

try:
    from kafka import KafkaProducer
except ModuleNotFoundError:
    print(
        "Dependência em falta: kafka-python.\n"
        "Instala com:\n"
        "  python -m pip install -r "
        "02_medallion_pipeline/producao_consumo/streaming/scripts/python/requirements_streaming.txt",
        file=sys.stderr,
    )
    raise SystemExit(1)

BROKER = "localhost:19092"
TOPIC = "producao_consumo_events"
INTERVAL_SECONDS = 30


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


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )

    print(f"Produzindo no tópico '{TOPIC}' a cada {INTERVAL_SECONDS}s em {BROKER}...")
    while True:
        event = build_event()
        producer.send(TOPIC, key=event["event_id"], value=event)
        producer.flush()
        print(f"Publicado: {event}")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
