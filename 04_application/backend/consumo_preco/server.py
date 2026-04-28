#!/usr/bin/env python3
"""Backend HTTP API para dados de produção vs consumo a partir do Trino."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import List
from urllib.parse import parse_qs, urlparse

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "trino")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "gold")
GOLD_TABLE = "iceberg.gold.producao_vs_consumo_hourly"
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "60"))

BASE_QUERY = f"""
SELECT
    timestamp_utc,
    consumo_total_kwh,
    producao_total_kwh,
    producao_dgm_kwh,
    producao_pre_kwh,
    saldo_kwh,
    ratio_producao_consumo,
    flag_defice,
    flag_excedente,
    flag_missing_source
FROM {GOLD_TABLE}
ORDER BY timestamp_utc
"""


class ProducaoConsumoService:
    def __init__(self) -> None:
        self._cache: List[dict] | None = None
        self._cache_loaded_at: float | None = None
        self._last_error: str | None = None

    def _connect(self):
        try:
            import trino
        except ImportError as exc:
            raise RuntimeError(
                "Dependência em falta: instale com 'pip install trino'."
            ) from exc

        return trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA,
            http_scheme="http",
        )

    @staticmethod
    def _to_float(value: object | None) -> float | None:
        if value is None:
            return None
        if isinstance(value, (float, int)):
            return float(value)
        text = str(value).strip()
        if not text:
            return None
        return float(text)

    @staticmethod
    def _to_bool(value: object | None) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).strip().lower() == "true"

    @staticmethod
    def _to_timestamp(value: object) -> datetime:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value

        text = str(value).strip().replace(" ", "T", 1)
        if text.endswith(" UTC"):
            text = text.removesuffix(" UTC") + "+00:00"
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)

    def _load_rows(self) -> List[dict]:
        connection = self._connect()
        cursor = connection.cursor()
        try:
            cursor.execute(BASE_QUERY)
            columns = [desc[0] for desc in cursor.description or []]
            rows = cursor.fetchall()
        finally:
            cursor.close()
            connection.close()

        out: List[dict] = []
        for row in rows:
            raw = dict(zip(columns, row))
            ts = raw.get("timestamp_utc")
            if ts is None:
                continue
            timestamp = self._to_timestamp(ts)
            out.append(
                {
                    "timestamp_utc": timestamp.isoformat(),
                    "consumo_total_kwh": self._to_float(raw.get("consumo_total_kwh")),
                    "producao_total_kwh": self._to_float(raw.get("producao_total_kwh")),
                    "producao_dgm_kwh": self._to_float(raw.get("producao_dgm_kwh")),
                    "producao_pre_kwh": self._to_float(raw.get("producao_pre_kwh")),
                    "saldo_kwh": self._to_float(raw.get("saldo_kwh")),
                    "ratio_producao_consumo": self._to_float(raw.get("ratio_producao_consumo")),
                    "flag_defice": self._to_bool(raw.get("flag_defice")),
                    "flag_excedente": self._to_bool(raw.get("flag_excedente")),
                    "flag_missing_source": self._to_bool(raw.get("flag_missing_source")),
                }
            )
        return out

    def points(self) -> List[dict]:
        now = time.monotonic()
        stale = (
            self._cache is None
            or self._cache_loaded_at is None
            or (now - self._cache_loaded_at) >= CACHE_TTL_SECONDS
        )
        if stale:
            try:
                self._cache = self._load_rows()
                self._cache_loaded_at = now
                self._last_error = None
            except Exception as exc:  # noqa: BLE001
                self._last_error = str(exc)
                if self._cache is None:
                    self._cache = []
        return self._cache

    def _aggregate(self, fmt: str) -> List[dict]:
        totals: dict[str, dict] = {}

        for point in self.points():
            key = datetime.fromisoformat(point["timestamp_utc"]).strftime(fmt)
            agg = totals.setdefault(
                key,
                {
                    "periodo": key,
                    "consumo_total_kwh": 0.0,
                    "producao_total_kwh": 0.0,
                    "saldo_kwh": 0.0,
                    "leituras": 0,
                    "horas_defice": 0,
                    "horas_excedente": 0,
                    "horas_missing_source": 0,
                },
            )

            agg["consumo_total_kwh"] += point["consumo_total_kwh"] or 0.0
            agg["producao_total_kwh"] += point["producao_total_kwh"] or 0.0
            agg["saldo_kwh"] += point["saldo_kwh"] or 0.0
            agg["leituras"] += 1
            agg["horas_defice"] += int(point["flag_defice"])
            agg["horas_excedente"] += int(point["flag_excedente"])
            agg["horas_missing_source"] += int(point["flag_missing_source"])

        result: List[dict] = []
        for key in sorted(totals):
            agg = totals[key]
            consumo = agg["consumo_total_kwh"]
            producao = agg["producao_total_kwh"]
            agg["ratio_producao_consumo"] = (producao / consumo) if consumo else None
            result.append(agg)

        return result

    def daily_series(self) -> List[dict]:
        return self._aggregate("%Y-%m-%d")

    def monthly_series(self) -> List[dict]:
        return self._aggregate("%Y-%m")

    def overview(self) -> dict:
        points = self.points()
        if not points:
            return {
                "registos": 0,
                "intervalo": None,
                "consumo_total_kwh": 0.0,
                "producao_total_kwh": 0.0,
                "saldo_total_kwh": 0.0,
                "ratio_global_producao_consumo": None,
                "ultimo_ponto": None,
            }

        consumo_total = sum(point["consumo_total_kwh"] or 0.0 for point in points)
        producao_total = sum(point["producao_total_kwh"] or 0.0 for point in points)
        saldo_total = sum(point["saldo_kwh"] or 0.0 for point in points)
        last = points[-1]

        return {
            "registos": len(points),
            "intervalo": {
                "inicio": points[0]["timestamp_utc"],
                "fim": last["timestamp_utc"],
            },
            "consumo_total_kwh": consumo_total,
            "producao_total_kwh": producao_total,
            "saldo_total_kwh": saldo_total,
            "ratio_global_producao_consumo": (producao_total / consumo_total) if consumo_total else None,
            "horas_defice": sum(int(point["flag_defice"]) for point in points),
            "horas_excedente": sum(int(point["flag_excedente"]) for point in points),
            "horas_missing_source": sum(int(point["flag_missing_source"]) for point in points),
            "ultimo_ponto": last,
            "gerado_em": datetime.now(timezone.utc).isoformat(),
        }

    def debug_info(self) -> dict:
        points = self.points()
        return {
            "cache_registos": len(points),
            "cache_loaded_at_monotonic": self._cache_loaded_at,
            "source_table": GOLD_TABLE,
            "trino": {
                "host": TRINO_HOST,
                "port": TRINO_PORT,
                "user": TRINO_USER,
                "catalog": TRINO_CATALOG,
                "schema": TRINO_SCHEMA,
            },
            "last_error": self._last_error,
        }


SERVICE = ProducaoConsumoService()


class Handler(BaseHTTPRequestHandler):
    server_version = "ProducaoConsumoHTTP/2.0"

    def _send_json(self, payload: dict | list, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/health":
            self._send_json({"status": "ok", "source": GOLD_TABLE})
            return

        if path == "/api/overview":
            self._send_json(SERVICE.overview())
            return

        if path == "/api/debug":
            self._send_json(SERVICE.debug_info())
            return

        if path == "/api/timeseries":
            params = parse_qs(parsed.query)
            group = params.get("group", ["day"])[0]

            if group == "month":
                self._send_json(SERVICE.monthly_series())
                return

            if group == "day":
                self._send_json(SERVICE.daily_series())
                return

            self._send_json(
                {"error": "Parâmetro 'group' inválido. Use 'day' ou 'month'."},
                status=400,
            )
            return

        self._send_json({"error": "Rota não encontrada."}, status=404)


def run(host: str = "0.0.0.0", port: int = 8000) -> None:
    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"API disponível em http://{host}:{port}")
    httpd.serve_forever()


if __name__ == "__main__":
    run()
