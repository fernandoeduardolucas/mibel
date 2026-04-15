#!/usr/bin/env python3
"""Backend HTTP API for produção vs consumo dashboard."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[3]
CONSUMPTION_CSV = ROOT / "02_medallion_pipeline/producao_consumo/01_bronze/data/raw/consumo-total-nacional.csv"
PRODUCTION_CSV = ROOT / "02_medallion_pipeline/producao_consumo/01_bronze/data/raw/energia-produzida-total-nacional.csv"


@dataclass(frozen=True)
class Point:
    timestamp: datetime
    consumo_total: float
    producao_total: float

    @property
    def saldo(self) -> float:
        return self.producao_total - self.consumo_total


class ProducaoConsumoService:
    def __init__(self, consumo_path: Path, producao_path: Path):
        self._consumo_path = consumo_path
        self._producao_path = producao_path
        self._cache: List[Point] | None = None
        self._cache_stamp: Tuple[float, float] | None = None

    def _read_csv_totals(self, csv_path: Path, total_field: str) -> Dict[str, float]:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            rows = {}
            for row in reader:
                timestamp = (row.get("datahora") or "").strip()
                total_raw = (row.get(total_field) or "").strip()
                if not timestamp or not total_raw:
                    continue
                rows[timestamp] = float(total_raw)
            return rows

    def _build_points(self) -> List[Point]:
        consumo = self._read_csv_totals(self._consumo_path, "total")
        producao = self._read_csv_totals(self._producao_path, "total")

        joined: List[Point] = []
        for timestamp_iso in sorted(set(consumo) & set(producao)):
            timestamp = datetime.fromisoformat(timestamp_iso)
            joined.append(
                Point(
                    timestamp=timestamp,
                    consumo_total=consumo[timestamp_iso],
                    producao_total=producao[timestamp_iso],
                )
            )
        return joined

    def _stamp(self) -> Tuple[float, float]:
        return (self._consumo_path.stat().st_mtime, self._producao_path.stat().st_mtime)

    def points(self) -> List[Point]:
        new_stamp = self._stamp()
        if self._cache is None or self._cache_stamp != new_stamp:
            self._cache = self._build_points()
            self._cache_stamp = new_stamp
        return self._cache

    def _aggregate(self, fmt: str) -> List[dict]:
        totals: Dict[str, dict] = {}
        for point in self.points():
            key = point.timestamp.strftime(fmt)
            agg = totals.setdefault(
                key,
                {"periodo": key, "consumo_total": 0.0, "producao_total": 0.0, "saldo": 0.0, "leituras": 0},
            )
            agg["consumo_total"] += point.consumo_total
            agg["producao_total"] += point.producao_total
            agg["saldo"] += point.saldo
            agg["leituras"] += 1

        return [totals[key] for key in sorted(totals)]

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
                "consumo_total": 0.0,
                "producao_total": 0.0,
                "saldo_total": 0.0,
                "cobertura_percentual": 0.0,
                "ultimo_ponto": None,
            }

        consumo_total = sum(point.consumo_total for point in points)
        producao_total = sum(point.producao_total for point in points)
        saldo_total = producao_total - consumo_total
        cobertura = (producao_total / consumo_total * 100) if consumo_total else 0.0
        last = points[-1]

        return {
            "registos": len(points),
            "intervalo": {
                "inicio": points[0].timestamp.isoformat(),
                "fim": last.timestamp.isoformat(),
            },
            "consumo_total": consumo_total,
            "producao_total": producao_total,
            "saldo_total": saldo_total,
            "cobertura_percentual": cobertura,
            "ultimo_ponto": {
                "timestamp": last.timestamp.isoformat(),
                "consumo_total": last.consumo_total,
                "producao_total": last.producao_total,
                "saldo": last.saldo,
            },
            "gerado_em": datetime.now(timezone.utc).isoformat(),
        }


SERVICE = ProducaoConsumoService(CONSUMPTION_CSV, PRODUCTION_CSV)


class Handler(BaseHTTPRequestHandler):
    server_version = "ProducaoConsumoHTTP/1.0"

    def _send_json(self, payload: dict | list, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
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
            self._send_json({"status": "ok"})
            return

        if path == "/api/overview":
            self._send_json(SERVICE.overview())
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
            self._send_json({"error": "Parâmetro 'group' inválido. Use 'day' ou 'month'."}, status=400)
            return

        self._send_json({"error": "Rota não encontrada."}, status=404)


def run(host: str = "0.0.0.0", port: int = 8000) -> None:
    httpd = ThreadingHTTPServer((host, port), Handler)
    print(f"API disponível em http://{host}:{port}")
    httpd.serve_forever()


if __name__ == "__main__":
    run()
