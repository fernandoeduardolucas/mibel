#!/usr/bin/env python3
"""Backend HTTP API for produção vs consumo dashboard."""

from __future__ import annotations

import csv
import json
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO
from pathlib import Path
from typing import Dict, List
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[3]
DOCKER_COMPOSE_FILE = ROOT / "01_bootstrap/tead_2.0_v1.2/docker-compose.yml"
GOLD_TABLE = "iceberg.gold.producao_vs_consumo_hourly"
TRINO_QUERY = f"""
SELECT
    timestamp_utc,
    consumo_total_kwh,
    producao_total_kwh,
    producao_dgm_kwh,
    producao_pre_kwh
FROM {GOLD_TABLE}
ORDER BY timestamp_utc
"""
CACHE_TTL_SECONDS = 60


@dataclass(frozen=True)
class Point:
    timestamp: datetime
    consumo_total: float | None
    producao_total: float | None
    producao_dgm: float | None
    producao_pre: float | None

    @property
    def has_consumo(self) -> bool:
        return self.consumo_total is not None

    @property
    def has_producao(self) -> bool:
        return self.producao_total is not None

    @property
    def has_complete_data(self) -> bool:
        return self.has_consumo and self.has_producao

    @property
    def saldo(self) -> float | None:
        if not self.has_complete_data:
            return None
        return (self.producao_total or 0.0) - (self.consumo_total or 0.0)

    @property
    def ratio_producao_consumo(self) -> float | None:
        if not self.has_complete_data or not self.consumo_total:
            return None
        return (self.producao_total or 0.0) / self.consumo_total

    @property
    def flag_defice(self) -> bool:
        return self.saldo is not None and self.saldo < 0

    @property
    def flag_excedente(self) -> bool:
        return self.saldo is not None and self.saldo > 0

    @property
    def flag_missing_source(self) -> bool:
        return not self.has_complete_data


class ProducaoConsumoService:
    def __init__(self):
        self._cache: List[Point] | None = None
        self._cache_loaded_at: float | None = None

    @staticmethod
    def _parse_float(value: str | None) -> float | None:
        text = (value or "").strip()
        if not text:
            return None
        return float(text)

    @staticmethod
    def _parse_timestamp(value: str) -> datetime:
        raw = value.strip()
        iso_value = raw.replace(" ", "T", 1)
        if iso_value.endswith(" UTC"):
            iso_value = iso_value.removesuffix(" UTC") + "+00:00"
        if iso_value.endswith("Z"):
            iso_value = iso_value[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(iso_value)
        except ValueError:
            pass

        for fmt in ("%Y-%m-%d %H:%M:%S.%f %Z", "%Y-%m-%d %H:%M:%S %Z"):
            try:
                parsed = datetime.strptime(raw, fmt)
                return parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        raise ValueError(f"Formato de timestamp inesperado vindo do Trino: {value!r}")

    def _fetch_gold_rows(self) -> List[dict]:
        command = [
            "docker",
            "compose",
            "-f",
            str(DOCKER_COMPOSE_FILE),
            "exec",
            "-T",
            "trino",
            "trino",
            "--output-format",
            "CSV_HEADER",
            "--execute",
            TRINO_QUERY,
        ]
        completed = subprocess.run(command, check=True, capture_output=True, text=True, cwd=ROOT)
        reader = csv.DictReader(StringIO(completed.stdout))
        return list(reader)

    def _build_points(self) -> List[Point]:
        points: List[Point] = []
        for row in self._fetch_gold_rows():
            timestamp_raw = (row.get("timestamp_utc") or "").strip()
            if not timestamp_raw:
                continue
            points.append(
                Point(
                    timestamp=self._parse_timestamp(timestamp_raw),
                    consumo_total=self._parse_float(row.get("consumo_total_kwh")),
                    producao_total=self._parse_float(row.get("producao_total_kwh")),
                    producao_dgm=self._parse_float(row.get("producao_dgm_kwh")),
                    producao_pre=self._parse_float(row.get("producao_pre_kwh")),
                )
            )
        return points

    def points(self) -> List[Point]:
        now = time.monotonic()
        needs_refresh = self._cache is None or self._cache_loaded_at is None or (now - self._cache_loaded_at) >= CACHE_TTL_SECONDS
        if needs_refresh:
            self._cache = self._build_points()
            self._cache_loaded_at = now
        return self._cache

    def _aggregate(self, fmt: str) -> List[dict]:
        totals: Dict[str, dict] = {}
        for point in self.points():
            key = point.timestamp.strftime(fmt)
            agg = totals.setdefault(
                key,
                {
                    "periodo": key,
                    "consumo_total": 0.0,
                    "producao_total": 0.0,
                    "producao_dgm": 0.0,
                    "producao_pre": 0.0,
                    "saldo": 0.0,
                    "ratio_producao_consumo": None,
                    "defice_horas": 0,
                    "excedente_horas": 0,
                    "missing_horas": 0,
                    "leituras": 0,
                    "leituras_completas": 0,
                },
            )
            agg["leituras"] += 1

            if point.has_consumo:
                agg["consumo_total"] += point.consumo_total or 0.0
            if point.has_producao:
                agg["producao_total"] += point.producao_total or 0.0
                agg["producao_dgm"] += point.producao_dgm or 0.0
                agg["producao_pre"] += point.producao_pre or 0.0

            if point.flag_missing_source:
                agg["missing_horas"] += 1
                continue

            agg["leituras_completas"] += 1
            agg["saldo"] += point.saldo or 0.0
            if point.flag_defice:
                agg["defice_horas"] += 1
            elif point.flag_excedente:
                agg["excedente_horas"] += 1

        rows = []
        for key in sorted(totals):
            row = totals[key]
            if row["consumo_total"] > 0:
                row["ratio_producao_consumo"] = row["producao_total"] / row["consumo_total"]
            rows.append(row)
        return rows

    def daily_series(self) -> List[dict]:
        return self._aggregate("%Y-%m-%d")

    def monthly_series(self) -> List[dict]:
        return self._aggregate("%Y-%m")

    def analytics(self) -> dict:
        points = self.points()
        complete = [point for point in points if point.has_complete_data]
        deficit_hours = [point for point in complete if point.flag_defice]

        top_deficit = sorted(deficit_hours, key=lambda p: p.saldo or 0.0)[:10]

        total_producao = sum(point.producao_total or 0.0 for point in points)
        total_pre = sum(point.producao_pre or 0.0 for point in points)
        total_dgm = sum(point.producao_dgm or 0.0 for point in points)

        monthly = self.monthly_series()
        monthly_balance = [
            {
                "periodo": row["periodo"],
                "saldo": row["saldo"],
                "ratio_producao_consumo": row["ratio_producao_consumo"],
                "defice_horas": row["defice_horas"],
                "excedente_horas": row["excedente_horas"],
                "missing_horas": row["missing_horas"],
            }
            for row in monthly
        ]

        trend_delta = 0.0
        if len(monthly_balance) >= 2:
            trend_delta = monthly_balance[-1]["saldo"] - monthly_balance[0]["saldo"]

        return {
            "questao_defice": {
                "horas_defice": len(deficit_hours),
                "horas_com_dados": len(complete),
                "percentual_defice": (len(deficit_hours) / len(complete) * 100) if complete else 0.0,
                "piores_horas": [
                    {
                        "timestamp": point.timestamp.isoformat(),
                        "consumo_total": point.consumo_total,
                        "producao_total": point.producao_total,
                        "saldo": point.saldo,
                        "ratio_producao_consumo": point.ratio_producao_consumo,
                    }
                    for point in top_deficit
                ],
            },
            "questao_dependencia_pre_dgm": {
                "producao_total": total_producao,
                "producao_pre": total_pre,
                "producao_dgm": total_dgm,
                "share_pre_percentual": (total_pre / total_producao * 100) if total_producao else 0.0,
                "share_dgm_percentual": (total_dgm / total_producao * 100) if total_producao else 0.0,
            },
            "questao_tendencia_desbalanceamento": {
                "delta_saldo_primeiro_ultimo_mes": trend_delta,
                "serie_mensal": monthly_balance,
            },
        }

    def overview(self) -> dict:
        points = self.points()
        if not points:
            return {
                "registos": 0,
                "intervalo": None,
                "consumo_total": 0.0,
                "producao_total": 0.0,
                "saldo_total": 0.0,
                "ratio_producao_consumo": 0.0,
                "horas_defice": 0,
                "horas_excedente": 0,
                "horas_missing_source": 0,
                "share_pre_percentual": 0.0,
                "share_dgm_percentual": 0.0,
                "ultimo_ponto": None,
            }

        consumo_total = sum(point.consumo_total or 0.0 for point in points)
        producao_total = sum(point.producao_total or 0.0 for point in points)
        saldo_total = producao_total - consumo_total
        deficit_count = sum(1 for point in points if point.flag_defice)
        excedent_count = sum(1 for point in points if point.flag_excedente)
        missing_count = sum(1 for point in points if point.flag_missing_source)
        total_pre = sum(point.producao_pre or 0.0 for point in points)
        total_dgm = sum(point.producao_dgm or 0.0 for point in points)

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
            "ratio_producao_consumo": (producao_total / consumo_total) if consumo_total else 0.0,
            "horas_defice": deficit_count,
            "horas_excedente": excedent_count,
            "horas_missing_source": missing_count,
            "share_pre_percentual": (total_pre / producao_total * 100) if producao_total else 0.0,
            "share_dgm_percentual": (total_dgm / producao_total * 100) if producao_total else 0.0,
            "ultimo_ponto": {
                "timestamp": last.timestamp.isoformat(),
                "consumo_total": last.consumo_total,
                "producao_total": last.producao_total,
                "saldo": last.saldo,
                "flag_missing_source": last.flag_missing_source,
            },
            "gerado_em": datetime.now(timezone.utc).isoformat(),
        }


SERVICE = ProducaoConsumoService()


class Handler(BaseHTTPRequestHandler):
    server_version = "ProducaoConsumoHTTP/1.1"

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

        if path == "/api/analytics":
            self._send_json(SERVICE.analytics())
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
