#!/usr/bin/env python3
"""Backend HTTP API for consumo vs preço dashboard."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[3]


#NAO PODE USAR O CSV So PODE USAR a base de dados gold
CONSUMPTION_CSV = ROOT / "02_medallion_pipeline/consumo_preco/01_bronze/data/raw/consumo-total-nacional.csv"
PRICE_CSV = ROOT / "02_medallion_pipeline/consumo_preco/01_bronze/data/raw/Day-ahead Market Prices_20230101_20260311.csv"


@dataclass(frozen=True)
class Point:
    timestamp: datetime
    consumo_mwh: float
    preco_eur_mwh: float

    @property
    def custo_estimado_eur(self) -> float:
        return self.consumo_mwh * self.preco_eur_mwh


class ConsumoPrecoService:
    def __init__(self, consumo_path: Path, preco_path: Path):
        self._consumo_path = consumo_path
        self._preco_path = preco_path
        self._cache: List[Point] | None = None
        self._cache_stamp: Tuple[float, float] | None = None
        self._last_debug: dict = {}

    @staticmethod
    def _to_utc_naive(dt: datetime) -> datetime:
        """
        Normaliza datetime para UTC naive.

        - Se vier timezone-aware, converte para UTC e remove tzinfo.
        - Se vier naive, assume que já está em UTC e mantém naive.
        """
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.replace(tzinfo=None)

    @staticmethod
    def _parse_datetime(raw: str) -> datetime:
        value = raw.strip()
        if not value:
            raise ValueError("Timestamp vazio.")

        candidates = (
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M",
        )

        for fmt in candidates:
            try:
                return ConsumoPrecoService._to_utc_naive(datetime.strptime(value, fmt))
            except ValueError:
                continue

        try:
            return ConsumoPrecoService._to_utc_naive(datetime.fromisoformat(value))
        except ValueError as exc:
            raise ValueError(f"Formato de datetime inválido: {raw}") from exc

    @staticmethod
    def _parse_date(raw: str) -> date:
        value = raw.strip()
        candidates = (
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%Y/%m/%d",
        )

        for fmt in candidates:
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue

        raise ValueError(f"Formato de data inválido: {raw}")

    @staticmethod
    def _parse_float(raw: str) -> float:
        value = raw.strip().replace(",", ".")
        if not value:
            raise ValueError("Valor numérico vazio.")
        return float(value)

    @staticmethod
    def _hour_bucket(ts: datetime) -> datetime:
        return ts.replace(minute=0, second=0, microsecond=0)

    @staticmethod
    def _price_hour_to_timestamp_v1(base_date: date, hour_raw: str) -> datetime:
        """
        Convenção V1:
        Hour=1  -> 00:00 do próprio dia
        Hour=2  -> 01:00 do próprio dia
        ...
        Hour=24 -> 23:00 do próprio dia
        Hour=25 -> 00:00 do dia seguinte
        """
        hour_value = int(hour_raw.strip())
        if hour_value < 1 or hour_value > 25:
            raise ValueError(f"Hora inválida no ficheiro de preços: {hour_raw}")

        if hour_value == 25:
            return datetime.combine(base_date + timedelta(days=1), time.min)

        return datetime.combine(base_date, time.min) + timedelta(hours=hour_value - 1)

    @staticmethod
    def _price_hour_to_timestamp_v2(base_date: date, hour_raw: str) -> datetime:
        """
        Convenção V2:
        Hour=1  -> 01:00 do próprio dia
        Hour=2  -> 02:00 do próprio dia
        ...
        Hour=24 -> 00:00 do dia seguinte
        Hour=25 -> 01:00 do dia seguinte
        """
        hour_value = int(hour_raw.strip())
        if hour_value < 1 or hour_value > 25:
            raise ValueError(f"Hora inválida no ficheiro de preços: {hour_raw}")

        return datetime.combine(base_date, time.min) + timedelta(hours=hour_value)

    def _read_consumption_hourly(self) -> Dict[datetime, float]:
        """
        Lê consumo com granularidade de 15 minutos e converte para energia horária estimada em MWh.

        Assunção:
        - coluna `total` representa potência média por intervalo de 15 minutos
        - energia do intervalo = valor * 0.25 horas
        """
        hourly_mwh: Dict[datetime, float] = {}

        with self._consumo_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                timestamp_raw = (row.get("datahora") or "").strip()
                total_raw = (row.get("total") or "").strip()

                if not timestamp_raw or not total_raw:
                    continue

                try:
                    ts = self._parse_datetime(timestamp_raw)
                    total_value = self._parse_float(total_raw)
                except ValueError:
                    continue

                bucket = self._hour_bucket(ts)
                hourly_mwh[bucket] = hourly_mwh.get(bucket, 0.0) + (total_value * 0.25)

        return hourly_mwh

    def _read_price_hourly_with_parser(
        self,
        parser_name: str,
        parser_func,
    ) -> Tuple[Dict[datetime, float], dict]:
        rows: Dict[datetime, float] = {}
        raw_preview: List[List[str]] = []
        header_found: List[str] | None = None

        with self._preco_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle, delimiter=";")

            idx_date = idx_hour = idx_price = None

            for i, raw_row in enumerate(reader):
                normalized = [cell.strip() for cell in raw_row]

                if i < 8:
                    raw_preview.append(normalized)

                if not normalized or not any(normalized):
                    continue

                lowered = [cell.lower() for cell in normalized]
                if "date" in lowered and "hour" in lowered and "portugal" in lowered:
                    header_found = normalized
                    idx_date = lowered.index("date")
                    idx_hour = lowered.index("hour")
                    idx_price = lowered.index("portugal")
                    break

            if header_found is None or idx_date is None or idx_hour is None or idx_price is None:
                raise ValueError(
                    "Não foi possível identificar o cabeçalho do ficheiro de preços "
                    "(esperado: Date;Hour;Portugal;...)."
                )

            for raw_row in reader:
                if not raw_row:
                    continue

                row = [cell.strip() for cell in raw_row]
                if len(row) <= max(idx_date, idx_hour, idx_price):
                    continue

                date_raw = row[idx_date]
                hour_raw = row[idx_hour]
                price_raw = row[idx_price]

                if not date_raw or not hour_raw or not price_raw:
                    continue

                try:
                    base_date = self._parse_date(date_raw)
                    ts = parser_func(base_date, hour_raw)
                    ts = self._to_utc_naive(ts)
                    price = self._parse_float(price_raw)
                except (ValueError, TypeError):
                    continue

                rows[ts] = price

        debug = {
            "parser": parser_name,
            "header_found": header_found,
            "raw_preview": raw_preview,
            "records": len(rows),
            "sample_first_5": [
                {"timestamp": ts.isoformat(), "preco_eur_mwh": rows[ts]}
                for ts in sorted(rows.keys())[:5]
            ],
            "sample_last_5": [
                {"timestamp": ts.isoformat(), "preco_eur_mwh": rows[ts]}
                for ts in sorted(rows.keys())[-5:]
            ],
        }

        return rows, debug

    def _choose_best_price_series(
        self,
        consumo_horario: Dict[datetime, float],
    ) -> Tuple[Dict[datetime, float], dict]:
        price_v1, debug_v1 = self._read_price_hourly_with_parser(
            "v1_hour_minus_1",
            self._price_hour_to_timestamp_v1,
        )
        price_v2, debug_v2 = self._read_price_hourly_with_parser(
            "v2_hour_direct",
            self._price_hour_to_timestamp_v2,
        )

        inter_v1 = len(set(consumo_horario) & set(price_v1))
        inter_v2 = len(set(consumo_horario) & set(price_v2))

        if inter_v2 > inter_v1:
            return price_v2, {
                "selected_parser": "v2_hour_direct",
                "intersection_v1": inter_v1,
                "intersection_v2": inter_v2,
                "candidate_v1": debug_v1,
                "candidate_v2": debug_v2,
            }

        return price_v1, {
            "selected_parser": "v1_hour_minus_1",
            "intersection_v1": inter_v1,
            "intersection_v2": inter_v2,
            "candidate_v1": debug_v1,
            "candidate_v2": debug_v2,
        }

    def _build_points(self) -> List[Point]:
        consumo_horario = self._read_consumption_hourly()
        preco_horario, price_debug = self._choose_best_price_series(consumo_horario)

        consumo_keys = sorted(consumo_horario.keys())
        preco_keys = sorted(preco_horario.keys())
        intersecao = sorted(set(consumo_horario) & set(preco_horario))

        self._last_debug = {
            "consumo_path": str(self._consumo_path),
            "preco_path": str(self._preco_path),
            "consumo_exists": self._consumo_path.exists(),
            "preco_exists": self._preco_path.exists(),
            "consumo_records_hourly": len(consumo_horario),
            "preco_records_hourly": len(preco_horario),
            "intersecao_records": len(intersecao),
            "consumo_first_5": [
                {"timestamp": ts.isoformat(), "consumo_mwh": consumo_horario[ts]}
                for ts in consumo_keys[:5]
            ],
            "consumo_last_5": [
                {"timestamp": ts.isoformat(), "consumo_mwh": consumo_horario[ts]}
                for ts in consumo_keys[-5:]
            ],
            "preco_first_5": [
                {"timestamp": ts.isoformat(), "preco_eur_mwh": preco_horario[ts]}
                for ts in preco_keys[:5]
            ],
            "preco_last_5": [
                {"timestamp": ts.isoformat(), "preco_eur_mwh": preco_horario[ts]}
                for ts in preco_keys[-5:]
            ],
            "intersecao_first_5": [ts.isoformat() for ts in intersecao[:5]],
            "intersecao_last_5": [ts.isoformat() for ts in intersecao[-5:]],
            "price_debug": price_debug,
        }

        print("---- DEBUG BUILD POINTS ----")
        print(f"Consumo horário: {len(consumo_horario)} registos")
        print(f"Preço horário: {len(preco_horario)} registos")
        print(f"Interseção: {len(intersecao)} registos")
        print(f"Parser selecionado preço: {price_debug['selected_parser']}")
        print(f"Interseção V1: {price_debug['intersection_v1']}")
        print(f"Interseção V2: {price_debug['intersection_v2']}")

        if consumo_keys:
            print("Primeiros 5 timestamps consumo:")
            for ts in consumo_keys[:5]:
                print(f"  {ts} -> {consumo_horario[ts]}")
        else:
            print("Consumo horário vazio")

        if preco_keys:
            print("Primeiros 5 timestamps preço:")
            for ts in preco_keys[:5]:
                print(f"  {ts} -> {preco_horario[ts]}")
        else:
            print("Preço horário vazio")

        joined: List[Point] = []
        for ts in intersecao:
            joined.append(
                Point(
                    timestamp=ts,
                    consumo_mwh=consumo_horario[ts],
                    preco_eur_mwh=preco_horario[ts],
                )
            )

        return joined

    def _stamp(self) -> Tuple[float, float]:
        return (self._consumo_path.stat().st_mtime, self._preco_path.stat().st_mtime)

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
                {
                    "periodo": key,
                    "consumo_mwh": 0.0,
                    "custo_estimado_eur": 0.0,
                    "soma_precos_eur_mwh": 0.0,
                    "leituras": 0,
                },
            )
            agg["consumo_mwh"] += point.consumo_mwh
            agg["custo_estimado_eur"] += point.custo_estimado_eur
            agg["soma_precos_eur_mwh"] += point.preco_eur_mwh
            agg["leituras"] += 1

        result: List[dict] = []
        for key in sorted(totals):
            agg = totals[key]
            consumo_mwh = agg["consumo_mwh"]
            custo_estimado_eur = agg["custo_estimado_eur"]
            leituras = agg["leituras"]

            preco_medio_simples = (agg["soma_precos_eur_mwh"] / leituras) if leituras else 0.0
            preco_medio_ponderado = (custo_estimado_eur / consumo_mwh) if consumo_mwh else 0.0

            result.append(
                {
                    "periodo": key,
                    "consumo_mwh": consumo_mwh,
                    "custo_estimado_eur": custo_estimado_eur,
                    "preco_medio_simples_eur_mwh": preco_medio_simples,
                    "preco_medio_ponderado_eur_mwh": preco_medio_ponderado,
                    "leituras": leituras,
                }
            )

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
                "consumo_total_mwh": 0.0,
                "custo_estimado_total_eur": 0.0,
                "preco_medio_simples_eur_mwh": 0.0,
                "preco_medio_ponderado_eur_mwh": 0.0,
                "ultimo_ponto": None,
            }

        consumo_total_mwh = sum(point.consumo_mwh for point in points)
        custo_estimado_total_eur = sum(point.custo_estimado_eur for point in points)
        preco_medio_simples = sum(point.preco_eur_mwh for point in points) / len(points)
        preco_medio_ponderado = (
            custo_estimado_total_eur / consumo_total_mwh if consumo_total_mwh else 0.0
        )
        last = points[-1]

        return {
            "registos": len(points),
            "intervalo": {
                "inicio": points[0].timestamp.isoformat(),
                "fim": last.timestamp.isoformat(),
            },
            "consumo_total_mwh": consumo_total_mwh,
            "custo_estimado_total_eur": custo_estimado_total_eur,
            "preco_medio_simples_eur_mwh": preco_medio_simples,
            "preco_medio_ponderado_eur_mwh": preco_medio_ponderado,
            "ultimo_ponto": {
                "timestamp": last.timestamp.isoformat(),
                "consumo_mwh": last.consumo_mwh,
                "preco_eur_mwh": last.preco_eur_mwh,
                "custo_estimado_eur": last.custo_estimado_eur,
            },
            "gerado_em": datetime.now(timezone.utc).isoformat(),
        }

    def debug_info(self) -> dict:
        _ = self.points()
        return {
            "cache_registos": len(self._cache or []),
            "cache_stamp": self._cache_stamp,
            "paths": {
                "consumo": str(self._consumo_path),
                "preco": str(self._preco_path),
            },
            "last_debug": self._last_debug,
        }


SERVICE = ConsumoPrecoService(CONSUMPTION_CSV, PRICE_CSV)


class Handler(BaseHTTPRequestHandler):
    server_version = "ConsumoPrecoHTTP/1.0"

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
            self._send_json({"status": "ok"})
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