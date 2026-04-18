from __future__ import annotations

import time
from collections import defaultdict
from datetime import datetime

from app.config import CACHE_TTL_SECONDS
from app.models.energy_point import EnergyPoint
from app.repositories.producao_consumo_repository import ProducaoConsumoRepository


class ProducaoConsumoService:
    """Camada de negócio: cache, agregações e analítica."""

    def __init__(self, repository: ProducaoConsumoRepository):
        self.repository = repository
        self._cache: list[EnergyPoint] | None = None
        self._cache_loaded_at: float | None = None

    def _refresh_if_needed(self) -> None:
        now = time.monotonic()
        stale = (
            self._cache is None
            or self._cache_loaded_at is None
            or (now - self._cache_loaded_at) >= CACHE_TTL_SECONDS
        )
        if stale:
            self._cache = self.repository.list_hourly()
            self._cache_loaded_at = now

    def hourly(self, start: datetime | None = None, end: datetime | None = None) -> list[dict]:
        self._refresh_if_needed()
        assert self._cache is not None

        series = []
        for point in self._cache:
            if start and point.timestamp < start:
                continue
            if end and point.timestamp > end:
                continue
            series.append(
                {
                    "timestamp_utc": point.timestamp.isoformat(),
                    "consumo_total_kwh": point.consumo_total_kwh,
                    "producao_total_kwh": point.producao_total_kwh,
                    "producao_dgm_kwh": point.producao_dgm_kwh,
                    "producao_pre_kwh": point.producao_pre_kwh,
                    "saldo_kwh": point.saldo_kwh,
                    "ratio_producao_consumo": point.ratio_producao_consumo,
                    "flag_defice": point.flag_defice,
                    "flag_excedente": point.flag_excedente,
                    "flag_missing_source": point.flag_missing_source,
                }
            )
        return series

    def _aggregate(self, period_format: str) -> list[dict]:
        self._refresh_if_needed()
        assert self._cache is not None

        bucket: dict[str, dict] = defaultdict(
            lambda: {
                "periodo": "",
                "consumo_total_kwh": 0.0,
                "producao_total_kwh": 0.0,
                "producao_dgm_kwh": 0.0,
                "producao_pre_kwh": 0.0,
                "saldo_kwh": 0.0,
                "defice_horas": 0,
                "excedente_horas": 0,
                "missing_horas": 0,
                "leituras": 0,
                "leituras_completas": 0,
                "ratio_producao_consumo": None,
            }
        )

        for point in self._cache:
            key = point.timestamp.strftime(period_format)
            agg = bucket[key]
            agg["periodo"] = key
            agg["leituras"] += 1

            agg["consumo_total_kwh"] += point.consumo_total_kwh or 0.0
            agg["producao_total_kwh"] += point.producao_total_kwh or 0.0
            agg["producao_dgm_kwh"] += point.producao_dgm_kwh or 0.0
            agg["producao_pre_kwh"] += point.producao_pre_kwh or 0.0
            agg["saldo_kwh"] += point.saldo_kwh or 0.0

            if point.flag_missing_source:
                agg["missing_horas"] += 1
                continue

            agg["leituras_completas"] += 1
            if point.flag_defice:
                agg["defice_horas"] += 1
            elif point.flag_excedente:
                agg["excedente_horas"] += 1

        rows = []
        for key in sorted(bucket):
            row = bucket[key]
            if row["consumo_total_kwh"] > 0:
                row["ratio_producao_consumo"] = (
                    row["producao_total_kwh"] / row["consumo_total_kwh"]
                )
            rows.append(row)
        return rows

    def daily(self) -> list[dict]:
        return self._aggregate("%Y-%m-%d")

    def monthly(self) -> list[dict]:
        return self._aggregate("%Y-%m")

    def analytics(self) -> dict:
        self._refresh_if_needed()
        assert self._cache is not None

        total_horas = len(self._cache)
        total_consumo = sum(point.consumo_total_kwh or 0.0 for point in self._cache)
        total_producao = sum(point.producao_total_kwh or 0.0 for point in self._cache)
        total_saldo = sum(point.saldo_kwh or 0.0 for point in self._cache)

        defice_horas = sum(1 for point in self._cache if point.flag_defice)
        excedente_horas = sum(1 for point in self._cache if point.flag_excedente)
        missing_horas = sum(1 for point in self._cache if point.flag_missing_source)

        top_defices = sorted(
            (point for point in self._cache if point.saldo_kwh is not None),
            key=lambda point: point.saldo_kwh or 0.0,
        )[:10]

        return {
            "total_horas": total_horas,
            "total_consumo_kwh": total_consumo,
            "total_producao_kwh": total_producao,
            "saldo_total_kwh": total_saldo,
            "ratio_global_producao_consumo": (
                (total_producao / total_consumo) if total_consumo else None
            ),
            "horas_defice": defice_horas,
            "horas_excedente": excedente_horas,
            "horas_missing_source": missing_horas,
            "top_10_piores_defices": [
                {
                    "timestamp_utc": point.timestamp.isoformat(),
                    "saldo_kwh": point.saldo_kwh,
                    "consumo_total_kwh": point.consumo_total_kwh,
                    "producao_total_kwh": point.producao_total_kwh,
                }
                for point in top_defices
            ],
        }

    def test_database_connection(self) -> dict[str, str | bool]:
        return self.repository.test_connection()
