from __future__ import annotations

from datetime import datetime, timezone

from app.config import BASE_QUERY
from app.db.trino_client import TrinoClient
from app.models.energy_point import EnergyPoint


class ProducaoConsumoRepository:
    """Camada repositório: traduz rows da BD para modelos de domínio."""

    def __init__(self, client: TrinoClient):
        self.client = client

    def test_connection(self) -> dict[str, str | bool]:
        return self.client.test_connection()

    @staticmethod
    def _parse_float(value: object | None) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)

        text = str(value).strip()
        if not text:
            return None
        return float(text)

    @staticmethod
    def _parse_bool(value: object | None) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).strip().lower() == "true"

    @staticmethod
    def _parse_timestamp(value: object) -> datetime:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value

        raw = str(value).strip()
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

    def list_hourly(self) -> list[EnergyPoint]:
        rows = self.client.run_query(BASE_QUERY)
        points: list[EnergyPoint] = []

        for row in rows:
            timestamp_raw = row.get("timestamp_utc")
            if timestamp_raw is None:
                continue

            points.append(
                EnergyPoint(
                    timestamp=self._parse_timestamp(timestamp_raw),
                    consumo_total_kwh=self._parse_float(row.get("consumo_total_kwh")),
                    producao_total_kwh=self._parse_float(row.get("producao_total_kwh")),
                    producao_dgm_kwh=self._parse_float(row.get("producao_dgm_kwh")),
                    producao_pre_kwh=self._parse_float(row.get("producao_pre_kwh")),
                    saldo_kwh=self._parse_float(row.get("saldo_kwh")),
                    ratio_producao_consumo=self._parse_float(
                        row.get("ratio_producao_consumo")
                    ),
                    flag_defice=self._parse_bool(row.get("flag_defice")),
                    flag_excedente=self._parse_bool(row.get("flag_excedente")),
                    flag_missing_source=self._parse_bool(row.get("flag_missing_source")),
                )
            )
        return points
