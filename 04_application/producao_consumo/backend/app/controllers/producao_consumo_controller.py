from __future__ import annotations

from datetime import datetime
from urllib.parse import parse_qs, urlparse

from app.services.producao_consumo_service import ProducaoConsumoService


class ProducaoConsumoController:
    """Camada controller: recebe request HTTP e retorna payload REST."""

    def __init__(self, service: ProducaoConsumoService):
        self.service = service

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)

    def health(self) -> tuple[int, dict]:
        return 200, {"status": "ok"}

    def get_hourly(self, raw_query: str) -> tuple[int, dict]:
        params = parse_qs(raw_query)
        start = self._parse_datetime(params.get("start", [None])[0])
        end = self._parse_datetime(params.get("end", [None])[0])
        return 200, {"data": self.service.hourly(start=start, end=end)}

    def get_daily(self) -> tuple[int, dict]:
        return 200, {"data": self.service.daily()}

    def get_monthly(self) -> tuple[int, dict]:
        return 200, {"data": self.service.monthly()}

    def get_analytics(self) -> tuple[int, dict]:
        return 200, {"data": self.service.analytics()}

    def test_database_connection(self) -> tuple[int, dict]:
        return 200, {"data": self.service.test_database_connection()}

    def route(self, path_with_query: str) -> tuple[int, dict]:
        parsed = urlparse(path_with_query)
        path = parsed.path

        if path == "/health":
            return self.health()
        if path == "/api/v1/producao-consumo/hourly":
            return self.get_hourly(parsed.query)
        if path == "/api/v1/producao-consumo/daily":
            return self.get_daily()
        if path == "/api/v1/producao-consumo/monthly":
            return self.get_monthly()
        if path == "/api/v1/producao-consumo/analytics":
            return self.get_analytics()
        if path == "/api/v1/producao-consumo/db-connection":
            return self.test_database_connection()

        return 404, {
            "error": "endpoint_not_found",
            "message": "Use /health ou /api/v1/producao-consumo/*",
        }
