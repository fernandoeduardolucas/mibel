from __future__ import annotations

from datetime import date
from urllib.parse import parse_qs, urlparse

from app.services.meteo_producao_service import MeteoProducaoService


class MeteoProducaoController:
    """Camada controller: recebe request HTTP e retorna payload REST."""

    def __init__(self, service: MeteoProducaoService):
        self.service = service

    @staticmethod
    def _parse_date(value: str | None) -> date | None:
        if not value:
            return None
        return date.fromisoformat(value.strip()[:10])

    def health(self) -> tuple[int, dict]:
        return 200, {"status": "ok"}

    def get_daily(self, raw_query: str) -> tuple[int, dict]:
        params = parse_qs(raw_query)
        start = self._parse_date(params.get("start", [None])[0])
        end   = self._parse_date(params.get("end",   [None])[0])
        return 200, {"data": self.service.daily(start=start, end=end)}

    def get_latest(self, raw_query: str) -> tuple[int, dict]:
        params = parse_qs(raw_query)
        n_raw = params.get("n", ["30"])[0]
        n = int(n_raw) if n_raw.isdigit() else 30
        return 200, {"data": self.service.latest(n=n)}

    def get_correlations(self) -> tuple[int, dict]:
        return 200, {"data": self.service.correlations()}

    def get_analytics(self) -> tuple[int, dict]:
        return 200, {"data": self.service.analytics()}

    def test_database_connection(self) -> tuple[int, dict]:
        return 200, {"data": self.service.test_database_connection()}

    def route(self, path_with_query: str) -> tuple[int, dict]:
        parsed = urlparse(path_with_query)
        path   = parsed.path

        if path == "/health":
            return self.health()
        if path == "/api/v1/meteo-producao/daily":
            return self.get_daily(parsed.query)
        if path == "/api/v1/meteo-producao/latest":
            return self.get_latest(parsed.query)
        if path == "/api/v1/meteo-producao/correlations":
            return self.get_correlations()
        if path == "/api/v1/meteo-producao/analytics":
            return self.get_analytics()
        if path == "/api/v1/meteo-producao/db-connection":
            return self.test_database_connection()

        return 404, {
            "error": "endpoint_not_found",
            "message": "Use /health ou /api/v1/meteo-producao/*",
        }
