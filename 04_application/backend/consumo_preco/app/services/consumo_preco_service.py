"""Serviço de negócio — consumo_preco com cache TTL."""
from __future__ import annotations

import time

from app.config import CACHE_TTL_SECONDS
from app.repositories.consumo_preco_repository import ConsumoPrecoeRepository


class ConsumoPrecoService:
    def __init__(self) -> None:
        self._repo = ConsumoPrecoeRepository()
        self._cache: dict = {}
        self._cache_ts: dict[str, float] = {}

    def _cached(self, key: str, fn):
        now = time.monotonic()
        if key not in self._cache or (now - self._cache_ts.get(key, 0)) > CACHE_TTL_SECONDS:
            self._cache[key] = fn()
            self._cache_ts[key] = now
        return self._cache[key]

    def overview(self) -> dict:
        return self._cached("overview", self._repo.get_overview)

    def timeseries(self, group: str) -> list[dict]:
        if group == "month":
            return self._cached("ts_monthly", self._repo.get_timeseries_monthly)
        return self._cached("ts_daily", self._repo.get_timeseries_daily)

    def analytics(self) -> dict:
        return self._cached("analytics", self._repo.get_analytics)

    def db_connection(self) -> dict:
        return self._repo.test_connection()
