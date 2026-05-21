"""Modelo de domínio — ponto horário de consumo e preço."""
from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class EnergyPoint:
    ts_utc: str
    consumo_total: float
    market_price_pt: float

    @property
    def custo_estimado_eur(self) -> float:
        return self.consumo_total * self.market_price_pt
