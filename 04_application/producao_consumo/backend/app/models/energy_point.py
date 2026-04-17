from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class EnergyPoint:
    timestamp: datetime
    consumo_total_kwh: float | None
    producao_total_kwh: float | None
    producao_dgm_kwh: float | None
    producao_pre_kwh: float | None
    saldo_kwh: float | None
    ratio_producao_consumo: float | None
    flag_defice: bool
    flag_excedente: bool
    flag_missing_source: bool

    @property
    def has_complete_data(self) -> bool:
        return (
            self.consumo_total_kwh is not None
            and self.producao_total_kwh is not None
            and not self.flag_missing_source
        )
