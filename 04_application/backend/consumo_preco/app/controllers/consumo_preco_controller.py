"""Controlador — despacha pedidos HTTP para o serviço consumo_preco."""
from __future__ import annotations

from urllib.parse import parse_qs, urlparse

from app.services.consumo_preco_service import ConsumoPrecoService

_service = ConsumoPrecoService()


def dispatch(path: str, query_string: str) -> tuple[object, int]:
    """Devolve (payload, http_status)."""
    params = parse_qs(query_string)

    if path == "/health":
        return {"status": "ok"}, 200

    if path == "/api/v1/consumo-preco/overview":
        return _service.overview(), 200

    if path == "/api/v1/consumo-preco/timeseries":
        group = params.get("group", ["day"])[0]
        if group not in ("day", "month"):
            return {"error": "Parâmetro 'group' inválido. Use 'day' ou 'month'."}, 400
        return _service.timeseries(group), 200

    if path == "/api/v1/consumo-preco/analytics":
        return _service.analytics(), 200

    if path == "/api/v1/consumo-preco/db-connection":
        return _service.db_connection(), 200

    return {"error": "Rota não encontrada."}, 404
