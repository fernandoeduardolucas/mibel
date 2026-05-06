#!/usr/bin/env python3
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from app.config import GOLD_SQL_PATH, HOST, PORT
from app.controllers.producao_consumo_controller import ProducaoConsumoController
from app.db.trino_client import TrinoClient
from app.repositories.producao_consumo_repository import ProducaoConsumoRepository
from app.services.producao_consumo_service import ProducaoConsumoService


def build_controller() -> ProducaoConsumoController:
    client = TrinoClient()
    repository = ProducaoConsumoRepository(client)
    service = ProducaoConsumoService(repository)
    return ProducaoConsumoController(service)


CONTROLLER = build_controller()


class RequestHandler(BaseHTTPRequestHandler):
    server_version = "ProducaoConsumoMVC/1.0"

    def _send_json(self, status_code: int, payload: dict) -> None:
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(encoded)

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:
        try:
            status, payload = CONTROLLER.route(self.path)
        except Exception as exc:  # noqa: BLE001
            self._send_json(
                500,
                {
                    "error": "internal_server_error",
                    "message": str(exc),
                },
            )
            return
        self._send_json(status, payload)


def main() -> None:
    print(f"[INFO] SQL base (gold): {GOLD_SQL_PATH}")
    print(f"[INFO] Backend MVC ativo em http://{HOST}:{PORT}")
    print(
        "[INFO] Endpoints: /health, /api/v1/producao-consumo/* e "
        "/api/v1/producao-consumo/db-connection"
    )

    server = ThreadingHTTPServer((HOST, PORT), RequestHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
