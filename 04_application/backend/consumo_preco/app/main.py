"""Servidor HTTP consumo_preco — padrão idêntico a DP-01 / DP-03."""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from app.config import HTTP_HOST, HTTP_PORT
from app.controllers.consumo_preco_controller import dispatch


class Handler(BaseHTTPRequestHandler):
    server_version = "ConsumoPrecoeHTTP/1.0"

    def log_message(self, fmt: str, *args) -> None:
        print(f"[{self.address_string()}] {fmt % args}")

    def _send_json(self, payload: object, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8")
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
        try:
            payload, status = dispatch(parsed.path, parsed.query)
        except Exception as exc:
            payload, status = {"error": str(exc)}, 500
        self._send_json(payload, status)


def run() -> None:
    httpd = ThreadingHTTPServer((HTTP_HOST, HTTP_PORT), Handler)
    print(f"DP-02 API disponível em http://{HTTP_HOST}:{HTTP_PORT}")
    print(f"Endpoints:")
    print(f"  GET /health")
    print(f"  GET /api/v1/consumo-preco/overview")
    print(f"  GET /api/v1/consumo-preco/timeseries?group=day|month")
    print(f"  GET /api/v1/consumo-preco/analytics")
    print(f"  GET /api/v1/consumo-preco/db-connection")
    httpd.serve_forever()
