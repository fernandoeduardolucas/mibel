"""Configuração do backend consumo_preco."""
from __future__ import annotations
import os

TRINO_HOST    = os.getenv("TRINO_HOST",    "localhost")
TRINO_PORT    = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER    = os.getenv("TRINO_USER",    "trino")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA  = os.getenv("TRINO_SCHEMA",  "gold")

GOLD_TABLE    = os.getenv("TRINO_TABLE",   "iceberg.gold.dp_energy_market_hourly")

HTTP_HOST     = os.getenv("API_HOST", "0.0.0.0")
HTTP_PORT     = int(os.getenv("PORT",  "8000"))

CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "60"))
