"""Cliente Trino — idêntico ao padrão DP-01 / DP-03."""
from __future__ import annotations

import trino

from app.config import (
    TRINO_CATALOG,
    TRINO_HOST,
    TRINO_PORT,
    TRINO_SCHEMA,
    TRINO_USER,
)


class TrinoClient:
    def _connect(self) -> trino.dbapi.Connection:
        return trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA,
            http_scheme="http",
        )

    def run_query(self, query: str) -> list[dict]:
        connection = self._connect()
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description or []]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            cursor.close()
            connection.close()
