from __future__ import annotations

from app.config import (
    TRINO_CATALOG,
    TRINO_HOST,
    TRINO_PORT,
    TRINO_SCHEMA,
    TRINO_USER,
)


class TrinoClient:
    """Executa SQL no Trino via JDBC URL (sem docker exec)."""

    def _connect(self):
        try:
            import trino
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "Dependência em falta: instale 'trino' (pip install trino)."
            ) from exc

        return trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA,
            http_scheme="http",
        )

    def test_connection(self) -> dict[str, str | bool]:
        connection = self._connect()
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT 1 AS ok")
            result = cursor.fetchone()
            connected = bool(result and result[0] == 1)
            return {
                "connected": connected,
                "host": TRINO_HOST,
                "port": str(TRINO_PORT),
                "user": TRINO_USER,
                "catalog": TRINO_CATALOG,
                "schema": TRINO_SCHEMA,
            }
        finally:
            cursor.close()
            connection.close()

    def run_query(self, query: str) -> list[dict[str, str]]:
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
