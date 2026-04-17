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

    def run_query(self, query: str) -> list[dict[str, str]]:
        try:
            import trino
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "Dependência em falta: instale 'trino' (pip install trino)."
            ) from exc

        connection = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA,
            http_scheme="http",
        )

        cursor = connection.cursor()
        try:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description or []]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            cursor.close()
            connection.close()
