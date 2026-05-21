"""
Workflow Flyte: gate de qualidade entre camadas — consumo_preco.

Executa os checks SQL de qualidade para uma camada e lança excepção
se existirem verificações com status FAIL. Checks WARN são registados
mas não bloqueiam a promoção de dados.

Uso típico no pipeline:
    1. ingest_bronze        → quality_gate(layer="bronze")
    2. bronze_to_silver     → quality_gate(layer="silver")
    3. silver_to_gold       → quality_gate(layer="gold")

Execução standalone:
    pyflyte run workflows/flyte_quality_checks.py quality_gate_bronze
    pyflyte run workflows/flyte_quality_checks.py quality_gate_silver
    pyflyte run workflows/flyte_quality_checks.py quality_gate_gold
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import trino
from flytekit import task, workflow
from flytekit.exceptions.user import FlyteRecoverableException

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))

_FLYTE_ENV = {"TRINO_HOST": "host.docker.internal"}

# Ficheiros SQL em 04_quality/sql/ relativo à raiz do sub-pipeline consumo_preco
_SQL_DIR = Path(__file__).parent.parent / "04_quality" / "sql"

_LAYER_SQL = {
    "bronze": _SQL_DIR / "01_bronze_checks.sql",
    "silver": _SQL_DIR / "02_silver_checks.sql",
    "gold":   _SQL_DIR / "03_gold_checks.sql",
}


def _trino_conn() -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="admin",
        catalog="iceberg",
        schema="bronze",
        request_timeout=300,
    )


def _run_checks(layer: str) -> list[dict]:
    """
    Executa o SQL de qualidade para a camada indicada.

    O ficheiro SQL contém um único bloco UNION ALL seguido de queries
    de detalhe separadas por ';'. Executa apenas o primeiro bloco
    (sumário de checks com check_name / status / valor_pct / threshold_pct / detalhe).

    Retorna lista de dicts com os resultados de cada verificação.
    """
    sql_path = _LAYER_SQL[layer]
    if not sql_path.exists():
        raise FileNotFoundError(
            f"Ficheiro SQL de qualidade não encontrado: {sql_path}\n"
            f"Verifique se 04_quality/sql/{sql_path.name} existe."
        )

    raw_sql = sql_path.read_text(encoding="utf-8")

    # Extrai apenas o primeiro statement (bloco UNION ALL com ORDER BY final).
    # Usa versão sem comentários para localizar o ';' real — evita falsos positivos
    # em linhas de comentário (ex: "-- aceite como extra; threshold de 23").
    sql_no_comments = re.sub(r"--[^\n]*", "", raw_sql)
    first_semi = sql_no_comments.find(";")
    summary_sql = (sql_no_comments[:first_semi] if first_semi != -1 else sql_no_comments).strip()

    conn = _trino_conn()
    cur = conn.cursor()
    cur.execute(summary_sql)
    columns = [d[0] for d in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    conn.close()
    return rows


@task(retries=2, environment=_FLYTE_ENV)
def quality_gate(layer: str) -> int:
    """
    Gate de qualidade para a camada indicada ('bronze', 'silver' ou 'gold').

    Comportamento:
    - PASS  → regista no log, não bloqueia.
    - WARN  → regista no log com aviso, não bloqueia (dados promovidos com ressalva).
    - FAIL  → lança FlyteRecoverableException (com retries=2) — bloqueia promoção.

    Retorna o número de checks com status PASS.
    """
    if layer not in _LAYER_SQL:
        raise ValueError(
            f"Camada inválida: '{layer}'. Valores aceites: 'bronze', 'silver', 'gold'."
        )

    results = _run_checks(layer)

    fails  = [r for r in results if r.get("status") == "FAIL"]
    warns  = [r for r in results if r.get("status") == "WARN"]
    passes = [r for r in results if r.get("status") == "PASS"]

    print(f"\n{'=' * 60}")
    print(f"QUALITY GATE — {layer.upper()} ({len(results)} checks)")
    print(f"{'=' * 60}")
    print(f"  PASS : {len(passes)}")
    print(f"  WARN : {len(warns)}")
    print(f"  FAIL : {len(fails)}")
    print()

    for r in passes:
        print(f"  [PASS] {r['check_name']}")

    for r in warns:
        print(f"  [WARN] {r['check_name']} → {r.get('detalhe', '')}")

    for r in fails:
        print(f"  [FAIL] {r['check_name']} → {r.get('detalhe', '')}")

    if fails:
        fail_names = "; ".join(r["check_name"] for r in fails)
        raise FlyteRecoverableException(
            f"[consumo_preco] Quality gate FALHOU na camada {layer}: "
            f"{len(fails)} check(s) FAIL: {fail_names}"
        )

    print(f"  → Gate APROVADO para camada {layer}.\n")
    return len(passes)


# ---------------------------------------------------------------------------
# Workflows por camada — execução directa sem passar layer como argumento
# ---------------------------------------------------------------------------

@workflow
def quality_gate_bronze() -> int:
    """Gate de qualidade para a camada Bronze (consumo_preco)."""
    return quality_gate(layer="bronze")


@workflow
def quality_gate_silver() -> int:
    """Gate de qualidade para a camada Silver (consumo_preco)."""
    return quality_gate(layer="silver")


@workflow
def quality_gate_gold() -> int:
    """Gate de qualidade para a camada Gold (consumo_preco)."""
    return quality_gate(layer="gold")
