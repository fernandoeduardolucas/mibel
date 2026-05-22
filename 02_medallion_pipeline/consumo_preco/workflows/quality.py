#!/usr/bin/env python3
"""
Quality gate — consumo_preco.

Executa o SQL de checks para a camada indicada e imprime os resultados.
Termina com exit code 1 se existirem checks com status FAIL.

Uso:
    python quality.py --layer bronze [--trino-host localhost]
    python quality.py --layer silver [--trino-host localhost]
    python quality.py --layer gold   [--trino-host localhost]
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

import trino

_SQL_DIR = Path(__file__).parent.parent / "04_quality" / "sql"

_LAYER_SQL = {
    "bronze": _SQL_DIR / "01_bronze_checks.sql",
    "silver": _SQL_DIR / "02_silver_checks.sql",
    "gold":   _SQL_DIR / "03_gold_checks.sql",
}


def _trino(host: str, port: int = 8080) -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=host, port=port, user="admin",
        catalog="iceberg", schema="bronze",
        request_timeout=300,
    )


def run_quality_gate(layer: str, trino_host: str, port: int = 8080) -> bool:
    """
    Executa os checks de qualidade para a camada indicada.
    Retorna True se todos os checks passaram (sem FAIL), False caso contrário.
    """
    if layer not in _LAYER_SQL:
        raise ValueError(f"Camada inválida: '{layer}'. Use bronze, silver ou gold.")

    sql_path = _LAYER_SQL[layer]
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL de qualidade não encontrado: {sql_path}")

    raw_sql = sql_path.read_text(encoding="utf-8")
    # Remove comentários de linha e extrai o primeiro statement (bloco UNION ALL)
    sql_no_comments = re.sub(r"--[^\n]*", "", raw_sql)
    first_semi = sql_no_comments.find(";")
    query = (sql_no_comments[:first_semi] if first_semi != -1 else sql_no_comments).strip()

    conn = _trino(trino_host, port)
    cur = conn.cursor()
    cur.execute(query)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    conn.close()

    fails  = [r for r in rows if r.get("status") == "FAIL"]
    warns  = [r for r in rows if r.get("status") == "WARN"]
    passes = [r for r in rows if r.get("status") == "PASS"]

    print(f"\n{'=' * 56}")
    print(f"QUALITY GATE — {layer.upper()}  ({len(rows)} checks)")
    print(f"{'=' * 56}")
    print(f"  PASS : {len(passes)}   WARN : {len(warns)}   FAIL : {len(fails)}")
    print()

    for r in passes:
        print(f"  [PASS] {r['check_name']}")
    for r in warns:
        print(f"  [WARN] {r['check_name']}  →  {r.get('detalhe', '')}")
    for r in fails:
        print(f"  [FAIL] {r['check_name']}  →  {r.get('detalhe', '')}")

    if fails:
        print(f"\n  ✗ Gate FALHOU: {len(fails)} check(s) FAIL na camada {layer}.\n")
        return False

    print(f"\n  ✓ Gate APROVADO para camada {layer}.\n")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Quality gate — consumo_preco")
    parser.add_argument("--layer",      required=True, choices=["bronze", "silver", "gold"])
    parser.add_argument("--trino-host", default=os.getenv("TRINO_HOST", "localhost"))
    parser.add_argument("--trino-port", type=int, default=int(os.getenv("TRINO_PORT", "8080")))
    args = parser.parse_args()

    ok = run_quality_gate(args.layer, args.trino_host, args.trino_port)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
