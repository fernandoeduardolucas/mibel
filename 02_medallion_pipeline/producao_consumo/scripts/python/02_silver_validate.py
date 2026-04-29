#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


def main() -> None:
    # Tarefa Silver: garantir que os 2 parquet clean da Bronze existem antes do SQL Silver.
    parser = argparse.ArgumentParser(description="Passo Silver: valida pré-condições")
    parser.add_argument("--clean-dir", required=True, type=Path)
    args = parser.parse_args()

    consumo = args.clean_dir / "consumo_total_nacional_bronze_raw.parquet"
    producao = args.clean_dir / "energia_produzida_total_nacional_bronze_raw.parquet"

    missing = [str(p) for p in [consumo, producao] if not p.exists()]
    if missing:
        raise SystemExit(f"Faltam ficheiros clean da Bronze: {missing}")

    print("Silver pronto para executar SQL (dados clean da Bronze existem).")


if __name__ == "__main__":
    main()
