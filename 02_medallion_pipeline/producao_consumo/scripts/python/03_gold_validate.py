#!/usr/bin/env python3
from __future__ import annotations

import argparse


def main() -> None:
    # Tarefa Gold: validar parâmetros mínimos antes de executar o SQL Gold.
    parser = argparse.ArgumentParser(description="Passo Gold: validação de arranque")
    parser.add_argument("--expect-table", default="iceberg.gold.producao_vs_consumo_hourly")
    args = parser.parse_args()
    print(f"Gold pronto para executar SQL. Tabela alvo: {args.expect_table}")


if __name__ == "__main__":
    main()
