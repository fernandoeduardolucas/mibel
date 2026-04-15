#!/usr/bin/env python3
"""Runner único (cross-platform) para a pipeline Medallion de producao_consumo.

Fluxo:
1) Sobe stack Docker Compose (sem build por padrão)
2) Instala dependências da Bronze
3) Executa limpeza + upload Bronze
4) Executa SQL Bronze, Silver e Gold via Trino dentro do Docker
5) Faz validação rápida da Gold
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str], *, cwd: Path | None = None, input_text: str | None = None) -> None:
    print(f"\n>>> {' '.join(cmd)}")
    subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        text=True,
        input=input_text,
        check=True,
    )


def must_exist(path: Path, description: str) -> None:
    if not path.exists():
        raise SystemExit(f"Erro: {description} não encontrado: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Corre a medallion pipeline de producao_consumo")
    parser.add_argument("--build", action="store_true", help="faz build no docker compose up")
    args = parser.parse_args()

    pipeline_root = Path(__file__).resolve().parent
    repo_root = pipeline_root.parent.parent

    compose_file = repo_root / "01_bootstrap" / "tead_2.0_v1.2" / "docker-compose.yml"
    bronze_dir = pipeline_root / "01_bronze"
    bronze_sql = bronze_dir / "sql" / "bronze_trino.sql"
    silver_sql = pipeline_root / "02_silver" / "sql" / "01_silver_trino.sql"
    gold_sql = pipeline_root / "03_gold" / "sql" / "01_gold_trino.sql"
    bronze_requirements = bronze_dir / "scripts" / "python" / "requirements_bronze.txt"

    for path, desc in [
        (compose_file, "docker-compose.yml"),
        (bronze_sql, "SQL Bronze"),
        (silver_sql, "SQL Silver"),
        (gold_sql, "SQL Gold"),
        (bronze_requirements, "requirements da Bronze"),
    ]:
        must_exist(path, desc)

    if shutil.which("docker") is None:
        raise SystemExit("Erro: comando 'docker' não encontrado.")

    python_cmd = shutil.which("python") or shutil.which("python3")
    if python_cmd is None:
        raise SystemExit("Erro: não foi encontrado python/python3 no PATH.")

    compose_up = ["docker", "compose", "-f", str(compose_file), "up", "-d"]
    if args.build:
        compose_up.append("--build")
    run(compose_up)

    run([python_cmd, "-m", "pip", "install", "-r", str(bronze_requirements)])

    env = os.environ.copy()
    env.update(
        {
            "S3_ENDPOINT_URL": "http://localhost:9000",
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "S3_BUCKET": "warehouse",
            "S3_PREFIX": "bronze/clean",
        }
    )

    print("\n>>> Bronze clean + upload")
    subprocess.run(
        [
            python_cmd,
            "scripts/python/bronze_clean_upload.py",
            "--consumo",
            "data/raw/consumo-total-nacional.csv",
            "--producao",
            "data/raw/energia-produzida-total-nacional.csv",
            "--out-dir",
            "data/clean",
            "--upload",
        ],
        cwd=str(bronze_dir),
        env=env,
        check=True,
        text=True,
    )

    for stage_name, sql_file in [("Bronze", bronze_sql), ("Silver", silver_sql), ("Gold", gold_sql)]:
        print(f"\n>>> SQL {stage_name} via Docker/Trino: {sql_file}")
        run(
            ["docker", "compose", "-f", str(compose_file), "exec", "-T", "trino", "trino"],
            input_text=sql_file.read_text(encoding="utf-8"),
        )

    run(
        [
            "docker",
            "compose",
            "-f",
            str(compose_file),
            "exec",
            "-T",
            "trino",
            "trino",
            "--execute",
            "SELECT COUNT(*) AS linhas_gold FROM iceberg.gold.producao_vs_consumo_hourly;",
        ]
    )

    print("\nPipeline Medallion (producao_consumo) concluída com sucesso.")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        print(f"\nErro: comando falhou com exit code {exc.returncode}", file=sys.stderr)
        raise
