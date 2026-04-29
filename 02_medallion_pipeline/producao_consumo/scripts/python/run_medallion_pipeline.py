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
import platform
import shutil
import subprocess
import sys
import time
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


def docker_engine_running() -> bool:
    result = subprocess.run(["docker", "info"], text=True, capture_output=True)
    return result.returncode == 0


def try_start_docker_engine() -> bool:
    """Tenta arrancar o Docker Engine de forma best-effort conforme o SO."""
    system = platform.system().lower()
    start_commands: list[list[str]] = []

    if system == "linux":
        start_commands = [
            ["systemctl", "start", "docker"],
            ["service", "docker", "start"],
        ]
    elif system == "darwin":
        start_commands = [["open", "-a", "Docker"]]
    elif system == "windows":
        start_commands = [
            [
                "powershell",
                "-NoProfile",
                "-Command",
                "Start-Process 'C:\\Program Files\\Docker\\Docker\\Docker Desktop.exe'",
            ]
        ]

    for cmd in start_commands:
        if shutil.which(cmd[0]) is None:
            continue
        print(f">>> Docker Engine indisponível. A tentar arrancar com: {' '.join(cmd)}")
        subprocess.run(cmd, text=True, capture_output=True)
        for _ in range(20):
            if docker_engine_running():
                print(">>> Docker Engine está disponível.")
                return True
            time.sleep(1)

    return docker_engine_running()


def ensure_docker_engine_running() -> None:
    if docker_engine_running():
        return

    if try_start_docker_engine():
        return

    raise SystemExit(
        "Erro: Docker Engine não está a correr.\n"
        "Tentativa automática de arranque falhou. Arranca o Docker manualmente e repete."
    )


def create_local_venv(pipeline_root: Path, base_python: str) -> Path:
    """Cria (se necessário) e devolve o python de um venv local da pipeline."""
    venv_dir = pipeline_root / ".venv_medallion"
    if os.name == "nt":
        venv_python = venv_dir / "Scripts" / "python.exe"
    else:
        venv_python = venv_dir / "bin" / "python"

    if not venv_python.exists():
        print(f"\n>>> Criando virtualenv local em: {venv_dir}")
        run([base_python, "-m", "venv", str(venv_dir)])

    return venv_python


def main() -> None:
    parser = argparse.ArgumentParser(description="Corre a medallion pipeline de producao_consumo")
    parser.add_argument("--build", action="store_true", help="faz build no docker compose up")
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    pipeline_root = script_dir.parent.parent
    repo_root = pipeline_root.parent.parent

    compose_file = repo_root / "01_bootstrap" / "tead_2.0_v1.2" / "docker-compose.yml"
    bronze_dir = pipeline_root / "01_bronze"
    bronze_sql = bronze_dir / "sql" / "bronze_trino.sql"
    silver_sql = pipeline_root / "02_silver" / "sql" / "01_silver_trino.sql"
    gold_sql = pipeline_root / "03_gold" / "sql" / "01_gold_trino.sql"
    bronze_requirements = bronze_dir / "scripts" / "python" / "requirements.txt"

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
    ensure_docker_engine_running()

    python_cmd = sys.executable
    if not python_cmd or "WindowsApps" in python_cmd or not Path(python_cmd).exists():
        raise RuntimeError(
            "Python inválido detectado. Use o executável real do Python, não o alias WindowsApps."
        )
    venv_python = create_local_venv(pipeline_root, python_cmd)

    compose_up = ["docker", "compose", "-f", str(compose_file), "up", "-d"]
    if args.build:
        compose_up.append("--build")
    run(compose_up)

    run([str(venv_python), "-m", "pip", "install", "--upgrade", "pip"])
    run([str(venv_python), "-m", "pip", "install", "-r", str(bronze_requirements)])

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
            str(venv_python),
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
