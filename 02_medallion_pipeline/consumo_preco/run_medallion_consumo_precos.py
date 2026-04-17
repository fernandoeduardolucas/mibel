#!/usr/bin/env python3
"""Runner único (cross-platform) para a pipeline Medallion de consumo_preco.

Fluxo:
1) Sobe stack Docker Compose (sem build por padrão)
2) Espera que o Trino esteja disponível
3) Cria/usa virtualenv local
4) Instala dependências da Bronze
5) Executa limpeza + upload Bronze
6) Executa SQL Bronze, Silver e Gold via Trino dentro do Docker
7) Faz validação rápida da Gold
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


DEFAULT_VALIDATION_QUERY = """
SHOW TABLES FROM iceberg.gold
""".strip()


def run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    input_text: str | None = None,
) -> None:
    print(f"\n>>> {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        text=True,
        input=input_text,
        capture_output=True,
    )

    if result.stdout:
        print(result.stdout)

    if result.returncode != 0:
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        raise subprocess.CalledProcessError(
            result.returncode,
            cmd,
            output=result.stdout,
            stderr=result.stderr,
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
    """Cria (se necessário) e devolve o executável Python do venv local da pipeline."""
    venv_dir = pipeline_root / ".venv_medallion_consumo_preco"
    if os.name == "nt":
        venv_python = venv_dir / "Scripts" / "python.exe"
    else:
        venv_python = venv_dir / "bin" / "python"

    if not venv_python.exists():
        print(f"\n>>> A criar virtualenv local em: {venv_dir}")
        run([base_python, "-m", "venv", str(venv_dir)])

    return venv_python


def wait_for_trino(compose_file: Path, attempts: int = 30, sleep_seconds: int = 2) -> None:
    """Espera até o Trino responder a uma query simples."""
    cmd = [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "exec",
        "-T",
        "trino",
        "trino",
        "--execute",
        "SELECT 1;",
    ]

    last_stderr = ""
    for attempt in range(1, attempts + 1):
        print(f"\n>>> A verificar disponibilidade do Trino ({attempt}/{attempts})")
        result = subprocess.run(cmd, text=True, capture_output=True)
        if result.returncode == 0:
            print(">>> Trino está disponível.")
            return

        last_stderr = result.stderr or result.stdout or ""
        time.sleep(sleep_seconds)

    raise SystemExit(
        "Erro: Trino não ficou disponível dentro do tempo esperado.\n"
        f"Último erro:\n{last_stderr}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Corre a medallion pipeline de consumo_preco"
    )
    parser.add_argument("--build", action="store_true", help="faz build no docker compose up")

    parser.add_argument(
        "--bronze-sql",
        default="01_bronze/SQL/bronze_consumo_precos_trino.sql",
        help="caminho relativo (a partir da raiz da pipeline) para o SQL Bronze",
    )
    parser.add_argument(
        "--silver-sql",
        default="02_silver/SQL/silver_consumo_precos_trino.sql",
        help="caminho relativo (a partir da raiz da pipeline) para o SQL Silver",
    )
    parser.add_argument(
        "--gold-sql",
        default="03_gold/SQL/gold_consumo_precos_trino.sql",
        help="caminho relativo (a partir da raiz da pipeline) para o SQL Gold",
    )
    parser.add_argument(
        "--validation-query",
        default=DEFAULT_VALIDATION_QUERY,
        help="query SQL final de validação da Gold",
    )

    args = parser.parse_args()

    pipeline_root = Path(__file__).resolve().parent
    repo_root = pipeline_root.parent.parent

    compose_file = repo_root / "01_bootstrap" / "tead_2.0_v1.2" / "docker-compose.yml"

    bronze_dir = pipeline_root / "01_bronze"
    bronze_script = bronze_dir / "scripts" / "python" / "bronze_clean_upload_consumo_precos.py"
    bronze_requirements = bronze_dir / "scripts" / "python" / "requirements_bronze.txt"

    consumo_raw = bronze_dir / "data" / "raw" / "consumo-total-nacional.csv"
    precos_raw = bronze_dir / "data" / "raw" / "Day-ahead Market Prices_20230101_20260311.csv"

    bronze_sql = pipeline_root / args.bronze_sql
    silver_sql = pipeline_root / args.silver_sql
    gold_sql = pipeline_root / args.gold_sql

    for path, desc in [
        (compose_file, "docker-compose.yml"),
        (bronze_script, "script Bronze consumo_preco"),
        (bronze_requirements, "requirements da Bronze"),
        (consumo_raw, "dataset raw de consumo"),
        (precos_raw, "dataset raw de preços"),
        (bronze_sql, "SQL Bronze"),
        (silver_sql, "SQL Silver"),
        (gold_sql, "SQL Gold"),
    ]:
        must_exist(path, desc)

    if shutil.which("docker") is None:
        raise SystemExit("Erro: comando 'docker' não encontrado.")
    ensure_docker_engine_running()

    python_cmd = sys.executable
    if not python_cmd or "WindowsApps" in python_cmd or not Path(python_cmd).exists():
        raise RuntimeError(
            "Python inválido detectado. Usa o executável real do Python, não o alias WindowsApps."
        )

    venv_python = create_local_venv(pipeline_root, python_cmd)

    compose_up = ["docker", "compose", "-f", str(compose_file), "up", "-d"]
    if args.build:
        compose_up.append("--build")
    run(compose_up)

    wait_for_trino(compose_file)

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
    run(
        [
            str(venv_python),
            "scripts/python/bronze_clean_upload_consumo_precos.py",
            "--consumo",
            "data/raw/consumo-total-nacional.csv",
            "--precos",
            "data/raw/Day-ahead Market Prices_20230101_20260311.csv",
            "--out-dir",
            "data/clean",
            "--upload",
        ],
        cwd=bronze_dir,
        env=env,
    )

    for stage_name, sql_file in [
        ("Bronze", bronze_sql),
        ("Silver", silver_sql),
        ("Gold", gold_sql),
    ]:
        print(f"\n>>> SQL {stage_name} via Docker/Trino: {sql_file}")
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
            ],
            input_text=sql_file.read_text(encoding="utf-8"),
        )

    print("\n>>> Validação final da Gold")
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
            args.validation_query,
        ]
    )

    print("\nPipeline Medallion (consumo_preco) concluída com sucesso.")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        print(f"\nErro: comando falhou com exit code {exc.returncode}", file=sys.stderr)
        raise
