#!/usr/bin/env python3
"""
Orquestrador Medallion — consumo_preco.

Fluxo (modo padrão):
  1. Sobe Docker Compose e aguarda Trino
  2. Cria venv local e instala dependências (trino, boto3, pandas)
  3. Aplica DDL via Trino CLI (IF NOT EXISTS — idempotente)
  4. Bronze ingest        (workflows/bronze.py)
  5. Quality gate Bronze  (workflows/quality.py --layer bronze)
  6. Silver transform     (workflows/silver.py)
  7. Quality gate Silver  (workflows/quality.py --layer silver)
  8. Gold build           (workflows/gold.py)
  9. Quality gate Gold    (workflows/quality.py --layer gold)
 10. Validação final: COUNT nas tabelas Gold

Uso rápido (stack já a correr):
    python run_medallion_consumo_precos.py --skip-docker

Flags:
    --skip-docker   não faz compose up (stack já a correr)
    --skip-ddl      não reaplicar DDL (tabelas já criadas)
    --no-quality    salta os quality gates
    --build         faz --build no compose up
"""

from __future__ import annotations

import argparse
import os
import platform
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path


# ---------------------------------------------------------------------------
# Subprocess helpers
# ---------------------------------------------------------------------------

def run(cmd: list[str], *, cwd: Path | None = None) -> None:
    """Executa um comando e lança exceção se falhar."""
    print(f"\n>>> {' '.join(str(c) for c in cmd)}")
    result = subprocess.run(cmd, cwd=str(cwd) if cwd else None, text=True)
    if result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, cmd)


def run_capture(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True)


def must_exist(path: Path, label: str) -> None:
    if not path.exists():
        raise SystemExit(f"Erro: {label} não encontrado em: {path}")


# ---------------------------------------------------------------------------
# Docker helpers
# ---------------------------------------------------------------------------

def docker_ok() -> bool:
    return run_capture(["docker", "info"]).returncode == 0


def try_start_docker() -> bool:
    system = platform.system().lower()
    cmds: list[list[str]] = []
    if system == "linux":
        cmds = [["systemctl", "start", "docker"], ["service", "docker", "start"]]
    elif system == "darwin":
        cmds = [["open", "-a", "Docker"]]
    elif system == "windows":
        cmds = [["powershell", "-NoProfile", "-Command",
                  "Start-Process 'C:\\Program Files\\Docker\\Docker\\Docker Desktop.exe'"]]
    for cmd in cmds:
        if shutil.which(cmd[0]) is None:
            continue
        print(f">>> A tentar arrancar Docker: {' '.join(cmd)}")
        subprocess.run(cmd, capture_output=True)
        for _ in range(20):
            if docker_ok():
                return True
            time.sleep(2)
    return docker_ok()


def ensure_docker() -> None:
    if not docker_ok() and not try_start_docker():
        raise SystemExit(
            "Docker Engine não está a correr.\n"
            "Arranca o Docker manualmente e repete."
        )


def wait_for_trino(compose_file: Path, attempts: int = 40, sleep_s: int = 3) -> None:
    cmd = [
        "docker", "compose", "-f", str(compose_file),
        "exec", "-T", "trino", "trino", "--execute", "SELECT 1;",
    ]
    for i in range(1, attempts + 1):
        print(f">>> Trino disponível? ({i}/{attempts})")
        if run_capture(cmd).returncode == 0:
            print(">>> Trino OK.")
            return
        time.sleep(sleep_s)
    raise SystemExit("Trino não ficou disponível dentro do tempo esperado.")


# ---------------------------------------------------------------------------
# Venv helper
# ---------------------------------------------------------------------------

def create_venv(pipeline_root: Path, base_python: str) -> Path:
    venv_dir = pipeline_root / ".venv_medallion_consumo_preco"
    if os.name == "nt":
        venv_python = venv_dir / "Scripts" / "python.exe"
    else:
        venv_python = venv_dir / "bin" / "python"
    if not venv_python.exists():
        print(f"\n>>> A criar virtualenv em: {venv_dir}")
        run([base_python, "-m", "venv", str(venv_dir)])
    return venv_python


# ---------------------------------------------------------------------------
# DDL helper — executa via Trino CLI dentro do Docker
# ---------------------------------------------------------------------------

def apply_ddl(compose_file: Path, sql_file: Path, label: str) -> None:
    print(f"\n>>> DDL {label}: {sql_file.name}")
    sql_text = sql_file.read_text(encoding="utf-8")
    # Remove comentários de linha para evitar bytes não-ASCII no Windows
    sql_clean = re.sub(r"--[^\n]*", "", sql_text)
    statements = [s.strip() for s in sql_clean.split(";") if s.strip()]
    for stmt in statements:
        subprocess.run(
            ["docker", "compose", "-f", str(compose_file),
             "exec", "-T", "trino", "trino"],
            input=(stmt + ";").encode("utf-8"),
            capture_output=True,
        )
    print(f"    DDL {label} aplicado.")


# ---------------------------------------------------------------------------
# Execução de scripts no venv
# ---------------------------------------------------------------------------

def run_script(venv_python: Path, script: Path, extra_args: list[str] | None = None) -> None:
    """Chama um script Python no venv. Lança exceção se falhar."""
    cmd = [str(venv_python), str(script)] + (extra_args or [])
    run(cmd, cwd=script.parent)


def run_quality(venv_python: Path, workflows_dir: Path, layer: str) -> None:
    print(f"\n{'=' * 58}")
    print(f"QUALITY GATE — {layer.upper()}")
    print(f"{'=' * 58}")
    result = subprocess.run(
        [str(venv_python), str(workflows_dir / "quality.py"), "--layer", layer],
        cwd=str(workflows_dir),
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(
            f"Quality gate FALHOU na camada {layer}. "
            "Corrige os problemas antes de prosseguir."
        )


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Orquestrador Medallion — consumo_preco")
    parser.add_argument("--skip-docker", action="store_true", help="Não faz compose up")
    parser.add_argument("--skip-ddl",   action="store_true", help="Não reaplicar DDL")
    parser.add_argument("--no-quality", action="store_true", help="Salta quality gates")
    parser.add_argument("--build",      action="store_true", help="--build no compose up")
    args = parser.parse_args()

    pipeline_root = Path(__file__).resolve().parent
    repo_root     = pipeline_root.parent.parent

    compose_file  = repo_root / "01_docker_stack" / "docker-compose.yml"
    workflows_dir = pipeline_root / "workflows"

    bronze_sql = pipeline_root / "01_bronze" / "bronze_consumo_precos_trino.sql"
    silver_sql = pipeline_root / "02_silver" / "sql" / "silver_consumo_precos_trino.sql"
    gold_sql   = pipeline_root / "03_gold"   / "sql" / "gold_consumo_precos_trino.sql"

    # --- pré-requisitos ---
    must_exist(compose_file,  "docker-compose.yml")
    must_exist(workflows_dir, "pasta workflows/")
    if not args.skip_ddl:
        must_exist(bronze_sql, "DDL Bronze")
        must_exist(silver_sql, "DDL Silver")
        must_exist(gold_sql,   "DDL Gold")
    if shutil.which("docker") is None:
        raise SystemExit("Comando 'docker' não encontrado no PATH.")

    python_cmd = sys.executable
    if not python_cmd or "WindowsApps" in python_cmd or not Path(python_cmd).exists():
        raise SystemExit(
            "Python inválido. Usa o executável real do Python (não o alias WindowsApps)."
        )

    # --- venv ---
    venv_python = create_venv(pipeline_root, python_cmd)
    requirements = workflows_dir / "requirements.txt"
    run([str(venv_python), "-m", "pip", "install", "--upgrade", "pip", "-q"])
    if requirements.exists():
        run([str(venv_python), "-m", "pip", "install", "-r", str(requirements), "-q"])
    else:
        run([str(venv_python), "-m", "pip", "install", "trino", "boto3", "pandas", "pyarrow", "-q"])

    # --- Docker ---
    if not args.skip_docker:
        ensure_docker()
        compose_up = ["docker", "compose", "-f", str(compose_file), "up", "-d"]
        if args.build:
            compose_up.append("--build")
        run(compose_up)
        wait_for_trino(compose_file)
    else:
        print("\n>>> --skip-docker: compose up ignorado.")

    # --- DDL ---
    if not args.skip_ddl:
        print(f"\n{'=' * 58}")
        print("FASE 0 — DDL (schemas e tabelas Iceberg)")
        print(f"{'=' * 58}")
        apply_ddl(compose_file, bronze_sql, "Bronze")
        apply_ddl(compose_file, silver_sql, "Silver")
        apply_ddl(compose_file, gold_sql,   "Gold")
    else:
        print("\n>>> --skip-ddl: DDL ignorado.")

    # =========================================================================
    # FASE 1 — Bronze ingest
    # =========================================================================
    print(f"\n{'=' * 58}")
    print("FASE 1 — Bronze ingest (consumo + preços)")
    print(f"{'=' * 58}")
    run_script(venv_python, workflows_dir / "bronze.py")

    if not args.no_quality:
        run_quality(venv_python, workflows_dir, "bronze")

    # =========================================================================
    # FASE 2 — Silver transform
    # =========================================================================
    print(f"\n{'=' * 58}")
    print("FASE 2 — Silver transform (agregação + normalização UTC)")
    print(f"{'=' * 58}")
    run_script(venv_python, workflows_dir / "silver.py")

    if not args.no_quality:
        run_quality(venv_python, workflows_dir, "silver")

    # =========================================================================
    # FASE 3 — Gold build
    # =========================================================================
    print(f"\n{'=' * 58}")
    print("FASE 3 — Gold build (features + ML table)")
    print(f"{'=' * 58}")
    run_script(venv_python, workflows_dir / "gold.py")

    if not args.no_quality:
        run_quality(venv_python, workflows_dir, "gold")

    # =========================================================================
    # Validação final
    # =========================================================================
    print(f"\n{'=' * 58}")
    print("VALIDAÇÃO FINAL — contagem tabelas Gold")
    print(f"{'=' * 58}")
    validation_sql = (
        "SELECT 'dp_energy_market_hourly' AS tabela, COUNT(*) AS total_linhas, "
        "CAST(MIN(ts_utc) AS VARCHAR) AS inicio, CAST(MAX(ts_utc) AS VARCHAR) AS fim "
        "FROM iceberg.gold.dp_energy_market_hourly "
        "UNION ALL "
        "SELECT 'feat_load_forecasting_hourly', COUNT(*), "
        "CAST(MIN(ts_utc) AS VARCHAR), CAST(MAX(ts_utc) AS VARCHAR) "
        "FROM iceberg.gold.feat_load_forecasting_hourly;"
    )
    subprocess.run(
        ["docker", "compose", "-f", str(compose_file),
         "exec", "-T", "trino", "trino", "--execute", validation_sql],
        text=True,
    )

    print(f"\n{'=' * 58}")
    print("Pipeline consumo_preco concluída com sucesso!")
    print("  Bronze : bronze.consumo_raw + bronze.preco_raw")
    print("  Silver : silver.consumo_hourly + silver.preco_hourly")
    print("  Gold   : gold.dp_energy_market_hourly")
    print("           gold.feat_load_forecasting_hourly")
    print(f"{'=' * 58}\n")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        print(f"\nErro: comando falhou (exit {exc.returncode})", file=sys.stderr)
        sys.exit(exc.returncode)
