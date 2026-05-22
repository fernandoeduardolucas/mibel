#!/usr/bin/env python3
"""
Orquestrador — Streaming_Data pipeline (DP-02 API).

Obtém dados de consumo e preços via Energy-Charts API (Fraunhofer ISE)
e executa a pipeline Medallion completa:
  Bronze (API fetch) → Quality → Silver → Quality → Gold → Quality

Fontes:
  - Consumo horário: https://api.energy-charts.info/total_power?country=pt (ENTSO-E)
  - Preços horários: https://api.energy-charts.info/price?bzn=PT (OMIE/MIBEL)

Sem autenticação. Tabelas Iceberg com sufixo _api coexistem com o pipeline estático.

Exemplos:
    python run_streaming_pipeline.py --days 7 --skip-docker
    python run_streaming_pipeline.py --start 2024-01-01 --end 2024-12-31 --skip-docker
    python run_streaming_pipeline.py --today --skip-docker --no-quality
    python run_streaming_pipeline.py --skip-docker --skip-ddl
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
from datetime import date, timedelta
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers de subprocess
# ---------------------------------------------------------------------------

def run(cmd: list[str], *, cwd: Path | None = None, env: dict | None = None) -> None:
    print(f"\n>>> {' '.join(str(c) for c in cmd)}")
    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        text=True,
    )
    if result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, cmd)


def must_exist(path: Path, description: str) -> None:
    if not path.exists():
        raise SystemExit(f"Erro: {description} não encontrado em: {path}")


# ---------------------------------------------------------------------------
# Docker helpers
# ---------------------------------------------------------------------------

def docker_engine_running() -> bool:
    return subprocess.run(["docker", "info"], text=True, capture_output=True).returncode == 0


def try_start_docker_engine() -> bool:
    system = platform.system().lower()
    start_commands: list[list[str]] = []
    if system == "linux":
        start_commands = [["systemctl", "start", "docker"]]
    elif system == "darwin":
        start_commands = [["open", "-a", "Docker"]]
    elif system == "windows":
        start_commands = [[
            "powershell", "-NoProfile", "-Command",
            "Start-Process 'C:\\Program Files\\Docker\\Docker\\Docker Desktop.exe'",
        ]]
    for cmd in start_commands:
        if shutil.which(cmd[0]) is None:
            continue
        subprocess.run(cmd, text=True, capture_output=True)
        for _ in range(20):
            if docker_engine_running():
                return True
            time.sleep(1)
    return docker_engine_running()


def ensure_docker_engine_running() -> None:
    if docker_engine_running():
        return
    if try_start_docker_engine():
        return
    raise SystemExit("Erro: Docker Engine não está a correr. Arranca o Docker e repete.")


def wait_for_trino(compose_file: Path, attempts: int = 30, sleep_seconds: int = 2) -> None:
    cmd = [
        "docker", "compose", "-f", str(compose_file),
        "exec", "-T", "trino", "trino", "--execute", "SELECT 1;",
    ]
    for attempt in range(1, attempts + 1):
        print(f"\n>>> Trino disponível? ({attempt}/{attempts})")
        result = subprocess.run(cmd, text=True, capture_output=True)
        if result.returncode == 0:
            print(">>> Trino OK.")
            return
        time.sleep(sleep_seconds)
    raise SystemExit("Erro: Trino não ficou disponível dentro do tempo esperado.")


# ---------------------------------------------------------------------------
# Venv helper
# ---------------------------------------------------------------------------

def create_local_venv(repo_root: Path, base_python: str) -> Path:
    venv_dir = repo_root / ".venv_streaming_dp02"
    venv_python = (
        venv_dir / "Scripts" / "python.exe"
        if os.name == "nt"
        else venv_dir / "bin" / "python"
    )
    if not venv_python.exists():
        print(f"\n>>> A criar virtualenv em: {venv_dir}")
        run([base_python, "-m", "venv", str(venv_dir)])
    return venv_python


# ---------------------------------------------------------------------------
# DDL helper
# ---------------------------------------------------------------------------

def apply_ddl(compose_file: Path, sql_file: Path, stage_name: str) -> None:
    print(f"\n>>> DDL {stage_name}: {sql_file.name}")
    sql_text  = sql_file.read_text(encoding="utf-8")
    sql_clean = re.sub(r"--[^\n]*", "", sql_text)
    statements = [s.strip() for s in sql_clean.split(";") if s.strip()]
    for stmt in statements:
        subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "exec", "-T", "trino", "trino"],
            input=(stmt + ";").encode("utf-8"),
            capture_output=True,
        )
    print(f"    DDL {stage_name} aplicado.")


# ---------------------------------------------------------------------------
# pyflyte runner
# ---------------------------------------------------------------------------

def pyflyte_run(
    venv_python: Path,
    workflows_dir: Path,
    workflow_file: str,
    workflow_name: str,
    params: dict[str, str],
) -> None:
    cmd = [
        str(venv_python), "-m", "flytekit.clis.sdk_in_container.pyflyte",
        "run",
        str(workflows_dir / workflow_file),
        workflow_name,
    ]
    for key, value in params.items():
        cmd += [f"--{key}", value]
    run(cmd, cwd=workflows_dir)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Orquestrador Streaming_Data pipeline (DP-02 API — Energy-Charts)"
    )

    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--days",  type=int, metavar="N",
                     help="Últimos N dias (default: 7)")
    grp.add_argument("--today", action="store_true",
                     help="Apenas hoje")
    grp.add_argument("--full",  action="store_true",
                     help="Período máximo disponível na API (~2 anos)")

    parser.add_argument("--start", type=str, metavar="YYYY-MM-DD",
                        help="Data início (usar com --end)")
    parser.add_argument("--end",   type=str, metavar="YYYY-MM-DD",
                        help="Data fim (usar com --start)")

    parser.add_argument("--build",       action="store_true", help="faz --build no compose up")
    parser.add_argument("--skip-docker", action="store_true", help="salta o compose up")
    parser.add_argument("--skip-ddl",    action="store_true", help="salta a aplicação de DDL")
    parser.add_argument("--no-quality",  action="store_true", help="salta os quality gates")

    args = parser.parse_args()

    today = date.today()

    if args.start and args.end:
        start_date = date.fromisoformat(args.start)
        end_date   = date.fromisoformat(args.end)
    elif args.today:
        start_date = end_date = today
    elif args.full:
        start_date = date(2015, 1, 1)
        end_date   = today
    else:
        days = args.days or 7
        end_date   = today
        start_date = today - timedelta(days=days - 1)

    streaming_root = Path(__file__).resolve().parent
    repo_root      = streaming_root.parent.parent.parent

    compose_file  = repo_root / "01_docker_stack" / "docker-compose.yml"
    workflows_dir = streaming_root / "workflows"

    bronze_sql = streaming_root / "01_bronze" / "bronze_streaming_ddl.sql"
    silver_sql = streaming_root / "02_silver" / "sql" / "silver_streaming_ddl.sql"
    gold_sql   = streaming_root / "03_gold"   / "sql" / "gold_streaming_ddl.sql"

    must_exist(compose_file,  "docker-compose.yml")
    must_exist(workflows_dir, "pasta workflows/")
    if not args.skip_ddl:
        must_exist(bronze_sql, "DDL Bronze SQL")
        must_exist(silver_sql, "DDL Silver SQL")
        must_exist(gold_sql,   "DDL Gold SQL")

    if shutil.which("docker") is None:
        raise SystemExit("Erro: comando 'docker' não encontrado no PATH.")

    python_cmd = sys.executable
    if not python_cmd or "WindowsApps" in python_cmd or not Path(python_cmd).exists():
        raise SystemExit("Python inválido. Usa o executável real do Python.")

    # --- venv ---
    venv_python = create_local_venv(repo_root, python_cmd)
    run([str(venv_python), "-m", "pip", "install", "--upgrade", "pip", "-q"])
    requirements = workflows_dir / "requirements.txt"
    if requirements.exists():
        run([str(venv_python), "-m", "pip", "install", "-r", str(requirements), "-q"])

    # --- Docker ---
    if not args.skip_docker:
        ensure_docker_engine_running()
        compose_up = ["docker", "compose", "-f", str(compose_file), "up", "-d"]
        if args.build:
            compose_up.append("--build")
        run(compose_up)
        wait_for_trino(compose_file)
    else:
        print("\n>>> --skip-docker: compose up ignorado.")

    # --- DDL ---
    if not args.skip_ddl:
        print("\n" + "=" * 60)
        print("FASE 0 — DDL (tabelas Iceberg _api)")
        print("=" * 60)
        apply_ddl(compose_file, bronze_sql, "Bronze API")
        apply_ddl(compose_file, silver_sql, "Silver API")
        apply_ddl(compose_file, gold_sql,   "Gold API")
    else:
        print("\n>>> --skip-ddl: DDL ignorado.")

    start_str = start_date.isoformat()
    end_str   = end_date.isoformat()

    print(f"\n{'=' * 60}")
    print(f"Pipeline Streaming_Data (DP-02 API) — {start_str} → {end_str}")
    print(f"{'=' * 60}")

    # --- FASE 1: Bronze fetch (consumo + preço em paralelo) ---
    print("\n" + "=" * 60)
    print(f"FASE 1 — Bronze fetch API ({start_str} → {end_str})")
    print("=" * 60)
    pyflyte_run(
        venv_python, workflows_dir,
        "flyte_fetch_bronze_api.py", "fetch_bronze_api",
        {"start_date": start_str, "end_date": end_str},
    )

    # --- Quality gate Bronze ---
    if not args.no_quality:
        print("\n" + "=" * 60)
        print("QUALITY GATE — Bronze API")
        print("=" * 60)
        pyflyte_run(
            venv_python, workflows_dir,
            "flyte_quality_checks.py", "quality_gate_bronze_api",
            {},
        )

    # --- FASE 2: Silver transform (completo — garante consistência com histórico) ---
    print("\n" + "=" * 60)
    print("FASE 2 — Silver transform COMPLETO")
    print("=" * 60)
    pyflyte_run(
        venv_python, workflows_dir,
        "flyte_bronze_to_silver.py", "bronze_to_silver_api_full",
        {},
    )

    # --- Quality gate Silver ---
    if not args.no_quality:
        print("\n" + "=" * 60)
        print("QUALITY GATE — Silver API")
        print("=" * 60)
        pyflyte_run(
            venv_python, workflows_dir,
            "flyte_quality_checks.py", "quality_gate_silver_api",
            {},
        )

    # --- FASE 3: Gold transform (window functions requerem histórico completo) ---
    print("\n" + "=" * 60)
    print("FASE 3 — Gold transform COMPLETO")
    print("=" * 60)
    pyflyte_run(
        venv_python, workflows_dir,
        "flyte_silver_to_gold.py", "silver_to_gold_api_full",
        {},
    )

    # --- Quality gate Gold ---
    if not args.no_quality:
        print("\n" + "=" * 60)
        print("QUALITY GATE — Gold API")
        print("=" * 60)
        pyflyte_run(
            venv_python, workflows_dir,
            "flyte_quality_checks.py", "quality_gate_gold_api",
            {},
        )

    # --- Validação final ---
    print("\n" + "=" * 60)
    print("VALIDAÇÃO FINAL — tabelas Gold API")
    print("=" * 60)
    validation_sql = (
        "SELECT 'dp_energy_market_api_hourly' AS tabela, COUNT(*) AS total_linhas, "
        "CAST(MIN(ts_utc) AS VARCHAR) AS inicio, CAST(MAX(ts_utc) AS VARCHAR) AS fim "
        "FROM iceberg.gold.dp_energy_market_api_hourly "
        "UNION ALL "
        "SELECT 'feat_load_forecasting_api_hourly', COUNT(*), "
        "CAST(MIN(ts_utc) AS VARCHAR), CAST(MAX(ts_utc) AS VARCHAR) "
        "FROM iceberg.gold.feat_load_forecasting_api_hourly;"
    )
    subprocess.run(
        [
            "docker", "compose", "-f", str(compose_file),
            "exec", "-T", "trino", "trino",
            "--execute", validation_sql,
        ],
        text=True,
    )

    print(f"\n{'=' * 60}")
    print("Streaming_Data pipeline (DP-02 API) concluída com sucesso!")
    print(f"  Período  : {start_str} → {end_str}")
    print(f"  Bronze   : bronze.consumo_api_raw + bronze.preco_api_raw")
    print(f"  Silver   : silver.consumo_api_hourly + silver.preco_api_hourly")
    print(f"  Gold     : gold.dp_energy_market_api_hourly + gold.feat_load_forecasting_api_hourly")
    print(f"  Fontes   : Energy-Charts API (Fraunhofer ISE) — sem autenticação")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        print(f"\nErro: comando falhou (exit code {exc.returncode})", file=sys.stderr)
        sys.exit(exc.returncode)
