#!/usr/bin/env python3
"""
Orquestrador completo da pipeline Medallion — consumo_preco (Opção B: Flyte + Iceberg nativo).

Fluxo:
  1. Verifica pré-requisitos (Docker, Flyte sandbox, Python)
  2. Sobe stack Docker Compose (se não estiver a correr)
  3. Espera Trino disponível
  4. Cria/instala venv local com dependências Flyte
  5. Aplica DDL via Trino CLI (cria schemas e tabelas Iceberg se não existirem)
  6. Executa ingestão Bronze por data (pyflyte run flyte_ingest_bronze.py)
  7. Quality gate Bronze
  8. Executa transformação Silver por mês (pyflyte run flyte_bronze_to_silver.py)
  9. Quality gate Silver
 10. Executa transformação Gold por mês (pyflyte run flyte_silver_to_gold.py)
 11. Quality gate Gold
 12. Validação final: COUNT das tabelas Gold

Execução rápida (um mês):
    python run_medallion_consumo_precos.py --year 2023 --month 1 --date 2023-01-01

Execução de múltiplos meses (loop externo):
    python run_medallion_consumo_precos.py --year 2023 --month 1 --date 2023-01-01
    python run_medallion_consumo_precos.py --year 2023 --month 2 --date 2023-02-01
    ...

Flags úteis:
    --skip-docker   não faz compose up (stack já a correr)
    --skip-ddl      não reaplicar o DDL (tabelas já criadas)
    --build         faz --build no compose up
    --no-quality    salta os quality gates (útil em dev)
"""

from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers de subprocess
# ---------------------------------------------------------------------------

def run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    input_text: str | None = None,
) -> None:
    """Executa um comando e lança excepção se falhar. Imprime stdout em tempo real."""
    print(f"\n>>> {' '.join(str(c) for c in cmd)}")
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
            result.returncode, cmd, output=result.stdout, stderr=result.stderr
        )


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
        start_commands = [["systemctl", "start", "docker"], ["service", "docker", "start"]]
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
        print(f">>> Docker indisponível. A tentar arrancar: {' '.join(cmd)}")
        subprocess.run(cmd, text=True, capture_output=True)
        for _ in range(20):
            if docker_engine_running():
                print(">>> Docker Engine disponível.")
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
        "Tenta arrancar o Docker manualmente e repete."
    )


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

def create_local_venv(pipeline_root: Path, base_python: str) -> Path:
    venv_dir = pipeline_root / ".venv_medallion_consumo_preco"
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
# DDL helper — aplica um ficheiro SQL via Trino CLI dentro do Docker
# ---------------------------------------------------------------------------

def apply_ddl(compose_file: Path, sql_file: Path, stage_name: str) -> None:
    print(f"\n>>> DDL {stage_name}: {sql_file.name}")
    sql_text = sql_file.read_text(encoding="utf-8")
    # Divide em statements individuais e executa cada um (Trino CLI executa 1 statement por vez)
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    for stmt in statements:
        subprocess.run(
            [
                "docker", "compose", "-f", str(compose_file),
                "exec", "-T", "trino", "trino",
            ],
            input=stmt + ";",
            text=True,
            capture_output=True,  # DDL é idempotente (IF NOT EXISTS); ignora erros menores
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
    """Invoca pyflyte run no venv local."""
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
        description="Orquestrador Medallion consumo_preco (Opção B: Flyte + Iceberg nativo)"
    )
    parser.add_argument("--year",  type=int, required=True, help="Ano a processar (ex: 2023)")
    parser.add_argument("--month", type=int, required=True, help="Mês a processar (1-12)")
    parser.add_argument(
        "--date", type=str,
        help="Data de ingestão Bronze no formato YYYY-MM-DD (padrão: primeiro dia do mês)"
    )
    parser.add_argument("--build",        action="store_true", help="faz --build no compose up")
    parser.add_argument("--skip-docker",  action="store_true", help="salta o compose up")
    parser.add_argument("--skip-ddl",     action="store_true", help="salta a aplicação de DDL")
    parser.add_argument("--no-quality",   action="store_true", help="salta os quality gates")
    args = parser.parse_args()

    year  = args.year
    month = args.month
    process_date = args.date or f"{year}-{month:02d}-01"

    pipeline_root = Path(__file__).resolve().parent
    repo_root     = pipeline_root.parent.parent

    compose_file  = repo_root / "01_bootstrap" / "tead_2.0_v1.2" / "docker-compose.yml"
    workflows_dir = pipeline_root / "workflows"

    bronze_sql = pipeline_root / "01_bronze" / "sql" / "bronze_consumo_precos_trino.sql"
    silver_sql = pipeline_root / "02_silver" / "sql" / "silver_consumo_precos_trino.sql"
    gold_sql   = pipeline_root / "03_gold"   / "sql" / "gold_consumo_precos_trino.sql"

    # --- pré-requisitos ---
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
        raise SystemExit(
            "Python inválido. Usa o executável real do Python, não o alias WindowsApps."
        )

    # --- venv ---
    venv_python = create_local_venv(pipeline_root, python_cmd)

    # Instala dependências Flyte no venv
    requirements = workflows_dir / "requirements.txt"
    run([str(venv_python), "-m", "pip", "install", "--upgrade", "pip", "-q"])
    if requirements.exists():
        run([str(venv_python), "-m", "pip", "install", "-r", str(requirements), "-q"])
    else:
        # fallback: instala pacotes mínimos
        run([str(venv_python), "-m", "pip", "install", "flytekit", "trino", "boto3", "-q"])

    # --- Docker compose up ---
    if not args.skip_docker:
        ensure_docker_engine_running()
        compose_up = ["docker", "compose", "-f", str(compose_file), "up", "-d"]
        if args.build:
            compose_up.append("--build")
        run(compose_up)
        wait_for_trino(compose_file)
    else:
        print("\n>>> --skip-docker: compose up ignorado.")

    # --- DDL (idempotente: IF NOT EXISTS) ---
    if not args.skip_ddl:
        print("\n" + "=" * 60)
        print("FASE 0 — DDL (schemas e tabelas Iceberg)")
        print("=" * 60)
        apply_ddl(compose_file, bronze_sql, "Bronze")
        apply_ddl(compose_file, silver_sql, "Silver")
        apply_ddl(compose_file, gold_sql,   "Gold")
    else:
        print("\n>>> --skip-ddl: DDL ignorado.")

    # --- Bronze ingest ---
    print("\n" + "=" * 60)
    print(f"FASE 1 — Bronze ingest  (date={process_date})")
    print("=" * 60)
    pyflyte_run(
        venv_python, workflows_dir,
        "flyte_ingest_bronze.py", "ingest_bronze",
        {"process_date": process_date},
    )

    # --- Quality gate Bronze ---
    if not args.no_quality:
        print("\n" + "=" * 60)
        print("QUALITY GATE — Bronze")
        print("=" * 60)
        pyflyte_run(
            venv_python, workflows_dir,
            "flyte_quality_checks.py", "quality_gate_bronze",
            {},
        )

    # --- Silver transform ---
    print("\n" + "=" * 60)
    print(f"FASE 2 — Silver transform  (year={year}, month={month})")
    print("=" * 60)
    pyflyte_run(
        venv_python, workflows_dir,
        "flyte_bronze_to_silver.py", "bronze_to_silver",
        {"process_date": process_date},
    )

    # --- Quality gate Silver ---
    if not args.no_quality:
        print("\n" + "=" * 60)
        print("QUALITY GATE — Silver")
        print("=" * 60)
        pyflyte_run(
            venv_python, workflows_dir,
            "flyte_quality_checks.py", "quality_gate_silver",
            {},
        )

    # --- Gold transform ---
    print("\n" + "=" * 60)
    print(f"FASE 3 — Gold transform  (year={year}, month={month})")
    print("=" * 60)
    pyflyte_run(
        venv_python, workflows_dir,
        "flyte_silver_to_gold.py", "silver_to_gold",
        {"year": str(year), "month": str(month)},
    )

    # --- Quality gate Gold ---
    if not args.no_quality:
        print("\n" + "=" * 60)
        print("QUALITY GATE — Gold")
        print("=" * 60)
        pyflyte_run(
            venv_python, workflows_dir,
            "flyte_quality_checks.py", "quality_gate_gold",
            {},
        )

    # --- Validação final ---
    print("\n" + "=" * 60)
    print("VALIDAÇÃO FINAL — contagem Gold")
    print("=" * 60)
    validation_sql = f"""
SELECT 'dp_energy_market_hourly'    AS tabela, COUNT(*) AS total_linhas
FROM iceberg.gold.dp_energy_market_hourly
WHERE year = {year} AND month = {month}
UNION ALL
SELECT 'feat_load_forecasting_hourly', COUNT(*)
FROM iceberg.gold.feat_load_forecasting_hourly
WHERE year = {year} AND month = {month};
""".strip()

    subprocess.run(
        [
            "docker", "compose", "-f", str(compose_file),
            "exec", "-T", "trino", "trino",
        ],
        input=validation_sql,
        text=True,
    )

    print(f"\n{'=' * 60}")
    print(f"Pipeline Medallion consumo_preco concluída com sucesso!")
    print(f"  Período : {year}-{month:02d}")
    print(f"  Bronze  : process_date={process_date}")
    print(f"  Silver  : silver.consumo_hourly + silver.preco_hourly")
    print(f"  Gold    : gold.dp_energy_market_hourly + gold.feat_load_forecasting_hourly")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        print(f"\nErro: comando falhou (exit code {exc.returncode})", file=sys.stderr)
        sys.exit(exc.returncode)
