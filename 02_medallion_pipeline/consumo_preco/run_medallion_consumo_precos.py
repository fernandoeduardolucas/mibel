#!/usr/bin/env python3
"""
Orquestrador completo da pipeline Medallion — consumo_preco (Opção B: Flyte + Iceberg nativo).

Fluxo padrão (sem parâmetros, equivalente a --full):
  1. Verifica pré-requisitos (Docker, Flyte sandbox, Python)
  2. Sobe stack Docker Compose (se não estiver a correr)
  3. Espera Trino disponível
  4. Cria/instala venv local com dependências Flyte
  5. Aplica DDL via Trino CLI (cria schemas e tabelas Iceberg se não existirem)
  6. Executa ingestão Bronze COMPLETA (ingest_bronze_full — lê CSVs na íntegra)
  7. Executa transformação Silver COMPLETA
  8. Executa transformação Gold COMPLETA
  9. Quality gates finais (Bronze, Silver, Gold)
 10. Validação final: COUNT e intervalo das tabelas Gold

Execução rápida (tudo de uma vez):
    python run_medallion_consumo_precos.py --skip-docker --skip-ddl

Execução completa (inclui Docker e DDL):
    python run_medallion_consumo_precos.py

Execução de um mês específico:
    python run_medallion_consumo_precos.py --year 2023 --month 1

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

def run(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    input_text: str | None = None,
) -> None:
    """Executa um comando e lança excepção se falhar."""
    print(f"\n>>> {' '.join(str(c) for c in cmd)}")
    if input_text is None:
        result = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            env=env,
            text=True,
        )
    else:
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
        if result.stderr:
            print(result.stderr, file=sys.stderr)

    if result.returncode != 0:
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
    # Remove comentários de linha para evitar caracteres Unicode problemáticos no Windows
    sql_clean = re.sub(r'--[^\n]*', '', sql_text)
    # Divide em statements individuais e executa cada um (Trino CLI executa 1 statement por vez)
    statements = [s.strip() for s in sql_clean.split(";") if s.strip()]
    for stmt in statements:
        subprocess.run(
            [
                "docker", "compose", "-f", str(compose_file),
                "exec", "-T", "trino", "trino",
            ],
            input=(stmt + ";").encode("utf-8"),
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
    # flytekit 1.x instala 'pyflyte' como console script, não como módulo invocável
    pyflyte_bin = venv_python.parent / ("pyflyte.exe" if os.name == "nt" else "pyflyte")
    cmd = [
        str(pyflyte_bin),
        "run",
        str(workflows_dir / workflow_file),
        workflow_name,
    ]
    for key, value in params.items():
        cmd += [f"--{key}", value]
    run(cmd, cwd=workflows_dir)


# ---------------------------------------------------------------------------
# Trino helper — deteta meses presentes na Bronze
# ---------------------------------------------------------------------------

def detect_months_from_bronze(compose_file: Path) -> list[tuple[int, int]]:
    """
    Consulta o Trino para obter a lista de (year, month) distintos
    presentes em iceberg.bronze.consumo_raw.
    Retorna lista ordenada de tuplos (year, month).
    """
    sql = (
        "SELECT DISTINCT YEAR(process_date) AS y, MONTH(process_date) AS m "
        "FROM iceberg.bronze.consumo_raw "
        "ORDER BY y, m;"
    )
    result = subprocess.run(
        [
            "docker", "compose", "-f", str(compose_file),
            "exec", "-T", "trino", "trino",
            "--output-format", "TSV_HEADER",
            "--execute", sql,
        ],
        capture_output=True,
        text=True,
    )
    months = []
    if result.returncode != 0:
        print(f"[AVISO] Não foi possível detetar meses no Bronze: {result.stderr.strip()}")
        return months

    lines = result.stdout.strip().splitlines()
    # Primeira linha é o cabeçalho (y\tm)
    for line in lines[1:]:
        parts = line.strip().split("\t")
        if len(parts) == 2:
            try:
                months.append((int(parts[0]), int(parts[1])))
            except ValueError:
                continue
    return months


def detect_dates_from_bronze(compose_file: Path) -> list[str]:
    """
    Consulta o Trino para obter a lista de process_date presentes em ambas as
    tabelas Bronze. Retorna datas YYYY-MM-DD ordenadas para backfill Silver.
    """
    sql = (
        "SELECT CAST(c.process_date AS VARCHAR) AS process_date "
        "FROM (SELECT DISTINCT process_date FROM iceberg.bronze.consumo_raw) c "
        "INNER JOIN (SELECT DISTINCT process_date FROM iceberg.bronze.preco_raw) p "
        "ON c.process_date = p.process_date "
        "ORDER BY c.process_date;"
    )
    result = subprocess.run(
        [
            "docker", "compose", "-f", str(compose_file),
            "exec", "-T", "trino", "trino",
            "--output-format", "TSV_HEADER",
            "--execute", sql,
        ],
        capture_output=True,
        text=True,
    )
    dates: list[str] = []
    if result.returncode != 0:
        print(f"[AVISO] Não foi possível detetar datas no Bronze: {result.stderr.strip()}")
        return dates

    lines = result.stdout.strip().splitlines()
    for line in lines[1:]:
        value = line.strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
            dates.append(value)
    return dates


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Orquestrador Medallion consumo_preco (Opção B: Flyte + Iceberg nativo)"
    )
    parser.add_argument("--full", action="store_true",
                        help="Carrega a totalidade dos dados raw; também é o modo padrão sem --year/--month")
    parser.add_argument("--year",  type=int, help="Ano a processar (ex: 2023) — ignorado com --full")
    parser.add_argument("--month", type=int, help="Mês a processar (1-12) — ignorado com --full")
    parser.add_argument(
        "--date", type=str,
        help="Data de ingestão Bronze YYYY-MM-DD (ignorado com --full)"
    )
    parser.add_argument("--build",        action="store_true", help="faz --build no compose up")
    parser.add_argument("--skip-docker",  action="store_true", help="salta o compose up")
    parser.add_argument("--skip-ddl",     action="store_true", help="salta a aplicação de DDL")
    parser.add_argument("--no-quality",   action="store_true", help="salta os quality gates")
    args = parser.parse_args()

    if not args.full:
        if args.year is None and args.month is None:
            args.full = True
            print(">>> Sem --year/--month: a correr em modo FULL (todo o período raw).")
        elif args.year is None or args.month is None:
            raise SystemExit(
                "Erro: para correr um mês específico indica --year e --month. "
                "Sem parâmetros, o runner carrega tudo automaticamente."
            )

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
        run([str(venv_python), "-m", "pip", "install", "flytekit", "trino", "boto3", "pandas", "-q"])

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

    # =========================================================================
    # MODO --full : lê os CSVs na íntegra, sem especificar datas
    # =========================================================================
    if args.full:
        # --- Fase 1: Bronze ingest COMPLETO ---
        print("\n" + "=" * 60)
        print("FASE 1 — Bronze ingest COMPLETO (todos os dados raw)")
        print("=" * 60)
        pyflyte_run(
            venv_python, workflows_dir,
            "flyte_ingest_bronze.py", "ingest_bronze_full",
            {},
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

        # --- Fase 2: Silver transform COMPLETO ---
        print("\n" + "=" * 60)
        print("FASE 2 — Silver transform COMPLETO (todo o período Bronze)")
        print("=" * 60)
        pyflyte_run(
            venv_python, workflows_dir,
            "flyte_bronze_to_silver.py", "bronze_to_silver_full",
            {},
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

        # --- Fase 3: Gold transform COMPLETO ---
        print("\n" + "=" * 60)
        print("FASE 3 — Gold transform COMPLETO (todo o período Silver)")
        print("=" * 60)
        pyflyte_run(
            venv_python, workflows_dir,
            "flyte_silver_to_gold.py", "silver_to_gold_full",
            {},
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
        print("VALIDAÇÃO FINAL — contagem Gold (total)")
        print("=" * 60)
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
            [
                "docker", "compose", "-f", str(compose_file),
                "exec", "-T", "trino", "trino",
                "--execute", validation_sql,
            ],
            text=True,
        )

        print(f"\n{'=' * 60}")
        print(f"Pipeline Medallion consumo_preco concluída com sucesso!")
        print(f"  Período  : todo o intervalo disponível nos 2 ficheiros raw")
        print(f"  Bronze   : bronze.consumo_raw + bronze.preco_raw")
        print(f"  Silver   : silver.consumo_hourly + silver.preco_hourly")
        print(f"  Gold     : gold.dp_energy_market_hourly + gold.feat_load_forecasting_hourly")
        print(f"{'=' * 60}\n")
        return

    # =========================================================================
    # MODO mês específico (--year / --month)
    # =========================================================================
    year  = args.year
    month = args.month
    process_date = args.date or f"{year}-{month:02d}-01"

    # --- Bronze ingest (um dia / data de referência) ---
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
    print("FASE 3 — Gold transform  (full)")
    print("=" * 60)
    pyflyte_run(
        venv_python, workflows_dir,
        "flyte_silver_to_gold.py", "silver_to_gold_full",
        {},
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
    validation_sql = (
        f"SELECT 'dp_energy_market_hourly' AS tabela, COUNT(*) AS total_linhas "
        f"FROM iceberg.gold.dp_energy_market_hourly "
        f"WHERE year = {year} AND month = {month} "
        f"UNION ALL "
        f"SELECT 'feat_load_forecasting_hourly', COUNT(*) "
        f"FROM iceberg.gold.feat_load_forecasting_hourly "
        f"WHERE year = {year} AND month = {month};"
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
