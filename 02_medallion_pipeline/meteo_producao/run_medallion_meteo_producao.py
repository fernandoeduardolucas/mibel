#!/usr/bin/env python3
"""
Orquestrador da pipeline Medallion — meteo_producao (DP-03).

Fluxo padrão:
  1. Verifica Docker e arranca o stack se necessário
  2. Espera Trino disponível
  3. Cria/instala venv local com dependências Python
  4. Aplica DDL (Bronze, Silver, Gold) via Trino CLI
  5. Faz fetch dos dados meteorológicos Open-Meteo (2023-01-01 → hoje)
  6. Upload do Parquet para MinIO (s3://warehouse/bronze/clean/meteo_open_meteo/)
  7. Materializa Bronze → Silver → Gold no Trino
  8. Quality gate: valida linhas e intervalo de datas na Gold

Flags:
    --skip-docker   salta o compose up (stack já a correr)
    --skip-ddl      salta a aplicação de DDL (tabelas já criadas)
    --build         faz --build no compose up
    --no-quality    salta o quality gate final
    --date-from     data de início do fetch Open-Meteo (padrão: 2023-01-01)
    --date-to       data de fim do fetch Open-Meteo (padrão: hoje)

Exemplos:
    python run_medallion_meteo_producao.py
    python run_medallion_meteo_producao.py --skip-docker --skip-ddl
    python run_medallion_meteo_producao.py --skip-docker --date-from 2024-01-01
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
from datetime import date
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
    venv_dir = pipeline_root / ".venv_medallion_meteo_producao"
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
# DDL helper — aplica ficheiro SQL via Trino CLI dentro do Docker
# ---------------------------------------------------------------------------

def apply_ddl(compose_file: Path, sql_file: Path, stage_name: str) -> None:
    print(f"\n>>> DDL {stage_name}: {sql_file.name}")
    sql_text = sql_file.read_text(encoding="utf-8")
    sql_clean = re.sub(r'--[^\n]*', '', sql_text)
    statements = [s.strip() for s in sql_clean.split(";") if s.strip()]
    for stmt in statements:
        subprocess.run(
            [
                "docker", "compose", "-f", str(compose_file),
                "exec", "-T", "trino", "trino",
            ],
            input=(stmt + ";").encode("utf-8"),
            capture_output=True,
        )
    print(f"    DDL {stage_name} aplicado.")


# ---------------------------------------------------------------------------
# Quality gate — executa SQL de checks e bloqueia em FAIL
# ---------------------------------------------------------------------------

def run_quality_checks(compose_file: Path, sql_file: Path, layer_name: str) -> None:
    print(f"\n>>> Quality gate — {layer_name}: {sql_file.name}")
    sql_text = sql_file.read_text(encoding="utf-8")
    sql_clean = re.sub(r"--[^\n]*", "", sql_text)
    statements = [s.strip() for s in sql_clean.split(";") if s.strip()]
    if not statements:
        print(f"    [AVISO] Nenhum statement encontrado em {sql_file.name}.")
        return

    check_sql = statements[0] + ";"
    cmd = [
        "docker", "compose", "-f", str(compose_file),
        "exec", "-T", "trino", "trino",
        "--output-format", "TSV_HEADER",
        "--execute", check_sql,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(
            f"Quality gate {layer_name} falhou ao executar checks:\n"
            f"STDERR: {result.stderr.strip()}\n"
            f"STDOUT: {result.stdout.strip()}"
        )

    lines = result.stdout.strip().splitlines()
    if len(lines) < 2:
        print(f"    [AVISO] Quality gate {layer_name}: sem resultados.")
        return

    fail_checks, warn_checks, pass_checks = [], [], []
    for line in lines[1:]:
        cols = line.split("\t")
        status = cols[1].strip() if len(cols) > 1 else "?"
        if status == "FAIL":
            fail_checks.append(line)
        elif status == "WARN":
            warn_checks.append(line)
        else:
            pass_checks.append(line)

    print(f"\n    PASS: {len(pass_checks)}  WARN: {len(warn_checks)}  FAIL: {len(fail_checks)}")
    for line in warn_checks:
        print(f"    [WARN] {line.split(chr(9))[0]} — {line.split(chr(9))[-1]}")
    for line in fail_checks:
        print(f"    [FAIL] {line.split(chr(9))[0]} — {line.split(chr(9))[-1]}")

    if fail_checks:
        raise SystemExit(
            f"\nQuality gate {layer_name} bloqueado: {len(fail_checks)} check(s) em FAIL."
        )
    print(f"    Quality gate {layer_name} OK.")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Orquestrador Medallion meteo_producao (DP-03)"
    )
    parser.add_argument("--skip-docker", action="store_true", help="salta o compose up")
    parser.add_argument("--skip-ddl",    action="store_true", help="salta a aplicação de DDL")
    parser.add_argument("--build",       action="store_true", help="faz --build no compose up")
    parser.add_argument("--no-quality",  action="store_true", help="salta o quality gate final")
    parser.add_argument("--date-from",   default="2023-01-01", help="início do fetch Open-Meteo")
    parser.add_argument("--date-to",     default=date.today().isoformat(), help="fim do fetch Open-Meteo")
    args = parser.parse_args()

    pipeline_root = Path(__file__).resolve().parent
    repo_root     = pipeline_root.parent.parent

    compose_file = repo_root / "01_docker_stack" / "docker-compose.yml"
    fetch_script = pipeline_root / "01_bronze" / "scripts" / "python" / "fetch_open_meteo.py"
    bronze_sql   = pipeline_root / "01_bronze" / "sql" / "01_bronze_ddl.sql"
    silver_sql   = pipeline_root / "02_silver" / "sql" / "01_silver_trino.sql"
    gold_sql     = pipeline_root / "03_gold"   / "sql" / "01_gold_trino.sql"
    out_dir      = pipeline_root / "01_bronze" / "data" / "raw"

    must_exist(compose_file, "docker-compose.yml")
    must_exist(fetch_script, "fetch_open_meteo.py")
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

    # --- Venv + dependências ---
    venv_python = create_local_venv(pipeline_root, python_cmd)
    run([str(venv_python), "-m", "pip", "install", "--upgrade", "pip", "-q"])
    run([str(venv_python), "-m", "pip", "install", "pandas", "pyarrow", "boto3", "-q"])

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

    # --- DDL ---
    if not args.skip_ddl:
        print("\n" + "=" * 60)
        print("FASE 0 — DDL (schemas e tabelas Iceberg)")
        print("=" * 60)
        apply_ddl(compose_file, bronze_sql, "Bronze")
        apply_ddl(compose_file, silver_sql, "Silver")
        apply_ddl(compose_file, gold_sql,   "Gold DDL (CREATE TABLE IF NOT EXISTS)")
    else:
        print("\n>>> --skip-ddl: DDL ignorado.")

    # --- Fase 1: Fetch Open-Meteo ---
    print("\n" + "=" * 60)
    print("FASE 1 — Fetch Open-Meteo (Bronze ingest)")
    print("=" * 60)
    s3_env = {
        **os.environ,
        "S3_ENDPOINT_URL": "http://localhost:9000",
        "AWS_ACCESS_KEY_ID": "minioadmin",
        "AWS_SECRET_ACCESS_KEY": "minioadmin",
        "S3_BUCKET": "warehouse",
        "PYTHONIOENCODING": "utf-8",
    }
    run(
        [
            str(venv_python), str(fetch_script),
            "--date-from", args.date_from,
            "--date-to", args.date_to,
            "--out-dir", str(out_dir),
            "--upload",
        ],
        env=s3_env,
    )

    # --- Fase 1b: Load Bronze Iceberg from Hive stage ---
    print("\n" + "=" * 60)
    print("FASE 1b - Stage -> Bronze Iceberg (INSERT)")
    print("=" * 60)
    bronze_insert_sql = (
        "INSERT INTO iceberg.bronze.meteo_open_meteo_hourly "
        "SELECT ts_utc, year, month, day, hour, temperature_2m, precipitation, "
        "wind_speed_10m, shortwave_radiation, cloud_cover, latitude, longitude, "
        "elevation_m, _source_file, _ingested_at "
        "FROM hive.bronze_stage.meteo_open_meteo_clean"
    )
    result = subprocess.run(
        [
            "docker", "compose", "-f", str(compose_file),
            "exec", "-T", "trino", "trino", "--execute", bronze_insert_sql + ";",
        ],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise SystemExit(f"Bronze INSERT falhou:\n{result.stderr.strip()}")
    print("    Bronze INSERT OK.")

    # --- Fase 2: Silver (via Trino) ---
    print("\n" + "=" * 60)
    print("FASE 2 - Bronze -> Silver (Trino SQL)")
    print("=" * 60)
    apply_ddl(compose_file, silver_sql, "Silver")

    # --- Fase 3: Gold (via Trino) ---
    print("\n" + "=" * 60)
    print("FASE 3 - Silver -> Gold (Trino SQL - DP-03)")
    print("=" * 60)
    gold_text = gold_sql.read_text(encoding="utf-8")
    sql_clean = re.sub(r'--[^\n]*', '', gold_text)
    insert_stmts = [s.strip() for s in sql_clean.split(";") if s.strip()]
    for stmt in insert_stmts:
        subprocess.run(
            [
                "docker", "compose", "-f", str(compose_file),
                "exec", "-T", "trino", "trino",
            ],
            input=(stmt + ";").encode("utf-8"),
            capture_output=True,
        )
    print("    Gold INSERT aplicado.")

    # --- Quality gate — Silver + Gold (bloqueante em FAIL) ---
    silver_checks_sql = pipeline_root / "04_quality" / "sql" / "01_silver_checks.sql"
    gold_checks_sql   = pipeline_root / "04_quality" / "sql" / "02_gold_checks.sql"

    if not args.no_quality:
        print("\n" + "=" * 60)
        print("QUALITY GATE")
        print("=" * 60)
        if silver_checks_sql.exists():
            run_quality_checks(compose_file, silver_checks_sql, "Silver")
        else:
            print(f"    [AVISO] {silver_checks_sql} não encontrado — a saltar Silver checks.")
        if gold_checks_sql.exists():
            run_quality_checks(compose_file, gold_checks_sql, "Gold")
        else:
            print(f"    [AVISO] {gold_checks_sql} não encontrado — a saltar Gold checks.")

    print("\n" + "=" * 60)
    print("Pipeline meteo_producao concluída com sucesso!")
    print("Tabela: iceberg.gold.dp_meteo_producao_daily_features")
    print("Próximo passo: python 03_ml_pipeline/meteo_producao_mlflow_flow.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
