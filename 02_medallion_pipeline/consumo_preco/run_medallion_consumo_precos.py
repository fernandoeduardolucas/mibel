#!/usr/bin/env python3
"""
Orquestrador completo da pipeline Medallion — consumo_preco (DP-02).

Fontes de dados:
  - Consumo: consumo-total-nacional.csv (REN, granularidade 15 min)
  - Preços:  Day-ahead Market Prices_*.csv (OMIE, granularidade horária)

Fluxo padrão (sem parâmetros, equivalente a --full):
  1. Verifica pré-requisitos (Docker, Python)
  2. Sobe stack Docker Compose (se não estiver a correr)
  3. Espera Trino disponível
  4. Cria/instala venv local com dependências Flyte
  5. Aplica DDL via Trino CLI (cria schemas e tabelas Iceberg se não existirem)
  6. Faz upload dos CSVs raw para MinIO (warehouse/raw/)
  7. Executa ingestão Bronze COMPLETA via Flyte (lê CSVs do MinIO)
  8. Quality gate Bronze
  9. Executa transformação Silver COMPLETA via Flyte
 10. Quality gate Silver
 11. Executa transformação Gold COMPLETA via Flyte
 12. Quality gate Gold
 13. Validação final: COUNT e intervalo das tabelas Gold

Execução rápida (stack já a correr, DDL já aplicado):
    python run_medallion_consumo_precos.py --skip-docker --skip-ddl

Execução completa (inclui Docker e DDL):
    python run_medallion_consumo_precos.py

Execução de um mês específico:
    python run_medallion_consumo_precos.py --year 2023 --month 1

Flags úteis:
    --skip-docker    não faz compose up (stack já a correr)
    --skip-ddl       não reaplicar o DDL (tabelas já criadas)
    --skip-upload    não re-faz upload dos CSVs para MinIO
    --build          faz --build no compose up
    --no-quality     salta os quality gates (útil em dev)
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
# MinIO upload helper — envia CSVs raw para o bucket warehouse/raw/
# ---------------------------------------------------------------------------

def upload_raw_csvs_to_minio(raw_dir: Path) -> None:
    """
    Faz upload dos dois CSVs raw para MinIO (warehouse/raw/).
    Necessário antes de correr o workflow Flyte Bronze que os lê do MinIO.
    """
    print("\n>>> A verificar/instalar boto3 para upload MinIO...")
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "boto3", "-q"],
        check=True,
    )

    import boto3  # noqa: PLC0415 — importação intencional pós-install

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    )

    bucket = "warehouse"

    # Garante que o bucket existe
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)
        print(f"    Bucket '{bucket}' criado.")

    uploads = [
        (raw_dir / "consumo-total-nacional.csv",
         "raw/consumo-total-nacional.csv"),
        (raw_dir / "Day-ahead Market Prices_20230101_20260311.csv",
         "raw/Day-ahead Market Prices_20230101_20260311.csv"),
    ]

    for local_path, s3_key in uploads:
        if not local_path.exists():
            raise SystemExit(f"Erro: ficheiro raw não encontrado: {local_path}")
        print(f"    Upload: {local_path.name} -> s3://{bucket}/{s3_key}")
        s3.upload_file(str(local_path), bucket, s3_key)

    print(">>> Upload de CSVs raw para MinIO concluído.")


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
        description="Orquestrador Medallion consumo_preco (DP-02: Flyte + Iceberg)"
    )
    parser.add_argument("--full", action="store_true",
                        help="Carrega a totalidade dos dados raw; também é o modo padrão")
    parser.add_argument("--year",  type=int, help="Ano a processar (ex: 2023)")
    parser.add_argument("--month", type=int, help="Mês a processar (1-12)")
    parser.add_argument(
        "--date", type=str,
        help="Data de ingestão Bronze YYYY-MM-DD (usado com --year/--month)"
    )
    parser.add_argument("--build",        action="store_true", help="faz --build no compose up")
    parser.add_argument("--skip-docker",  action="store_true", help="salta o compose up")
    parser.add_argument("--skip-ddl",     action="store_true", help="salta a aplicação de DDL")
    parser.add_argument("--skip-upload",  action="store_true", help="salta upload dos CSVs para MinIO")
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
    raw_dir       = pipeline_root / "01_bronze" / "data" / "raw"

    bronze_sql = pipeline_root / "01_bronze" / "bronze_consumo_precos_trino.sql"
    silver_sql = pipeline_root / "02_silver" / "sql" / "silver_consumo_precos_trino.sql"
    gold_sql   = pipeline_root / "03_gold"   / "sql" / "gold_consumo_precos_trino.sql"

    # --- pré-requisitos ---
    must_exist(compose_file,  "docker-compose.yml")
    must_exist(workflows_dir, "pasta workflows/")
    must_exist(raw_dir,       "pasta com CSVs raw (01_bronze/data/raw/)")
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

    # Instala dependências no venv
    run([str(venv_python), "-m", "pip", "install", "--upgrade", "pip", "-q"])
    requirements = workflows_dir / "requirements.txt"
    if requirements.exists():
        run([str(venv_python), "-m", "pip", "install", "-r", str(requirements), "-q"])
    else:
        run([str(venv_python), "-m", "pip", "install",
             "flytekit", "trino", "boto3", "pandas", "-q"])

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

    # --- Upload CSVs raw para MinIO ---
    if not args.skip_upload:
        print("\n" + "=" * 60)
        print("FASE 0.5 — Upload CSVs raw para MinIO (warehouse/raw/)")
        print("=" * 60)
        upload_raw_csvs_to_minio(raw_dir)
    else:
        print("\n>>> --skip-upload: upload de CSVs ignorado.")

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
        print("VALIDAÇÃO FINAL — contagem e intervalo das tabelas Gold")
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
        print("Pipeline Medallion consumo_preco (DP-02) concluída com sucesso!")
        print(f"  Período  : todo o intervalo disponível nos ficheiros raw")
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

    # --- Bronze ingest (completo — os CSVs contêm todo o histórico) ---
    print("\n" + "=" * 60)
    print(f"FASE 1 — Bronze ingest COMPLETO (process_date lógico={process_date})")
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

    # --- Silver transform para o mês ---
    print("\n" + "=" * 60)
    print(f"FASE 2 — Silver transform  (process_date={process_date})")
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

    # --- Gold transform (sempre full — window functions precisam de todo o histórico) ---
    print("\n" + "=" * 60)
    print("FASE 3 — Gold transform COMPLETO (window functions requerem histórico completo)")
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
    print(f"VALIDAÇÃO FINAL — contagem Gold para {year}-{month:02d}")
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
    print("Pipeline Medallion consumo_preco (DP-02) concluída com sucesso!")
    print(f"  Período : {year}-{month:02d}")
    print(f"  Bronze  : bronze.consumo_raw + bronze.preco_raw (histórico completo)")
    print(f"  Silver  : silver.consumo_hourly + silver.preco_hourly")
    print(f"  Gold    : gold.dp_energy_market_hourly + gold.feat_load_forecasting_hourly")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        print(f"\nErro: comando falhou (exit code {exc.returncode})", file=sys.stderr)
        sys.exit(exc.returncode)
