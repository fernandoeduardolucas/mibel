#!/usr/bin/env python3
"""Orquestrador da pipeline Medallion de producao_consumo.

Fluxo:
1) Sobe stack Docker Compose (saltar com --skip-docker)
2) Aguarda Trino disponível
3) Executa limpeza + upload Bronze (scripts Python)
4) Aplica DDL Bronze, Silver e Gold via Trino (saltar com --skip-ddl)
5) Quality gate e validação final

Exemplos:
    # Carga completa (cria e povoa todas as camadas)
    python run_medallion_pipeline.py

    # Stack já a correr
    python run_medallion_pipeline.py --skip-docker

    # Stack e DDL já aplicados (só Gold)
    python run_medallion_pipeline.py --skip-docker --skip-ddl

    # Backfill incremental de um intervalo específico
    python run_medallion_pipeline.py --skip-docker --skip-ddl \\
        --date-from 2023-01-01 --date-to 2023-06-30

    # Incluir build das imagens Docker
    python run_medallion_pipeline.py --build
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
    print(f"\n>>> {' '.join(str(c) for c in cmd)}")
    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env=env,
        text=True,
        input=input_text,
        capture_output=(input_text is not None),
    )
    if input_text is not None:
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode, cmd,
            output=getattr(result, "stdout", None),
            stderr=getattr(result, "stderr", None),
        )


def run_with_retry(
    cmd: list[str],
    *,
    max_attempts: int = 3,
    delay: int = 5,
    **kwargs,
) -> None:
    for attempt in range(1, max_attempts + 1):
        try:
            run(cmd, **kwargs)
            return
        except subprocess.CalledProcessError as exc:
            if attempt == max_attempts:
                raise
            print(
                f"[AVISO] Tentativa {attempt}/{max_attempts} falhou (exit {exc.returncode}). "
                f"A repetir em {delay}s...",
                file=sys.stderr,
            )
            time.sleep(delay)


def must_exist(path: Path, description: str) -> None:
    if not path.exists():
        raise SystemExit(f"Erro: {description} não encontrado: {path}")


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
        "Arranca o Docker manualmente e repete."
    )


def wait_for_trino(compose_file: Path, attempts: int = 30, sleep_seconds: int = 2) -> None:
    cmd = [
        "docker", "compose", "-f", str(compose_file),
        "exec", "-T", "trino", "trino", "--execute", "SELECT 1;",
    ]
    for attempt in range(1, attempts + 1):
        print(f"\n>>> Trino disponível? ({attempt}/{attempts})")
        if subprocess.run(cmd, text=True, capture_output=True).returncode == 0:
            print(">>> Trino OK.")
            return
        time.sleep(sleep_seconds)
    raise SystemExit("Erro: Trino não ficou disponível dentro do tempo esperado.")


# ---------------------------------------------------------------------------
# Venv helper
# ---------------------------------------------------------------------------

def create_local_venv(pipeline_root: Path, base_python: str) -> Path:
    venv_dir = pipeline_root / ".venv_medallion"
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
# DDL helper — aplica ficheiro SQL statement a statement via Trino CLI
# ---------------------------------------------------------------------------

def apply_ddl(compose_file: Path, sql_file: Path, stage_name: str) -> None:
    print(f"\n>>> DDL {stage_name}: {sql_file.name}")
    sql_text = sql_file.read_text(encoding="utf-8")
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
# Trino execute helper (com saída visível e retry)
# ---------------------------------------------------------------------------

def trino_execute(compose_file: Path, sql: str, *, max_attempts: int = 3) -> None:
    cmd = [
        "docker", "compose", "-f", str(compose_file),
        "exec", "-T", "trino", "trino",
    ]
    for attempt in range(1, max_attempts + 1):
        result = subprocess.run(
            cmd,
            input=sql.encode("utf-8"),
            capture_output=True,
        )
        if result.stdout:
            print(result.stdout.decode("utf-8", errors="replace"))
        if result.returncode == 0:
            return
        if attempt < max_attempts:
            print(
                f"[AVISO] Tentativa {attempt}/{max_attempts} falhou. A repetir em 5s...",
                file=sys.stderr,
            )
            if result.stderr:
                print(result.stderr.decode("utf-8", errors="replace"), file=sys.stderr)
            time.sleep(5)
        else:
            if result.stderr:
                print(result.stderr.decode("utf-8", errors="replace"), file=sys.stderr)
            raise subprocess.CalledProcessError(result.returncode, cmd)


# ---------------------------------------------------------------------------
# Quality gate — executa SQL de checks e bloqueia em FAIL
# ---------------------------------------------------------------------------

def run_quality_checks(compose_file: Path, sql_file: Path, layer_name: str) -> None:
    print(f"\n>>> Quality gate — {layer_name}: {sql_file.name}")
    sql_text = sql_file.read_text(encoding="utf-8")
    # Remove comentários de linha e separa em statements
    sql_clean = re.sub(r"--[^\n]*", "", sql_text)
    statements = [s.strip() for s in sql_clean.split(";") if s.strip()]
    if not statements:
        print(f"    [AVISO] Nenhum statement encontrado em {sql_file.name}.")
        return

    # Primeiro statement é o UNION ALL com todos os checks
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
            f"Quality gate {layer_name} falhou ao executar checks:\n{result.stderr.strip()}"
        )

    lines = result.stdout.strip().splitlines()
    if len(lines) < 2:
        print(f"    [AVISO] Quality gate {layer_name}: sem resultados.")
        return

    # Apresenta resultados formatados
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
# Gold incremental — DELETE + INSERT com filtro temporal
# ---------------------------------------------------------------------------

def build_gold_incremental_sql(date_from: str, date_to: str) -> str:
    return (
        f"DELETE FROM iceberg.gold.dp_energia_balance_hourly\n"
        f"WHERE CAST(timestamp_utc AS DATE) BETWEEN DATE '{date_from}' AND DATE '{date_to}';\n"
        f"\n"
        f"INSERT INTO iceberg.gold.dp_energia_balance_hourly\n"
        f"WITH consumo_hourly AS (\n"
        f"    SELECT\n"
        f"        date_trunc('hour', timestamp_utc) AS timestamp_utc,\n"
        f"        SUM(consumo_total_kwh) AS consumo_total_kwh\n"
        f"    FROM iceberg.silver.consumo_total_nacional_15min\n"
        f"    WHERE CAST(timestamp_utc AS DATE) BETWEEN DATE '{date_from}' AND DATE '{date_to}'\n"
        f"    GROUP BY 1\n"
        f"),\n"
        f"producao_hourly AS (\n"
        f"    SELECT\n"
        f"        date_trunc('hour', timestamp_utc) AS timestamp_utc,\n"
        f"        SUM(producao_total_kwh) AS producao_total_kwh,\n"
        f"        SUM(producao_dgm_kwh)   AS producao_dgm_kwh,\n"
        f"        SUM(producao_pre_kwh)   AS producao_pre_kwh\n"
        f"    FROM iceberg.silver.energia_produzida_total_nacional_15min\n"
        f"    WHERE CAST(timestamp_utc AS DATE) BETWEEN DATE '{date_from}' AND DATE '{date_to}'\n"
        f"    GROUP BY 1\n"
        f")\n"
        f"SELECT\n"
        f"    COALESCE(c.timestamp_utc, p.timestamp_utc)              AS timestamp_utc,\n"
        f"    c.consumo_total_kwh,\n"
        f"    p.producao_total_kwh,\n"
        f"    p.producao_dgm_kwh,\n"
        f"    p.producao_pre_kwh,\n"
        f"    p.producao_total_kwh - c.consumo_total_kwh               AS saldo_kwh,\n"
        f"    CASE\n"
        f"        WHEN c.consumo_total_kwh IS NULL OR c.consumo_total_kwh = 0 THEN NULL\n"
        f"        ELSE p.producao_total_kwh / c.consumo_total_kwh\n"
        f"    END                                                      AS ratio_producao_consumo,\n"
        f"    CASE\n"
        f"        WHEN c.consumo_total_kwh IS NOT NULL\n"
        f"         AND p.producao_total_kwh IS NOT NULL\n"
        f"         AND p.producao_total_kwh < c.consumo_total_kwh\n"
        f"        THEN true ELSE false\n"
        f"    END                                                      AS flag_defice,\n"
        f"    CASE\n"
        f"        WHEN c.consumo_total_kwh IS NOT NULL\n"
        f"         AND p.producao_total_kwh IS NOT NULL\n"
        f"         AND p.producao_total_kwh > c.consumo_total_kwh\n"
        f"        THEN true ELSE false\n"
        f"    END                                                      AS flag_excedente,\n"
        f"    CASE\n"
        f"        WHEN c.timestamp_utc IS NULL OR p.timestamp_utc IS NULL THEN true\n"
        f"        ELSE false\n"
        f"    END                                                      AS flag_missing_source,\n"
        f"    YEAR(COALESCE(c.timestamp_utc, p.timestamp_utc))         AS ano,\n"
        f"    MONTH(COALESCE(c.timestamp_utc, p.timestamp_utc))        AS mes\n"
        f"FROM consumo_hourly c\n"
        f"FULL OUTER JOIN producao_hourly p ON c.timestamp_utc = p.timestamp_utc\n"
        f"ORDER BY 1;"
    )


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Orquestrador Medallion producao_consumo"
    )
    parser.add_argument("--build",       action="store_true", help="faz --build no compose up")
    parser.add_argument("--skip-docker", action="store_true", help="salta o compose up")
    parser.add_argument("--skip-ddl",    action="store_true",
                        help="salta Bronze upload + Bronze/Silver DDL (tabelas já existem e estão actualizadas)")
    parser.add_argument("--no-quality",  action="store_true", help="salta o quality gate final")
    parser.add_argument(
        "--date-from", type=str, metavar="YYYY-MM-DD",
        help="Início do intervalo para backfill incremental Gold (ex: 2023-01-01)",
    )
    parser.add_argument(
        "--date-to", type=str, metavar="YYYY-MM-DD",
        help="Fim do intervalo para backfill incremental Gold (ex: 2023-06-30)",
    )
    args = parser.parse_args()

    incremental = args.date_from is not None or args.date_to is not None
    if incremental:
        if args.date_from is None or args.date_to is None:
            raise SystemExit("Erro: --date-from e --date-to têm de ser usados em conjunto.")
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", args.date_from):
            raise SystemExit("Erro: --date-from deve estar no formato YYYY-MM-DD.")
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", args.date_to):
            raise SystemExit("Erro: --date-to deve estar no formato YYYY-MM-DD.")

    pipeline_root = Path(__file__).resolve().parent
    repo_root     = pipeline_root.parent.parent

    compose_file        = repo_root      / "01_docker_stack" / "docker-compose.yml"
    bronze_dir          = pipeline_root  / "01_bronze"
    bronze_sql          = bronze_dir     / "sql" / "bronze_trino.sql"
    silver_sql          = pipeline_root  / "02_silver" / "sql" / "01_silver_trino.sql"
    gold_sql            = pipeline_root  / "03_gold"   / "sql" / "01_gold_trino.sql"
    bronze_requirements = bronze_dir     / "scripts" / "python" / "requirements_bronze.txt"

    must_exist(compose_file, "docker-compose.yml")
    must_exist(bronze_requirements, "requirements da Bronze")
    if not args.skip_ddl:
        must_exist(bronze_sql, "DDL Bronze SQL")
        must_exist(silver_sql, "DDL Silver SQL")
    if not (args.skip_ddl or incremental):
        must_exist(gold_sql, "DDL Gold SQL")

    if shutil.which("docker") is None:
        raise SystemExit("Erro: comando 'docker' não encontrado no PATH.")

    python_cmd = sys.executable
    if not python_cmd or "WindowsApps" in python_cmd or not Path(python_cmd).exists():
        raise SystemExit(
            "Python inválido. Usa o executável real do Python, não o alias WindowsApps."
        )

    venv_python = create_local_venv(pipeline_root, python_cmd)

    # -------------------------------------------------------------------------
    # Docker compose up
    # -------------------------------------------------------------------------
    if not args.skip_docker:
        ensure_docker_engine_running()
        compose_up = ["docker", "compose", "-f", str(compose_file), "up", "-d"]
        if args.build:
            compose_up.append("--build")
        run(compose_up)
        wait_for_trino(compose_file)
    else:
        print("\n>>> --skip-docker: compose up ignorado.")

    # -------------------------------------------------------------------------
    # pip install
    # -------------------------------------------------------------------------
    run([str(venv_python), "-m", "pip", "install", "--upgrade", "pip", "-q"])
    run([str(venv_python), "-m", "pip", "install", "-r", str(bronze_requirements), "-q"])

    # -------------------------------------------------------------------------
    # FASE 1 — Bronze clean + upload + DDL Bronze + DDL Silver
    # (saltar com --skip-ddl quando as tabelas já existem e estão actualizadas)
    # -------------------------------------------------------------------------
    if not args.skip_ddl:
        print("\n" + "=" * 60)
        print("FASE 1 — Bronze clean + upload")
        print("=" * 60)
        env = os.environ.copy()
        env.update({
            "S3_ENDPOINT_URL":       "http://localhost:9000",
            "AWS_ACCESS_KEY_ID":     "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "S3_BUCKET":             "warehouse",
            "S3_PREFIX":             "bronze/clean",
        })
        run_with_retry(
            [
                str(venv_python),
                "scripts/python/bronze_clean_upload.py",
                "--consumo",  "data/raw/consumo-total-nacional.csv",
                "--producao", "data/raw/energia-produzida-total-nacional.csv",
                "--out-dir",  "data/clean",
                "--upload",
            ],
            cwd=bronze_dir,
            env=env,
            max_attempts=3,
        )

        print("\n" + "=" * 60)
        print("FASE 2 — DDL Bronze (Hive + Iceberg) + INSERT Bronze")
        print("=" * 60)
        apply_ddl(compose_file, bronze_sql, "Bronze")

        print("\n" + "=" * 60)
        print("FASE 3 — DDL Silver (CTAS de Bronze)")
        print("=" * 60)
        apply_ddl(compose_file, silver_sql, "Silver")
    else:
        print("\n>>> --skip-ddl: Bronze upload + Bronze/Silver DDL ignorados.")

    # -------------------------------------------------------------------------
    # FASE 4 — Gold
    # -------------------------------------------------------------------------
    if incremental:
        print("\n" + "=" * 60)
        print(f"FASE 4 — Gold incremental: {args.date_from} → {args.date_to}")
        print("=" * 60)
        gold_inc_sql = build_gold_incremental_sql(args.date_from, args.date_to)
        stmts = [s.strip() for s in gold_inc_sql.split(";") if s.strip()]
        for stmt in stmts:
            trino_execute(compose_file, stmt + ";", max_attempts=3)
    else:
        print("\n" + "=" * 60)
        print("FASE 4 — Gold full (DROP + CREATE + INSERT)")
        print("=" * 60)
        apply_ddl(compose_file, gold_sql, "Gold")

    # -------------------------------------------------------------------------
    # Quality gate — Silver + Gold (bloqueante em FAIL)
    # -------------------------------------------------------------------------
    silver_checks_sql = pipeline_root / "04_quality" / "sql" / "01_silver_checks.sql"
    gold_checks_sql   = pipeline_root / "04_quality" / "sql" / "02_gold_checks.sql"

    if not args.no_quality:
        print("\n" + "=" * 60)
        print("QUALITY GATE")
        print("=" * 60)
        if not incremental:
            if silver_checks_sql.exists():
                run_quality_checks(compose_file, silver_checks_sql, "Silver")
            else:
                print(f"    [AVISO] {silver_checks_sql} não encontrado — a saltar Silver checks.")
        if gold_checks_sql.exists():
            run_quality_checks(compose_file, gold_checks_sql, "Gold")
        else:
            print(f"    [AVISO] {gold_checks_sql} não encontrado — a saltar Gold checks.")

    modo = f"backfill incremental {args.date_from} → {args.date_to}" if incremental else "full rebuild"
    print(f"\n{'=' * 60}")
    print("Pipeline Medallion producao_consumo concluída com sucesso!")
    print(f"  Modo     : {modo}")
    print(f"  Bronze   : bronze.consumo_total_nacional + bronze.energia_produzida_total_nacional")
    print(f"  Silver   : silver.consumo_total_nacional_15min + silver.energia_produzida_total_nacional_15min")
    print(f"  Gold     : gold.dp_energia_balance_hourly")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as exc:
        print(f"\nErro: comando falhou (exit code {exc.returncode})", file=sys.stderr)
        sys.exit(exc.returncode)
