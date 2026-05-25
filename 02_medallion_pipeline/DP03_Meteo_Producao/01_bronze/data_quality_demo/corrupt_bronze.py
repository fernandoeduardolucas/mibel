#!/usr/bin/env python3
"""
corrupt_bronze.py — Injeta dados sujos nas tabelas Bronze para demonstração académica.

Demonstra o valor da camada Silver na arquitetura Medallion: Bronze preserva os dados
tal como chegam (incluindo erros), Silver limpa e valida.

Fluxo típico:
  1. Corromper:       python corrupt_bronze.py --dp meteo --type all --pct 5
  2. Re-executar Silver: python corrupt_bronze.py --rerun-silver --dp meteo
  3. Ver quality flags no Trino:
       SELECT _quality_flag, COUNT(*) FROM iceberg.silver.meteo_open_meteo_hourly GROUP BY 1;
  4. Restaurar:       python corrupt_bronze.py --restore --dp meteo

Tipos de corrupção disponíveis:
  nulls       — NULLs em coluna chave (ex: temperature_2m → NULL)
                Silver deteta com _quality_flag = 'null_values'
  outofrange  — Valores fisicamente impossíveis (temp=-50°C, radiation=-200, cloud=150%)
                Silver deteta com _quality_flag = 'out_of_range'
  duplicates  — Timestamps duplicados com _ingested_at antigo (1970)
                Silver deduplica via ROW_NUMBER (mantém registo mais recente)
  timestamps  — Timestamps não-horários (minute=30)
                Quality gate reporta FAIL no check de alinhamento temporal
  all         — Todos os tipos acima
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Caminhos e configuração por DP
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
COMPOSE_FILE = REPO_ROOT / "01_docker_stack" / "docker-compose.yml"

DP_CONFIG: dict[str, dict] = {
    "meteo": {
        "table": "iceberg.bronze.meteo_open_meteo_hourly",
        "backup": "iceberg.bronze.meteo_open_meteo_hourly_backup",
        "silver_sql": (
            REPO_ROOT
            / "02_medallion_pipeline"
            / "DP03_Meteo_Producao"
            / "02_silver"
            / "sql"
            / "01_silver_trino.sql"
        ),
        "null_col": "temperature_2m",
        "supports_outofrange": True,
        "supports_timestamps": True,
    },
    "streaming": {
        "table": "iceberg.bronze.consumo_api_raw",
        "backup": "iceberg.bronze.consumo_api_raw_backup",
        "silver_sql": (
            REPO_ROOT
            / "02_medallion_pipeline"
            / "DP02_Consumo_Preco"
            / "Streaming_Data"
            / "02_silver"
            / "sql"
            / "silver_streaming_ddl.sql"
        ),
        "null_col": "total",
        "supports_outofrange": False,
        "supports_timestamps": False,
    },
}

# Máximo de timestamps por IN clause para evitar SQL demasiado longo
MAX_IN_CLAUSE = 500


# ---------------------------------------------------------------------------
# Helpers Trino
# ---------------------------------------------------------------------------

def _trino_cmd() -> list[str]:
    return ["docker", "compose", "-f", str(COMPOSE_FILE), "exec", "-T", "trino", "trino"]


def run_trino_exec(sql: str) -> None:
    result = subprocess.run(_trino_cmd(), input=sql.encode("utf-8"), capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.decode("utf-8", errors="replace"))


def run_trino_query(sql: str) -> list[str]:
    """Executa um SELECT e devolve as linhas de resultado (sem cabeçalho)."""
    cmd = _trino_cmd() + ["--output-format", "TSV_HEADER", "--execute", sql]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr)
    lines = result.stdout.strip().splitlines()
    return [line.strip() for line in lines[1:] if line.strip()]


def query_scalar(sql: str) -> str:
    rows = run_trino_query(sql)
    return rows[0] if rows else "0"


def get_count(table: str) -> int:
    return int(query_scalar(f"SELECT COUNT(*) FROM {table}"))


def get_timestamps(table: str, n: int, order: str = "ASC") -> list[str]:
    return run_trino_query(
        f"SELECT CAST(ts_utc AS VARCHAR) FROM {table} ORDER BY ts_utc {order} LIMIT {n}"
    )


def apply_sql_file(sql_file: Path, label: str) -> None:
    """Executa ficheiro SQL statement a statement via Trino CLI (mesmo padrão do orquestrador)."""
    print(f"\n>>> A executar {label}: {sql_file.name}")
    if not sql_file.exists():
        raise FileNotFoundError(f"Ficheiro SQL não encontrado: {sql_file}")
    sql_text = sql_file.read_text(encoding="utf-8")
    sql_clean = re.sub(r"--[^\n]*", "", sql_text)
    stmts = [s.strip() for s in sql_clean.split(";") if s.strip()]
    for i, stmt in enumerate(stmts, 1):
        preview = stmt[:70].replace("\n", " ")
        print(f"    [{i}/{len(stmts)}] {preview}...")
        run_trino_exec(stmt + ";")
    print(f"    {label} concluído ({len(stmts)} statements).")


# ---------------------------------------------------------------------------
# Snapshot / Restore
# ---------------------------------------------------------------------------

def ensure_backup(cfg: dict) -> None:
    table, backup = cfg["table"], cfg["backup"]
    parts = backup.split(".")
    exists = query_scalar(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_catalog = '{parts[0]}' "
        f"  AND table_schema = '{parts[1]}' "
        f"  AND table_name = '{parts[2]}'"
    )
    if exists == "0":
        print(f"\n>>> A criar snapshot: {backup}")
        run_trino_exec(f"CREATE TABLE {backup} AS SELECT * FROM {table};")
        count = get_count(backup)
        print(f"    Snapshot criado — {count} linhas.")
    else:
        print(f"\n>>> Snapshot já existe ({backup}). A reutilizar.")


def cmd_restore(cfg: dict) -> None:
    table, backup = cfg["table"], cfg["backup"]
    parts = backup.split(".")
    exists = query_scalar(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_catalog = '{parts[0]}' "
        f"  AND table_schema = '{parts[1]}' "
        f"  AND table_name = '{parts[2]}'"
    )
    if exists == "0":
        sys.exit(f"Erro: snapshot {backup} não existe. Corre primeiro uma corrupção para criar o snapshot.")
    print(f"\n>>> A restaurar {table} a partir de {backup}...")
    count_backup = get_count(backup)
    run_trino_exec(f"DELETE FROM {table};")
    run_trino_exec(f"INSERT INTO {table} SELECT * FROM {backup};")
    count = get_count(table)
    print(f"    Restaurado: {count}/{count_backup} linhas.")


# ---------------------------------------------------------------------------
# Funções de corrupção
# ---------------------------------------------------------------------------

def corrupt_nulls(cfg: dict, n: int) -> int:
    """Coloca NULL na coluna chave de n linhas (primeiras por ts_utc)."""
    table, null_col = cfg["table"], cfg["null_col"]
    n = min(n, MAX_IN_CLAUSE)
    timestamps = get_timestamps(table, n, order="ASC")
    if not timestamps:
        return 0
    ts_in = ", ".join(f"TIMESTAMP '{ts}'" for ts in timestamps)
    run_trino_exec(f"UPDATE {table} SET {null_col} = NULL WHERE ts_utc IN ({ts_in});")
    print(f"    nulls: {len(timestamps)} linhas → {null_col} = NULL")
    return len(timestamps)


def corrupt_outofrange(cfg: dict, n: int) -> int:
    """Injeta valores fisicamente impossíveis para Portugal nas últimas n linhas."""
    if not cfg["supports_outofrange"]:
        print("    [SKIP] out_of_range não disponível para este DP.")
        return 0
    table = cfg["table"]
    n = min(n, MAX_IN_CLAUSE)
    # usa as últimas N linhas para não sobrepor com as nulls (que usaram as primeiras)
    timestamps = get_timestamps(table, n, order="DESC")
    if not timestamps:
        return 0
    ts_in = ", ".join(f"TIMESTAMP '{ts}'" for ts in timestamps)
    run_trino_exec(
        f"UPDATE {table} "
        f"SET temperature_2m = -50.0, shortwave_radiation = -200.0, cloud_cover = 150.0 "
        f"WHERE ts_utc IN ({ts_in});"
    )
    print(f"    out_of_range: {len(timestamps)} linhas → temp=-50°C | radiation=-200 W/m² | cloud=150%")
    return len(timestamps)


def corrupt_duplicates(cfg: dict, n: int) -> int:
    """Insere cópias de n linhas com _ingested_at=1970 (Silver mantém o original por ser mais recente)."""
    table = cfg["table"]
    if "meteo" in table:
        run_trino_exec(
            f"INSERT INTO {table} "
            f"SELECT ts_utc, year, month, day, hour, "
            f"       temperature_2m, precipitation, wind_speed_10m, "
            f"       shortwave_radiation, cloud_cover, "
            f"       latitude, longitude, elevation_m, "
            f"       '_corrupted_duplicate' AS _source_file, "
            f"       '1970-01-01T00:00:00+00:00' AS _ingested_at "
            f"FROM {table} ORDER BY ts_utc LIMIT {n};"
        )
    else:
        run_trino_exec(
            f"INSERT INTO {table} "
            f"SELECT ts_utc, total, "
            f"       'http://corrupted.example/duplicate' AS source_url, "
            f"       fetch_date, process_date "
            f"FROM {table} ORDER BY ts_utc LIMIT {n};"
        )
    print(f"    duplicates: {n} linhas inseridas com mesmo ts_utc")
    print(f"                (_ingested_at=1970 → Silver deduplica e mantém o original)")
    return n


def corrupt_timestamps(cfg: dict, n: int) -> int:
    """Insere n linhas com ts_utc deslocado +30 minutos (minute != 0 → falha quality gate)."""
    if not cfg["supports_timestamps"]:
        print("    [SKIP] timestamps não disponível para este DP.")
        return 0
    table = cfg["table"]
    run_trino_exec(
        f"INSERT INTO {table} "
        f"SELECT ts_utc + INTERVAL '30' MINUTE AS ts_utc, "
        f"       year, month, day, hour, "
        f"       temperature_2m, precipitation, wind_speed_10m, "
        f"       shortwave_radiation, cloud_cover, "
        f"       latitude, longitude, elevation_m, "
        f"       '_corrupted_ts_misaligned' AS _source_file, "
        f"       '1970-01-01T00:00:00+00:00' AS _ingested_at "
        f"FROM {table} ORDER BY ts_utc LIMIT {n};"
    )
    print(f"    timestamps: {n} linhas inseridas com minute=30")
    print(f"                (quality gate 'temporal alignment' vai reportar FAIL)")
    return n


# ---------------------------------------------------------------------------
# Sumário pós-corrupção
# ---------------------------------------------------------------------------

def print_summary(cfg: dict, dp: str) -> None:
    table = cfg["table"]
    print(f"\n{'─' * 62}")
    print(f" Estado Bronze após corrupção: {table}")
    print(f"{'─' * 62}")
    total = get_count(table)
    print(f"  Total de linhas:  {total}")

    if dp == "meteo":
        nulls = query_scalar(f"SELECT COUNT(*) FROM {table} WHERE temperature_2m IS NULL")
        oor = query_scalar(
            f"SELECT COUNT(*) FROM {table} "
            f"WHERE temperature_2m < -10.0 OR shortwave_radiation < 0.0 OR cloud_cover > 100.0"
        )
        print(f"  temperature_2m IS NULL:       {nulls}")
        print(f"  Out-of-range (temp/rad/cloud): {oor}")
    elif dp == "streaming":
        nulls = query_scalar(f"SELECT COUNT(*) FROM {table} WHERE total IS NULL")
        zeros = query_scalar(
            f"SELECT COUNT(*) FROM {table} WHERE total IS NOT NULL AND total = 0.0"
        )
        print(f"  total IS NULL:  {nulls}")
        print(f"  total = 0.0:    {zeros}")

    print()
    print("Próximos passos:")
    print(f"  1. Re-executar Silver:")
    print(f"       python corrupt_bronze.py --rerun-silver --dp {dp}")
    print(f"  2. Ver distribuição de quality flags no Trino:")
    if dp == "meteo":
        print(
            "       SELECT _quality_flag, COUNT(*)"
            " FROM iceberg.silver.meteo_open_meteo_hourly GROUP BY 1;"
        )
    elif dp == "streaming":
        print(
            "       SELECT COUNT(*) FROM iceberg.silver.consumo_api_hourly;"
        )
    print(f"  3. Restaurar Bronze:")
    print(f"       python corrupt_bronze.py --restore --dp {dp}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dp", choices=list(DP_CONFIG), default="meteo",
        help="Data product alvo (default: meteo)",
    )
    parser.add_argument(
        "--type", dest="corruption_type",
        choices=["nulls", "outofrange", "duplicates", "timestamps", "all"],
        help="Tipo de corrupção a injetar",
    )
    parser.add_argument(
        "--pct", type=float, default=5.0,
        help="Percentagem de linhas a corromper (default: 5.0)",
    )
    parser.add_argument(
        "--n", type=int, default=None,
        help="Número absoluto de linhas (sobrepõe --pct)",
    )
    parser.add_argument(
        "--restore", action="store_true",
        help="Restaura a tabela Bronze a partir do snapshot",
    )
    parser.add_argument(
        "--rerun-silver", action="store_true",
        help="Re-executa o SQL Silver sem re-ingerir Bronze (para ver o efeito da corrupção)",
    )
    args = parser.parse_args()

    if not COMPOSE_FILE.exists():
        sys.exit(
            f"Erro: {COMPOSE_FILE} não encontrado.\n"
            "Corre o script a partir da raiz do repositório ou de qualquer subdirectoria."
        )

    cfg = DP_CONFIG[args.dp]

    if args.restore:
        cmd_restore(cfg)
        return

    if args.rerun_silver:
        apply_sql_file(cfg["silver_sql"], f"Silver ({args.dp})")
        print(f"\nSilver reconstruído a partir do Bronze {'corrompido' if not args.restore else 'restaurado'}.")
        if args.dp == "meteo":
            print(
                "Verifica os quality flags:\n"
                "  SELECT _quality_flag, COUNT(*)"
                " FROM iceberg.silver.meteo_open_meteo_hourly GROUP BY 1;"
            )
        return

    if not args.corruption_type:
        parser.error("É necessário especificar --type, --restore ou --rerun-silver.")

    total = get_count(cfg["table"])
    if total == 0:
        sys.exit(
            f"Erro: {cfg['table']} está vazia.\n"
            "Executa primeiro o pipeline Bronze antes de corromper."
        )

    n = args.n if args.n is not None else max(1, int(total * args.pct / 100))
    n = min(n, total)

    print(f"\n{'═' * 62}")
    print(f"  corrupt_bronze.py — demo de qualidade de dados")
    print(f"{'═' * 62}")
    print(f"  DP:      {args.dp}  →  {cfg['table']}")
    print(f"  Linhas:  {total} total  |  {n} a corromper  ({n / total * 100:.1f}%)")
    print(f"  Tipo:    {args.corruption_type}")
    print(f"{'═' * 62}")

    ensure_backup(cfg)

    types = (
        ["nulls", "outofrange", "duplicates", "timestamps"]
        if args.corruption_type == "all"
        else [args.corruption_type]
    )

    for t in types:
        print(f"\n[{t.upper()}]")
        if t == "nulls":
            corrupt_nulls(cfg, n)
        elif t == "outofrange":
            corrupt_outofrange(cfg, n)
        elif t == "duplicates":
            corrupt_duplicates(cfg, n)
        elif t == "timestamps":
            corrupt_timestamps(cfg, n)

    print_summary(cfg, args.dp)


if __name__ == "__main__":
    main()
