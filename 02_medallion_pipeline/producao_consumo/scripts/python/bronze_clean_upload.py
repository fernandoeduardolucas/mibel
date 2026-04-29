#!/usr/bin/env python3
"""
Pipeline Bronze (clean only) para os ficheiros:
- consumo-total-nacional.csv
- energia-produzida-total-nacional.csv

Este script faz apenas limpeza e escrita local em Parquet.
Para upload para MinIO/S3 usa o script separado: bronze_upload.py
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


# Remove o BOM (Byte Order Mark) se existir no cabeçalho do CSV.
def _strip_bom(text: str) -> str:
    return text.replace("\ufeff", "")


# Normaliza nomes de colunas: minúsculas, sem espaços extra e sem BOM.
def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_strip_bom(str(c)).strip().lower() for c in df.columns]
    return df


# Aplica regras comuns aos dois datasets.
def _parse_common(df: pd.DataFrame) -> pd.DataFrame:
    df = _normalize_columns(df)

    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype(str).str.strip()

    df["timestamp_utc"] = pd.to_datetime(df["datahora"], utc=True).dt.tz_localize(None).dt.floor("ms")
    df["data_local"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    for col in ["dia", "mes", "ano"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    total_num = pd.to_numeric(df["total"], errors="coerce")
    zero_penalty = (total_num.fillna(0) == 0).astype(int)

    df["duplicate_count"] = df.groupby("datahora")["datahora"].transform("size").astype("Int64")
    df["flag_duplicate_timestamp"] = df["duplicate_count"] > 1

    temp = df[["datahora"]].copy()
    temp["zero_penalty"] = zero_penalty
    temp["total_num"] = total_num
    temp["row_pos"] = range(len(df))
    temp = temp.sort_values(
        by=["datahora", "zero_penalty", "total_num", "row_pos"],
        ascending=[True, True, False, True],
        kind="mergesort",
    )
    temp["duplicate_rank"] = temp.groupby("datahora").cumcount() + 1
    temp = temp.sort_values("row_pos")
    df["duplicate_rank"] = temp["duplicate_rank"].astype("Int64").to_numpy()

    df["flag_bad_timestamp"] = df["timestamp_utc"].isna()
    df["flag_bad_date"] = pd.isna(df["data_local"])
    df["flag_zero_total"] = total_num.fillna(0) == 0

    return df


def clean_consumo(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = _parse_common(df)

    for col in ["bt", "mt", "at", "mat", "total"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["flag_bad_total"] = df["total"].isna()

    out = pd.DataFrame(
        {
            "datahora_raw": df["datahora"],
            "timestamp_utc": df["timestamp_utc"],
            "dia": df["dia"],
            "mes": df["mes"],
            "ano": df["ano"],
            "data_local": df["data_local"],
            "hora_local_raw": df["time"],
            "consumo_bt_kwh": df["bt"],
            "consumo_mt_kwh": df["mt"],
            "consumo_at_kwh": df["at"],
            "consumo_mat_kwh": df["mat"],
            "consumo_total_kwh": df["total"],
            "duplicate_count": df["duplicate_count"],
            "duplicate_rank": df["duplicate_rank"],
            "flag_duplicate_timestamp": df["flag_duplicate_timestamp"],
            "flag_bad_timestamp": df["flag_bad_timestamp"],
            "flag_bad_date": df["flag_bad_date"],
            "flag_bad_total": df["flag_bad_total"],
            "flag_zero_row": df["flag_zero_total"],
            "ingest_ts_utc": pd.Timestamp.now("UTC"),
        }
    )
    return out.sort_values("timestamp_utc", kind="mergesort").reset_index(drop=True)


def clean_producao(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = _parse_common(df)

    for col in ["dgm", "pre", "total"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["flag_bad_total"] = df["total"].isna()

    out = pd.DataFrame(
        {
            "datahora_raw": df["datahora"],
            "timestamp_utc": df["timestamp_utc"],
            "dia": df["dia"],
            "mes": df["mes"],
            "ano": df["ano"],
            "data_local": df["data_local"],
            "hora_local_raw": df["time"],
            "producao_dgm_kwh": df["dgm"],
            "producao_pre_kwh": df["pre"],
            "producao_total_kwh": df["total"],
            "duplicate_count": df["duplicate_count"],
            "duplicate_rank": df["duplicate_rank"],
            "flag_duplicate_timestamp": df["flag_duplicate_timestamp"],
            "flag_bad_timestamp": df["flag_bad_timestamp"],
            "flag_bad_date": df["flag_bad_date"],
            "flag_bad_total": df["flag_bad_total"],
            "flag_zero_row": df["flag_zero_total"],
            "ingest_ts_utc": pd.Timestamp.now("UTC"),
        }
    )
    return out.sort_values("timestamp_utc", kind="mergesort").reset_index(drop=True)


def write_parquet(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(out_path, index=False, engine="pyarrow")
    except ImportError as exc:
        raise SystemExit("Falta a dependência 'pyarrow'. Instala com: pip install pyarrow") from exc


def print_quality_report(name: str, df: pd.DataFrame, total_col: str) -> None:
    print(f"\n=== {name} ===")
    print(f"Rows: {len(df):,}")
    print(f"Min timestamp: {df['timestamp_utc'].min()}")
    print(f"Max timestamp: {df['timestamp_utc'].max()}")
    print(f"Duplicate timestamp rows: {int(df['flag_duplicate_timestamp'].sum()):,}")
    print(f"Bad timestamps: {int(df['flag_bad_timestamp'].sum()):,}")
    print(f"Bad totals: {int(df['flag_bad_total'].sum()):,}")
    print(f"Zero rows: {int(df['flag_zero_row'].sum()):,}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--consumo", required=True, type=Path)
    parser.add_argument("--producao", required=True, type=Path)
    parser.add_argument("--out-dir", default=Path("./output"), type=Path)
    args = parser.parse_args()

    consumo_df = clean_consumo(args.consumo)
    producao_df = clean_producao(args.producao)

    consumo_out = args.out_dir / "consumo_total_nacional_clean.parquet"
    producao_out = args.out_dir / "energia_produzida_total_nacional_clean.parquet"

    write_parquet(consumo_df, consumo_out)
    write_parquet(producao_df, producao_out)

    print_quality_report("Consumo", consumo_df, "consumo_total_kwh")
    print_quality_report("Produção", producao_df, "producao_total_kwh")

    print(f"\nWrote: {consumo_out}")
    print(f"Wrote: {producao_out}")


if __name__ == "__main__":
    main()
