#!/usr/bin/env python3
"""
Pipeline Bronze para os ficheiros:
- consumo-total-nacional.csv
- energia-produzida-total-nacional.csv

O script faz 4 coisas principais:
1) Lê os CSV originais (raw)
2) Limpa e normaliza os dados com pandas
3) Grava versões limpas em Parquet
4) Opcionalmente faz upload para o MinIO/S3
   - mantém o upload dos ficheiros limpos (clean)
   - acrescenta também o upload dos ficheiros raw

Exemplo de execução:
python bronze_clean_upload.py \
  --consumo consumo-total-nacional.csv \
  --producao energia-produzida-total-nacional.csv \
  --out-dir ./output \
  --upload

Variáveis de ambiente para upload:
S3_ENDPOINT_URL=http://localhost:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
S3_BUCKET=warehouse
S3_PREFIX=bronze/clean
"""
from __future__ import annotations

import argparse
import os
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
# Aqui tratamos parsing temporal, casting básico e flags de qualidade.
def _parse_common(df: pd.DataFrame) -> pd.DataFrame:
    df = _normalize_columns(df)

    # Limpa espaços em colunas de texto.
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()

    # Converte timestamps e datas. Quando falha, fica NaT.
    df["timestamp_utc"] = (
    pd.to_datetime(df["datahora"], utc=True)
    .dt.tz_localize(None)
    .dt.floor("ms")
)
    df["data_local"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # Converte colunas de calendário para inteiros anuláveis.
    for col in ["dia", "mes", "ano"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Política de duplicados da Bronze:
    # não apagamos linhas; apenas marcamos e ordenamos.
    total_num = pd.to_numeric(df["total"], errors="coerce")
    zero_penalty = (total_num.fillna(0) == 0).astype(int)

    # Quantas linhas existem por timestamp.
    df["duplicate_count"] = df.groupby("datahora")["datahora"].transform("size").astype("Int64")
    df["flag_duplicate_timestamp"] = df["duplicate_count"] > 1

    # Ranking estável dentro de cada timestamp duplicado.
    # Preferimos linhas com total não-zero e total maior.
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

    # Flags de qualidade.
    df["flag_bad_timestamp"] = df["timestamp_utc"].isna()
    df["flag_bad_date"] = pd.isna(df["data_local"])
    df["flag_zero_total"] = total_num.fillna(0) == 0

    return df


# Limpeza específica do dataset de consumo.
def clean_consumo(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = _parse_common(df)

    # Converte colunas numéricas do consumo.
    for col in ["bt", "mt", "at", "mat", "total"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["flag_bad_total"] = df["total"].isna()

    # Reorganiza e renomeia para a estrutura alvo da Bronze.
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
            "ingest_ts_utc": pd.Timestamp.utcnow(),
        }
    )
    return out.sort_values("timestamp_utc", kind="mergesort").reset_index(drop=True)


# Limpeza específica do dataset de produção.
def clean_producao(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = _parse_common(df)

    # Converte colunas numéricas da produção.
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
            "ingest_ts_utc": pd.Timestamp.utcnow(),
        }
    )
    return out.sort_values("timestamp_utc", kind="mergesort").reset_index(drop=True)


# Escreve o DataFrame em Parquet.
def write_parquet(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(out_path, index=False, engine="pyarrow")
    except ImportError as exc:
        raise SystemExit(
            "Falta a dependência 'pyarrow'. Instala com: pip install pyarrow"
        ) from exc


# Faz upload de um ficheiro local para MinIO/S3.
def upload_file(local_path: Path, bucket: str, key: str) -> None:
    try:
        import boto3
    except ImportError as exc:
        raise SystemExit(
            "Falta a dependência 'boto3'. Instala com: pip install boto3"
        ) from exc

    endpoint = os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    s3.upload_file(str(local_path), bucket, key)


# Mostra um pequeno relatório de qualidade para validação rápida.
def print_quality_report(name: str, df: pd.DataFrame, total_col: str) -> None:
    print(f"\n=== {name} ===")
    print(f"Rows: {len(df):,}")
    print(f"Min timestamp: {df['timestamp_utc'].min()}")
    print(f"Max timestamp: {df['timestamp_utc'].max()}")
    print(f"Duplicate timestamp rows: {int(df['flag_duplicate_timestamp'].sum()):,}")
    print(f"Bad timestamps: {int(df['flag_bad_timestamp'].sum()):,}")
    print(f"Bad totals: {int(df['flag_bad_total'].sum()):,}")
    print(f"Zero rows: {int(df['flag_zero_row'].sum()):,}")
    print("\nSample duplicate timestamps:")
    sample = (
        df.loc[df["flag_duplicate_timestamp"], ["timestamp_utc", total_col, "duplicate_rank"]]
        .head(8)
        .to_string(index=False)
    )
    print(sample if not sample.strip().startswith("Empty") else "No duplicates found.")


# Função principal do script.
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--consumo", required=True, type=Path)
    parser.add_argument("--producao", required=True, type=Path)
    parser.add_argument("--out-dir", default=Path("./output"), type=Path)
    parser.add_argument("--upload", action="store_true")
    args = parser.parse_args()

    # 1) Limpar os dois datasets.
    consumo_df = clean_consumo(args.consumo)
    producao_df = clean_producao(args.producao)

    # 2) Definir caminhos de saída locais.
    consumo_out = args.out_dir / "consumo_total_nacional_clean.parquet"
    producao_out = args.out_dir / "energia_produzida_total_nacional_clean.parquet"

    # 3) Gravar versões limpas em Parquet.
    write_parquet(consumo_df, consumo_out)
    write_parquet(producao_df, producao_out)

    # 4) Mostrar relatório de qualidade no terminal.
    print_quality_report("Consumo", consumo_df, "consumo_total_kwh")
    print_quality_report("Produção", producao_df, "producao_total_kwh")

    print(f"\nWrote: {consumo_out}")
    print(f"Wrote: {producao_out}")

    # 5) Fazer upload para MinIO/S3 se o utilizador pedir.
    # Mantemos o upload dos ficheiros clean e acrescentamos também o raw.
    if args.upload:
        bucket = os.environ.get("S3_BUCKET", "warehouse")
        prefix = os.environ.get("S3_PREFIX", "bronze/clean").strip("/")

        # Upload dos ficheiros raw para as localizações pedidas.
        raw_consumo_key = "bronze/raw/consumo_total_nacional/consumo-total-nacional.csv"
        raw_producao_key = "bronze/raw/energia_produzida_total_nacional/energia-produzida-total-nacional.csv"
        upload_file(args.consumo, bucket, raw_consumo_key)
        upload_file(args.producao, bucket, raw_producao_key)

        # Upload dos ficheiros clean em Parquet.
        clean_consumo_key = f"{prefix}/consumo_total_nacional/{consumo_out.name}"
        clean_producao_key = f"{prefix}/energia_produzida_total_nacional/{producao_out.name}"
        upload_file(consumo_out, bucket, clean_consumo_key)
        upload_file(producao_out, bucket, clean_producao_key)

        print(f"\nUploaded raw:   s3://{bucket}/{raw_consumo_key}")
        print(f"Uploaded raw:   s3://{bucket}/{raw_producao_key}")
        print(f"Uploaded clean: s3://{bucket}/{clean_consumo_key}")
        print(f"Uploaded clean: s3://{bucket}/{clean_producao_key}")


if __name__ == "__main__":
    main()
