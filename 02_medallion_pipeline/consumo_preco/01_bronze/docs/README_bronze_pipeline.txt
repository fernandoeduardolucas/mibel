README — Bronze Pipeline

Script principal
scripts/python/bronze_clean_upload_consumo_precos.py

Datasets suportados
1. consumo-total-nacional.csv
2. Day-ahead Market Prices_*.csv

Esquema de saída — consumo
- datahora_raw
- timestamp_utc
- dia
- mes
- ano
- data_local
- hora_local_raw
- consumo_bt_kwh
- consumo_mt_kwh
- consumo_at_kwh
- consumo_mat_kwh
- consumo_total_kwh
- duplicate_count
- duplicate_rank
- flag_duplicate_timestamp
- flag_bad_timestamp
- flag_bad_date
- flag_bad_total
- flag_zero_row
- flag_date_mismatch
- source_file_name
- run_id
- ingest_ts_utc

Esquema de saída — preços
- date_raw
- delivery_date_local
- market_hour
- timestamp_utc
- preco_pt_eur_mwh
- preco_es_eur_mwh
- duplicate_count
- duplicate_rank
- flag_duplicate_timestamp
- flag_bad_timestamp
- flag_bad_date
- flag_bad_hour
- flag_bad_preco_pt
- flag_bad_preco_es
- flag_zero_preco_pt
- market_timezone_assumed
- source_file_name
- run_id
- ingest_ts_utc

Pontos de atenção
- O dataset de preços tem horas 1..25. Isto exige uma política temporal explícita.
- O dataset de consumo já vem com offset UTC na coluna datahora.
- A presença de duplicate_rank não implica deduplicação automática; apenas priorização observável.
