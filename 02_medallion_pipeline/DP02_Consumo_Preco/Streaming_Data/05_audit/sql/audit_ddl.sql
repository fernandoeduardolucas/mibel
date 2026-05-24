-- =============================================================================
-- DDL — Auditoria operacional (DP-02 Streaming_Data)
-- Schema  : iceberg.audit
-- Tabelas : pipeline_runs | dataset_lineage
--
-- Idempotente — seguro para executar múltiplas vezes (CREATE IF NOT EXISTS).
-- Aplicado automaticamente pelo orquestrador em cada execução.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.audit
WITH (location = 's3a://warehouse/audit/');

-- ---------------------------------------------------------------------------
-- pipeline_runs — registo de cada execução do pipeline
-- ---------------------------------------------------------------------------
-- Particionado por dia (start_ts) para consultas temporais eficientes.
-- Uma linha por execução; status = SUCCESS | FAILED.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.audit.pipeline_runs (
    run_id            VARCHAR,
    pipeline_name     VARCHAR,
    pipeline_version  VARCHAR,
    start_ts          TIMESTAMP(6) WITH TIME ZONE,
    end_ts            TIMESTAMP(6) WITH TIME ZONE,
    duration_seconds  DOUBLE,
    status            VARCHAR,
    rows_bronze       BIGINT,
    rows_silver       BIGINT,
    rows_gold         BIGINT,
    source            VARCHAR,
    param_start_date  VARCHAR,
    param_end_date    VARCHAR,
    error_message     VARCHAR
)
WITH (
    format       = 'PARQUET',
    partitioning = ARRAY['day(start_ts)']
);

-- ---------------------------------------------------------------------------
-- dataset_lineage — mapa upstream → downstream por execução
-- ---------------------------------------------------------------------------
-- Estático (não muda entre versões do pipeline): materializado a cada run.
-- Permite rastrear: "que tabelas alimentaram esta gold em run_id=X?"
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iceberg.audit.dataset_lineage (
    run_id      VARCHAR,
    upstream    VARCHAR,
    downstream  VARCHAR,
    recorded_at TIMESTAMP(6) WITH TIME ZONE
)
WITH (
    format = 'PARQUET'
);
