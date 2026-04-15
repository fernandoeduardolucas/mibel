$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = $ScriptDir
$ComposeFile = Join-Path $RepoRoot "01_bootstrap/tead_2.0_v1.2/docker-compose.yml"
$BronzeDir = Join-Path $RepoRoot "02_medallion_pipeline/producao_consumo/01_bronze"
$BronzeSql = Join-Path $RepoRoot "02_medallion_pipeline/producao_consumo/01_bronze/sql/bronze_trino.sql"
$SilverSql = Join-Path $RepoRoot "02_medallion_pipeline/producao_consumo/02_silver/sql/01_silver_trino.sql"
$GoldSql = Join-Path $RepoRoot "02_medallion_pipeline/producao_consumo/03_gold/sql/01_gold_trino.sql"

function Log([string]$Message) {
    Write-Host "`n[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message"
}

function Require-Command([string]$CommandName) {
    if (-not (Get-Command $CommandName -ErrorAction SilentlyContinue)) {
        throw "Comando obrigatório não encontrado: $CommandName"
    }
}

function Invoke-TrinoSql([string]$SqlFile, [string]$StageName) {
    Log "A executar SQL da camada $StageName via Docker (Trino): $SqlFile"
    Get-Content -Raw $SqlFile | docker compose -f $ComposeFile exec -T trino trino
}

Require-Command docker
Require-Command python

if (-not (Test-Path $ComposeFile)) {
    throw "docker-compose.yml não encontrado em: $ComposeFile"
}

Log "A subir a stack Docker (build incluído)"
docker compose -f $ComposeFile up -d --build

Log "A instalar dependências Python da Bronze"
python -m pip install -r (Join-Path $BronzeDir "scripts/python/requirements_bronze.txt")

Log "A correr limpeza + upload Bronze"
Push-Location $BronzeDir
try {
    $env:S3_ENDPOINT_URL = "http://localhost:9000"
    $env:AWS_ACCESS_KEY_ID = "minioadmin"
    $env:AWS_SECRET_ACCESS_KEY = "minioadmin"
    $env:S3_BUCKET = "warehouse"
    $env:S3_PREFIX = "bronze/clean"

    python scripts/python/bronze_clean_upload.py `
        --consumo data/raw/consumo-total-nacional.csv `
        --producao data/raw/energia-produzida-total-nacional.csv `
        --out-dir data/clean `
        --upload
}
finally {
    Pop-Location
}

Invoke-TrinoSql -SqlFile $BronzeSql -StageName "Bronze"
Invoke-TrinoSql -SqlFile $SilverSql -StageName "Silver"
Invoke-TrinoSql -SqlFile $GoldSql -StageName "Gold"

Log "Pipeline Medallion concluída com sucesso."
Log "Validação rápida (Gold):"
docker compose -f $ComposeFile exec -T trino trino --execute "SELECT COUNT(*) AS linhas_gold FROM iceberg.gold.producao_vs_consumo_hourly;"
