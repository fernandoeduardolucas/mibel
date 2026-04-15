@echo off
setlocal enabledelayedexpansion

REM Runner simples para executar SQL Medallion no Trino.
REM Requisitos:
REM   - Trino CLI instalado e no PATH
REM   - Variável TRINO_HOST definida (default: localhost)
REM   - Catálogo/schema já configurados

if "%TRINO_HOST%"=="" set "TRINO_HOST=localhost"
if "%TRINO_PORT%"=="" set "TRINO_PORT=8080"
if "%TRINO_USER%"=="" set "TRINO_USER=admin"

set "SCRIPT_DIR=%~dp0"

call :run_sql "%SCRIPT_DIR%..\bronze\sql\bronze_trino.sql"
if errorlevel 1 exit /b 1

call :run_sql "%SCRIPT_DIR%..\silver\sql\01_silver_trino.sql"
if errorlevel 1 exit /b 1

call :run_sql "%SCRIPT_DIR%..\gold\sql\01_gold_trino.sql"
if errorlevel 1 exit /b 1

REM Data quality checks por camada
call :run_sql "%SCRIPT_DIR%..\bronze\sql\99_checks.sql"
if errorlevel 1 exit /b 1

call :run_sql "%SCRIPT_DIR%..\silver\sql\99_checks.sql"
if errorlevel 1 exit /b 1

call :run_sql "%SCRIPT_DIR%..\gold\sql\99_checks.sql"
if errorlevel 1 exit /b 1

echo Pipeline medallion concluido com sucesso.
exit /b 0

:run_sql
set "FILE=%~1"
echo [RUN] %FILE%
trino --server "http://%TRINO_HOST%:%TRINO_PORT%" --user "%TRINO_USER%" --file "%FILE%"
if errorlevel 1 (
    echo [ERRO] Falha ao executar: %FILE%
    exit /b 1
)
exit /b 0