@echo off
setlocal enabledelayedexpansion

REM Runner simples para executar SQL Medallion no Trino.
REM Requisitos:
REM   - Trino CLI instalado e no PATH
REM     (ou variável TRINO_CMD/TRINO_EXE configurada)
REM   - Variável TRINO_HOST definida (default: localhost)
REM   - Catálogo/schema já configurados

if "%TRINO_HOST%"=="" set "TRINO_HOST=localhost"
if "%TRINO_PORT%"=="" set "TRINO_PORT=8080"
if "%TRINO_USER%"=="" set "TRINO_USER=admin"
if "%TRINO_CMD%"=="" set "TRINO_CMD=trino"
if not "%TRINO_EXE%"=="" set "TRINO_CMD=%TRINO_EXE%"

set "SCRIPT_DIR=%~dp0"

call :check_trino_cli
if errorlevel 1 exit /b 1

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
%TRINO_CMD% --server "http://%TRINO_HOST%:%TRINO_PORT%" --user "%TRINO_USER%" --file "%FILE%"
if errorlevel 1 (
    echo [ERRO] Falha ao executar: %FILE%
    exit /b 1
)
exit /b 0

:check_trino_cli
where /Q "%TRINO_CMD%"
if errorlevel 1 (
    echo [ERRO] Trino CLI nao encontrado: "%TRINO_CMD%"
    echo        Instale o Trino CLI e adicione ao PATH, ou configure TRINO_CMD com o executavel completo.
    echo        Exemplo:
    echo          set TRINO_CMD=C:\tools\trino.exe
    exit /b 1
)
exit /b 0
