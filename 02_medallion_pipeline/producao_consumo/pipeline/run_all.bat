@echo off
setlocal enabledelayedexpansion

REM Runner simples para executar SQL Medallion no Trino.
REM Modos de execucao:
REM   1) Trino CLI local (TRINO_CMD/TRINO_EXE ou comando `trino` no PATH)
REM   2) Docker Compose (fallback automatico quando nao encontra CLI local)

if "%TRINO_HOST%"=="" set "TRINO_HOST=localhost"
if "%TRINO_PORT%"=="" set "TRINO_PORT=8080"
if "%TRINO_USER%"=="" set "TRINO_USER=admin"
if "%TRINO_DOCKER_SERVICE%"=="" set "TRINO_DOCKER_SERVICE=trino"
if "%TRINO_DOCKER_SERVER%"=="" set "TRINO_DOCKER_SERVER=http://trino:8080"

set "SCRIPT_DIR=%~dp0"
set "DEFAULT_COMPOSE_FILE=%SCRIPT_DIR%..\..\..\01_bootstrap\tead_2.0_v1.2\docker-compose.yml"
if "%TRINO_COMPOSE_FILE%"=="" set "TRINO_COMPOSE_FILE=%DEFAULT_COMPOSE_FILE%"

call :resolve_trino_mode
if errorlevel 1 exit /b 1

echo [INFO] Modo de execucao: %TRINO_MODE%

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

:resolve_trino_mode
if not "%TRINO_EXE%"=="" set "TRINO_CMD=%TRINO_EXE%"

if not "%TRINO_CMD%"=="" (
    set "TRINO_MODE=cli"
    call :check_trino_cli
    exit /b %errorlevel%
)

where /Q trino
if not errorlevel 1 (
    set "TRINO_CMD=trino"
    set "TRINO_MODE=cli"
    call :check_trino_cli
    exit /b %errorlevel%
)

set "TRINO_MODE=docker"
call :check_docker_compose
exit /b %errorlevel%

:run_sql
set "FILE=%~1"
echo [RUN] %FILE%

if /I "%TRINO_MODE%"=="docker" (
    docker compose -f "%TRINO_COMPOSE_FILE%" exec -T %TRINO_DOCKER_SERVICE% trino --server "%TRINO_DOCKER_SERVER%" --user "%TRINO_USER%" --file "%FILE%"
) else (
    %TRINO_CMD% --server "http://%TRINO_HOST%:%TRINO_PORT%" --user "%TRINO_USER%" --file "%FILE%"
)

if errorlevel 1 (
    echo [ERRO] Falha ao executar: %FILE%
    if /I "%TRINO_MODE%"=="docker" (
        echo        Verifique se o stack Compose esta ativo: docker compose -f "%TRINO_COMPOSE_FILE%" up -d
    )
    exit /b 1
)
exit /b 0

:check_trino_cli
where /Q "%TRINO_CMD%"
if errorlevel 1 (
    echo [ERRO] Trino CLI nao encontrado: "%TRINO_CMD%"
    echo        Instale o Trino CLI e adicione ao PATH, configure TRINO_CMD com o executavel completo,
    echo        ou use o modo Docker Compose removendo TRINO_CMD/TRINO_EXE.
    echo        Exemplo CLI local:
    echo          set TRINO_CMD=C:\tools\trino.exe
    exit /b 1
)
exit /b 0

:check_docker_compose
where /Q docker
if errorlevel 1 (
    echo [ERRO] Docker nao encontrado no PATH.
    echo        Instale o Docker Desktop/Engine e tente novamente.
    exit /b 1
)

if not exist "%TRINO_COMPOSE_FILE%" (
    echo [ERRO] docker-compose.yml nao encontrado: "%TRINO_COMPOSE_FILE%"
    echo        Defina TRINO_COMPOSE_FILE com o caminho correto do arquivo compose.
    exit /b 1
)

docker compose version >nul 2>&1
if errorlevel 1 (
    echo [ERRO] Docker Compose v2 nao disponivel (comando: docker compose).
    exit /b 1
)

exit /b 0
