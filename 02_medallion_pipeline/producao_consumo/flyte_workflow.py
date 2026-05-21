"""Workflows Flyte para orquestrar a pipeline Medallion de producao_consumo.

Objetivo:
- Reutilizar o runner run_medallion_pipeline.py já existente.
- Permitir execução local (`python`) e com Flyte (`pyflyte run`).

Execução local:
    python 02_medallion_pipeline/producao_consumo/flyte_workflow.py

Execução Flyte (remoto):
    pyflyte run --remote -p flytesnacks -d development \\
      02_medallion_pipeline/producao_consumo/flyte_workflow.py medallion_full_wf

Backfill de intervalo específico:
    pyflyte run --remote -p flytesnacks -d development \\
      02_medallion_pipeline/producao_consumo/flyte_workflow.py medallion_backfill_wf \\
      --date_from 2023-01-01 --date_to 2023-06-30
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

try:
    from flytekit import task, workflow
except ModuleNotFoundError:
    def task(*args, **kwargs):
        def decorator(func):
            return func

        if args and callable(args[0]) and len(args) == 1 and not kwargs:
            return args[0]
        return decorator

    def workflow(func=None, **kwargs):
        if func is not None and callable(func):
            return func

        def decorator(f):
            return f

        return decorator


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNNER    = REPO_ROOT / "02_medallion_pipeline" / "producao_consumo" / "run_medallion_pipeline.py"


def _run_runner(extra_args: list[str]) -> str:
    cmd = [sys.executable, str(RUNNER), "--skip-docker"] + extra_args
    print(f"\n>>> {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        text=True,
        capture_output=True,
    )
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        raise RuntimeError(
            f"Falha ao executar runner producao_consumo\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    return f"OK: {RUNNER.name}"


@task(retries=3)
def run_producao_consumo_medallion(build: bool = False) -> str:
    extra: list[str] = []
    if build:
        extra.append("--build")
    return _run_runner(extra)


@task(retries=3)
def run_producao_consumo_backfill(
    date_from: str = "2023-01-01",
    date_to:   str = "2023-12-31",
) -> str:
    return _run_runner([
        "--skip-ddl",
        "--date-from", date_from,
        "--date-to",   date_to,
    ])


@task(retries=3)
def run_consumo_preco_medallion(build: bool = False) -> str:
    script = (
        REPO_ROOT
        / "02_medallion_pipeline"
        / "consumo_preco"
        / "run_medallion_consumo_precos.py"
    )
    cmd = [sys.executable, str(script), "--skip-docker"]
    if build:
        cmd.append("--build")
    print(f"\n>>> {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(REPO_ROOT), text=True, capture_output=True)
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        raise RuntimeError(
            f"Falha ao executar runner consumo_preco\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    return f"OK: {script.name}"


@workflow
def medallion_full_wf(build_images: bool = False) -> str:
    """Executa as duas pipelines sequencialmente (producao_consumo → consumo_preco)."""
    result_pc = run_producao_consumo_medallion(build=build_images)
    result_cp = run_consumo_preco_medallion(build=build_images)
    return result_pc + " | " + result_cp


@workflow
def medallion_backfill_wf(
    date_from: str = "2023-01-01",
    date_to:   str = "2023-12-31",
) -> str:
    """Executa backfill incremental Gold para o intervalo especificado."""
    return run_producao_consumo_backfill(date_from=date_from, date_to=date_to)


if __name__ == "__main__":
    print(medallion_full_wf(build_images=False))
