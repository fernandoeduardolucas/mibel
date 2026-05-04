"""Workflows Flyte para orquestrar as pipelines Medallion deste repositório.

Objetivo:
- Reutilizar os runners já existentes para `producao_consumo` e `consumo_preco`.
- Permitir execução local (`python`) e com Flyte (`pyflyte run`).

Execução local:
    python 02_medallion_pipeline/producao_consumo/flyte_workflow.py

Execução Flyte (remoto):
    pyflyte run --remote -p flytesnacks -d development \
      02_medallion_pipeline/producao_consumo/flyte_workflow.py medallion_full_wf
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


def _run_python(script_path: Path, *, build: bool = False) -> str:
    cmd = [sys.executable, str(script_path)]
    if build:
        cmd.append("--build")

    result = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        text=True,
        capture_output=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Falha ao executar {script_path}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )

    return f"OK: {script_path.name}"


@task
def run_producao_consumo_medallion(build: bool = False) -> str:
    script = REPO_ROOT / "02_medallion_pipeline" / "producao_consumo" / "run_medallion_pipeline.py"
    return _run_python(script, build=build)


@task
def run_consumo_preco_medallion(build: bool = False) -> str:
    script = (
        REPO_ROOT
        / "02_medallion_pipeline"
        / "consumo_preco"
        / "run_medallion_consumo_precos.py"
    )
    return _run_python(script, build=build)


@task(retries=2)
def run_producao_consumo_backfill(days: int = 1, build: bool = False) -> str:
    """Executa reruns simples da pipeline como estratégia de backfill.

    Nota: o runner atual ainda não recebe janela temporal; por isso o backfill
    é implementado como reruns idempotentes para recuperar falhas recentes.
    """
    if days < 1:
        raise ValueError("days tem de ser >= 1")

    outputs: list[str] = []
    for day_index in range(days):
        result = run_producao_consumo_medallion(build=build)
        outputs.append(f"d-{day_index}: {result}")
    return " | ".join(outputs)


@task
def run_selected_pipelines(
    run_producao_consumo: bool = True,
    run_consumo_preco: bool = True,
    build_images: bool = False,
    producao_consumo_backfill_days: int = 0,
) -> str:
    outputs: list[str] = []

    if run_producao_consumo:
        outputs.append(_run_python(
            REPO_ROOT / "02_medallion_pipeline" / "producao_consumo" / "run_medallion_pipeline.py",
            build=build_images,
        ))

    if run_consumo_preco:
        outputs.append(_run_python(
            REPO_ROOT / "02_medallion_pipeline" / "consumo_preco" / "run_medallion_consumo_precos.py",
            build=build_images,
        ))

    if producao_consumo_backfill_days > 0:
        if producao_consumo_backfill_days < 1:
            raise ValueError("producao_consumo_backfill_days tem de ser >= 1")

        for day_index in range(producao_consumo_backfill_days):
            result = _run_python(
                REPO_ROOT / "02_medallion_pipeline" / "producao_consumo" / "run_medallion_pipeline.py",
                build=build_images,
            )
            outputs.append(f"backfill_d-{day_index}: {result}")

    if not outputs:
        return "Nada para executar: ativa pelo menos uma pipeline."

    return " | ".join(outputs)


@workflow
def medallion_full_wf(
    run_producao_consumo: bool = True,
    run_consumo_preco: bool = True,
    build_images: bool = False,
    producao_consumo_backfill_days: int = 0,
) -> str:
    return run_selected_pipelines(
        run_producao_consumo=run_producao_consumo,
        run_consumo_preco=run_consumo_preco,
        build_images=build_images,
        producao_consumo_backfill_days=producao_consumo_backfill_days,
    )


if __name__ == "__main__":
    print(
        medallion_full_wf(
            run_producao_consumo=True,
            run_consumo_preco=True,
            build_images=False,
        )
    )
