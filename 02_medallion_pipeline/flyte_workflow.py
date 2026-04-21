"""Workflows Flyte para orquestrar as pipelines Medallion deste repositório.

Objetivo:
- Reutilizar os runners já existentes para `producao_consumo` e `consumo_preco`.
- Permitir execução local (`python`) e com Flyte (`pyflyte run`).

Execução local:
    python 02_medallion_pipeline/flyte_workflow.py

Execução Flyte (remoto):
    pyflyte run --remote -p flytesnacks -d development \
      02_medallion_pipeline/flyte_workflow.py medallion_full_wf
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


REPO_ROOT = Path(__file__).resolve().parent.parent


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


@workflow
def medallion_full_wf(
    run_producao_consumo: bool = True,
    run_consumo_preco: bool = True,
    build_images: bool = False,
) -> str:
    outputs: list[str] = []

    if run_producao_consumo:
        outputs.append(run_producao_consumo_medallion(build=build_images))

    if run_consumo_preco:
        outputs.append(run_consumo_preco_medallion(build=build_images))

    if not outputs:
        return "Nada para executar: ativa pelo menos uma pipeline."

    return " | ".join(outputs)


if __name__ == "__main__":
    print(
        medallion_full_wf(
            run_producao_consumo=True,
            run_consumo_preco=True,
            build_images=False,
        )
    )
