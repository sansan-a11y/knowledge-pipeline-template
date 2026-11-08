"""The user-facing ``km`` command line interface."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Annotated

import typer

from .errors import PipelineError
from .manifest import (
    STAGE_CONTRACTS,
    compiled_manifest,
    load_pipeline,
)
from .models import STAGE_ORDER, Stage
from .runner import PipelineRunner, StageState
from .scaffold import scaffold_files

app = typer.Typer(
    name="km",
    help="Manifest-driven knowledge pipeline SDK",
    no_args_is_help=True,
    add_completion=False,
)


def _echo_error(exc: PipelineError) -> None:
    payload = exc.to_dict()
    typer.echo(json.dumps(payload, ensure_ascii=False, default=str, indent=2))
    details = exc.details.get("errors") or exc.details.get("violations")
    if details:
        for detail in details:
            typer.echo(f"- {detail}")


def _load(
    manifest: Path,
    project_root: Path | None,
    *,
    check_source: bool = True,
):
    return load_pipeline(
        manifest,
        project_root=project_root,
        check_source=check_source,
    )


def _default_manifest(manifest: Path | None) -> Path:
    return (manifest or Path.cwd() / "pipeline.yaml").resolve()


def _default_project_root(project_root: Path | None, manifest: Path) -> Path:
    return (project_root or manifest.parent).resolve()


@app.command("init")
def init_pipeline(
    pipeline_name: Annotated[
        str, typer.Argument(help="Pipeline id; hyphens are converted to underscores")
    ],
    directory: Annotated[
        Path | None, typer.Option("--directory", "-d", help="Output directory")
    ] = None,
    force: Annotated[
        bool, typer.Option("--force", help="Overwrite generated files")
    ] = False,
) -> None:
    """Create a runnable manifest and six handler stubs."""

    pipeline_id = pipeline_name.replace("-", "_")
    if not re.fullmatch(r"[a-z][a-z0-9_]{2,63}", pipeline_id):
        typer.echo(
            "Pipeline id must match [a-z][a-z0-9_]{2,63} after converting hyphens.",
            err=True,
        )
        raise typer.Exit(code=2)
    destination = (directory or Path.cwd() / pipeline_id).resolve()
    files = scaffold_files(pipeline_id)
    existing = [destination / relative for relative in files if (destination / relative).exists()]
    if existing and not force:
        typer.echo(f"Refusing to overwrite existing generated files in {destination}", err=True)
        raise typer.Exit(code=1)
    for relative, content in files.items():
        path = destination / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    typer.echo(f"Created {pipeline_id} at {destination}")
    typer.echo(f"Next: cd {destination}; km validate; km test")


@app.command("validate")
def validate_command(
    manifest: Annotated[
        Path | None, typer.Option("--manifest", "-m", help="pipeline.yaml path")
    ] = None,
    project_root: Annotated[
        Path | None, typer.Option("--project-root", help="Handler import root")
    ] = None,
) -> None:
    """Validate manifest, source policy, and all six handler signatures."""

    path = _default_manifest(manifest)
    try:
        pipeline = _load(path, _default_project_root(project_root, path))
    except PipelineError as exc:
        _echo_error(exc)
        raise typer.Exit(code=1) from exc
    for stage in STAGE_ORDER:
        handler = pipeline.handlers[stage]
        typer.echo(f"{stage.value:<11} {handler.entrypoint}")
    typer.echo(
        f"Valid: {pipeline.manifest.pipeline_id} "
        f"{pipeline.manifest.pipeline_version} (manifest v1)"
    )


@app.command("test")
def test_command(
    manifest: Annotated[
        Path | None, typer.Option("--manifest", "-m", help="pipeline.yaml path")
    ] = None,
    project_root: Annotated[
        Path | None, typer.Option("--project-root")
    ] = None,
) -> None:
    """Run a fixture smoke test and prove validation blocks publish."""

    path = _default_manifest(manifest)
    root = _default_project_root(project_root, path)
    try:
        pipeline = _load(path, root)
        request_fixture = pipeline.manifest.spec.fixtures.request
        if not request_fixture:
            raise PipelineError(
                code="E_MANIFEST_INVALID",
                message="spec.fixtures.request is required for km test",
            )
        runner = PipelineRunner(pipeline)
        report = runner.run(root / request_fixture, persist=False)
        if report.status != StageState.SUCCEEDED:
            failed = next(
                (record for record in report.stages if record.status == StageState.FAILED),
                None,
            )
            if failed and failed.error:
                typer.echo(json.dumps(failed.error, ensure_ascii=False, indent=2))
            raise typer.Exit(code=1)
        failure_report = runner.run(
            root / request_fixture,
            options_override={"forceValidationFailure": True},
            persist=False,
        )
        if (
            failure_report.stage(Stage.VALIDATION).status != StageState.FAILED
            or failure_report.stage(Stage.PUBLISH).status != StageState.BLOCKED
        ):
            typer.echo(
                "Validation failure contract test failed: publish was not blocked.",
                err=True,
            )
            raise typer.Exit(code=1)
    except PipelineError as exc:
        _echo_error(exc)
        raise typer.Exit(code=1) from exc
    typer.echo("PASS sample smoke test")
    typer.echo("PASS validation failure blocks publish")


@app.command("run")
def run_command(
    input_file: Annotated[
        Path,
        typer.Option("--input", "-i", help="IngestRequest JSON fixture"),
    ],
    manifest: Annotated[
        Path | None, typer.Option("--manifest", "-m")
    ] = None,
    project_root: Annotated[
        Path | None, typer.Option("--project-root")
    ] = None,
    task_id: Annotated[
        str | None, typer.Option("--task-id", help="Stable business task id")
    ] = None,
    repair_from: Annotated[
        Stage | None, typer.Option("--repair-from", help="First stage to execute")
    ] = None,
) -> None:
    """Execute all six stages with the local artifact and fake resources."""

    path = _default_manifest(manifest)
    root = _default_project_root(project_root, path)

    def on_event(event) -> None:
        if event.state in {
            StageState.RUNNING,
            StageState.SUCCEEDED,
            StageState.FAILED,
            StageState.BLOCKED,
        }:
            typer.echo(f"{event.stage.value:<11} {event.state.value}")

    try:
        runner = PipelineRunner(path, project_root=root)
        report = runner.run(
            input_file,
            task_id=task_id,
            repair_from=repair_from,
            event_callback=on_event,
        )
    except PipelineError as exc:
        _echo_error(exc)
        raise typer.Exit(code=1) from exc
    typer.echo("")
    for record in report.stages:
        duration = (
            f"{record.duration_ms / 1000:.2f}s"
            if record.duration_ms is not None
            else "-"
        )
        typer.echo(f"{record.stage.value:<11} {record.status.value:<10} {duration}")
        if record.error:
            error = record.error["error"]
            typer.echo(f"  {error['code']}: {error['user_message']}")
            typer.echo(f"  action: {error['action']}")
    typer.echo(f"task {report.task_id}: {report.status.value}")
    if report.status != StageState.SUCCEEDED:
        raise typer.Exit(code=1)


@app.command("inspect")
def inspect_command(
    last_run: Annotated[
        bool, typer.Option("--last-run", help="Inspect the most recent local run")
    ] = False,
    fixes: Annotated[
        bool, typer.Option("--fixes", help="Show repair commands")
    ] = False,
    project_root: Annotated[
        Path | None, typer.Option("--project-root")
    ] = None,
) -> None:
    """Show stage status, errors, and repair entrypoints."""

    root = (project_root or Path.cwd()).resolve()
    runner = PipelineRunner(root / "pipeline.yaml", project_root=root)
    try:
        report = runner.load_last_report()
    except FileNotFoundError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Task: {report.task_id} ({report.status.value})")
    for record in report.stages:
        duration = f"{record.duration_ms} ms" if record.duration_ms is not None else "-"
        typer.echo(f"{record.stage.value:<11} {record.status.value:<10} {duration}")
        if record.summary.get("warnings"):
            typer.echo(f"  warnings: {record.summary['warnings']}")
        if record.error:
            error = record.error["error"]
            typer.echo(f"  {error['code']}: {error['user_message']}")
            typer.echo(f"  action: {error['action']}")
    if fixes:
        typer.echo("")
        typer.echo("Repair:")
        for record in report.stages:
            if record.status in {StageState.FAILED, StageState.BLOCKED}:
                typer.echo(
                    f"km run --input fixtures/request.json "
                    f"--repair-from {record.stage.value}"
                )


@app.command("diagnose")
def diagnose_command(
    manifest: Annotated[
        Path | None, typer.Option("--manifest", "-m")
    ] = None,
    project_root: Annotated[
        Path | None, typer.Option("--project-root")
    ] = None,
) -> None:
    """Check the local manifest, imports, fixtures, and SDK compatibility."""

    path = _default_manifest(manifest)
    root = _default_project_root(project_root, path)
    typer.echo(f"manifest: {path}")
    typer.echo(f"project root: {root}")
    try:
        pipeline = _load(path, root)
    except PipelineError as exc:
        _echo_error(exc)
        raise typer.Exit(code=1) from exc
    typer.echo(
        f"SDK: {pipeline.manifest.apiVersion} / contract "
        f"{pipeline.manifest.spec.runtime.contractVersion}"
    )
    for stage in STAGE_ORDER:
        config = pipeline.manifest.spec.stages.get(stage)
        typer.echo(
            f"{stage.value:<11} import=ok "
            f"timeout={config.timeoutSeconds:g}s retry="
            f"{config.retry.maxRetries if config.retry else 0}"
        )
    for name in ("request", "source"):
        fixture = getattr(pipeline.manifest.spec.fixtures, name)
        if fixture:
            typer.echo(f"fixture {name}: ok ({root / fixture})")
    for resource_name in ("llm", "embedding", "index"):
        resource = getattr(pipeline.manifest.spec.resources, resource_name)
        typer.echo(
            f"resource {resource_name}: "
            f"{resource.profile if resource else 'not declared'}"
        )


@app.command("definitions-check")
def definitions_check_command(
    manifest: Annotated[
        Path | None, typer.Option("--manifest", "-m")
    ] = None,
    project_root: Annotated[
        Path | None, typer.Option("--project-root")
    ] = None,
) -> None:
    """Generate Definitions and verify the fixed Dagster runtime contract."""

    path = _default_manifest(manifest)
    root = _default_project_root(project_root, path)
    try:
        from .dagster_runtime import build_definitions

        defs = build_definitions(path, project_root=root)
    except PipelineError as exc:
        _echo_error(exc)
        raise typer.Exit(code=1) from exc
    typer.echo(
        f"Definitions valid: {len(defs.assets or [])} assets, "
        f"{len(defs.jobs or [])} jobs"
    )


@app.command("compile")
def compile_command(
    manifest: Annotated[
        Path | None, typer.Option("--manifest", "-m")
    ] = None,
    output: Annotated[
        Path | None, typer.Option("--output", "-o", help="compiled-manifest.json path")
    ] = None,
    project_root: Annotated[
        Path | None, typer.Option("--project-root")
    ] = None,
) -> None:
    """Write the platform registration artifact."""

    path = _default_manifest(manifest)
    root = _default_project_root(project_root, path)
    try:
        pipeline = _load(path, root)
    except PipelineError as exc:
        _echo_error(exc)
        raise typer.Exit(code=1) from exc
    payload = compiled_manifest(pipeline.manifest)
    destination = output or root / "compiled-manifest.json"
    destination.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    typer.echo(f"Wrote {destination}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
