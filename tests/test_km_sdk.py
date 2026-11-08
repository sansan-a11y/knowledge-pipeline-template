"""Contract and integration tests for the manifest-driven KM SDK."""

from __future__ import annotations

import time
from pathlib import Path

import dagster as dg
import pytest
from pydantic import ValidationError

from km_sdk import (
    IngestRequest,
    ParsedChunk,
    ParsedDocument,
    Stage,
    StageContext,
    ValidationReport,
    ValidationRuleResult,
)
from km_sdk.artifacts import ArtifactClient
from km_sdk.dagster_runtime import build_definitions
from km_sdk.errors import PipelineError
from km_sdk.manifest import (
    LoadedPipeline,
    load_pipeline,
    scan_project_source,
    validate_handler_signature,
    validate_manifest_data,
)
from km_sdk.resources import LocalKnowledgeIndex
from km_sdk.runner import PipelineRunner, StageState
from km_sdk.scaffold import scaffold_files


PROJECT_ROOT = Path(__file__).resolve().parents[1] / "pipelines" / "manual_extraction"
MANIFEST_PATH = PROJECT_ROOT / "pipeline.yaml"
REQUEST_PATH = PROJECT_ROOT / "fixtures" / "request.json"
RUN_TAGS = {
    "tenant": "demo-tenant",
    "pipeline_id": "manual_extraction",
    "doc_id": "demo",
    "doc_version": "v1",
}


def _runner(tmp_path: Path, *, index=None) -> PipelineRunner:
    return PipelineRunner(
        MANIFEST_PATH,
        project_root=PROJECT_ROOT,
        artifacts=ArtifactClient(tmp_path / "artifacts"),
        index=index,
    )


def test_models_round_trip_and_validation_report_consistency():
    parsed = ParsedDocument(
        doc_id="doc",
        doc_version="v1",
        source_hash="sha",
        parser_version="v1",
        chunks=[ParsedChunk(chunk_id="c1", text="hello", page=1)],
    )
    assert ParsedDocument.model_validate_json(parsed.model_dump_json()) == parsed

    rule = ValidationRuleResult(
        rule_id="R1",
        passed=False,
        severity="ERROR",
        threshold=0,
        observed=1,
    )
    with pytest.raises(ValidationError, match="blocking_failures"):
        ValidationReport(
            doc_id="doc",
            doc_version="v1",
            policy_version="v1",
            status="failed",
            blocking_failures=0,
            warnings=0,
            rules=[rule],
        )


def test_manifest_schema_rejects_missing_stage_and_secret():
    pipeline = load_pipeline(MANIFEST_PATH, project_root=PROJECT_ROOT)
    data = pipeline.manifest.model_dump(mode="json")
    del data["spec"]["stages"]["publish"]
    with pytest.raises(PipelineError) as missing:
        validate_manifest_data(data)
    assert missing.value.code == "E_MANIFEST_INVALID"
    assert any("publish" in error for error in missing.value.details["errors"])

    data = pipeline.manifest.model_dump(mode="json")
    data["spec"]["options"]["api_token"] = "plain-text-secret"
    with pytest.raises(PipelineError, match="invalid"):
        validate_manifest_data(data)


def test_handler_signature_rejects_wrong_names_and_async():
    def wrong(ctx: StageContext, payload: IngestRequest):
        return None

    with pytest.raises(PipelineError) as error:
        validate_handler_signature(Stage.INGEST, wrong)
    assert error.value.code == "E_HANDLER_SIGNATURE"
    assert "expected parameters" in error.value.message

    async def asynchronous(
        ctx: StageContext,
        request: IngestRequest,
    ):
        return None

    with pytest.raises(PipelineError, match="async"):
        validate_handler_signature(Stage.INGEST, asynchronous)


def test_source_policy_rejects_dagster_and_environment_access(tmp_path):
    (tmp_path / "handlers.py").write_text(
        "import dagster as dg\n"
        "import os\n"
        "VALUE = os.environ.get('SECRET')\n",
        encoding="utf-8",
    )
    with pytest.raises(PipelineError) as error:
        scan_project_source(tmp_path)
    assert error.value.code == "E_MANIFEST_INVALID"
    assert len(error.value.details["errors"]) == 2


def test_init_scaffold_is_syntactically_valid():
    files = scaffold_files("sample_pipeline")
    for path, content in files.items():
        if path.endswith(".py"):
            compile(content, path, "exec")
    assert {
        "pipeline.yaml",
        "handlers.py",
        "fixtures/request.json",
        "fixtures/sample.pdf",
        "tests/test_handlers.py",
        "README.md",
        "GETTING_STARTED.md",
        "HANDLER_API.md",
        "TROUBLESHOOTING.md",
    } == set(files)


def test_full_pipeline_repair_and_validation_gate(tmp_path):
    runner = _runner(tmp_path)
    report = runner.run(REQUEST_PATH, persist=False)
    assert report.status == StageState.SUCCEEDED
    assert {record.status for record in report.stages} == {StageState.SUCCEEDED}
    assert report.stage(Stage.VALIDATION).summary["blocking_failures"] == 0
    assert report.stage(Stage.PUBLISH).summary["is_visible"] is True

    failure = runner.run(
        REQUEST_PATH,
        options_override={"forceValidationFailure": True},
        persist=False,
    )
    assert failure.stage(Stage.VALIDATION).status == StageState.FAILED
    assert failure.stage(Stage.PUBLISH).status == StageState.BLOCKED
    assert failure.stage(Stage.VALIDATION).error["error"]["code"] == "E_VALIDATION_FAILED"

    repaired = runner.run(REQUEST_PATH, repair_from=Stage.PARSE, persist=False)
    assert repaired.status == StageState.SUCCEEDED
    assert repaired.stage(Stage.INGEST).status == StageState.SKIPPED
    assert repaired.stage(Stage.PARSE).attempt >= 2


def test_publish_is_idempotent_and_failure_preserves_visible_version(tmp_path):
    artifacts = ArtifactClient(tmp_path / "artifacts")
    first = PipelineRunner(
        MANIFEST_PATH,
        project_root=PROJECT_ROOT,
        artifacts=artifacts,
    )
    report = first.run(REQUEST_PATH, persist=False)
    visible = LocalKnowledgeIndex(artifacts).get_visible("demo", "v1")
    assert visible is not None
    assert visible.is_visible is True
    original_id = visible.publication_id

    rerun = first.run(REQUEST_PATH, persist=False)
    assert rerun.status == StageState.SUCCEEDED
    assert rerun.stage(Stage.PUBLISH).summary["publication_id"] == original_id

    failing = PipelineRunner(
        MANIFEST_PATH,
        project_root=PROJECT_ROOT,
        artifacts=artifacts,
        index=LocalKnowledgeIndex(artifacts, fail_before_switch=True),
    )
    failed = failing.run(REQUEST_PATH, persist=False)
    assert failed.stage(Stage.PUBLISH).status == StageState.FAILED
    assert failed.stage(Stage.PUBLISH).error["error"]["code"] == "E_PUBLISH_ROLLBACK"
    still_visible = LocalKnowledgeIndex(artifacts).get_visible("demo", "v1")
    assert still_visible is not None
    assert still_visible.publication_id == original_id


def test_unknown_exception_and_timeout_are_mapped(tmp_path):
    runner = _runner(tmp_path)
    original = runner.pipeline.handlers[Stage.INGEST].function

    def broken(ctx, request):
        raise RuntimeError("boom")

    runner.pipeline.handlers[Stage.INGEST].function = broken
    try:
        report = runner.run(REQUEST_PATH, persist=False)
    finally:
        runner.pipeline.handlers[Stage.INGEST].function = original
    assert report.stage(Stage.INGEST).error["error"]["code"] == "E_HANDLER_BUG"

    wrong_runner = _runner(tmp_path / "wrong-return")
    original = wrong_runner.pipeline.handlers[Stage.INGEST].function

    def wrong_return(ctx, request):
        return {"not": "an IngestedDocument"}

    wrong_runner.pipeline.handlers[Stage.INGEST].function = wrong_return
    try:
        report = wrong_runner.run(REQUEST_PATH, persist=False)
    finally:
        wrong_runner.pipeline.handlers[Stage.INGEST].function = original
    assert report.stage(Stage.INGEST).error["error"]["code"] == "E_DATA_CONTRACT"

    timeout_runner = _runner(tmp_path / "timeout")
    original = timeout_runner.pipeline.handlers[Stage.INGEST].function
    original_timeout = timeout_runner.manifest.spec.stages.ingest.timeoutSeconds

    def slow(ctx, request):
        time.sleep(0.2)
        return original(ctx, request)

    timeout_runner.pipeline.handlers[Stage.INGEST].function = slow
    timeout_runner.manifest.spec.stages.ingest.timeoutSeconds = 0.01
    try:
        report = timeout_runner.run(REQUEST_PATH, persist=False)
    finally:
        timeout_runner.pipeline.handlers[Stage.INGEST].function = original
        timeout_runner.manifest.spec.stages.ingest.timeoutSeconds = original_timeout
    assert report.stage(Stage.INGEST).error["error"]["code"] == "E_STAGE_TIMEOUT"


def test_dagster_definitions_and_materialization(tmp_path):
    pipeline = load_pipeline(MANIFEST_PATH, project_root=PROJECT_ROOT)
    defs = build_definitions(
        pipeline,
        artifact_root=tmp_path / "artifacts",
        check_source=False,
    )
    assert len(defs.assets or []) == 6
    assert len(defs.jobs or []) == 7

    instance = dg.DagsterInstance.ephemeral()
    partitions_def = next(iter(defs.assets)).partitions_def
    instance.add_dynamic_partitions(partitions_def.name, ["demo__v1"])
    result = defs.resolve_job_def("manual_extraction__full").execute_in_process(
        instance=instance,
        partition_key="demo__v1",
        tags=RUN_TAGS,
    )
    assert result.success
    checks = {
        check.check_name: check.passed
        for check in result.get_asset_check_evaluations()
    }
    assert checks == {
        "source_contract": True,
        "chunk_contract": True,
        "extraction_contract": True,
        "draft_contract": True,
        "publication_gate_passed": True,
        "publication_committed": True,
    }
    observations = instance.get_records_for_run(
        result.run_id,
        of_type=dg.DagsterEventType.ASSET_OBSERVATION,
    ).records
    started = {
        record.asset_observation.metadata["km_stage_id"].value
        for record in observations
        if record.asset_observation
        and record.asset_observation.metadata.get("km_stage_state")
        and record.asset_observation.metadata["km_stage_state"].value == "STARTED"
    }
    assert started == {stage.value for stage in Stage}


def test_dagster_validation_failure_blocks_publish(tmp_path):
    pipeline = load_pipeline(MANIFEST_PATH, project_root=PROJECT_ROOT)
    manifest = pipeline.manifest.model_copy(deep=True)
    manifest.spec.stages.validation.options["forceValidationFailure"] = True
    failed_pipeline = LoadedPipeline(
        manifest=manifest,
        manifest_path=pipeline.manifest_path,
        project_root=pipeline.project_root,
        handlers=pipeline.handlers,
    )
    defs = build_definitions(
        failed_pipeline,
        artifact_root=tmp_path / "artifacts",
        check_source=False,
    )
    instance = dg.DagsterInstance.ephemeral()
    partitions_def = next(iter(defs.assets)).partitions_def
    instance.add_dynamic_partitions(partitions_def.name, ["demo__v1"])
    result = defs.resolve_job_def("manual_extraction__full").execute_in_process(
        instance=instance,
        partition_key="demo__v1",
        raise_on_error=False,
        tags=RUN_TAGS,
    )
    assert result.success is False
    checks = {
        check.check_name: check.passed
        for check in result.get_asset_check_evaluations()
    }
    assert checks["publication_gate_passed"] is False
    assert "publication_committed" not in checks
    published_key = dg.AssetKey(
        ["km", "manual_extraction", "published_document"]
    )
    materialized = {
        event.asset_key
        for event in result.get_asset_materialization_events()
    }
    assert published_key not in materialized
