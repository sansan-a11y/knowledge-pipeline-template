"""Generate Dagster Definitions from a validated pipeline manifest."""

import hashlib
import json
from contextlib import contextmanager
from pathlib import Path
import time
from typing import Any, Iterator, Mapping

import dagster as dg
from dagster import ConfigurableResource

from .artifacts import ArtifactClient
from .errors import PipelineError
from .manifest import CONTRACT_VERSION, LoadedPipeline, PipelineManifest, load_pipeline
from .models import (
    STAGE_ORDER,
    TERMINAL_ASSETS,
    AssembledDocument,
    ExtractionResult,
    IngestedDocument,
    IngestRequest,
    ParsedDocument,
    PublishedDocument,
    Stage,
    ValidationReport,
)
from .resources import LocalKnowledgeIndex, LocalLlmGateway
from .runner import PipelineArtifactStore, PipelineRunner


class ArtifactStoreResource(ConfigurableResource):
    """Dagster resource wrapper for the local atomic ArtifactClient."""

    root_dir: str = ".km/artifacts"

    def client(self) -> ArtifactClient:
        return ArtifactClient(self.root_dir)


class LlmResource(ConfigurableResource):
    """Default local LLM resource; platforms may replace this resource key."""

    profile: str = "llm-default"

    def structured_extraction(self, parsed: ParsedDocument, *, options: Mapping[str, object]):
        return LocalLlmGateway().structured_extraction(parsed, options=options)


class IndexResource(ConfigurableResource):
    """Default local index resource; platforms may replace this resource key."""

    profile: str = "knowledge-index-default"
    root_dir: str = ".km/artifacts"

    def _index(self) -> LocalKnowledgeIndex:
        return LocalKnowledgeIndex(ArtifactClient(self.root_dir))

    def commit(
        self,
        document: PublishedDocument,
        *,
        assembled: AssembledDocument,
        validation_report: ValidationReport,
    ) -> None:
        self._index().commit(
            document,
            assembled=assembled,
            validation_report=validation_report,
        )

    def is_visible(self, publication_id: str) -> bool:
        return self._index().is_visible(publication_id)


class ProfileResource(ConfigurableResource):
    profile: str = "default"


CHECK_NAMES: dict[Stage, str] = {
    Stage.INGEST: "source_contract",
    Stage.PARSE: "chunk_contract",
    Stage.EXTRACTION: "extraction_contract",
    Stage.ASSEMBLE: "draft_contract",
    Stage.VALIDATION: "publication_gate_passed",
    Stage.PUBLISH: "publication_committed",
}

UPSTREAM: dict[Stage, tuple[Stage, ...]] = {
    Stage.INGEST: (),
    Stage.PARSE: (Stage.INGEST,),
    Stage.EXTRACTION: (Stage.PARSE,),
    Stage.ASSEMBLE: (Stage.PARSE, Stage.EXTRACTION),
    Stage.VALIDATION: (Stage.PARSE, Stage.ASSEMBLE),
    Stage.PUBLISH: (Stage.ASSEMBLE, Stage.VALIDATION),
}


def terminal_asset_key(pipeline_id: str, stage: Stage) -> dg.AssetKey:
    return dg.AssetKey(["km", pipeline_id, TERMINAL_ASSETS[stage]])


def canonical_group(pipeline_id: str, stage: Stage) -> str:
    return f"km__{pipeline_id}__{stage.value}"


def _emit_observation(
    context: dg.AssetExecutionContext,
    *,
    stage: Stage,
    state: str,
    attempt: int,
    pipeline_id: str,
    metadata: Mapping[str, object] | None = None,
) -> None:
    payload = {
        "km_signal_version": 1,
        "km_pipeline_id": pipeline_id,
        "km_stage_id": stage.value,
        "km_stage_attempt": attempt,
        "km_stage_state": state,
        **dict(metadata or {}),
    }
    try:
        context.log_event(
            dg.AssetObservation(
                asset_key=context.asset_key,
                partition=context.partition_key,
                metadata=payload,
                description=f"{stage.value} {state.lower()}",
            )
        )
    except Exception:
        context.log.warning(
            "Unable to emit stage observation for %s",
            stage.value,
            exc_info=True,
        )


@contextmanager
def _stage_signal(
    context: dg.AssetExecutionContext,
    *,
    stage: Stage,
    pipeline_id: str,
    attempt: int,
) -> Iterator[None]:
    _emit_observation(
        context,
        stage=stage,
        state="STARTED",
        attempt=attempt,
        pipeline_id=pipeline_id,
    )
    started = time.monotonic()
    try:
        yield
    except Exception as exc:
        code = getattr(exc, "code", "E_HANDLER_BUG")
        _emit_observation(
            context,
            stage=stage,
            state="FAILED",
            attempt=attempt,
            pipeline_id=pipeline_id,
            metadata={
                "error_code": code,
                "error_class": type(exc).__name__,
                "duration_ms": int((time.monotonic() - started) * 1000),
            },
        )
        raise


def _summary(stage: Stage, output: object) -> dict[str, object]:
    return PipelineRunner._summary(stage, output)  # type: ignore[arg-type]


def _check_passed(stage: Stage, output: object) -> bool:
    if stage == Stage.INGEST:
        value: IngestedDocument = output  # type: ignore[assignment]
        return bool(value.content_hash and value.source_uri and value.byte_size >= 0)
    if stage == Stage.PARSE:
        value = output  # type: ignore[assignment]
        return bool(value.chunks) and all(chunk.text for chunk in value.chunks)
    if stage == Stage.EXTRACTION:
        value = output  # type: ignore[assignment]
        ids = {entity.id for entity in value.entities}
        return bool(value.entities) and all(
            relation.source_id in ids and relation.target_id in ids
            for relation in value.relations
        )
    if stage == Stage.ASSEMBLE:
        value = output  # type: ignore[assignment]
        return bool(value.entities) and bool(value.citations)
    if stage == Stage.VALIDATION:
        value = output  # type: ignore[assignment]
        return (
            value.status == "passed"
            and value.blocking_failures == 0
            and all(
                rule.passed
                for rule in value.rules
                if rule.severity == "ERROR"
            )
        )
    value = output  # type: ignore[assignment]
    return value.is_visible is True


def _runtime_metadata(
    manifest: PipelineManifest,
    stage: Stage,
    *,
    attempt: int,
    artifact_uri: str,
    summary: Mapping[str, object],
) -> dict[str, object]:
    options_hash = hashlib.sha256(
        json.dumps(
            manifest.merged_options(stage),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()
    return {
        "km_contract_version": CONTRACT_VERSION,
        "km_pipeline_id": manifest.pipeline_id,
        "km_pipeline_version": manifest.pipeline_version,
        "km_stage_id": stage.value,
        "km_stage_role": "terminal",
        "km_stage_attempt": attempt,
        "owner": manifest.metadata.owner,
        "artifact_uri": artifact_uri,
        "options_hash": options_hash,
        **summary,
    }


class DagsterPipelineRuntime:
    """Bridge between a manifest handler and a generated Dagster asset."""

    def __init__(
        self,
        pipeline: LoadedPipeline,
        *,
        resource_defs: Mapping[str, object],
        request_override: IngestRequest | None = None,
    ) -> None:
        self.pipeline = pipeline
        self.manifest = pipeline.manifest
        self.resource_defs = resource_defs
        self.request_override = request_override

    def request_for(
        self,
        context: dg.AssetExecutionContext,
        artifact_store: ArtifactStoreResource,
    ) -> IngestRequest:
        tags = context.run.tags if context.run else {}
        missing_tags = [
            tag
            for tag in self.manifest.spec.partition.requiredTags
            if not tags.get(tag)
        ]
        if missing_tags:
            raise PipelineError(
                code="E_DATA_CONTRACT",
                message=f"run is missing required tags: {', '.join(missing_tags)}",
                stage=Stage.INGEST,
                details={"missing_tags": missing_tags},
            )
        if self.request_override is not None:
            request = self.request_override
        elif tags.get("km_request_json"):
            request = IngestRequest.model_validate_json(
                tags["km_request_json"]
            )
        else:
            fixture = self.manifest.spec.fixtures.request
            if fixture:
                path = self.pipeline.project_root / fixture
                request = IngestRequest.model_validate_json(
                    path.read_text(encoding="utf-8")
                )
            else:
                if not context.has_partition_key:
                    raise PipelineError(
                        code="E_DATA_CONTRACT",
                        message="a document partition is required",
                        stage=Stage.INGEST,
                    )
                doc_id, doc_version = context.partition_key.split("__", maxsplit=1)
                request = IngestRequest(
                    doc_id=doc_id,
                    doc_version=doc_version,
                    tenant=tags.get("tenant", "unknown"),
                    source_uri=tags.get("source_uri", f"partition://{context.partition_key}"),
                    mime_type=tags.get("mime_type"),
                )
        expected_partition = f"{request.doc_id}__{request.doc_version}"
        if context.partition_key != expected_partition:
            raise PipelineError(
                code="E_DATA_CONTRACT",
                message="request fixture does not match the selected document partition",
                stage=Stage.INGEST,
                details={
                    "partition_key": context.partition_key,
                    "request_doc_id": request.doc_id,
                    "request_doc_version": request.doc_version,
                },
            )
        tag_identity = {
            "pipeline_id": (tags.get("pipeline_id"), self.manifest.pipeline_id),
            "doc_id": (tags.get("doc_id"), request.doc_id),
            "doc_version": (tags.get("doc_version"), request.doc_version),
            "tenant": (tags.get("tenant"), request.tenant),
        }
        mismatches = [
            f"{name}: tag={actual!r}, request={expected!r}"
            for name, (actual, expected) in tag_identity.items()
            if actual and actual != expected
        ]
        if mismatches:
            raise PipelineError(
                code="E_DATA_CONTRACT",
                message="run tags do not match the ingest request",
                stage=Stage.INGEST,
                details={"mismatches": mismatches},
            )
        return request

    def execute(
        self,
        context: dg.AssetExecutionContext,
        *,
        stage: Stage,
        artifact_store: ArtifactStoreResource,
        llm: object | None = None,
        index: object | None = None,
    ) -> tuple[PipelineArtifactStore, int, BaseException | None]:
        artifacts = artifact_store.client()
        request = self.request_for(context, artifact_store)
        store = PipelineArtifactStore(
            artifacts,
            pipeline_id=self.manifest.pipeline_id,
            tenant=request.tenant,
            doc_id=request.doc_id,
            doc_version=request.doc_version,
        )
        attempt = store.next_attempt(stage)
        runner = PipelineRunner(
            self.pipeline,
            artifacts=artifacts,
            llm=llm,  # type: ignore[arg-type]
            index=index,  # type: ignore[arg-type]
            check_source=False,
        )
        try:
            with _stage_signal(
                context,
                stage=stage,
                pipeline_id=self.manifest.pipeline_id,
                attempt=attempt,
            ):
                invocation = runner._invoke_once(
                    request,
                    stage=stage,
                    task_id=context.run_id,
                    store=store,
                    options_override=None,
                )
                attempt = invocation.attempt
        except PipelineError as exc:
            return store, attempt, exc
        return store, attempt, None


def _make_asset(
    runtime: DagsterPipelineRuntime,
    *,
    stage: Stage,
    partitions_def: dg.DynamicPartitionsDefinition,
) -> dg.AssetsDefinition:
    manifest = runtime.manifest
    config = manifest.spec.stages.get(stage)
    key = terminal_asset_key(manifest.pipeline_id, stage)
    check_name = CHECK_NAMES[stage]
    retry_policy = None
    if config.retry:
        retry_policy = dg.RetryPolicy(
            max_retries=config.retry.maxRetries,
            delay=config.retry.delaySeconds,
            backoff=(
                dg.Backoff.EXPONENTIAL
                if config.retry.backoff == "exponential"
                else None
            ),
        )

    def generated_asset(
        context: dg.AssetExecutionContext,
    ) -> Iterator[dg.Output | dg.AssetCheckResult]:
        artifact_store: ArtifactStoreResource = context.resources.artifact_store
        llm = context.resources.llm if stage == Stage.EXTRACTION else None
        index = context.resources.index if stage == Stage.PUBLISH else None
        store, attempt, error = runtime.execute(
            context,
            stage=stage,
            artifact_store=artifact_store,
            llm=llm,
            index=index,
        )
        if error is not None:
            if error.code == "E_VALIDATION_FAILED":
                latest = store.load(Stage.VALIDATION, ValidationReport)
                uri = store.latest_output_key(stage)
                if uri is None:
                    raise dg.Failure(
                        description="validation report was not persisted",
                        allow_retries=False,
                    )
                artifact_hash = store.artifacts.sha256(uri)
                context.set_data_version(
                    context.asset_key,
                    dg.DataVersion(f"sha256:{artifact_hash}"),
                )
                yield dg.Output(
                    value=store.artifacts.uri(uri),
                    metadata=_runtime_metadata(
                        manifest,
                        stage,
                        attempt=attempt,
                        artifact_uri=store.artifacts.uri(uri),
                        summary=_summary(stage, latest),
                    ),
                )
                yield dg.AssetCheckResult(
                    check_name=check_name,
                    passed=False,
                    metadata=_summary(stage, latest),
                )
                return
            raise dg.Failure(
                description=f"{error.code}: {error.message}",
                metadata={
                    "error_code": error.code,
                    "stage": stage.value,
                    "action": error.action,
                    **error.details,
                },
                allow_retries=error.retryable,
            ) from error

        latest_key = store.latest_output_key(stage)
        if latest_key is None:
            raise dg.Failure(
                description=f"{stage.value} output artifact was not persisted",
                metadata={"error_code": "E_DATA_CONTRACT"},
                allow_retries=False,
            )
        output_type = runtime.pipeline and {
            Stage.INGEST: IngestedDocument,
            Stage.PARSE: ParsedDocument,
            Stage.EXTRACTION: ExtractionResult,
            Stage.ASSEMBLE: AssembledDocument,
            Stage.VALIDATION: ValidationReport,
            Stage.PUBLISH: PublishedDocument,
        }[stage]
        output = output_type.model_validate(store.artifacts.get_json(latest_key))
        artifact_uri = store.artifacts.uri(latest_key)
        summary = _summary(stage, output)
        artifact_hash = store.artifacts.sha256(latest_key)
        context.set_data_version(
            context.asset_key,
            dg.DataVersion(f"sha256:{artifact_hash}"),
        )
        yield dg.Output(
            value=artifact_uri,
            metadata=_runtime_metadata(
                manifest,
                stage,
                attempt=attempt,
                artifact_uri=artifact_uri,
                summary=summary,
            ),
        )
        yield dg.AssetCheckResult(
            check_name=check_name,
            passed=_check_passed(stage, output),
            metadata=summary,
        )

    decorated = dg.asset(
        key=key,
        required_resource_keys={
            "artifact_store",
            *({"llm"} if stage == Stage.EXTRACTION else set()),
            *({"index"} if stage == Stage.PUBLISH else set()),
        },
        partitions_def=partitions_def,
        group_name=canonical_group(manifest.pipeline_id, stage),
        owners=[manifest.metadata.owner],
        kinds={"python", "manifest", "object-store"},
        metadata={
            "km_contract_version": CONTRACT_VERSION,
            "km_pipeline_id": manifest.pipeline_id,
            "km_pipeline_version": manifest.pipeline_version,
            "km_stage_id": stage.value,
            "km_stage_role": "terminal",
            "owner": manifest.metadata.owner,
            "timeout_seconds": config.timeoutSeconds,
        },
        deps=[terminal_asset_key(manifest.pipeline_id, upstream) for upstream in UPSTREAM[stage]],
        check_specs=[
            dg.AssetCheckSpec(
                name=check_name,
                asset=key,
                blocking=True,
            )
        ],
        retry_policy=retry_policy,
    )(generated_asset)
    return decorated


def build_definitions(
    pipeline: LoadedPipeline | str | Path,
    *,
    project_root: str | Path | None = None,
    artifact_root: str | Path | None = None,
    resource_overrides: Mapping[str, object] | None = None,
    request_override: IngestRequest | None = None,
    check_source: bool = True,
) -> dg.Definitions:
    if isinstance(pipeline, (str, Path)):
        loaded = load_pipeline(
            pipeline,
            project_root=project_root,
            check_source=check_source,
        )
    else:
        loaded = pipeline
    manifest = loaded.manifest
    root = str(artifact_root or loaded.project_root / ".km" / "artifacts")
    partitions = dg.DynamicPartitionsDefinition(
        name=f"km__{manifest.pipeline_id}__documents"
    )
    resource_defs: dict[str, object] = {
        "artifact_store": ArtifactStoreResource(root_dir=root),
        "llm": LlmResource(
            profile=(
                manifest.spec.resources.llm.profile
                if manifest.spec.resources.llm
                else "llm-default"
            )
        ),
        "index": IndexResource(
            profile=(
                manifest.spec.resources.index.profile
                if manifest.spec.resources.index
                else "knowledge-index-default"
            ),
            root_dir=root,
        ),
        "embedding": ProfileResource(
            profile=(
                manifest.spec.resources.embedding.profile
                if manifest.spec.resources.embedding
                else "embedding-default"
            )
        ),
    }
    if resource_overrides:
        resource_defs.update(resource_overrides)
    runtime = DagsterPipelineRuntime(
        loaded,
        resource_defs=resource_defs,
        request_override=request_override,
    )
    assets = [
        _make_asset(runtime, stage=stage, partitions_def=partitions)
        for stage in STAGE_ORDER
    ]
    full_job = dg.define_asset_job(
        name=f"{manifest.pipeline_id}__full",
        selection=dg.AssetSelection.assets(
            *[terminal_asset_key(manifest.pipeline_id, stage) for stage in STAGE_ORDER]
        ).upstream(),
    )
    repair_jobs = [
        dg.define_asset_job(
            name=f"{manifest.pipeline_id}__repair_{stage.value}",
            selection=dg.AssetSelection.assets(
                terminal_asset_key(manifest.pipeline_id, stage)
            ),
        )
        for stage in STAGE_ORDER
    ]
    defs = dg.Definitions(
        assets=assets,
        jobs=[full_job, *repair_jobs],
        resources=resource_defs,
        metadata={
            "km_pipeline_id": manifest.pipeline_id,
            "km_pipeline_version": manifest.pipeline_version,
            "km_contract_version": CONTRACT_VERSION,
            "km_handler_api_version": 1,
            "km_manifest_schema_hash": _schema_hash(),
        },
    )
    validate_definitions(defs, manifest)
    return defs


def validate_definitions(
    defs: dg.Definitions,
    manifest: PipelineManifest,
) -> None:
    violations: list[str] = []
    specs: dict[dg.AssetKey, dg.AssetSpec] = {}
    checks: dict[dg.AssetKey, list[dg.AssetCheckSpec]] = {}
    for assets_def in defs.assets or []:
        if not isinstance(assets_def, dg.AssetsDefinition):
            continue
        for spec in assets_def.specs:
            specs[spec.key] = spec
        for check in assets_def.check_specs:
            checks.setdefault(check.asset_key, []).append(check)
    for stage in STAGE_ORDER:
        key = terminal_asset_key(manifest.pipeline_id, stage)
        spec = specs.get(key)
        if spec is None:
            violations.append(f"{stage.value}: missing terminal asset {key.to_user_string()}")
            continue
        if spec.group_name != canonical_group(manifest.pipeline_id, stage):
            violations.append(f"{stage.value}: canonical group mismatch")
        expected_deps = {
            terminal_asset_key(manifest.pipeline_id, upstream)
            for upstream in UPSTREAM[stage]
        }
        actual_deps = {dep.asset_key for dep in spec.deps}
        if actual_deps != expected_deps:
            violations.append(
                f"{stage.value}: expected deps {sorted(k.to_user_string() for k in expected_deps)}, "
                f"found {sorted(k.to_user_string() for k in actual_deps)}"
            )
        stage_checks = checks.get(key, [])
        if not any(check.name == CHECK_NAMES[stage] and check.blocking for check in stage_checks):
            violations.append(f"{stage.value}: missing blocking {CHECK_NAMES[stage]} check")
    expected_jobs = {
        f"{manifest.pipeline_id}__full",
        *(f"{manifest.pipeline_id}__repair_{stage.value}" for stage in STAGE_ORDER),
    }
    actual_jobs = {job.name for job in defs.jobs or []}
    if expected_jobs - actual_jobs:
        violations.append(f"missing jobs: {sorted(expected_jobs - actual_jobs)}")
    if violations:
        raise PipelineError(
            code="E_MANIFEST_INVALID",
            message="generated Dagster definitions failed the runtime contract",
            details={"violations": violations},
        )


def _schema_hash() -> str:
    from .manifest import schema_hash

    return schema_hash()
