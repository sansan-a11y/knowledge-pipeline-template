"""Dagster-independent six-stage runner used by the CLI and tests."""

from __future__ import annotations

import json
import queue
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .artifacts import ArtifactClient
from .context import StageContext, StructuredLogger
from .errors import PipelineError, map_exception
from .manifest import LoadedPipeline, StageConfig, load_pipeline
from .models import (
    STAGE_ORDER,
    AssembledDocument,
    ExtractionResult,
    IngestedDocument,
    IngestRequest,
    ParsedDocument,
    PublishedDocument,
    Stage,
    ValidationReport,
)
from .resources import (
    KnowledgeIndex,
    LlmGateway,
    LocalKnowledgeIndex,
    LocalLlmGateway,
    publication_id,
    validation_report_id,
)


class StageState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    BLOCKED = "blocked"
    SKIPPED = "skipped"


class StageEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stage: Stage
    state: StageState
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    attempt: int | None = None


class StageRunRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stage: Stage
    status: StageState
    attempt: int | None = None
    duration_ms: int | None = None
    artifact_uri: str | None = None
    summary: dict[str, object] = Field(default_factory=dict)
    error: dict[str, object] | None = None


class PipelineRunReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: str
    pipeline_id: str
    pipeline_version: str
    partition_key: str
    doc_id: str
    doc_version: str
    tenant: str
    status: StageState
    started_at: datetime
    finished_at: datetime
    stages: list[StageRunRecord]
    events: list[StageEvent] = Field(default_factory=list)

    def stage(self, stage: Stage) -> StageRunRecord:
        return next(record for record in self.stages if record.stage == stage)


class PipelineArtifactStore:
    """Canonical stage output paths for a document partition."""

    def __init__(
        self,
        artifacts: ArtifactClient,
        *,
        pipeline_id: str,
        tenant: str,
        doc_id: str,
        doc_version: str,
    ) -> None:
        self.artifacts = artifacts
        self.pipeline_id = pipeline_id
        self.tenant = tenant
        self.doc_id = doc_id
        self.doc_version = doc_version

    @property
    def prefix(self) -> str:
        return (
            f"km/pipeline_id={self.pipeline_id}/tenant={self.tenant}/"
            f"doc_id={self.doc_id}/doc_version={self.doc_version}"
        )

    def _stage_prefix(self, stage: Stage) -> str:
        return f"{self.prefix}/stage={stage.value}"

    def output_key(self, stage: Stage, attempt: int) -> str:
        return f"{self._stage_prefix(stage)}/attempt={attempt}/output.json"

    def latest_output_key(self, stage: Stage) -> str | None:
        stage_dir = self.artifacts.root / self._stage_prefix(stage)
        if not stage_dir.is_dir():
            return None
        attempts: list[tuple[int, Path]] = []
        for candidate in stage_dir.glob("attempt=*/output.json"):
            try:
                attempt = int(candidate.parent.name.removeprefix("attempt="))
            except ValueError:
                continue
            attempts.append((attempt, candidate))
        if not attempts:
            return None
        _, latest = max(attempts, key=lambda item: item[0])
        return latest.relative_to(self.artifacts.root).as_posix()

    def next_attempt(self, stage: Stage) -> int:
        stage_dir = self.artifacts.root / self._stage_prefix(stage)
        attempts = []
        if stage_dir.is_dir():
            for candidate in stage_dir.glob("attempt=*"):
                try:
                    attempts.append(int(candidate.name.removeprefix("attempt=")))
                except ValueError:
                    continue
        return max(attempts, default=0) + 1

    def reserve_attempt(self, stage: Stage) -> int:
        attempt = self.next_attempt(stage)
        (self.artifacts.root / self._stage_prefix(stage) / f"attempt={attempt}").mkdir(
            parents=True,
            exist_ok=True,
        )
        return attempt

    def save(self, stage: Stage, attempt: int, model: BaseModel) -> str:
        return self.artifacts.put_json(
            self.output_key(stage, attempt),
            model.model_dump(mode="json"),
        )

    def load(
        self,
        stage: Stage,
        expected_type: type[BaseModel],
    ) -> BaseModel:
        key = self.latest_output_key(stage)
        if key is None:
            raise PipelineError(
                code="E_DATA_CONTRACT",
                message=f"{stage.value}: upstream artifact is missing; repair {stage.value} after restoring its input",
                stage=stage,
                retryable=False,
            )
        try:
            return expected_type.model_validate(self.artifacts.get_json(key))
        except (ValidationError, ValueError, OSError) as exc:
            raise PipelineError(
                code="E_DATA_CONTRACT",
                message=f"{stage.value}: upstream artifact does not match {expected_type.__name__}",
                stage=stage,
                details={"artifact": key, "exception": str(exc)},
            ) from exc


@dataclass(frozen=True)
class StageInvocation:
    stage: Stage
    attempt: int
    output: BaseModel
    artifact_uri: str
    duration_ms: int


class PipelineRunner:
    """Execute handlers in the fixed stage order without requiring Dagster."""

    def __init__(
        self,
        pipeline: LoadedPipeline | str | Path,
        *,
        project_root: str | Path | None = None,
        artifacts: ArtifactClient | None = None,
        llm: LlmGateway | None = None,
        index: KnowledgeIndex | None = None,
        check_source: bool = True,
    ) -> None:
        if isinstance(pipeline, (str, Path)):
            loaded = load_pipeline(
                pipeline,
                project_root=project_root,
                check_source=check_source,
            )
        else:
            loaded = pipeline
        self.pipeline = loaded
        self.manifest = loaded.manifest
        self.project_root = loaded.project_root
        self.artifacts = artifacts or ArtifactClient(loaded.project_root / ".km" / "artifacts")
        self.llm = llm or LocalLlmGateway()
        self.index = index or LocalKnowledgeIndex(self.artifacts)

    @property
    def artifact_store(self) -> ArtifactClient:
        return self.artifacts

    def _stage_store(self, request: IngestRequest) -> PipelineArtifactStore:
        return PipelineArtifactStore(
            self.artifacts,
            pipeline_id=self.manifest.pipeline_id,
            tenant=request.tenant,
            doc_id=request.doc_id,
            doc_version=request.doc_version,
        )

    def _options(
        self,
        stage: Stage,
        *,
        options_override: Mapping[str, object] | None,
    ) -> dict[str, object]:
        options = self.manifest.merged_options(stage)
        if options_override:
            options.update(options_override)
        return options

    def _context(
        self,
        request: IngestRequest,
        *,
        stage: Stage,
        attempt: int,
        task_id: str,
        options_override: Mapping[str, object] | None,
    ) -> StageContext:
        return StageContext(
            pipeline_id=self.manifest.pipeline_id,
            pipeline_version=self.manifest.pipeline_version,
            task_id=task_id,
            partition_key=f"{request.doc_id}__{request.doc_version}",
            doc_id=request.doc_id,
            doc_version=request.doc_version,
            tenant=request.tenant,
            stage=stage,
            attempt=attempt,
            options=self._options(stage, options_override=options_override),
            artifacts=self.artifacts,
            logger=StructuredLogger(
                f"km_sdk.{self.manifest.pipeline_id}.{stage.value}"
            ),
        )

    def _invoke(
        self,
        context: StageContext,
        *,
        request: IngestRequest,
        timeout_seconds: float,
    ) -> BaseModel:
        stage = context.stage
        store = PipelineArtifactStore(
            self.artifacts,
            pipeline_id=context.pipeline_id,
            tenant=context.tenant,
            doc_id=context.doc_id,
            doc_version=context.doc_version,
        )
        arguments: list[object] = [context]
        if stage == Stage.INGEST:
            arguments.append(request)
        else:
            arguments.extend(self._load_inputs(stage, store))
        if stage == Stage.EXTRACTION:
            arguments.append(self.llm)
        elif stage == Stage.PUBLISH:
            self._validate_report_gate(arguments[2], request)  # type: ignore[arg-type]
            arguments.append(self.index)

        result_queue: queue.Queue[tuple[str, object]] = queue.Queue(maxsize=1)

        def invoke() -> None:
            try:
                result = self.pipeline.handlers[stage].function(*arguments)
                result_queue.put(("result", result))
            except BaseException as exc:  # The queue carries handler exceptions back intact.
                result_queue.put(("error", exc))

        worker = threading.Thread(
            target=invoke,
            name=f"km-{self.manifest.pipeline_id}-{stage.value}",
            daemon=True,
        )
        started = time.monotonic()
        worker.start()
        try:
            kind, payload = result_queue.get(timeout=timeout_seconds)
        except queue.Empty as exc:
            raise PipelineError(
                code="E_STAGE_TIMEOUT",
                message=f"{stage.value} exceeded its {timeout_seconds:g}s timeout",
                retryable=self.manifest.spec.stages.get(stage).retry is not None,
                stage=stage,
                details={"timeout_seconds": timeout_seconds},
            ) from exc
        if kind == "error":
            raise map_exception(payload, stage=stage)  # type: ignore[arg-type]
        output_type = self._output_type(stage)
        try:
            output = output_type.model_validate(payload)
        except ValidationError as exc:
            raise PipelineError(
                code="E_DATA_CONTRACT",
                message=f"{stage.value}: handler must return {output_type.__name__}",
                stage=stage,
                details={"errors": exc.errors(include_url=False)},
            ) from exc
        self._validate_identity(stage, context, output)
        return output

    def _load_inputs(
        self,
        stage: Stage,
        store: PipelineArtifactStore,
    ) -> list[BaseModel]:
        if stage == Stage.PARSE:
            return [store.load(Stage.INGEST, IngestedDocument)]
        if stage == Stage.EXTRACTION:
            return [store.load(Stage.PARSE, ParsedDocument)]
        if stage == Stage.ASSEMBLE:
            return [
                store.load(Stage.PARSE, ParsedDocument),
                store.load(Stage.EXTRACTION, ExtractionResult),
            ]
        if stage == Stage.VALIDATION:
            return [
                store.load(Stage.PARSE, ParsedDocument),
                store.load(Stage.ASSEMBLE, AssembledDocument),
            ]
        if stage == Stage.PUBLISH:
            return [
                store.load(Stage.ASSEMBLE, AssembledDocument),
                store.load(Stage.VALIDATION, ValidationReport),
            ]
        return []

    @staticmethod
    def _output_type(stage: Stage) -> type[BaseModel]:
        return {
            Stage.INGEST: IngestedDocument,
            Stage.PARSE: ParsedDocument,
            Stage.EXTRACTION: ExtractionResult,
            Stage.ASSEMBLE: AssembledDocument,
            Stage.VALIDATION: ValidationReport,
            Stage.PUBLISH: PublishedDocument,
        }[stage]

    @staticmethod
    def _validate_identity(
        stage: Stage,
        context: StageContext,
        output: BaseModel,
    ) -> None:
        doc_id = getattr(output, "doc_id", None)
        doc_version = getattr(output, "doc_version", None)
        if doc_id != context.doc_id or doc_version != context.doc_version:
            raise PipelineError(
                code="E_DATA_CONTRACT",
                message=(
                    f"{stage.value}: output doc_id/doc_version "
                    f"({doc_id!r}, {doc_version!r}) does not match the task "
                    f"({context.doc_id!r}, {context.doc_version!r})"
                ),
                stage=stage,
                details={"artifact_doc_id": doc_id, "artifact_doc_version": doc_version},
            )

    def _validate_report_gate(
        self,
        report: ValidationReport,
        request: IngestRequest,
    ) -> None:
        errors: list[str] = []
        if report.status != "passed":
            errors.append("validation report status is failed")
        if report.blocking_failures != 0:
            errors.append(f"{report.blocking_failures} blocking validation failure(s)")
        failed_errors = [
            rule.rule_id
            for rule in report.rules
            if rule.severity == "ERROR" and not rule.passed
        ]
        if failed_errors:
            errors.append(f"failed ERROR rules: {', '.join(failed_errors)}")
        if report.doc_id != request.doc_id or report.doc_version != request.doc_version:
            errors.append("validation report document identity does not match the task")
        if errors:
            raise PipelineError(
                code="E_VALIDATION_FAILED",
                message="validation blocked publication: " + "; ".join(errors),
                retryable=False,
                stage=Stage.VALIDATION,
                details={
                    "blocking_failures": report.blocking_failures,
                    "failed_rules": failed_errors,
                },
            )

    def _validate_publish_gate(
        self,
        published: PublishedDocument,
        report: ValidationReport,
        *,
        request: IngestRequest,
    ) -> None:
        expected_publication_id = publication_id(
            self.manifest.pipeline_id,
            request.doc_id,
            request.doc_version,
            self.manifest.pipeline_version,
        )
        expected_report_id = validation_report_id(report)
        problems: list[str] = []
        if published.is_visible is not True:
            problems.append("is_visible must be true")
        if published.publication_id != expected_publication_id:
            problems.append("publication_id is not stable/recomputable")
        if published.validation_report_id != expected_report_id:
            problems.append("validation_report_id does not reference the current report")
        if not self.index.is_visible(published.publication_id):
            problems.append("index does not report the publication as visible")
        if problems:
            raise PipelineError(
                code="E_PUBLISH_GATE",
                message="publish result failed the visibility contract: "
                + "; ".join(problems),
                retryable=False,
                stage=Stage.PUBLISH,
                details={"problems": problems},
            )

    def _invoke_once(
        self,
        request: IngestRequest,
        *,
        stage: Stage,
        task_id: str,
        store: PipelineArtifactStore,
        options_override: Mapping[str, object] | None,
    ) -> StageInvocation:
        config: StageConfig = self.manifest.spec.stages.get(stage)
        attempt = store.reserve_attempt(stage)
        context = self._context(
            request,
            stage=stage,
            attempt=attempt,
            task_id=task_id,
            options_override=options_override,
        )
        started = time.monotonic()
        output = self._invoke(
            context,
            request=request,
            timeout_seconds=config.timeoutSeconds,
        )
        duration_ms = int((time.monotonic() - started) * 1000)
        artifact_uri = store.save(stage, attempt, output)
        if stage == Stage.VALIDATION:
            self._validate_report_gate(output, request)  # type: ignore[arg-type]
        elif stage == Stage.PUBLISH:
            report = store.load(Stage.VALIDATION, ValidationReport)
            self._validate_publish_gate(
                output,  # type: ignore[arg-type]
                report,  # type: ignore[arg-type]
                request=request,
            )
        elif not self._check_stage_contract(stage, output):
            raise PipelineError(
                code="E_DATA_CONTRACT",
                message=f"{stage.value}: terminal output failed its stage contract check",
                retryable=False,
                stage=stage,
                details={"attempt": attempt, "artifact": artifact_uri},
            )
        return StageInvocation(stage, attempt, output, artifact_uri, duration_ms)

    @staticmethod
    def _check_stage_contract(stage: Stage, output: BaseModel) -> bool:
        if stage == Stage.INGEST:
            value: IngestedDocument = output  # type: ignore[assignment]
            return bool(value.content_hash and value.source_uri)
        if stage == Stage.PARSE:
            value = output  # type: ignore[assignment]
            return bool(value.chunks) and all(chunk.text for chunk in value.chunks)
        if stage == Stage.EXTRACTION:
            value = output  # type: ignore[assignment]
            return bool(value.entities)
        if stage == Stage.ASSEMBLE:
            value = output  # type: ignore[assignment]
            return bool(value.entities) and bool(value.citations)
        return True

    def _invoke_with_retry(
        self,
        request: IngestRequest,
        *,
        stage: Stage,
        task_id: str,
        store: PipelineArtifactStore,
        options_override: Mapping[str, object] | None,
    ) -> StageInvocation:
        config = self.manifest.spec.stages.get(stage)
        retry = config.retry
        max_retries = retry.maxRetries if retry else 0
        for retry_number in range(max_retries + 1):
            try:
                return self._invoke_once(
                    request,
                    stage=stage,
                    task_id=task_id,
                    store=store,
                    options_override=options_override,
                )
            except PipelineError as exc:
                exc.details.setdefault(
                    "attempt",
                    max(store.next_attempt(stage) - 1, 1),
                )
                if not exc.retryable or retry_number >= max_retries:
                    raise
                delay = retry.delaySeconds if retry else 0
                if retry and retry.backoff == "exponential":
                    delay *= 2**retry_number
                if delay > 0:
                    time.sleep(delay)
        raise AssertionError("retry loop exhausted without a result")

    def run(
        self,
        request: IngestRequest | Mapping[str, object] | str | Path,
        *,
        task_id: str | None = None,
        options_override: Mapping[str, object] | None = None,
        repair_from: Stage | None = None,
        persist: bool = True,
        event_callback: Callable[[StageEvent], None] | None = None,
    ) -> PipelineRunReport:
        request_model = self.load_request(request)
        task = task_id or f"task_{uuid.uuid4().hex[:16]}"
        partition = f"{request_model.doc_id}__{request_model.doc_version}"
        if not request_model.doc_id or not request_model.doc_version:
            raise PipelineError(
                code="E_DATA_CONTRACT",
                message="doc_id and doc_version are required to build the partition",
            )
        store = self._stage_store(request_model)
        started_at = datetime.now(timezone.utc)
        events: list[StageEvent] = []
        records = {
            stage: StageRunRecord(stage=stage, status=StageState.PENDING)
            for stage in STAGE_ORDER
        }

        def transition(
            stage: Stage,
            state: StageState,
            *,
            attempt: int | None = None,
        ) -> None:
            records[stage].status = state
            event = StageEvent(stage=stage, state=state, attempt=attempt)
            events.append(event)
            if event_callback:
                event_callback(event)

        failed = False
        for stage in STAGE_ORDER:
            if repair_from and STAGE_ORDER.index(stage) < STAGE_ORDER.index(repair_from):
                transition(stage, StageState.SKIPPED)
                continue
            if failed:
                transition(stage, StageState.BLOCKED)
                continue
            transition(stage, StageState.RUNNING)
            stage_started = time.monotonic()
            try:
                invocation = self._invoke_with_retry(
                    request_model,
                    stage=stage,
                    task_id=task,
                    store=store,
                    options_override=options_override,
                )
            except PipelineError as exc:
                failed = True
                records[stage].duration_ms = int(
                    (time.monotonic() - stage_started) * 1000
                )
                records[stage].error = exc.to_dict()
                transition(stage, StageState.FAILED, attempt=exc.details.get("attempt"))
                continue
            records[stage].attempt = invocation.attempt
            records[stage].duration_ms = invocation.duration_ms
            records[stage].artifact_uri = invocation.artifact_uri
            records[stage].summary = self._summary(stage, invocation.output)
            transition(stage, StageState.SUCCEEDED, attempt=invocation.attempt)

        published = records[Stage.PUBLISH]
        succeeded = (
            not failed
            and published.status == StageState.SUCCEEDED
            and records[Stage.VALIDATION].status == StageState.SUCCEEDED
        )
        report = PipelineRunReport(
            task_id=task,
            pipeline_id=self.manifest.pipeline_id,
            pipeline_version=self.manifest.pipeline_version,
            partition_key=partition,
            doc_id=request_model.doc_id,
            doc_version=request_model.doc_version,
            tenant=request_model.tenant,
            status=StageState.SUCCEEDED if succeeded else StageState.FAILED,
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
            stages=[records[stage] for stage in STAGE_ORDER],
            events=events,
        )
        if persist:
            self.save_report(report)
        return report

    def load_request(
        self,
        request: IngestRequest | Mapping[str, object] | str | Path,
    ) -> IngestRequest:
        if isinstance(request, IngestRequest):
            return request
        if isinstance(request, Mapping):
            return IngestRequest.model_validate(request)
        path = Path(request)
        if not path.is_absolute() and not path.is_file():
            path = self.project_root / path
        try:
            return IngestRequest.model_validate_json(path.read_text(encoding="utf-8"))
        except (OSError, ValidationError) as exc:
            raise PipelineError(
                code="E_DATA_CONTRACT",
                message=f"unable to load ingest request fixture: {path}",
                details={"exception": str(exc)},
            ) from exc

    @staticmethod
    def _summary(stage: Stage, output: BaseModel) -> dict[str, object]:
        if stage == Stage.INGEST:
            value: IngestedDocument = output  # type: ignore[assignment]
            return {"content_hash": value.content_hash, "byte_size": value.byte_size}
        if stage == Stage.PARSE:
            value = output  # type: ignore[assignment]
            return {"chunk_count": len(value.chunks), "parser_version": value.parser_version}
        if stage == Stage.EXTRACTION:
            value = output  # type: ignore[assignment]
            return {
                "entity_count": len(value.entities),
                "relation_count": len(value.relations),
                "model_version": value.model_version,
                "prompt_version": value.prompt_version,
            }
        if stage == Stage.ASSEMBLE:
            value = output  # type: ignore[assignment]
            return {
                "entity_count": len(value.entities),
                "relation_count": len(value.relations),
                "citation_count": len(value.citations),
            }
        if stage == Stage.VALIDATION:
            value = output  # type: ignore[assignment]
            return {
                "validation_status": value.status,
                "blocking_failures": value.blocking_failures,
                "warnings": value.warnings,
                "rules": [
                    {
                        "rule_id": rule.rule_id,
                        "passed": rule.passed,
                        "severity": rule.severity,
                        "message": rule.message,
                    }
                    for rule in value.rules
                ],
            }
        value = output  # type: ignore[assignment]
        return {
            "publication_id": value.publication_id,
            "validation_report_id": value.validation_report_id,
            "index_namespace": value.index_namespace,
            "is_visible": value.is_visible,
        }

    def save_report(self, report: PipelineRunReport) -> None:
        store = ArtifactClient(self.project_root / ".km" / "runs")
        store.put_json(f"{report.task_id}.json", report.model_dump(mode="json"))
        store.put_json("latest.json", report.model_dump(mode="json"))

    def load_last_report(self) -> PipelineRunReport:
        store = ArtifactClient(self.project_root / ".km" / "runs")
        if not store.exists("latest.json"):
            raise FileNotFoundError("No local km run is available")
        return PipelineRunReport.model_validate(store.get_json("latest.json"))
