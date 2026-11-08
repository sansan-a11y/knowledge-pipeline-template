"""Six-stage handlers for the generated knowledge pipeline."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

from km_sdk import (
    AssembledDocument,
    ExtractionResult,
    IngestedDocument,
    IngestRequest,
    KnowledgeIndex,
    LlmGateway,
    ParsedChunk,
    ParsedDocument,
    PipelineError,
    PublishedDocument,
    Stage,
    StageContext,
    ValidationReport,
    ValidationRuleResult,
    publication_id,
    validation_report_id,
)


def _source_path(request: IngestRequest) -> Path:
    path = Path(__file__).resolve().parent / request.source_uri
    if not path.is_file():
        raise PipelineError(
            code="E_RESOURCE_UNAVAILABLE",
            message=f"source fixture does not exist: {request.source_uri}",
            retryable=False,
            stage=Stage.INGEST,
        )
    return path


def ingest(ctx: StageContext, request: IngestRequest) -> IngestedDocument:
    path = _source_path(request)
    payload = path.read_bytes()
    return IngestedDocument(
        doc_id=request.doc_id,
        doc_version=request.doc_version,
        tenant=request.tenant,
        source_uri=request.source_uri,
        mime_type=request.mime_type or "application/pdf",
        content_hash=hashlib.sha256(payload).hexdigest(),
        byte_size=len(payload),
    )


def parse(ctx: StageContext, source: IngestedDocument) -> ParsedDocument:
    request = IngestRequest(
        doc_id=source.doc_id,
        doc_version=source.doc_version,
        tenant=source.tenant,
        source_uri=source.source_uri,
        mime_type=source.mime_type,
    )
    text = _source_path(request).read_text(encoding="utf-8", errors="ignore")
    sentences = [
        sentence.strip()
        for sentence in text.replace("\n", " ").split(".")
        if sentence.strip() and not sentence.startswith("%PDF")
    ]
    chunks = [
        ParsedChunk(
            chunk_id=f"chunk-{index:03d}",
            text=f"{sentence}.",
            page=1,
            metadata={"section": "manual"},
        )
        for index, sentence in enumerate(sentences)
    ]
    if not chunks:
        raise PipelineError(
            code="E_DATA_CONTRACT",
            message="parser produced no chunks",
            retryable=False,
            stage=Stage.PARSE,
        )
    return ParsedDocument(
        doc_id=source.doc_id,
        doc_version=source.doc_version,
        source_hash=source.content_hash,
        parser_version="sample-parser-v1",
        chunks=chunks,
    )


def extraction(
    ctx: StageContext,
    parsed: ParsedDocument,
    llm: LlmGateway,
) -> ExtractionResult:
    entities, relations = llm.structured_extraction(
        parsed,
        options=ctx.options,
    )
    return ExtractionResult(
        doc_id=parsed.doc_id,
        doc_version=parsed.doc_version,
        schema_version="1",
        model_version="fake-model-v1",
        prompt_version="sample-prompt-v1",
        entities=entities,
        relations=relations,
    )


def assemble(
    ctx: StageContext,
    parsed: ParsedDocument,
    extracted: ExtractionResult,
) -> AssembledDocument:
    citations = [chunk.chunk_id for chunk in parsed.chunks]
    return AssembledDocument(
        doc_id=parsed.doc_id,
        doc_version=parsed.doc_version,
        title="機台手冊知識",
        sections=[
            {"heading": "Maintenance", "chunk_ids": citations},
        ],
        entities=extracted.entities,
        relations=extracted.relations,
        citations=citations,
        assembly_version="sample-assembly-v1",
    )


def validation(
    ctx: StageContext,
    parsed: ParsedDocument,
    assembled: AssembledDocument,
) -> ValidationReport:
    chunk_ids = {chunk.chunk_id for chunk in parsed.chunks}
    entity_ids = {entity.id for entity in assembled.entities}
    citations_valid = set(assembled.citations).issubset(chunk_ids)
    relations_valid = all(
        relation.source_id in entity_ids and relation.target_id in entity_ids
        for relation in assembled.relations
    )
    force_failure = bool(ctx.options.get("forceValidationFailure", False))
    rules = [
        ValidationRuleResult(
            rule_id="KE-REF-001",
            passed=citations_valid,
            severity="ERROR",
            threshold="all citations reference parsed chunks",
            observed=len(set(assembled.citations) - chunk_ids),
            message=None if citations_valid else "unknown citation chunk",
        ),
        ValidationRuleResult(
            rule_id="KE-GRAPH-001",
            passed=relations_valid,
            severity="ERROR",
            threshold="all relation endpoints exist",
            observed=sum(
                1
                for relation in assembled.relations
                if relation.source_id not in entity_ids
                or relation.target_id not in entity_ids
            ),
            message=None if relations_valid else "unknown relation endpoint",
        ),
        ValidationRuleResult(
            rule_id="KE-DEMO-FAIL",
            passed=not force_failure,
            severity="ERROR",
            threshold="forceValidationFailure=false",
            observed=force_failure,
            message="forced validation failure for the contract test",
        ),
    ]
    blocking = sum(
        not rule.passed and rule.severity == "ERROR" for rule in rules
    )
    warnings = sum(
        not rule.passed and rule.severity == "WARN" for rule in rules
    )
    return ValidationReport(
        doc_id=assembled.doc_id,
        doc_version=assembled.doc_version,
        policy_version="sample-policy-v1",
        status="passed" if blocking == 0 else "failed",
        blocking_failures=blocking,
        warnings=warnings,
        rules=rules,
    )


def publish(
    ctx: StageContext,
    assembled: AssembledDocument,
    report: ValidationReport,
    index: KnowledgeIndex,
) -> PublishedDocument:
    pub_id = publication_id(
        ctx.pipeline_id,
        assembled.doc_id,
        assembled.doc_version,
        ctx.pipeline_version,
    )
    published = PublishedDocument(
        doc_id=assembled.doc_id,
        doc_version=assembled.doc_version,
        publication_id=pub_id,
        validation_report_id=validation_report_id(report),
        index_namespace=f"km/{pub_id}",
        published_at=datetime.now(timezone.utc),
        is_visible=False,
    )
    try:
        index.commit(
            published,
            assembled=assembled,
            validation_report=report,
        )
    except Exception as exc:
        raise PipelineError(
            code="E_PUBLISH_ROLLBACK",
            message=f"publication switch failed; previous version remains visible: {exc}",
            retryable=False,
            stage=Stage.PUBLISH,
            details={"publication_id": pub_id},
        ) from exc
    visible = index.is_visible(pub_id)
    return published.model_copy(
        update={"is_visible": visible}
    )
