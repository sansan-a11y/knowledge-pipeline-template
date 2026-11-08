"""Runnable six-stage reference pipeline.

The data transformations are intentionally small. Replace their bodies with
real parsing, extraction, assembly and publishing adapters while keeping the
stage boundaries, terminal assets and checks intact.
"""

import hashlib
import time
from contextlib import contextmanager
from typing import Iterator

import dagster as dg

from .contract import (
    DOC_PARTITIONS,
    OWNER,
    PIPELINE_ID,
    PIPELINE_VERSION,
    Stage,
    TERMINAL_ASSETS,
    stage_group,
    terminal_metadata,
)
from .resources import TemplateStorage, sha256_text


def _emit_stage_signal(
    context: dg.AssetExecutionContext,
    *,
    stage: Stage,
    state: str,
    attempt: int,
    metadata: dict[str, object] | None = None,
) -> None:
    payload = {
        "km_signal_version": 1,
        "km_pipeline_id": PIPELINE_ID,
        "km_stage_id": stage.value,
        "km_stage_attempt": attempt,
        "km_stage_state": state,
        **(metadata or {}),
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
        # Telemetry must never hide the actual pipeline result or exception.
        context.log.warning(
            "Unable to emit stage observation for %s",
            stage.value,
            exc_info=True,
        )


@contextmanager
def stage_execution(
    context: dg.AssetExecutionContext,
    *,
    stage: Stage,
) -> Iterator[None]:
    attempt = context.retry_number + 1
    started = time.monotonic()
    _emit_stage_signal(
        context,
        stage=stage,
        state="STARTED",
        attempt=attempt,
    )
    try:
        yield
    except Exception as exc:
        failure_metadata = getattr(exc, "metadata", {}) or {}
        raw_error_code = getattr(exc, "error_code", None)
        if raw_error_code is None:
            raw_error_code = failure_metadata.get("error_code", "E_BUG")
        error_code = getattr(raw_error_code, "value", raw_error_code)
        _emit_stage_signal(
            context,
            stage=stage,
            state="FAILED",
            attempt=attempt,
            metadata={
                "error_code": str(error_code),
                "error_class": type(exc).__name__,
                "duration_ms": int((time.monotonic() - started) * 1000),
            },
        )
        raise


def _runtime_metadata(
    stage: Stage,
    partition_key: str,
    *,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    if "::" in partition_key:
        doc_id, doc_version = partition_key.split("::", maxsplit=1)
    elif "__v" in partition_key:
        doc_id, doc_version = partition_key.rsplit("__", maxsplit=1)
        doc_version = f"v{doc_version.removeprefix('v')}"
    else:
        doc_id, doc_version = partition_key, "v1"
    return {
        "km_contract_version": 1,
        "km_pipeline_id": PIPELINE_ID,
        "km_pipeline_version": PIPELINE_VERSION,
        "km_stage_id": stage.value,
        "doc_id": doc_id,
        "doc_version": doc_version,
        **(extra or {}),
    }


@dg.asset(
    key=TERMINAL_ASSETS[Stage.INGEST],
    partitions_def=DOC_PARTITIONS,
    group_name=stage_group(Stage.INGEST),
    owners=[OWNER],
    kinds={"python", "object-store"},
    metadata=terminal_metadata(Stage.INGEST),
    check_specs=[
        dg.AssetCheckSpec(
            name="source_contract",
            asset=TERMINAL_ASSETS[Stage.INGEST],
            blocking=True,
        )
    ],
)
def ingested_document(
    context: dg.AssetExecutionContext,
    storage: TemplateStorage,
) -> Iterator[dg.Output | dg.AssetCheckResult]:
    with stage_execution(context, stage=Stage.INGEST):
        source = storage.ensure_demo_source(context.partition_key)
        text = str(source.get("text", ""))
        content_hash = sha256_text(text)
        payload = {
            **source,
            "content_hash": content_hash,
            "mime_type": "application/pdf",
        }
        uri = storage.put_json(
            f"ingest/{context.partition_key.replace('::', '__')}/document.json",
            payload,
        )
        passed = bool(text) and bool(source.get("doc_id")) and bool(source.get("doc_version"))
        context.set_data_version(
            context.asset_key,
            dg.DataVersion(f"sha256:{content_hash}"),
        )
        yield dg.Output(
            value=uri,
            metadata=_runtime_metadata(
                Stage.INGEST,
                context.partition_key,
                extra={"content_hash": content_hash, "source_uri": source["source_uri"]},
            ),
        )
        yield dg.AssetCheckResult(
            check_name="source_contract",
            passed=passed,
            metadata={"content_hash": content_hash},
        )


@dg.asset(
    key=TERMINAL_ASSETS[Stage.PARSE],
    partitions_def=DOC_PARTITIONS,
    group_name=stage_group(Stage.PARSE),
    owners=[OWNER],
    kinds={"python", "parser"},
    metadata=terminal_metadata(Stage.PARSE),
    ins={
        "ingested_uri": dg.AssetIn(key=TERMINAL_ASSETS[Stage.INGEST]),
    },
    check_specs=[
        dg.AssetCheckSpec(
            name="chunk_contract",
            asset=TERMINAL_ASSETS[Stage.PARSE],
            blocking=True,
        )
    ],
)
def parsed_document(
    context: dg.AssetExecutionContext,
    storage: TemplateStorage,
    ingested_uri: str,
) -> Iterator[dg.Output | dg.AssetCheckResult]:
    with stage_execution(context, stage=Stage.PARSE):
        source = storage.get_json(ingested_uri)
        sentences = [
            sentence.strip()
            for sentence in str(source["text"]).replace("\n", " ").split(".")
            if sentence.strip()
        ]
        chunks = [
            {
                "chunk_id": f"chunk-{index:03d}",
                "text": f"{sentence}.",
                "page": 1,
            }
            for index, sentence in enumerate(sentences)
        ]
        manifest = {
            "doc_id": source["doc_id"],
            "doc_version": source["doc_version"],
            "source_uri": ingested_uri,
            "chunks": chunks,
        }
        uri = storage.put_json(
            f"parse/{context.partition_key.replace('::', '__')}/document.json",
            manifest,
        )
        passed = bool(chunks) and all(chunk["text"] for chunk in chunks)
        yield dg.Output(
            value=uri,
            metadata=_runtime_metadata(
                Stage.PARSE,
                context.partition_key,
                extra={"chunk_count": len(chunks)},
            ),
        )
        yield dg.AssetCheckResult(
            check_name="chunk_contract",
            passed=passed,
            metadata={"chunk_count": len(chunks)},
        )


@dg.asset(
    key=TERMINAL_ASSETS[Stage.EXTRACTION],
    partitions_def=DOC_PARTITIONS,
    group_name=stage_group(Stage.EXTRACTION),
    owners=[OWNER],
    kinds={"python", "llm"},
    metadata=terminal_metadata(Stage.EXTRACTION),
    ins={
        "parsed_uri": dg.AssetIn(key=TERMINAL_ASSETS[Stage.PARSE]),
    },
    check_specs=[
        dg.AssetCheckSpec(
            name="extraction_contract",
            asset=TERMINAL_ASSETS[Stage.EXTRACTION],
            blocking=True,
        )
    ],
)
def extracted_knowledge(
    context: dg.AssetExecutionContext,
    storage: TemplateStorage,
    parsed_uri: str,
) -> Iterator[dg.Output | dg.AssetCheckResult]:
    with stage_execution(context, stage=Stage.EXTRACTION):
        parsed = storage.get_json(parsed_uri)
        entities = [
            {"id": "pump-p-100", "name": "Pump P-100", "type": "EQUIPMENT"},
            {"id": "valve-v-12", "name": "Valve V-12", "type": "COMPONENT"},
            {"id": "seal", "name": "Mechanical Seal", "type": "COMPONENT"},
        ]
        relations = [
            {
                "source": "pump-p-100",
                "relation": "HAS_COMPONENT",
                "target": "valve-v-12",
            },
            {
                "source": "pump-p-100",
                "relation": "HAS_COMPONENT",
                "target": "seal",
            },
        ]
        extracted = {
            "doc_id": parsed["doc_id"],
            "doc_version": parsed["doc_version"],
            "source_uri": parsed_uri,
            "model_version": "fake-model-v1",
            "prompt_version": "extract-v1",
            "schema_version": 1,
            "entities": entities,
            "relations": relations,
        }
        uri = storage.put_json(
            f"extract/{context.partition_key.replace('::', '__')}/knowledge.json",
            extracted,
        )
        entity_ids = {entity["id"] for entity in entities}
        passed = all(
            relation["source"] in entity_ids and relation["target"] in entity_ids
            for relation in relations
        )
        yield dg.Output(
            value=uri,
            metadata=_runtime_metadata(
                Stage.EXTRACTION,
                context.partition_key,
                extra={
                    "entity_count": len(entities),
                    "relation_count": len(relations),
                    "model_version": "fake-model-v1",
                    "prompt_version": "extract-v1",
                },
            ),
        )
        yield dg.AssetCheckResult(
            check_name="extraction_contract",
            passed=passed,
            metadata={"entity_count": len(entities), "relation_count": len(relations)},
        )


@dg.asset(
    key=TERMINAL_ASSETS[Stage.ASSEMBLE],
    partitions_def=DOC_PARTITIONS,
    group_name=stage_group(Stage.ASSEMBLE),
    owners=[OWNER],
    kinds={"python"},
    metadata=terminal_metadata(Stage.ASSEMBLE),
    ins={
        "parsed_uri": dg.AssetIn(key=TERMINAL_ASSETS[Stage.PARSE]),
        "extracted_uri": dg.AssetIn(key=TERMINAL_ASSETS[Stage.EXTRACTION]),
    },
    check_specs=[
        dg.AssetCheckSpec(
            name="draft_contract",
            asset=TERMINAL_ASSETS[Stage.ASSEMBLE],
            blocking=True,
        )
    ],
)
def assembled_document(
    context: dg.AssetExecutionContext,
    storage: TemplateStorage,
    parsed_uri: str,
    extracted_uri: str,
) -> Iterator[dg.Output | dg.AssetCheckResult]:
    with stage_execution(context, stage=Stage.ASSEMBLE):
        parsed = storage.get_json(parsed_uri)
        extracted = storage.get_json(extracted_uri)
        chunk_ids = [chunk["chunk_id"] for chunk in parsed["chunks"]]
        draft = {
            "doc_id": parsed["doc_id"],
            "doc_version": parsed["doc_version"],
            "title": "Demo equipment knowledge",
            "entities": extracted["entities"],
            "relations": extracted["relations"],
            "citations": [{"chunk_id": chunk_id} for chunk_id in chunk_ids],
            "status": "draft",
        }
        uri = storage.put_json(
            f"assemble/{context.partition_key.replace('::', '__')}/draft.json",
            draft,
        )
        passed = bool(draft["entities"]) and bool(draft["citations"])
        yield dg.Output(
            value=uri,
            metadata=_runtime_metadata(
                Stage.ASSEMBLE,
                context.partition_key,
                extra={"citation_count": len(draft["citations"])},
            ),
        )
        yield dg.AssetCheckResult(
            check_name="draft_contract",
            passed=passed,
            metadata={"citation_count": len(draft["citations"])},
        )


def _build_validation_report(
    assembled: dict[str, object],
    parsed: dict[str, object],
    *,
    force_failure: bool,
) -> dict[str, object]:
    chunk_ids = {chunk["chunk_id"] for chunk in parsed["chunks"]}
    cited_ids = {citation["chunk_id"] for citation in assembled["citations"]}
    refs_valid = cited_ids <= chunk_ids
    entity_ids = {entity["id"] for entity in assembled["entities"]}
    relations_valid = all(
        relation["source"] in entity_ids and relation["target"] in entity_ids
        for relation in assembled["relations"]
    )
    rules = [
        {
            "rule_id": "KE-REF-001",
            "passed": refs_valid,
            "severity": "ERROR",
            "threshold": "all citations exist",
            "observed": len(cited_ids - chunk_ids),
        },
        {
            "rule_id": "KE-GRAPH-001",
            "passed": relations_valid,
            "severity": "ERROR",
            "threshold": "all relation endpoints exist",
            "observed": sum(
                1
                for relation in assembled["relations"]
                if relation["source"] not in entity_ids or relation["target"] not in entity_ids
            ),
        },
        {
            "rule_id": "KE-DEMO-FAIL",
            "passed": not force_failure,
            "severity": "ERROR",
            "threshold": "force_validation_failure=false",
            "observed": force_failure,
        },
    ]
    blocking_failures = [rule for rule in rules if not rule["passed"]]
    return {
        "doc_id": assembled["doc_id"],
        "doc_version": assembled["doc_version"],
        "validation_policy_version": "demo-policy-v1",
        "status": "passed" if not blocking_failures else "failed",
        "blocking_failures": len(blocking_failures),
        "warnings": 0,
        "rules": rules,
    }


@dg.asset(
    key=TERMINAL_ASSETS[Stage.VALIDATION],
    partitions_def=DOC_PARTITIONS,
    group_name=stage_group(Stage.VALIDATION),
    owners=[OWNER],
    kinds={"python", "data-quality"},
    metadata=terminal_metadata(Stage.VALIDATION),
    ins={
        "assembled_uri": dg.AssetIn(key=TERMINAL_ASSETS[Stage.ASSEMBLE]),
        "parsed_uri": dg.AssetIn(key=TERMINAL_ASSETS[Stage.PARSE]),
    },
    check_specs=[
        dg.AssetCheckSpec(
            name="publication_gate_passed",
            asset=TERMINAL_ASSETS[Stage.VALIDATION],
            blocking=True,
        )
    ],
)
def validation_report(
    context: dg.AssetExecutionContext,
    storage: TemplateStorage,
    assembled_uri: str,
    parsed_uri: str,
) -> Iterator[dg.Output | dg.AssetCheckResult]:
    with stage_execution(context, stage=Stage.VALIDATION):
        parsed = storage.get_json(parsed_uri)
        assembled = storage.get_json(assembled_uri)
        report = _build_validation_report(
            assembled,
            parsed,
            force_failure=storage.force_validation_failure,
        )
        uri = storage.put_json(
            f"validation/{context.partition_key.replace('::', '__')}/report.json",
            report,
        )
        yield dg.Output(
            value=uri,
            metadata=_runtime_metadata(
                Stage.VALIDATION,
                context.partition_key,
                extra={
                    "validation_status": report["status"],
                    "blocking_failures": report["blocking_failures"],
                },
            ),
        )
        yield dg.AssetCheckResult(
            check_name="publication_gate_passed",
            passed=report["status"] == "passed",
            metadata={
                "validation_report_id": hashlib.sha256(
                    str(report).encode("utf-8")
                ).hexdigest()[:16],
                "blocking_failures": report["blocking_failures"],
            },
        )


@dg.asset(
    key=TERMINAL_ASSETS[Stage.PUBLISH],
    partitions_def=DOC_PARTITIONS,
    group_name=stage_group(Stage.PUBLISH),
    owners=[OWNER],
    kinds={"python", "vector-index", "search"},
    metadata=terminal_metadata(Stage.PUBLISH),
    ins={
        "assembled_uri": dg.AssetIn(key=TERMINAL_ASSETS[Stage.ASSEMBLE]),
        "validation_uri": dg.AssetIn(key=TERMINAL_ASSETS[Stage.VALIDATION]),
    },
    check_specs=[
        dg.AssetCheckSpec(
            name="publication_committed",
            asset=TERMINAL_ASSETS[Stage.PUBLISH],
            blocking=True,
        )
    ],
)
def published_document(
    context: dg.AssetExecutionContext,
    storage: TemplateStorage,
    assembled_uri: str,
    validation_uri: str,
) -> Iterator[dg.Output | dg.AssetCheckResult]:
    with stage_execution(context, stage=Stage.PUBLISH):
        report = storage.get_json(validation_uri)
        if report["status"] != "passed" or report["blocking_failures"] != 0:
            raise dg.Failure(
                description="Validation report does not permit publication",
                metadata={"error_code": "E_VALIDATION_GATE"},
                allow_retries=False,
            )

        assembled = storage.get_json(assembled_uri)
        publication_id = hashlib.sha256(
            (
                f"{PIPELINE_ID}:{assembled['doc_id']}:{assembled['doc_version']}:"
                f"{PIPELINE_VERSION}"
            ).encode("utf-8")
        ).hexdigest()[:20]
        visible_uri = storage.publish(
            publication_id=publication_id,
            assembled_document_uri=assembled_uri,
            validation_report_uri=validation_uri,
            window_catalog_uri="template://knowledge/window.json",
        )
        visible = storage.get_visible_publication() or {}
        committed = (
            visible.get("publication_id") == publication_id
            and visible.get("is_visible") is True
        )
        yield dg.Output(
            value=visible_uri,
            metadata=_runtime_metadata(
                Stage.PUBLISH,
                context.partition_key,
                extra={
                    "publication_id": publication_id,
                    "validation_report_uri": validation_uri,
                    "is_visible": committed,
                },
            ),
        )
        yield dg.AssetCheckResult(
            check_name="publication_committed",
            passed=committed,
            metadata={"publication_id": publication_id},
        )


ALL_ASSETS = [
    ingested_document,
    parsed_document,
    extracted_knowledge,
    assembled_document,
    validation_report,
    published_document,
]
