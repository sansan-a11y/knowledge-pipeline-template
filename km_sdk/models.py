"""Stable public data contracts for KM pipeline handlers."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Stage(StrEnum):
    INGEST = "ingest"
    PARSE = "parse"
    EXTRACTION = "extraction"
    ASSEMBLE = "assemble"
    VALIDATION = "validation"
    PUBLISH = "publish"


STAGE_ORDER: tuple[Stage, ...] = tuple(Stage)

TERMINAL_ASSETS: dict[Stage, str] = {
    Stage.INGEST: "ingested_document",
    Stage.PARSE: "parsed_document",
    Stage.EXTRACTION: "extracted_knowledge",
    Stage.ASSEMBLE: "assembled_document",
    Stage.VALIDATION: "validation_report",
    Stage.PUBLISH: "published_document",
}


class KmModel(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class IngestRequest(KmModel):
    doc_id: str = Field(min_length=1)
    doc_version: str = Field(min_length=1)
    tenant: str = Field(min_length=1)
    source_uri: str = Field(min_length=1)
    mime_type: str | None = None


class IngestedDocument(KmModel):
    doc_id: str = Field(min_length=1)
    doc_version: str = Field(min_length=1)
    tenant: str = Field(min_length=1)
    source_uri: str = Field(min_length=1)
    mime_type: str = Field(min_length=1)
    content_hash: str = Field(min_length=1)
    byte_size: int = Field(ge=0)


class ParsedChunk(KmModel):
    chunk_id: str = Field(min_length=1)
    text: str
    page: int | None = Field(default=None, ge=1)
    metadata: dict[str, object] = Field(default_factory=dict)


class ParsedDocument(KmModel):
    doc_id: str = Field(min_length=1)
    doc_version: str = Field(min_length=1)
    source_hash: str = Field(min_length=1)
    parser_version: str = Field(min_length=1)
    chunks: list[ParsedChunk] = Field(default_factory=list)


class ExtractedEntity(KmModel):
    id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    type: str = Field(min_length=1)
    descriptions: list[str] = Field(default_factory=list)
    citations: list[str] = Field(default_factory=list)


class ExtractedRelation(KmModel):
    source_id: str = Field(min_length=1)
    relation: str = Field(min_length=1)
    target_id: str = Field(min_length=1)
    citations: list[str] = Field(default_factory=list)


class ExtractionResult(KmModel):
    doc_id: str = Field(min_length=1)
    doc_version: str = Field(min_length=1)
    schema_version: str = Field(min_length=1)
    model_version: str = Field(min_length=1)
    prompt_version: str = Field(min_length=1)
    entities: list[ExtractedEntity] = Field(default_factory=list)
    relations: list[ExtractedRelation] = Field(default_factory=list)

    @model_validator(mode="after")
    def relation_endpoints_exist(self) -> "ExtractionResult":
        entity_ids = {entity.id for entity in self.entities}
        invalid = [
            relation
            for relation in self.relations
            if relation.source_id not in entity_ids or relation.target_id not in entity_ids
        ]
        if invalid:
            raise ValueError("all relation endpoints must exist in entities")
        return self


class AssembledDocument(KmModel):
    doc_id: str = Field(min_length=1)
    doc_version: str = Field(min_length=1)
    title: str = Field(min_length=1)
    sections: list[dict[str, object]] = Field(default_factory=list)
    entities: list[ExtractedEntity] = Field(default_factory=list)
    relations: list[ExtractedRelation] = Field(default_factory=list)
    citations: list[str] = Field(default_factory=list)
    assembly_version: str = Field(min_length=1)


class ValidationRuleResult(KmModel):
    rule_id: str = Field(min_length=1)
    passed: bool
    severity: Literal["ERROR", "WARN"]
    threshold: object
    observed: object
    message: str | None = None


class ValidationReport(KmModel):
    doc_id: str = Field(min_length=1)
    doc_version: str = Field(min_length=1)
    policy_version: str = Field(min_length=1)
    status: Literal["passed", "failed"]
    blocking_failures: int = Field(ge=0)
    warnings: int = Field(ge=0)
    rules: list[ValidationRuleResult] = Field(default_factory=list)

    @model_validator(mode="after")
    def status_matches_rules(self) -> "ValidationReport":
        actual_blocking = sum(
            not rule.passed and rule.severity == "ERROR" for rule in self.rules
        )
        actual_warnings = sum(
            not rule.passed and rule.severity == "WARN" for rule in self.rules
        )
        if self.blocking_failures != actual_blocking:
            raise ValueError(
                "blocking_failures must equal failed rules with severity=ERROR"
            )
        if self.warnings != actual_warnings:
            raise ValueError("warnings must equal failed rules with severity=WARN")
        expected_status = "passed" if actual_blocking == 0 else "failed"
        if self.status != expected_status:
            raise ValueError(
                f"status must be {expected_status!r} for the supplied rule results"
            )
        return self


class PublishedDocument(KmModel):
    doc_id: str = Field(min_length=1)
    doc_version: str = Field(min_length=1)
    publication_id: str = Field(min_length=1)
    validation_report_id: str = Field(min_length=1)
    index_namespace: str = Field(min_length=1)
    published_at: datetime
    is_visible: bool
