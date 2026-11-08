"""Stable resource protocols and local fakes."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping, Protocol, runtime_checkable

from .artifacts import ArtifactClient
from .models import (
    AssembledDocument,
    ExtractedEntity,
    ExtractedRelation,
    ParsedDocument,
    PublishedDocument,
    ValidationReport,
)


@runtime_checkable
class LlmGateway(Protocol):
    def structured_extraction(
        self,
        parsed: ParsedDocument,
        *,
        options: Mapping[str, object],
    ) -> tuple[list[ExtractedEntity], list[ExtractedRelation]]: ...


@runtime_checkable
class KnowledgeIndex(Protocol):
    def commit(
        self,
        document: PublishedDocument,
        *,
        assembled: AssembledDocument,
        validation_report: ValidationReport,
    ) -> None: ...

    def is_visible(self, publication_id: str) -> bool: ...


@dataclass
class LocalLlmGateway:
    """Deterministic fake for local tests and `km test`."""

    def structured_extraction(
        self,
        parsed: ParsedDocument,
        *,
        options: Mapping[str, object],
    ) -> tuple[list[ExtractedEntity], list[ExtractedRelation]]:
        text = "\n".join(chunk.text for chunk in parsed.chunks)
        entities: list[ExtractedEntity] = []
        if re.search(r"\bpump\b", text, re.IGNORECASE):
            entities.append(
                ExtractedEntity(
                    id="pump-p-100",
                    name="Pump P-100",
                    type="EQUIPMENT",
                    descriptions=["Monthly lubrication is required."],
                    citations=[parsed.chunks[0].chunk_id] if parsed.chunks else [],
                )
            )
        if re.search(r"\bvalve\b", text, re.IGNORECASE):
            entities.append(
                ExtractedEntity(
                    id="valve-v-12",
                    name="Valve V-12",
                    type="COMPONENT",
                    descriptions=["Keep open during operation."],
                    citations=[parsed.chunks[1].chunk_id] if len(parsed.chunks) > 1 else [],
                )
            )
        if re.search(r"\bseal\b", text, re.IGNORECASE):
            entities.append(
                ExtractedEntity(
                    id="seal",
                    name="Mechanical Seal",
                    type="COMPONENT",
                    descriptions=["Inspect every three months."],
                    citations=[parsed.chunks[-1].chunk_id] if parsed.chunks else [],
                )
            )
        relations: list[ExtractedRelation] = []
        if any(entity.id == "pump-p-100" for entity in entities):
            for target in ("valve-v-12", "seal"):
                if any(entity.id == target for entity in entities):
                    relations.append(
                        ExtractedRelation(
                            source_id="pump-p-100",
                            relation="HAS_COMPONENT",
                            target_id=target,
                            citations=[entity.citations[0] for entity in entities if entity.id == target],
                        )
                    )
        return entities, relations


@dataclass
class LocalKnowledgeIndex:
    """Atomic visible pointer for local execution.

    The visible pointer is switched only after the candidate has been written
    and read back successfully. A failure before the switch leaves the previous
    visible version untouched.
    """

    artifacts: ArtifactClient
    fail_before_switch: bool = False

    def commit(
        self,
        document: PublishedDocument,
        *,
        assembled: AssembledDocument,
        validation_report: ValidationReport,
    ) -> None:
        version_key = (
            f"published/versions/{document.doc_id}/{document.doc_version}/"
            f"{document.publication_id}.json"
        )
        payload = {
            "published_document": document.model_dump(mode="json"),
            "assembled_document": assembled.model_dump(mode="json"),
            "validation_report": validation_report.model_dump(mode="json"),
            "is_visible": False,
        }
        version_uri = self.artifacts.put_json(version_key, payload)
        roundtrip = self.artifacts.get_json(version_uri)
        if roundtrip["published_document"]["publication_id"] != document.publication_id:
            raise RuntimeError("publication write-after-read check failed")
        if self.fail_before_switch:
            raise RuntimeError("simulated index failure before visibility switch")

        visible_payload = {
            **payload,
            "version_uri": version_uri,
            "is_visible": True,
        }
        self.artifacts.put_json(
            f"published/visible/{document.doc_id}/{document.doc_version}.json",
            visible_payload,
        )

    def is_visible(self, publication_id: str) -> bool:
        root = self.artifacts.root / "published" / "visible"
        if not root.exists():
            return False
        for path in root.rglob("*.json"):
            key = path.relative_to(self.artifacts.root).as_posix()
            payload = self.artifacts.get_json(key)
            if (
                payload.get("is_visible") is True
                and payload.get("published_document", {}).get("publication_id")
                == publication_id
            ):
                return True
        return False

    def get_visible(self, doc_id: str, doc_version: str) -> PublishedDocument | None:
        key = f"published/visible/{doc_id}/{doc_version}.json"
        if not self.artifacts.exists(key):
            return None
        payload = self.artifacts.get_json(key)
        if payload.get("is_visible") is not True:
            return None
        return PublishedDocument.model_validate(
            payload["published_document"]
        ).model_copy(update={"is_visible": True})


def publication_id(
    pipeline_id: str,
    doc_id: str,
    doc_version: str,
    pipeline_version: str,
) -> str:
    raw = f"{pipeline_id}:{doc_id}:{doc_version}:{pipeline_version}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def validation_report_id(report: ValidationReport) -> str:
    payload = report.model_dump_json(exclude_none=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
