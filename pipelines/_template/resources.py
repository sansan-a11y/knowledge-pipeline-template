"""Local fakes for the initial template.

Production pipelines must replace TemplateStorage with the platform object-store
resource and the model gateway resource supplied by the platform SDK.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import dagster as dg

ARTIFACT_SCHEME = "template://"


class TemplateStorage(dg.ConfigurableResource):
    """Small durable-enough fake used by local runs and tests."""

    root_dir: str = ".template_data"
    force_validation_failure: bool = False

    def _root(self) -> Path:
        root = Path(self.root_dir)
        root.mkdir(parents=True, exist_ok=True)
        return root

    def _path(self, key: str) -> Path:
        path = self._root() / key
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def _key(uri: str) -> str:
        if not uri.startswith(ARTIFACT_SCHEME):
            raise ValueError(f"Unsupported artifact URI: {uri}")
        return uri.removeprefix(ARTIFACT_SCHEME)

    def put_json(self, key: str, payload: Any) -> str:
        path = self._path(key)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(temporary, path)
        return f"{ARTIFACT_SCHEME}{key}"

    def get_json(self, uri: str) -> Any:
        return json.loads(self._path(self._key(uri)).read_text(encoding="utf-8"))

    def ensure_demo_source(self, partition_key: str) -> dict[str, object]:
        """Create deterministic input for the template smoke test."""

        key = f"sources/{partition_key.replace('::', '__')}.json"
        path = self._path(key)
        if not path.exists():
            self.put_json(
                key,
                {
                    "doc_id": "demo",
                    "doc_version": "v1",
                    "tenant": "demo-tenant",
                    "source_uri": "demo://manual/demo-v1.pdf",
                    "text": (
                        "Centrifugal Pump P-100 requires monthly lubrication. "
                        "The suction valve V-12 must remain open during operation. "
                        "Inspect the mechanical seal every three months."
                    ),
                },
            )
        return self.get_json(f"{ARTIFACT_SCHEME}{key}")

    def publish(
        self,
        *,
        publication_id: str,
        assembled_document_uri: str,
        validation_report_uri: str,
        window_catalog_uri: str,
    ) -> str:
        """Write a version, run fake checks, then atomically switch visibility."""

        version_payload = {
            "publication_id": publication_id,
            "assembled_document_uri": assembled_document_uri,
            "validation_report_uri": validation_report_uri,
            "window_catalog_uri": window_catalog_uri,
            "index_namespace": f"shadow/{publication_id}",
            "is_visible": False,
        }
        version_uri = self.put_json(
            f"published/versions/{publication_id}.json",
            version_payload,
        )

        smoke_test_passed = (
            self.get_json(version_uri)["validation_report_uri"] == validation_report_uri
        )
        if not smoke_test_passed:
            raise RuntimeError("publish smoke test failed")

        visible_payload = {
            **version_payload,
            "version_uri": version_uri,
            "is_visible": True,
        }
        return self.put_json("published/visible.json", visible_payload)

    def get_visible_publication(self) -> dict[str, object] | None:
        path = self._path("published/visible.json")
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
