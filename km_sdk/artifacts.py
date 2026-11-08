"""Atomic local artifact storage used by the manifest SDK."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from pathlib import Path, PurePosixPath
from typing import Any


class ArtifactClient:
    """Filesystem-backed ArtifactClient with atomic JSON writes.

    Platform deployments may replace this object with an adapter that exposes
    the same methods. Handlers only receive this stable interface.
    """

    scheme = "km://"

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _key(self, uri: str) -> str:
        value = uri.removeprefix(self.scheme) if uri.startswith(self.scheme) else uri
        path = PurePosixPath(value)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError(f"Unsafe artifact key: {uri}")
        normalized = str(path)
        if normalized in {"", "."}:
            raise ValueError("Artifact key cannot be empty")
        return normalized

    def _path(self, uri: str) -> Path:
        path = (self.root / self._key(uri)).resolve()
        if self.root not in path.parents and path != self.root:
            raise ValueError(f"Artifact path escapes root: {uri}")
        return path

    def uri(self, key: str) -> str:
        return f"{self.scheme}{self._key(key)}"

    def exists(self, uri: str) -> bool:
        return self._path(uri).is_file()

    def put_bytes(self, key: str, payload: bytes) -> str:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{uuid.uuid4().hex[:12]}.tmp")
        with temporary.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        return self.uri(key)

    def get_bytes(self, uri: str) -> bytes:
        return self._path(uri).read_bytes()

    def put_json(self, key: str, payload: Any) -> str:
        serialized = json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            default=self._json_default,
        )
        return self.put_bytes(key, serialized.encode("utf-8"))

    def get_json(self, uri: str) -> Any:
        return json.loads(self.get_bytes(uri).decode("utf-8"))

    def put_text(self, key: str, payload: str) -> str:
        return self.put_bytes(key, payload.encode("utf-8"))

    def get_text(self, uri: str) -> str:
        return self.get_bytes(uri).decode("utf-8")

    def sha256(self, uri: str) -> str:
        return hashlib.sha256(self.get_bytes(uri)).hexdigest()

    @staticmethod
    def _json_default(value: object) -> object:
        if hasattr(value, "model_dump"):
            return value.model_dump(mode="json")
        if hasattr(value, "isoformat"):
            return value.isoformat()
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
