"""Handler-facing execution context and structured logging."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

from .artifacts import ArtifactClient
from .models import Stage


class StructuredLogger:
    """Small logging facade that avoids leaking Dagster logging objects."""

    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)

    def _log(self, level: int, message: str, **fields: object) -> None:
        payload = json.dumps(fields, ensure_ascii=False, default=str, sort_keys=True)
        self._logger.log(level, "%s %s", message, payload)

    def debug(self, message: str, **fields: object) -> None:
        self._log(logging.DEBUG, message, **fields)

    def info(self, message: str, **fields: object) -> None:
        self._log(logging.INFO, message, **fields)

    def warning(self, message: str, **fields: object) -> None:
        self._log(logging.WARNING, message, **fields)

    def error(self, message: str, **fields: object) -> None:
        self._log(logging.ERROR, message, **fields)


@dataclass(frozen=True)
class StageContext:
    pipeline_id: str
    pipeline_version: str
    task_id: str
    partition_key: str
    doc_id: str
    doc_version: str
    tenant: str
    stage: Stage
    attempt: int
    options: Mapping[str, object]
    artifacts: ArtifactClient
    logger: StructuredLogger

    def __post_init__(self) -> None:
        object.__setattr__(self, "options", MappingProxyType(dict(self.options)))
