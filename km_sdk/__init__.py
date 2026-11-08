"""Public KM Pipeline SDK API.

User handlers import only this package. Dagster is an internal runtime detail.
"""

from .artifacts import ArtifactClient
from .context import StageContext, StructuredLogger
from .errors import PipelineError
from .models import (
    STAGE_ORDER,
    TERMINAL_ASSETS,
    AssembledDocument,
    ExtractedEntity,
    ExtractedRelation,
    ExtractionResult,
    IngestedDocument,
    IngestRequest,
    ParsedChunk,
    ParsedDocument,
    PublishedDocument,
    Stage,
    ValidationReport,
    ValidationRuleResult,
)
from .resources import (
    KnowledgeIndex,
    LlmGateway,
    LocalKnowledgeIndex,
    LocalLlmGateway,
    publication_id,
    validation_report_id,
)

__version__ = "0.1.0"

__all__ = [
    "ArtifactClient",
    "AssembledDocument",
    "ExtractedEntity",
    "ExtractedRelation",
    "ExtractionResult",
    "IngestRequest",
    "IngestedDocument",
    "KnowledgeIndex",
    "LlmGateway",
    "LocalKnowledgeIndex",
    "LocalLlmGateway",
    "ParsedChunk",
    "ParsedDocument",
    "PipelineError",
    "PublishedDocument",
    "STAGE_ORDER",
    "Stage",
    "StageContext",
    "StructuredLogger",
    "TERMINAL_ASSETS",
    "ValidationReport",
    "ValidationRuleResult",
    "publication_id",
    "validation_report_id",
]
