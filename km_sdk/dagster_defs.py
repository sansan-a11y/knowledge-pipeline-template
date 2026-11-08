"""Generic Dagster entrypoint driven by environment variables.

Set ``KM_PIPELINE_MANIFEST`` and ``KM_PIPELINE_ROOT``. The framework imports
this module for every manifest-based code location.
"""

from __future__ import annotations

import os
from pathlib import Path

from .dagster_runtime import build_definitions
from .errors import PipelineError


def build() :
    root = Path(os.environ.get("KM_PIPELINE_ROOT", os.getcwd())).resolve()
    manifest = Path(
        os.environ.get("KM_PIPELINE_MANIFEST", root / "pipeline.yaml")
    ).resolve()
    if not manifest.is_file():
        raise PipelineError(
            code="E_MANIFEST_INVALID",
            message=f"KM_PIPELINE_MANIFEST does not exist: {manifest}",
            details={
                "KM_PIPELINE_MANIFEST": str(manifest),
                "KM_PIPELINE_ROOT": str(root),
            },
        )
    return build_definitions(manifest, project_root=root)


defs = build()
