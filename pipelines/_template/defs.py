"""Dagster entry point for the initial pipeline template."""

import dagster as dg

from .assets import ALL_ASSETS
from .contract import (
    PIPELINE_ID,
    STAGES,
    TERMINAL_ASSETS,
    stage_group,
    validate_template,
)
from .resources import TemplateStorage

pipeline_job = dg.define_asset_job(
    name=f"{PIPELINE_ID}__full",
    selection=dg.AssetSelection.assets(*TERMINAL_ASSETS.values()).upstream(),
)

stage_jobs = [
    dg.define_asset_job(
        name=f"{PIPELINE_ID}__repair_{spec.stage.value}",
        selection=dg.AssetSelection.groups(stage_group(spec.stage)),
    )
    for spec in STAGES
]

defs = dg.Definitions(
    assets=ALL_ASSETS,
    jobs=[pipeline_job, *stage_jobs],
    resources={"storage": TemplateStorage()},
    metadata={
        "km_pipeline_id": PIPELINE_ID,
        "km_contract_version": 1,
    },
)

validate_template(defs)
