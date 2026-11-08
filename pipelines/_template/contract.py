"""Machine-readable contract for knowledge extraction pipelines."""

from dataclasses import dataclass
from enum import StrEnum
from typing import Iterable, Mapping

import dagster as dg

CONTRACT_VERSION = 1
PIPELINE_ID = "template_pipeline"
PIPELINE_VERSION = "0.1.0"
OWNER = "team:data-platform"

# The production template should replace this with the platform's dynamic
# document partition definition. A static demo partition keeps the initial
# template runnable without calling the Dagster instance first.
DOC_PARTITIONS = dg.StaticPartitionsDefinition(["demo__v1"])
DEMO_PARTITION = "demo__v1"


class Stage(StrEnum):
    INGEST = "ingest"
    PARSE = "parse"
    EXTRACTION = "extraction"
    ASSEMBLE = "assemble"
    VALIDATION = "validation"
    PUBLISH = "publish"


@dataclass(frozen=True)
class StageSpec:
    stage: Stage
    terminal_asset_key: dg.AssetKey


def asset_key(name: str) -> dg.AssetKey:
    return dg.AssetKey(["km", PIPELINE_ID, name])


TERMINAL_ASSETS: Mapping[Stage, dg.AssetKey] = {
    Stage.INGEST: asset_key("ingested_document"),
    Stage.PARSE: asset_key("parsed_document"),
    Stage.EXTRACTION: asset_key("extracted_knowledge"),
    Stage.ASSEMBLE: asset_key("assembled_document"),
    Stage.VALIDATION: asset_key("validation_report"),
    Stage.PUBLISH: asset_key("published_document"),
}

STAGES: tuple[StageSpec, ...] = tuple(
    StageSpec(stage=stage, terminal_asset_key=TERMINAL_ASSETS[stage])
    for stage in Stage
)
STAGE_ORDER = tuple(spec.stage for spec in STAGES)


def stage_group(stage: Stage) -> str:
    return f"km__{PIPELINE_ID}__{stage.value}"


def terminal_metadata(stage: Stage) -> dict[str, object]:
    return {
        "km_contract_version": CONTRACT_VERSION,
        "km_pipeline_id": PIPELINE_ID,
        "km_pipeline_version": PIPELINE_VERSION,
        "km_stage_id": stage.value,
        "km_stage_role": "terminal",
        "owner": OWNER,
    }


def internal_metadata(stage: Stage) -> dict[str, object]:
    """Metadata for assets inside a stage that are not the public boundary."""

    return {
        "km_contract_version": CONTRACT_VERSION,
        "km_pipeline_id": PIPELINE_ID,
        "km_pipeline_version": PIPELINE_VERSION,
        "km_stage_id": stage.value,
        "km_stage_role": "internal",
        "owner": OWNER,
    }


class TemplateContractError(ValueError):
    """Raised when a pipeline does not satisfy the shared stage contract."""


def _raw_metadata(metadata: Mapping[str, object], key: str) -> object | None:
    value = metadata.get(key)
    if value is None:
        return None
    return getattr(value, "value", value)


def _asset_specs(defs: dg.Definitions) -> Iterable[dg.AssetSpec]:
    for assets_def in defs.assets or []:
        if isinstance(assets_def, dg.AssetsDefinition):
            yield from assets_def.specs


def validate_template(defs: dg.Definitions) -> None:
    """Validate stage boundaries and fail fast during definition loading/CI."""

    violations: list[str] = []
    specs_by_key = {spec.key: spec for spec in _asset_specs(defs)}
    terminal_keys = set(TERMINAL_ASSETS.values())

    for stage, key in TERMINAL_ASSETS.items():
        spec = specs_by_key.get(key)
        if spec is None:
            violations.append(f"{stage.value}: missing terminal asset {key.to_user_string()}")
            continue

        metadata = spec.metadata or {}
        if _raw_metadata(metadata, "km_stage_id") != stage.value:
            violations.append(f"{stage.value}: invalid km_stage_id on {key.to_user_string()}")
        if _raw_metadata(metadata, "km_stage_role") != "terminal":
            violations.append(f"{stage.value}: asset is not marked as terminal")
        if spec.group_name != stage_group(stage):
            violations.append(
                f"{stage.value}: expected group {stage_group(stage)}, got {spec.group_name}"
            )
        if spec.partitions_def != DOC_PARTITIONS:
            violations.append(f"{stage.value}: terminal must use the shared DOC_PARTITIONS")

    asset_stage_by_key: dict[dg.AssetKey, str] = {}
    for key, spec in specs_by_key.items():
        metadata = spec.metadata or {}
        owned_by_pipeline = (
            tuple(key.path[:2]) == ("km", PIPELINE_ID)
            or _raw_metadata(metadata, "km_pipeline_id") == PIPELINE_ID
        )
        if not owned_by_pipeline:
            continue

        stage_value = _raw_metadata(metadata, "km_stage_id")
        if stage_value is None:
            violations.append(f"{key.to_user_string()}: missing km_stage_id")
            continue

        try:
            stage = Stage(str(stage_value))
        except ValueError:
            violations.append(f"{key.to_user_string()}: unknown stage {stage_value}")
            continue

        asset_stage_by_key[key] = stage.value
        role = _raw_metadata(metadata, "km_stage_role")
        expected_role = "terminal" if key in terminal_keys else "internal"
        if role != expected_role:
            violations.append(
                f"{key.to_user_string()}: expected km_stage_role={expected_role}, got {role}"
            )
        if spec.group_name != stage_group(stage):
            violations.append(
                f"{key.to_user_string()}: expected group {stage_group(stage)}, got {spec.group_name}"
            )
        if spec.partitions_def != DOC_PARTITIONS:
            violations.append(f"{key.to_user_string()}: must use the shared DOC_PARTITIONS")

    for key, spec in specs_by_key.items():
        current_stage = asset_stage_by_key.get(key)
        if current_stage is None:
            continue
        for dep in spec.deps:
            dep_stage = asset_stage_by_key.get(dep.asset_key)
            if dep_stage is None or dep_stage == current_stage:
                continue
            if dep.asset_key not in terminal_keys:
                violations.append(
                    f"{key.to_user_string()}: cross-stage dependency on "
                    f"non-terminal {dep.asset_key.to_user_string()}"
                )

    checks_by_asset: dict[dg.AssetKey, list[dg.AssetCheckSpec]] = {}
    for assets_def in defs.assets or []:
        if not isinstance(assets_def, dg.AssetsDefinition):
            continue
        for check_spec in assets_def.check_specs:
            checks_by_asset.setdefault(check_spec.asset_key, []).append(check_spec)

    for stage, key in TERMINAL_ASSETS.items():
        checks = checks_by_asset.get(key, [])
        if not checks:
            violations.append(f"{stage.value}: terminal asset has no asset checks")
        if stage in {Stage.VALIDATION, Stage.PUBLISH} and not any(
            check.blocking for check in checks
        ):
            violations.append(f"{stage.value}: terminal asset needs a blocking check")

    expected_job_names = {
        f"{PIPELINE_ID}__full",
        *(f"{PIPELINE_ID}__repair_{stage.value}" for stage in Stage),
    }
    loaded_job_names = {job.name for job in defs.jobs or []}
    missing_jobs = expected_job_names - loaded_job_names
    if missing_jobs:
        violations.append(f"missing jobs: {sorted(missing_jobs)}")

    if violations:
        details = "\n".join(f"- {violation}" for violation in violations)
        raise TemplateContractError(f"pipeline template contract failed:\n{details}")


if __name__ == "__main__":
    from .defs import defs

    validate_template(defs)
    print(f"Contract passed: {PIPELINE_ID} {PIPELINE_VERSION}")
