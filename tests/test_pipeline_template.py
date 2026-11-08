"""Contract and end-to-end tests for the initial pipeline template."""

import dagster as dg
import pytest

from pipelines._template.assets import ALL_ASSETS
from pipelines._template.contract import (
    DOC_PARTITIONS,
    DEMO_PARTITION,
    PIPELINE_ID,
    TERMINAL_ASSETS,
    Stage,
    TemplateContractError,
    internal_metadata,
    stage_group,
    validate_template,
)
from pipelines._template.defs import defs
from pipelines._template.resources import TemplateStorage


@pytest.fixture
def instance():
    return dg.DagsterInstance.ephemeral()


def _materialize(instance, tmp_path, *, force_validation_failure: bool = False):
    storage = TemplateStorage(
        root_dir=str(tmp_path / "artifacts"),
        force_validation_failure=force_validation_failure,
    )
    result = dg.materialize(
        ALL_ASSETS,
        instance=instance,
        resources={"storage": storage},
        partition_key=DEMO_PARTITION,
        raise_on_error=False,
    )
    return result, storage


def test_template_contract_passes():
    validate_template(defs)
    assert defs.resolve_job_def(f"{PIPELINE_ID}__full")
    for stage in Stage:
        assert defs.resolve_job_def(f"{PIPELINE_ID}__repair_{stage.value}")


def test_contract_rejects_missing_terminal_asset():
    with pytest.raises(TemplateContractError, match="missing terminal asset"):
        validate_template(
            dg.Definitions(
                assets=[
                    asset
                    for asset in ALL_ASSETS
                    if asset.key != TERMINAL_ASSETS[Stage.PUBLISH]
                ],
                jobs=[],
            )
        )


def test_contract_accepts_internal_asset_in_stage_group():
    @dg.asset(
        key=dg.AssetKey(["km", PIPELINE_ID, "page_classification"]),
        partitions_def=DOC_PARTITIONS,
        group_name=stage_group(Stage.PARSE),
        metadata=internal_metadata(Stage.PARSE),
    )
    def page_classification() -> str:
        return "template://parse/demo/classification.json"

    validate_template(
        dg.Definitions(
            assets=[*ALL_ASSETS, page_classification],
            jobs=list(defs.jobs or []),
        )
    )


def test_six_stage_pipeline_publishes(instance, tmp_path):
    result, storage = _materialize(instance, tmp_path)
    assert result.success

    checks = {check.check_name: check for check in result.get_asset_check_evaluations()}
    assert checks["publication_gate_passed"].passed is True
    assert checks["publication_committed"].passed is True

    visible = storage.get_visible_publication()
    assert visible is not None
    assert visible["is_visible"] is True
    assert visible["publication_id"]


def test_validation_failure_blocks_publish(instance, tmp_path):
    result, storage = _materialize(
        instance,
        tmp_path,
        force_validation_failure=True,
    )
    assert result.success is False

    checks = {check.check_name: check for check in result.get_asset_check_evaluations()}
    assert checks["publication_gate_passed"].passed is False
    assert "publication_committed" not in checks
    assert storage.get_visible_publication() is None


def test_stage_start_observations_are_emitted(instance, tmp_path):
    result, _ = _materialize(instance, tmp_path)
    records = instance.get_records_for_run(
        result.run_id,
        of_type=dg.DagsterEventType.ASSET_OBSERVATION,
    ).records
    started_stages = set()
    for record in records:
        observation = record.asset_observation
        if observation is None:
            continue
        metadata = {
            key: getattr(value, "value", value)
            for key, value in observation.metadata.items()
        }
        if metadata.get("km_stage_state") == "STARTED":
            started_stages.add(metadata["km_stage_id"])
    assert started_stages == {stage.value for stage in Stage}
