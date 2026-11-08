from pathlib import Path

from km_sdk import ArtifactClient
from km_sdk.runner import PipelineRunner


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_sample_pipeline_publishes(tmp_path):
    runner = PipelineRunner(
        PROJECT_ROOT / "pipeline.yaml",
        project_root=PROJECT_ROOT,
        artifacts=ArtifactClient(tmp_path / "artifacts"),
    )
    report = runner.run(PROJECT_ROOT / "fixtures/request.json", persist=False)
    assert report.status == "succeeded"
    assert report.stage("publish").summary["is_visible"] is True


def test_validation_failure_blocks_publish(tmp_path):
    runner = PipelineRunner(
        PROJECT_ROOT / "pipeline.yaml",
        project_root=PROJECT_ROOT,
        artifacts=ArtifactClient(tmp_path / "artifacts"),
    )
    report = runner.run(
        PROJECT_ROOT / "fixtures/request.json",
        options_override={"forceValidationFailure": True},
        persist=False,
    )
    assert report.stage("validation").status == "failed"
    assert report.stage("publish").status == "blocked"
