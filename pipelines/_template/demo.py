"""Run the template once with an ephemeral Dagster instance."""

import dagster as dg

from .contract import DEMO_PARTITION, PIPELINE_ID
from .defs import defs


def main() -> None:
    instance = dg.DagsterInstance.ephemeral()
    result = defs.resolve_job_def(f"{PIPELINE_ID}__full").execute_in_process(
        instance=instance,
        partition_key=DEMO_PARTITION,
    )
    checks = {check.check_name: check.passed for check in result.get_asset_check_evaluations()}
    print(f"success={result.success}")
    print(f"partition={DEMO_PARTITION}")
    print(f"checks={checks}")


if __name__ == "__main__":
    main()
