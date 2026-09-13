from pathlib import Path
from pyspark.sql import DataFrame


def publish_snapshot(fact_df: DataFrame, batch_id: str, run_id: str, output_root: Path,
                     batch_metadata: dict, fail_at: str | None = None) -> dict:
    # TODO D2: Stage a new snapshot; publish only after successful validation.
    # batch_metadata must include source content identity and committed batch history.
    # fail_at: after_stage or after_publish. Keep publication/history consistent.
    # Explain the single-writer, local-filesystem assumptions in the README.
    raise NotImplementedError('D2: implement publish_snapshot')
