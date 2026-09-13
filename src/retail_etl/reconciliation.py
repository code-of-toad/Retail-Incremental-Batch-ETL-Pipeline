from pyspark.sql import DataFrame


def reconcile(before_df: DataFrame, after_df: DataFrame, outcomes: dict) -> dict:
    # TODO D1/D2: Assert row accounting, unique keys, and money deltas from the README.
    # Report actionable mismatches; never declare success using row counts alone.
    raise NotImplementedError('D1/D2: implement reconcile')
