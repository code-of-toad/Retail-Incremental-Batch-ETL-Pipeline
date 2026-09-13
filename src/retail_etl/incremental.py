from pyspark.sql import DataFrame


def resolve_versions(incoming_df: DataFrame, current_df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    # TODO D1: First support empty current_df for initial version resolution.
    # TODO D2: Extend comparison to the committed current source fields.
    # Return candidates, conflict rejects, and ignored incoming occurrences.
    # Compare business payloads, excluding delivery metadata and derived attributes.
    # Candidates carry change_type INSERT/UPDATE; ignored rows carry ignore_reason.
    # Keep all source occurrences accounted for. See the README conflict policy.
    raise NotImplementedError('D1/D2: implement resolve_versions')


def apply_upserts(current_df: DataFrame, candidates_df: DataFrame) -> DataFrame:
    # TODO D2: Return the new source-state DataFrame at unique order-line grain.
    # Do not append corrections as extra fact rows. No writes belong in this function.
    raise NotImplementedError('D2: implement apply_upserts')
