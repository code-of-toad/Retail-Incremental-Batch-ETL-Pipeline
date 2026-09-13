import pytest

pytestmark = pytest.mark.exercise


@pytest.mark.skip(reason='Exercise TODO: remove this marker and write the test.')
def test_initial_duplicates_and_conflicts(spark, tmp_path, seed_rows, expected_batches, duplicate_products_rows):
    # D1: Assert 52 valid occurrences -> 44 candidates + 2 conflicts + 6 ignored.
    raise NotImplementedError('Write the exercise assertions yourself.')


@pytest.mark.skip(reason='Exercise TODO: remove this marker and write the test.')
def test_newer_stale_replayed_and_conflicting_versions(spark, tmp_path, seed_rows, expected_batches, duplicate_products_rows):
    # D2: Use Day 2 named source rows; compare to the first committed result.
    raise NotImplementedError('Write the exercise assertions yourself.')


@pytest.mark.skip(reason='Exercise TODO: remove this marker and write the test.')
def test_late_arrival_changes_historical_totals(spark, tmp_path, seed_rows, expected_batches, duplicate_products_rows):
    # D2: Assert O3901/O3902 appear under April 1 after Day 3.
    raise NotImplementedError('Write the exercise assertions yourself.')


@pytest.mark.skip(reason='Exercise TODO: remove this marker and write the test.')
def test_replay_preserves_business_snapshot(spark, tmp_path, seed_rows, expected_batches, duplicate_products_rows):
    # D2: Replay every batch; compare complete business rows and money, excluding attempt metadata.
    raise NotImplementedError('Write the exercise assertions yourself.')
