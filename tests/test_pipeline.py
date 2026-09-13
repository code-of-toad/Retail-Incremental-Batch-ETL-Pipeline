import pytest

pytestmark = pytest.mark.exercise


@pytest.mark.skip(reason='Exercise TODO: remove this marker and write the test.')
def test_three_batch_reconciliation(spark, tmp_path, seed_rows, expected_batches, duplicate_products_rows):
    # D1/D2: Use expected_batches; assert row counts, unique keys, totals, and outcome accounting.
    raise NotImplementedError('Write the exercise assertions yourself.')


@pytest.mark.skip(reason='Exercise TODO: remove this marker and write the test.')
def test_failure_before_publish(spark, tmp_path, seed_rows, expected_batches, duplicate_products_rows):
    # D2: Inject after_stage; verify prior result remains visible, then retry successfully.
    raise NotImplementedError('Write the exercise assertions yourself.')


@pytest.mark.skip(reason='Exercise TODO: remove this marker and write the test.')
def test_failure_after_publish(spark, tmp_path, seed_rows, expected_batches, duplicate_products_rows):
    # D2: Inject after_publish; restart from committed metadata and verify no duplicate effects.
    raise NotImplementedError('Write the exercise assertions yourself.')


@pytest.mark.skip(reason='Exercise TODO: remove this marker and write the test.')
def test_batch_identity_rejects_changed_source(spark, tmp_path, seed_rows, expected_batches, duplicate_products_rows):
    # D2: Copy seeds to tmp_path, commit a batch, alter a copy, and assert identity mismatch.
    raise NotImplementedError('Write the exercise assertions yourself.')
