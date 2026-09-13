import pytest

pytestmark = pytest.mark.exercise


@pytest.mark.skip(reason='Exercise TODO: remove this marker and write the test.')
def test_all_rejection_reasons(spark, tmp_path, seed_rows, expected_batches, duplicate_products_rows):
    # D1: Use R060 from Day 1; assert every required reason and raw evidence.
    raise NotImplementedError('Write the exercise assertions yourself.')


@pytest.mark.skip(reason='Exercise TODO: remove this marker and write the test.')
def test_dimension_keys_and_referential_integrity(spark, tmp_path, seed_rows, expected_batches, duplicate_products_rows):
    # D1: Use duplicate_products_rows and unknown product/store sales; assert batch failure vs row rejection.
    raise NotImplementedError('Write the exercise assertions yourself.')


@pytest.mark.skip(reason='Exercise TODO: remove this marker and write the test.')
def test_validation_row_accounting(spark, tmp_path, seed_rows, expected_batches, duplicate_products_rows):
    # D1: Assert 60 -> 52 candidates + 8 validation rejects; no duplicate-version rejection yet.
    raise NotImplementedError('Write the exercise assertions yourself.')


@pytest.mark.skip(reason='Exercise TODO: remove this marker and write the test.')
def test_compact_reasons_lambda(spark, tmp_path, seed_rows, expected_batches, duplicate_products_rows):
    # D1: Build arrays with null elements, an empty array, and strings; assert F.filter behavior.
    raise NotImplementedError('Write the exercise assertions yourself.')
