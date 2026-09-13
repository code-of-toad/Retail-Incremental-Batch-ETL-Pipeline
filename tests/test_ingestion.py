import pytest

pytestmark = pytest.mark.exercise


@pytest.mark.skip(reason='Exercise TODO: remove this marker and write the test.')
def test_explicit_sales_schema(spark, tmp_path, seed_rows, expected_batches, duplicate_products_rows):
    # D1: Compare all business field names/types to the README; do not only check counts.
    raise NotImplementedError('Write the exercise assertions yourself.')


@pytest.mark.skip(reason='Exercise TODO: remove this marker and write the test.')
def test_parse_missing_and_malformed(spark, tmp_path, seed_rows, expected_batches, duplicate_products_rows):
    # D1: Use Day 1 R053 and R055-R056; distinguish malformed quantity/date from missing order ID.
    raise NotImplementedError('Write the exercise assertions yourself.')


@pytest.mark.skip(reason='Exercise TODO: remove this marker and write the test.')
def test_source_identity_survives_replay(spark, tmp_path, seed_rows, expected_batches, duplicate_products_rows):
    # D1: Read twice with different run_id values and compare stable source identity.
    raise NotImplementedError('Write the exercise assertions yourself.')
