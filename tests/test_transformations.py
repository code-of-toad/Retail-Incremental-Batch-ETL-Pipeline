import pytest

pytestmark = pytest.mark.exercise


@pytest.mark.skip(reason='Exercise TODO: remove this marker and write the test.')
def test_decimal_measures_and_cancellation(spark, tmp_path, seed_rows, expected_batches, duplicate_products_rows):
    # D1: Assert O1001 line 1 = 20/19/7 and cancelled realized metrics are zero.
    raise NotImplementedError('Write the exercise assertions yourself.')


@pytest.mark.skip(reason='Exercise TODO: remove this marker and write the test.')
def test_enrichment_preserves_grain(spark, tmp_path, seed_rows, expected_batches, duplicate_products_rows):
    # D1: Assert keys and row multiplicities, then test a duplicate dimension failure upstream.
    raise NotImplementedError('Write the exercise assertions yourself.')


@pytest.mark.skip(reason='Exercise TODO: remove this marker and write the test.')
def test_dataframe_and_sql_agree(spark, tmp_path, seed_rows, expected_batches, duplicate_products_rows):
    # D1: Compare equivalent daily aggregation results and schemas without relying on row order.
    raise NotImplementedError('Write the exercise assertions yourself.')
