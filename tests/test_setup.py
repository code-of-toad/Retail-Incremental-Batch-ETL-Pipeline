import pytest
from retail_etl.config import SOURCE_FIELDS

pytestmark = pytest.mark.setup


@pytest.mark.parametrize('filename,count', [
    ('products.csv', 10), ('stores.csv', 5),
    ('sales_2026-04-01.csv', 60), ('sales_2026-04-02.csv', 50),
    ('sales_2026-04-03.csv', 40),
])
def test_seed_files_are_complete(seed_rows, filename, count):
    rows = seed_rows(filename)
    assert len(rows) == count
    assert all(None not in row and all(value is not None for value in row.values()) for row in rows)
    if filename.startswith('sales_'):
        assert tuple(rows[0]) == SOURCE_FIELDS
        assert len({row['source_row_id'] for row in rows}) == count


def test_local_spark_and_parquet(spark, tmp_path):
    path = str(tmp_path / 'setup_parquet')
    spark.range(3).write.parquet(path)
    assert spark.read.parquet(path).count() == 3
