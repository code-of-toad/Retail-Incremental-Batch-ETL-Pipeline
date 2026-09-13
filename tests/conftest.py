import json
import pytest
from retail_etl.config import PROJECT_ROOT
from retail_etl.seed_io import load_seed_rows
from retail_etl.session import make_spark


@pytest.fixture(scope='session')
def spark(tmp_path_factory):
    session = make_spark('retail-etl-tests', tmp_path_factory.mktemp('spark'))
    yield session
    session.stop()


@pytest.fixture
def seed_rows():
    # Returns a loader so tests can request any supplied delivery/dimension file.
    return load_seed_rows


@pytest.fixture(scope='session')
def expected_batches():
    path = PROJECT_ROOT / 'tests' / 'fixtures' / 'expected_batches.json'
    return json.loads(path.read_text(encoding='utf-8'))


@pytest.fixture
def duplicate_products_rows(seed_rows):
    # Copy source rows; never damage the committed dimension fixture.
    rows = seed_rows('products.csv')
    return rows + [dict(rows[0])]
