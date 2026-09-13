import csv
from pathlib import Path
from retail_etl.config import SEED_DIR


def load_seed_rows(filename: str, seed_dir: Path = SEED_DIR) -> list[dict[str, str]]:
    # Small local fixture loader only; the distributed ingestion exercise is separate.
    with (seed_dir / filename).open(newline='', encoding='utf-8') as source:
        return list(csv.DictReader(source))
