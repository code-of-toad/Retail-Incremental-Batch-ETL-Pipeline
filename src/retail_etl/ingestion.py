from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from retail_etl.config import SEED_DIR
from retail_etl.schemas import RAW_SALES_SCHEMA, PRODUCTS_SCHEMA, STORES_SCHEMA


def read_dimensions(spark: SparkSession, seed_dir: Path = SEED_DIR) -> tuple[DataFrame, DataFrame]:
    # TODO D1: Read both dimension CSVs with explicit schemas; return products, stores.
    raise NotImplementedError('D1: implement read_dimensions')


def read_batch(spark: SparkSession, path: Path, batch_id: str, run_id: str) -> DataFrame:
    # TODO D1: Read using RAW_SALES_SCHEMA; attach batch_id/run_id/source_file.
    # Source files already supply source_row_id. Preserve it, including duplicates' IDs.
    # Return raw source fields plus metadata, with no silently dropped occurrences.
    raise NotImplementedError('D1: implement read_batch')


def parse_sales(raw_df: DataFrame) -> DataFrame:
    # TODO D1: Preserve business fields as raw_<field>, and build typed business fields.
    # Add parse_errors: array<string>. Distinguish empty values from failed conversions.
    # ANSI mode is on: choose safe parsing rather than disabling errors globally.
    raise NotImplementedError('D1: implement parse_sales')
