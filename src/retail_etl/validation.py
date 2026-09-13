from pyspark.sql import DataFrame


def validate_dimensions(products_df: DataFrame, stores_df: DataFrame) -> None:
    # TODO D1: Raise ValueError for missing/blank required fields or duplicate IDs.
    raise NotImplementedError('D1: implement validate_dimensions')


def validate_sales(sales_df: DataFrame, products_df: DataFrame, stores_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    # TODO D1: Return accepted candidates and quarantine with rejection_reasons.
    # Start from parse_errors. Preserve raw values, source metadata, and every occurrence.
    # Validate required fields, domains, ranges, line discount, and dimension references.
    # Version duplicates belong to incremental.resolve_versions, not generic rejection.
    raise NotImplementedError('D1: implement validate_sales')


def compact_reasons(df: DataFrame, column_name: str = 'rejection_reasons') -> DataFrame:
    # TODO D1 lambda checkpoint: Remove null array elements with F.filter and your lambda.
    # The lambda receives a Spark Column expression, not a local Python string.
    raise NotImplementedError('D1: implement compact_reasons')
