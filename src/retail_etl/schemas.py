from pyspark.sql.types import (
    DateType, DecimalType, IntegerType, StringType, StructField,
    StructType, TimestampType,
)
from retail_etl.config import SOURCE_FIELDS

# Raw strings preserve invalid values until you diagnose them explicitly.
RAW_SALES_SCHEMA = StructType([
    StructField(name, StringType(), True) for name in SOURCE_FIELDS
])
PRODUCTS_SCHEMA = StructType([
    StructField('product_id', StringType(), True),
    StructField('product_name', StringType(), True),
    StructField('category', StringType(), True),
])
STORES_SCHEMA = StructType([
    StructField('store_id', StringType(), True),
    StructField('store_name', StringType(), True),
    StructField('province', StringType(), True),
])


def sales_schema() -> StructType:
    # TODO D1: Define the eleven typed business fields from the README contract.
    # WHAT: Allow nulls at parsing time so validation can diagnose missing/bad input.
    # WHY: Declaring nullable=False does not replace data-quality checks.
    raise NotImplementedError('D1: implement sales_schema')
