from pyspark.sql import DataFrame


def build_fact_sales(sales_df: DataFrame, products_df: DataFrame, stores_df: DataFrame) -> DataFrame:
    # TODO D1: Enrich resolved candidates without changing order-line grain.
    # Include the eleven business fields, metadata, product/store attributes,
    # gross_sales, net_sales, gross_margin (decimal(18,2)), and completed_units (int).
    # Keep cancelled lines but zero their realized metrics. Select columns explicitly.
    raise NotImplementedError('D1: implement build_fact_sales')


def aggregate_daily_sales(fact_df: DataFrame) -> DataFrame:
    # TODO D1: One row per sale_date/store_id with revenue, completed_units,
    # completed_orders, and gross_margin; count orders only among completed lines.
    raise NotImplementedError('D1: implement aggregate_daily_sales')
