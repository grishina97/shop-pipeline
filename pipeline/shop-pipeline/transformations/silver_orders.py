from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp

@dp.table(comment="Cleaned orders (silver)")
@dp.expect_or_drop("customer_not_null", "customer_id IS NOT NULL")  # no customer → drop the row
@dp.expect_or_drop("quantity_positive", "quantity > 0")             # quantity 0 → drop
@dp.expect_or_drop("price_positive", "unit_price > 0")              # negative price → drop
def silver_orders():
    return (
        spark.readStream.table("bronze_orders")          # read bronze as a stream: only new rows
        .select(                                         # in bronze everything is string → cast types
            col("order_id").cast("bigint"),
            to_timestamp("order_ts").alias("order_ts"),
            "customer_id", "country", "product_id", "product_name", "category",
            col("quantity").cast("int"),
            col("unit_price").cast("double"),
            "currency", "status", "source_file",
        )
        .dropDuplicates(["order_id"])                    # remove duplicate orders (duplicate in CSV)
    )