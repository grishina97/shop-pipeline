from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp

@dp.table(comment="Очищенные заказы (silver)")
@dp.expect_or_drop("customer_not_null", "customer_id IS NOT NULL")  # нет клиента → строку выбросить
@dp.expect_or_drop("quantity_positive", "quantity > 0")             # количество 0 → выбросить
@dp.expect_or_drop("price_positive", "unit_price > 0")              # отрицательная цена → выбросить
def silver_orders():
    return (
        spark.readStream.table("bronze_orders")          # читаем bronze как поток: только новые строки
        .select(                                         # в bronze всё строки (string) → приводим типы
            col("order_id").cast("bigint"),
            to_timestamp("order_ts").alias("order_ts"),
            "customer_id", "country", "product_id", "product_name", "category",
            col("quantity").cast("int"),
            col("unit_price").cast("double"),
            "currency", "status", "source_file",
        )
        .dropDuplicates(["order_id"])                    # убрать повторные заказы (дубликат в CSV)
    )