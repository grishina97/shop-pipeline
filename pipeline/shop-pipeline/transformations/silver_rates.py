from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date, lit
@dp.materialized_view(comment="Exchange rates against the EUR by day (silver)")
@dp.expect_or_fail("rate_positive", "rate_to_eur > 0")          # курс <= 0 → ОСТАНОВИТЬ пайплайн (без курса gold будет неверным)
def silver_rates():
    rates = (
        spark.read.table("bronze_rates")                         # MV читает таблицу целиком (не поток)
        .select(
            to_date("rate_date").alias("rate_date"),             # string → date
            "currency",
            (lit(1.0) / col("eur_to_currency")).alias("rate_to_eur"),  # 1 EUR = 24.3 CZK → 1 CZK = 0.041 EUR
        )
    )
    eur = (
        rates.select("rate_date").distinct()                     # все даты, за которые есть курсы
        .withColumn("currency", lit("EUR"))                      # добавить строку EUR на каждую дату
        .withColumn("rate_to_eur", lit(1.0))                     # 1 EUR = 1 EUR
    )
    return rates.unionByName(eur)                                # 4 валюты из API + EUR


