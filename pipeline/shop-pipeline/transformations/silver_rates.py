from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date, lit
@dp.materialized_view(comment="Exchange rates against the EUR by day (silver)")
@dp.expect_or_fail("rate_positive", "rate_to_eur > 0")          # rate <= 0 → STOP the pipeline (without the rate gold will be incorrect)
def silver_rates():
    rates = (
        spark.read.table("bronze_rates")                         # MV reads the entire table (not a stream)
        .select(
            to_date("rate_date").alias("rate_date"),             # string → date
            "currency",
            (lit(1.0) / col("eur_to_currency")).alias("rate_to_eur"),  # 1 EUR = 24.3 CZK → 1 CZK = 0.041 EUR
        )
    )
    eur = (
        rates.select("rate_date").distinct()                     # all dates for which rates exist
        .withColumn("currency", lit("EUR"))                      # add an EUR row for each date
        .withColumn("rate_to_eur", lit(1.0))                     # 1 EUR = 1 EUR
    )
    return rates.unionByName(eur)                                # 4 currencies from API + EUR


