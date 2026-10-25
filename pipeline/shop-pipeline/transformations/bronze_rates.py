import requests                                         # library for HTTP requests to API
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
API_URL = "https://api.frankfurter.dev/v1/2026-10-01.."  # rates from October 1 to today (time series)
@dp.materialized_view(comment="Exchange rates from Frankfurter API (bronze)")  # MV: re-queries API on every run
def bronze_rates():
    resp = requests.get(API_URL, params={"symbols": "CZK,USD,GBP,PLN"}, timeout=30)  # only needed currencies
    resp.raise_for_status()                             # if API returned an error → pipeline fails with a clear error
    data = resp.json()                                  # {"base":"EUR","rates":{"2026-10-20":{"CZK":24.3,...},...}}
    rows = [
        (rate_date, currency, float(rate))              # one row = date + currency + rate
        for rate_date, day_rates in data["rates"].items()
        for currency, rate in day_rates.items()
    ]
    return (
        spark.createDataFrame(rows, "rate_date string, currency string, eur_to_currency double")  # 1 EUR = X currency
        .withColumn("ingested_at", current_timestamp())  # when loaded
    )