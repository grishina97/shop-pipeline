import requests                                         # библиотека для HTTP-запросов к API
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
API_URL = "https://api.frankfurter.dev/v1/2026-10-01.."  # курсы с 1 октября по сегодня (time series)
@dp.materialized_view(comment="Exchange rates from Frankfurter API (bronze)")  # MV: при каждом запуске перезапрашивает API
def bronze_rates():
    resp = requests.get(API_URL, params={"symbols": "CZK,USD,GBP,PLN"}, timeout=30)  # только нужные валюты
    resp.raise_for_status()                             # если API вернул ошибку → пайплайн упадёт с понятной ошибкой
    data = resp.json()                                  # {"base":"EUR","rates":{"2026-10-20":{"CZK":24.3,...},...}}
    rows = [
        (rate_date, currency, float(rate))              # одна строка = дата + валюта + курс
        for rate_date, day_rates in data["rates"].items()
        for currency, rate in day_rates.items()
    ]
    return (
        spark.createDataFrame(rows, "rate_date string, currency string, eur_to_currency double")  # 1 EUR = X валюты
        .withColumn("ingested_at", current_timestamp())  # когда загрузили
    )