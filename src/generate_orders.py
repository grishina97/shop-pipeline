"""Generate one synthetic daily order file for the shop pipeline.

Runs as the first task (generate_orders) of the shop_daily job on serverless compute. Writes
orders_<YYYY-MM-DD>.csv into the raw files volume, which the Lakeflow pipeline then ingests with Auto Loader.
The data is synthetic: there is no real shop behind it, the file only keeps bronze_orders fresh.

Format and content follow sample_data/: same columns, prices per product and currency, country -> currency,
rows sorted by order_ts, order_ids shuffled within the day's range. The file contains the same intentional
data-quality issues as the samples (one duplicate row, two rows with quantity 0, one negative price, sometimes
an empty customer_id), which dbt flags with is_valid and filters out in fct_orders.

Idempotent: if the file for the date already exists, nothing is written.

Arguments:
  --raw-files-path  volume folder, e.g. /Volumes/workspace/shop/raw_files
  --orders-table    bronze orders table, e.g. workspace.shop.bronze_orders (used for the current max order_id)
  --date            optional YYYY-MM-DD, defaults to today in Europe/Prague
"""

import argparse
import csv
import os
import random
import tempfile
import shutil
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

TIMEZONE = ZoneInfo("Europe/Prague")
FIRST_ORDER_ID = 100001          # first id used when there is no data at all yet

COLUMNS = ["order_id", "order_ts", "customer_id", "country", "product_id", "product_name",
           "category", "quantity", "unit_price", "currency", "status"]

# country -> currency, as in sample_data/
COUNTRY_CURRENCY = {"CZ": "CZK", "DE": "EUR", "FR": "EUR", "GB": "GBP", "PL": "PLN", "US": "USD"}

# product_id -> (product_name, category, unit price per currency), prices copied from sample_data/
PRODUCTS = {
    "P001": ("Laptop", "electronics", {"EUR": 899.0, "CZK": 22025.5, "GBP": 764.15, "PLN": 3865.7, "USD": 970.92}),
    "P002": ("Headphones", "electronics", {"EUR": 79.9, "CZK": 1957.55, "GBP": 67.92, "PLN": 343.57, "USD": 86.29}),
    "P003": ("Keyboard", "electronics", {"EUR": 49.5, "CZK": 1212.75, "GBP": 42.07, "PLN": 212.85, "USD": 53.46}),
    "P004": ("Coffee beans 1kg", "food", {"EUR": 18.9, "CZK": 463.05, "GBP": 16.06, "PLN": 81.27, "USD": 20.41}),
    "P005": ("Green tea", "food", {"EUR": 7.5, "CZK": 183.75, "GBP": 6.38, "PLN": 32.25, "USD": 8.1}),
    "P006": ("Backpack", "accessories", {"EUR": 59.0, "CZK": 1445.5, "GBP": 50.15, "PLN": 253.7, "USD": 63.72}),
    "P007": ("Water bottle", "accessories", {"EUR": 15.0, "CZK": 367.5, "GBP": 12.75, "PLN": 64.5, "USD": 16.2}),
    "P008": ("Desk lamp", "home", {"EUR": 34.9, "CZK": 855.05, "GBP": 29.66, "PLN": 150.07, "USD": 37.69}),
    "P009": ("Chair cushion", "home", {"EUR": 22.0, "CZK": 539.0, "GBP": 18.7, "PLN": 94.6, "USD": 23.76}),
    "P010": ("Notebook A5", "office", {"EUR": 4.2, "CZK": 102.9, "GBP": 3.57, "PLN": 18.06, "USD": 4.54}),
}

QUANTITY_WEIGHTS = {1: 0.48, 2: 0.32, 3: 0.20}                           # roughly the sample distribution
STATUS_WEIGHTS = {"paid": 0.81, "cancelled": 0.10, "refunded": 0.09}
CUSTOMER_COUNT = 300                                                   # customers C0001..C0300
EMPTY_CUSTOMER_PROBABILITY = 0.4                                       # "sometimes" one row has no customer_id


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--raw-files-path", required=True)
    parser.add_argument("--orders-table", required=True)
    parser.add_argument("--date", default=None)
    return parser.parse_args()


def max_order_id(raw_files_path, orders_table):
    """Current max order_id: bronze_orders plus files in the volume that bronze has not ingested yet.

    The second part keeps ids unique if an earlier file was generated but the pipeline failed before loading it.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    current_max, ingested_files = None, set()
    if spark.catalog.tableExists(orders_table):
        current_max = spark.sql(
            f"select max(try_cast(order_id as bigint)) as max_id from {orders_table}"
        ).first()["max_id"]
        ingested_files = {r["source_file"] for r in spark.sql(f"select distinct source_file from {orders_table}").collect()}

    for name in os.listdir(raw_files_path):
        if not name.endswith(".csv") or name in ingested_files:
            continue
        with open(os.path.join(raw_files_path, name), newline="") as f:
            for row in csv.DictReader(f):
                if (row.get("order_id") or "").isdigit():
                    current_max = max(current_max or 0, int(row["order_id"]))

    return current_max if current_max is not None else FIRST_ORDER_ID - 1


def generate_rows(order_date, first_id, rng, until=None):
    """Return the day's rows, sorted by order_ts, with the intentional data-quality issues injected."""
    is_weekend = order_date.weekday() >= 5
    count = rng.randint(90, 110) if is_weekend else rng.randint(140, 160)

    # timestamps within the day; for today only up to now, so no order lies in the future
    day_start = datetime.combine(order_date, datetime.min.time())
    day_seconds = 24 * 3600
    if until is not None:
        day_seconds = max(3600, min(day_seconds, int((until - day_start).total_seconds())))
    timestamps = sorted(day_start + timedelta(seconds=rng.randrange(day_seconds)) for _ in range(count))

    ids = list(range(first_id, first_id + count))
    rng.shuffle(ids)                                                   # ids are not in time order, as in the samples

    rows = []
    for order_id, ts in zip(ids, timestamps):
        country = rng.choice(list(COUNTRY_CURRENCY))
        currency = COUNTRY_CURRENCY[country]
        product_id = rng.choice(list(PRODUCTS))
        name, category, prices = PRODUCTS[product_id]
        rows.append({
            "order_id": order_id,
            "order_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "customer_id": f"C{rng.randint(1, CUSTOMER_COUNT):04d}",
            "country": country,
            "product_id": product_id,
            "product_name": name,
            "category": category,
            "quantity": rng.choices(list(QUANTITY_WEIGHTS), weights=QUANTITY_WEIGHTS.values())[0],
            "unit_price": prices[currency],
            "currency": currency,
            "status": rng.choices(list(STATUS_WEIGHTS), weights=STATUS_WEIGHTS.values())[0],
        })

    # intentional data-quality issues, each on a different row
    zero_qty_1, zero_qty_2, negative, duplicate, no_customer = rng.sample(range(count), 5)
    rows[zero_qty_1]["quantity"] = 0
    rows[zero_qty_2]["quantity"] = 0
    rows[negative]["unit_price"] = -rows[negative]["unit_price"]
    if rng.random() < EMPTY_CUSTOMER_PROBABILITY:
        rows[no_customer]["customer_id"] = ""
    rows.insert(duplicate + 1, dict(rows[duplicate]))                  # exact duplicate right after the original
    return rows


def main():
    args = parse_args()
    now = datetime.now(TIMEZONE).replace(tzinfo=None)
    order_date = date.fromisoformat(args.date) if args.date else now.date()
    target = os.path.join(args.raw_files_path, f"orders_{order_date.isoformat()}.csv")

    if os.path.exists(target):
        print(f"{target} already exists, nothing to do")
        return

    first_id = max_order_id(args.raw_files_path, args.orders_table) + 1
    rng = random.Random(order_date.toordinal())                        # same date -> same file
    rows = generate_rows(order_date, first_id, rng, until=now if order_date == now.date() else None)

    # write locally first and copy the finished file, so Auto Loader never sees a partial file in the volume
    with tempfile.TemporaryDirectory() as tmp:
        local = os.path.join(tmp, os.path.basename(target))
        with open(local, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=COLUMNS, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
        shutil.copyfile(local, target)

    print(f"wrote {target}: {len(rows)} rows, order_id {first_id}..{first_id + len(rows) - 2}")


if __name__ == "__main__":
    main()
