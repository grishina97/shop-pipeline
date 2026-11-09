from pyspark import pipelines as dp                    # API for declarative pipelines
from pyspark.sql.functions import col, current_timestamp

# Volume with the daily order CSVs (synthetic, written by the generate_orders job task).
# The dev bundle target sets shop.raw_files_path to the personal dev volume; prod uses the default.
RAW_FILES_PATH = spark.conf.get("shop.raw_files_path", "/Volumes/workspace/shop/raw_files/")

@dp.table(comment="Raw orders from CSV files (bronze)")  # create streaming table: appends only new files
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")          # cloudFiles = Auto Loader: watches the folder, picks up only new files
        .option("cloudFiles.format", "csv")            # file format
        .option("header", "true")                      # first row = column names
        .load(RAW_FILES_PATH)                          # source path (our volume)
        .withColumn("source_file", col("_metadata.file_name"))  # which file the row came from
        .withColumn("ingested_at", current_timestamp())         # when loaded
    )