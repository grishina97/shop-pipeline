from pyspark import pipelines as dp                    # API for declarative pipelines
from pyspark.sql.functions import col, current_timestamp

@dp.table(comment="Raw orders from CSV files (bronze)")  # create streaming table: appends only new files
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")          # cloudFiles = Auto Loader: watches the folder, picks up only new files
        .option("cloudFiles.format", "csv")            # file format
        .option("header", "true")                      # first row = column names
        .load("/Volumes/workspace/shop/raw_files/")    # source path (our volume)
        .withColumn("source_file", col("_metadata.file_name"))  # which file the row came from
        .withColumn("ingested_at", current_timestamp())         # when loaded
    )