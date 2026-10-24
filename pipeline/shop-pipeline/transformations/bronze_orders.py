from pyspark import pipelines as dp                    # API декларативных пайплайнов
from pyspark.sql.functions import col, current_timestamp

@dp.table(comment="Raw orders from CSV files (bronze)")  # создание streaming table: дописывает только новые файлы
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")          # cloudFiles = Auto Loader: следит за папкой, берёт только новые файлы
        .option("cloudFiles.format", "csv")            # формат файлов
        .option("header", "true")                      # первая строка = названия колонок
        .load("/Volumes/workspace/shop/raw_files/")    # откуда читать (наш volume)
        .withColumn("source_file", col("_metadata.file_name"))  # из какого файла пришла строка
        .withColumn("ingested_at", current_timestamp())         # когда загрузили
    )