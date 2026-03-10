from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Show All Tables").getOrCreate()

spark.sql("SHOW TABLES IN nessie.taxi").show(truncate=False)