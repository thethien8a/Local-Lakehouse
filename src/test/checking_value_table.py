from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Checking Value Table").getOrCreate()

spark.sql("SELECT DISTINCT ingestion_date FROM nessie.taxi.silver LIMIT 10").show(truncate=False)
spark.stop()