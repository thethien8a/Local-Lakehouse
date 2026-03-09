import os

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Silver Test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

target_ref = os.getenv("NESSIE_REF", "main")
spark.sql(f"USE REFERENCE `{target_ref}` IN nessie")

print("[INFO] Current Nessie reference:")
spark.sql("SHOW REFERENCE IN nessie").show(truncate=False)

df = spark.table("nessie.taxi.silver")
df.show(10, truncate=False)
df.printSchema()

spark.stop()
