from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Checking Value Table").getOrCreate()
df_bronze = spark.table("nessie.taxi.bronze")
target_date = df_bronze.agg(F.max("ingestion_date")).collect()[0][0]
print(target_date)

spark.stop()