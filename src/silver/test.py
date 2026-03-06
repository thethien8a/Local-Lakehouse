from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Silver Test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.table("nessie.taxi.bronze")

# Hiển thị schema (kiểu dữ liệu của các cột)
df.printSchema()
