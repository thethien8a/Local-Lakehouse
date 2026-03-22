from pyspark.sql import SparkSession
from pyspark.sql import functions as F

table_name = "nessie.taxi.silver"  # Thay thế bằng tên bảng của bạn

spark = SparkSession.builder.appName("Show Schema Table").getOrCreate()

spark.table(table_name).printSchema()