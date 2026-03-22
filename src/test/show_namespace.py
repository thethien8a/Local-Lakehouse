from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Show Namespaces").getOrCreate()

spark.sql("SHOW NAMESPACES IN nessie").show(truncate=False)
spark.stop()