from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

BRANCH_NAME = f"etl_silver_{int(time.time())}"

spark = (
    SparkSession.builder
    .appName("Silver Transformation - NYC Taxi")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("[INFO] SparkSession đã sẵn sàng!")

# Tạo branch riêng để transform, tránh ảnh hưởng main
print(f"[INFO] Tạo branch '{BRANCH_NAME}' từ main...")
spark.sql(f"CREATE BRANCH IF NOT EXISTS `{BRANCH_NAME}` IN nessie FROM main")
spark.sql(f"USE REFERENCE `{BRANCH_NAME}` IN nessie")

print("[INFO] Đang đọc từ bảng Bronze...")
df_bronze = spark.table("nessie.taxi.bronze")
print(f"[INFO] Số dòng Bronze (raw): {df_bronze.count():,}") 

print("[INFO] Đang làm sạch dữ liệu...")


# Loại bỏ các dòng null ở tpep_pickup_datetime và tpep_dropoff_datetime
df_cleaned = df_bronze.filter(
    F.col("tpep_pickup_datetime").isNotNull() & 
    F.col("tpep_dropoff_datetime").isNotNull()
)


# Xét thời gian chỉ từ 1-2024
df_cleaned = df_cleaned.filter(F.col("tpep_pickup_datetime") >= "2024-01-01")

# vendor_id phải là: 1,2,6,7
df_cleaned = df_cleaned.filter(F.col("VendorID").isin(1,2,6,7))

# ratecode_id phải là: 1,2,3,4,5,6,99
df_cleaned = df_cleaned.filter(F.col("RatecodeID").isin(1,2,3,4,5,6,99))

# payment_type phải là: 0,1,2,3,4,5,6
df_cleaned = df_cleaned.filter(F.col("payment_type").isin(0,1,2,3,4,5,6))

# Các cột bắt buộc > 0
must_be_positive = ["fare_amount", "mta_tax", "improvement_surcharge", "total_amount", "trip_distance", "passenger_count"]
for col in must_be_positive:
    df_cleaned = df_cleaned.filter(F.col(col) > 0)

# Các cột cho phép = 0
can_be_zero = ["extra", "tip_amount", "tolls_amount", "congestion_surcharge", "Airport_fee"]
for col in can_be_zero:
    df_cleaned = df_cleaned.filter(F.col(col) >= 0)

# tpep_pickup_datetime must be before tpep_dropoff_datetime
df_cleaned = df_cleaned.filter(F.col("tpep_pickup_datetime") <= F.col("tpep_dropoff_datetime"))

row_count = df_cleaned.count()
print(f"[INFO] Số dòng sau khi làm sạch: {row_count:,}")
print(f"[INFO] Số dòng bị loại: {df_bronze.count() - row_count:,}")

# Tạo ra các cột mới (Feature Engineering)
df_cleaned = df_cleaned.withColumn("is_tip_more_total",
                                   F.when(F.col("tip_amount") > F.col("total_amount"), True).otherwise(False)) \
                       .withColumn("pickup_date", F.date_format(F.col("tpep_pickup_datetime"), "yyyy-MM-dd")) \
                       .withColumn("pickup_hour", F.hour(F.col("tpep_pickup_datetime"))) \
                       .withColumn("pickup_weekday", F.dayofweek(F.col("tpep_pickup_datetime"))) \
                       .withColumn("is_weekend", F.when(F.col("pickup_weekday").isin(1,7), True).otherwise(False)) \
                       .withColumn("time_bucket", F.when(F.col("pickup_hour").between(0,6), "early_morning").when(F.col("pickup_hour").between(7,12), "morning").when(F.col("pickup_hour").between(13,18), "afternoon").otherwise("evening")) \
                       .withColumn("trip_duration_min", (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60) \
                       .withColumn("avg_speed_mph", F.col("trip_distance") * 60 / F.col("trip_duration_min")) \
                       .withColumn("is_rush_hour", F.when((F.col("pickup_hour").between(7, 9)) | (F.col("pickup_hour").between(16, 19)), True).otherwise(False)) \
                       .withColumn("fare_per_mile", F.col("total_amount") / F.col("trip_distance")) \
                       .withColumn("fare_per_min", F.col("total_amount") / F.col("trip_duration_min"))

df_cleaned = df_cleaned.withColumnRenamed("VendorID", "vendor_id") \
                       .withColumnRenamed("RatecodeID", "ratecode_id") \
                       .withColumnRenamed("PULocationID", "pulocation_id") \
                       .withColumnRenamed("DOLocationID", "dolocation_id") \
                       .withColumnRenamed("Airport_fee", "airport_fee")

# Ghi vào Silver trên branch
df_cleaned.write.mode("overwrite").partitionBy("pickup_date").saveAsTable("nessie.taxi.silver")
print(f"[INFO] Đã ghi Silver trên branch {BRANCH_NAME}.")

# Validate trước khi merge
df_check = spark.table("nessie.taxi.silver")
silver_count = df_check.count()
null_pickup = df_check.filter(F.col("tpep_pickup_datetime").isNull()).count()
null_total = df_check.filter(F.col("total_amount").isNull()).count()



if silver_count > 0 and null_pickup == 0 and null_total == 0:
    print(f"[INFO] Validation OK — {silver_count:,} rows, no null pickup/total_amount")
    print(f"[INFO] Merge branch '{BRANCH_NAME}' vào main...")
    spark.sql(f"MERGE BRANCH `{BRANCH_NAME}` INTO main IN nessie")
    print("[INFO] Merge thành công!")
    
    # Dọn branch sau khi merge thành công
    spark.sql(f"DROP BRANCH IF EXISTS `{BRANCH_NAME}` IN nessie")

    spark.sql("USE REFERENCE main IN nessie")
    
    print("[INFO] Schema của bảng Silver:")
    df_cleaned.printSchema()
    
else:
    print(f"[ERROR] Validation FAILED — rows: {silver_count}, null_pickup: {null_pickup}, null_total: {null_total}")
    print(f"[ERROR] KHÔNG merge. Branch '{BRANCH_NAME}' giữ nguyên để debug.")

spark.stop()
print("[INFO] Hoàn tất Silver Transformation!")
