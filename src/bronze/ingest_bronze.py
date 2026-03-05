"""
BRONZE LAYER - Ingestion Script
================================
Mục tiêu: Đọc file dữ liệu NYC Taxi thô và ghi vào bảng Iceberg
được quản lý bởi Nessie Catalog trong MinIO.

KHÔNG làm sạch, KHÔNG biến đổi - giữ nguyên 100% dữ liệu gốc.
"""
from pyspark.sql import SparkSession
from src.config.path import RAW_DATA_PATH

spark = (
    SparkSession.builder
    .appName("Bronze Ingestion - NYC Taxi")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(f"[INFO] Đang đọc dữ liệu từ: {RAW_DATA_PATH}")
df_raw = spark.read.parquet(RAW_DATA_PATH)

print("[INFO] Schema (cấu trúc cột) của dữ liệu thô:")
df_raw.printSchema()
print("[INFO] Xem thử 5 dòng đầu:")
df_raw.show(5, truncate=False)

print("[INFO] Đang tạo database 'taxi' trong catalog Nessie...")
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.taxi")

print("[INFO] Đang ghi dữ liệu vào bảng Iceberg 'nessie.taxi.bronze'...")
(
    df_raw.writeTo("nessie.taxi.bronze")
    .tableProperty("write.format.default", "parquet") 
    .createOrReplace() 
)

print("[INFO] Ghi thành công vào bảng Bronze!")

print("[INFO] Kiểm tra lại bằng cách đọc từ Nessie Catalog...")
df_verify = spark.table("nessie.taxi.bronze")
print(f"[INFO] Số dòng trong bảng Bronze (qua Nessie): {df_verify.count():,}")

spark.stop()
print("[INFO] Hoàn tất Bronze Ingestion!")
