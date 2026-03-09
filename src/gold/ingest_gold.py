from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

def main() -> None:
    """
    Hàm chính để tổng hợp dữ liệu từ Silver thành các bảng Gold.
    Áp dụng mô hình Write-Audit-Publish (WAP) sử dụng Nessie.
    """
    spark = (
        SparkSession.builder
        .appName("Gold Transformation - NYC Taxi")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    print("[INFO] SparkSession đã sẵn sàng cho Gold Layer!")

    # WAP Pattern: Tạo branch mới cho Gold
    BRANCH_NAME = f"etl_gold_{int(time.time())}"
    
    print(f"[INFO] Tạo branch '{BRANCH_NAME}' từ main...")
    spark.sql(f"CREATE BRANCH IF NOT EXISTS `{BRANCH_NAME}` IN nessie FROM main")
    spark.sql(f"USE REFERENCE `{BRANCH_NAME}` IN nessie")

    print("[INFO] Đang đọc từ bảng Silver (nessie.taxi.silver)...")
    try:
        df_silver = spark.table("nessie.taxi.silver")
    except Exception as e:
        print(f"[ERROR] Không thể đọc bảng Silver. Hãy chắc chắn rằng bạn đã chạy ingest_silver.py. Lỗi: {e}")
        spark.stop()
        return

    # 1. TỔNG HỢP: Doanh thu theo giờ
    print("[INFO] Đang tổng hợp doanh thu theo giờ...")
    df_revenue_by_hour = (
        df_silver.groupBy("pickup_date", "pickup_hour")
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("*").alias("total_trips"),
            F.avg("total_amount").alias("avg_revenue_per_trip")
        )
        .orderBy("pickup_date", "pickup_hour")
    )
    
    # Ghi vào Iceberg
    df_revenue_by_hour.write.mode("overwrite").saveAsTable("nessie.taxi.gold_revenue_by_hour")
    print(f"[INFO] Đã ghi bảng nessie.taxi.gold_revenue_by_hour.")

    # 2. TỔNG HỢP: Khu vực nào tiềm năng nhất
    print("[INFO] Tổng hợp doanh thu, số lượng chuyến đi và doanh thu trung bình theo khu vực...")
    df_potential_locations = (
        df_silver.groupBy("pulocation_id")
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("*").alias("total_trips"),
            F.avg("total_amount").alias("avg_revenue_per_trip")
        )
    )
    
    # Ghi vào Iceberg
    df_potential_locations.write.mode("overwrite").saveAsTable("nessie.taxi.gold_revenue_by_location")
    print(f"[INFO] Đã ghi bảng nessie.taxi.gold_revenue_by_location.")
    
    # 3. TỔNG HỢP: Tiền tip trung bình theo khu vực
    print("[INFO] Tổng hợp tiền tip trung bình theo khu vực...")
    df_tip_by_location = (
        df_silver.groupBy("pulocation_id")
        .agg(
            F.avg("tip_amount").alias("avg_tip_amount")
        )
    )
    
    # Ghi vào Iceberg
    df_tip_by_location.write.mode("overwrite").saveAsTable("nessie.taxi.gold_tip_by_location")
    print(f"[INFO] Đã ghi bảng nessie.taxi.gold_tip_by_location.")
    
    # 4. TỔNG HỢP: Giá cả trung bình theo thời gian
    print("[INFO] Tổng hợp giá cả trung bình theo thời gian...")
    df_fare_by_hour = (
        df_silver.groupBy("pickup_date")
        .agg(
            F.avg("fare_amount").alias("avg_fare_amount")
        )
    )
    
    # Ghi vào Iceberg
    df_fare_by_hour.write.mode("overwrite").saveAsTable("nessie.taxi.gold_fare_by_date")
    print(f"[INFO] Đã ghi bảng nessie.taxi.gold_fare_by_date.")
    
    # 5. TỔNG HỢP: 
  