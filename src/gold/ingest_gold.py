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

    # -------------------------------------------------------------------------
    # 1. TỔNG HỢP: Doanh thu theo giờ
    # -------------------------------------------------------------------------
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
    df_revenue_by_hour.write.mode("overwrite").saveAsTable("nessie.taxi.gold_revenue_by_hour")
    print("[INFO] Đã ghi bảng nessie.taxi.gold_revenue_by_hour.")

    # -------------------------------------------------------------------------
    # 2. TỔNG HỢP: Khu vực nào tiềm năng nhất
    # -------------------------------------------------------------------------
    print("[INFO] Tổng hợp doanh thu, số lượng chuyến đi và doanh thu trung bình theo khu vực...")
    df_potential_locations = (
        df_silver.groupBy("pulocation_id")
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("*").alias("total_trips"),
            F.avg("total_amount").alias("avg_revenue_per_trip"),
            F.avg("tip_amount").alias("avg_tip_amount"),
            F.avg("fare_per_mile").alias("avg_fare_per_mile")
        )
    )
    df_potential_locations.write.mode("overwrite").saveAsTable("nessie.taxi.gold_revenue_by_location")
    print("[INFO] Đã ghi bảng nessie.taxi.gold_revenue_by_location.")

    # -------------------------------------------------------------------------
    # 3. TỔNG HỢP: Giá cả trung bình theo ngày
    # -------------------------------------------------------------------------
    print("[INFO] Tổng hợp giá cả trung bình theo ngày...")
    df_fare_by_date = (
        df_silver.groupBy("pickup_date")
        .agg(
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.count("*").alias("total_trips")
        )
    )
    df_fare_by_date.write.mode("overwrite").saveAsTable("nessie.taxi.gold_fare_by_date")
    print("[INFO] Đã ghi bảng nessie.taxi.gold_fare_by_date.")

    # -------------------------------------------------------------------------
    # 4. TỔNG HỢP: Cặp hành trình phổ biến nhất (Origin-Destination Pairs)
    # Trả lời: "Tài xế nên đứng ở đâu để đón chuyến nào sinh lời nhất?"
    # -------------------------------------------------------------------------
    print("[INFO] Tổng hợp cặp hành trình phổ biến nhất (OD Pairs)...")
    df_od_pairs = (
        df_silver.groupBy("pulocation_id", "dolocation_id")
        .agg(
            F.count("*").alias("total_trips"),
            F.avg("total_amount").alias("avg_revenue"),
            F.avg("trip_duration_min").alias("avg_duration_min"),
            F.avg("tip_amount").alias("avg_tip"),
            F.avg("avg_speed_mph").alias("avg_speed_mph")
        )
        .orderBy(F.desc("total_trips"))
    )
    df_od_pairs.write.mode("overwrite").saveAsTable("nessie.taxi.gold_od_pairs")
    print("[INFO] Đã ghi bảng nessie.taxi.gold_od_pairs.")

    # -------------------------------------------------------------------------
    # 5. TỔNG HỢP: Hiệu suất theo khung giờ (Rush/Non-Rush, Weekend/Weekday)
    # Trả lời: "Lái vào giờ nào kiếm nhiều tiền nhất trên mỗi phút?"
    # -------------------------------------------------------------------------
    print("[INFO] Tổng hợp hiệu suất theo khung giờ...")
    df_efficiency_by_timebucket = (
        df_silver.groupBy("time_bucket", "is_rush_hour", "is_weekend")
        .agg(
            F.count("*").alias("total_trips"),
            F.avg("fare_per_min").alias("avg_fare_per_min"),
            F.avg("fare_per_mile").alias("avg_fare_per_mile"),
            F.avg("avg_speed_mph").alias("avg_speed_mph"),
            F.avg("trip_duration_min").alias("avg_duration_min"),
            F.avg("tip_amount").alias("avg_tip"),
            F.avg("total_amount").alias("avg_total_amount")
        )
        .orderBy("time_bucket")
    )
    df_efficiency_by_timebucket.write.mode("overwrite").saveAsTable("nessie.taxi.gold_efficiency_by_timebucket")
    print("[INFO] Đã ghi bảng nessie.taxi.gold_efficiency_by_timebucket.")

    # -------------------------------------------------------------------------
    # 6. TỔNG HỢP: Hành vi tip theo phương thức thanh toán & ngày trong tuần
    # Trả lời: "Khách trả thẻ có tip nhiều hơn tiền mặt không? Cuối tuần tip nhiều hơn không?"
    # -------------------------------------------------------------------------
    print("[INFO] Tổng hợp hành vi tip theo payment_type & ngày trong tuần...")
    df_tip_behavior = (
        df_silver.groupBy("payment_type", "pickup_weekday", "is_weekend")
        .agg(
            F.count("*").alias("total_trips"),
            F.avg("tip_amount").alias("avg_tip"),
            F.sum(F.col("is_tip_more_total").cast("int")).alias("trips_tip_exceed_fare"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.avg("fare_amount").alias("avg_fare_amount")
        )
        .orderBy("payment_type", "pickup_weekday")
    )
    df_tip_behavior.write.mode("overwrite").saveAsTable("nessie.taxi.gold_tip_behavior")
    print("[INFO] Đã ghi bảng nessie.taxi.gold_tip_behavior.")

    # -------------------------------------------------------------------------
    # 7. TỔNG HỢP: Phân tích tắc đường theo khu vực & giờ
    # Trả lời: "Khu vực nào bị kẹt xe nhất? Vào giờ nào?"
    # -------------------------------------------------------------------------
    print("[INFO] Tổng hợp phân tích tắc đường theo khu vực & giờ...")
    df_traffic_by_zone = (
        df_silver.groupBy("pulocation_id", "pickup_hour", "is_rush_hour")
        .agg(
            F.avg("avg_speed_mph").alias("avg_speed_mph"),
            F.avg("trip_duration_min").alias("avg_duration_min"),
            F.avg("fare_per_min").alias("avg_fare_per_min"),
            F.count("*").alias("total_trips")
        )
        .orderBy("pulocation_id", "pickup_hour")
    )
    df_traffic_by_zone.write.mode("overwrite").saveAsTable("nessie.taxi.gold_traffic_by_zone")
    print("[INFO] Đã ghi bảng nessie.taxi.gold_traffic_by_zone.")

    # -------------------------------------------------------------------------
    # 8. TỔNG HỢP: Doanh thu Weekday vs Weekend theo khu vực
    # Trả lời: "Khu vực nào sôi động vào cuối tuần, khu vực nào mạnh ngày thường?"
    # -------------------------------------------------------------------------
    print("[INFO] Tổng hợp doanh thu Weekday vs Weekend theo khu vực...")
    df_revenue_weekday_weekend = (
        df_silver.groupBy("pulocation_id", "is_weekend")
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("*").alias("total_trips"),
            F.avg("fare_per_mile").alias("avg_fare_per_mile"),
            F.avg("tip_amount").alias("avg_tip"),
            F.avg("avg_speed_mph").alias("avg_speed_mph")
        )
        .orderBy("pulocation_id", "is_weekend")
    )
    df_revenue_weekday_weekend.write.mode("overwrite").saveAsTable("nessie.taxi.gold_revenue_weekday_weekend")
    print("[INFO] Đã ghi bảng nessie.taxi.gold_revenue_weekday_weekend.")

    # -------------------------------------------------------------------------
    # WAP Pattern: Merge branch về main sau khi tất cả bảng đã được ghi thành công
    # -------------------------------------------------------------------------
    print(f"[INFO] Merge branch '{BRANCH_NAME}' về main...")
    spark.sql(f"MERGE BRANCH `{BRANCH_NAME}` INTO main IN nessie")
    print(f"[INFO] Merge thành công! Dữ liệu Gold đã publish lên main.")
    
    spark.sql("USE REFERENCE main IN nessie")
    spark.sql(f"DROP BRANCH IF EXISTS `{BRANCH_NAME}` IN nessie")
    
    
    print("[INFO] Hoàn tất Gold Transformation!")
    spark.stop()


if __name__ == "__main__":
    main()
