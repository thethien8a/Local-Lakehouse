from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.config.path import RAW_DATA_PATH, LANDING_ZONE_PATH

def split_data():
    """
    Script này dùng để giả lập môi trường thực tế:
    Chia file dữ liệu lớn (cả tháng) thành các thư mục nhỏ theo từng ngày.
    Mỗi ngày sẽ có dữ liệu riêng nằm trong thư mục: /opt/bitnami/spark/data/landing/date=YYYY-MM-DD/
    """
    spark = SparkSession.builder.appName("Split Data By Day").getOrCreate()
    
    df = spark.read.parquet(RAW_DATA_PATH)
    
    df_with_date = df.withColumn("date", F.to_date("tpep_pickup_datetime"))
    
    df_filtered = df_with_date.filter(
        (F.col("date").isNotNull()) & 
        (F.col("date") >= "2024-01-01")
    )
    
    df_filtered.write.mode("overwrite").partitionBy("date").parquet(LANDING_ZONE_PATH)
    
    print("[INFO] Đã chia nhỏ dữ liệu thành công!")
    spark.stop()

if __name__ == "__main__":
    split_data()
