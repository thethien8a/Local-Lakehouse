import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.config.path import LANDING_ZONE_PATH
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Nhận tham số ngày từ command line (Airflow sẽ truyền vào)
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Ngày cần xử lý (Format: YYYY-MM-DD)")
    args = parser.parse_args()
    target_date = args.date
    

    spark = (
        SparkSession.builder
        .appName(f"Bronze Ingestion - {target_date}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Tạo namespace nessie.taxi nếu chưa tồn tại
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.taxi")
    
    # Đường dẫn file của ngày cụ thể
    daily_file_path = f"{LANDING_ZONE_PATH}/date={target_date}"
    logger.info(f"Đang đọc dữ liệu ngày {target_date} từ: {daily_file_path}")
    
    try:
        df_raw = spark.read.parquet(daily_file_path)
        # Thêm cột ingestion_date để biết dòng này được ingest ngày nào
        df_raw = df_raw.withColumn("ingestion_date", F.lit(target_date))
    except Exception as e:
        logger.warning(f"Không tìm thấy dữ liệu cho ngày {target_date}. Bỏ qua. Lỗi: {e}")
        spark.stop()
        return

    logger.info(f"Số dòng đọc được: {df_raw.count():,}")

    # Ghi vào bảng Bronze theo dạng APPEND
    table_name = "nessie.taxi.bronze"
    
    if spark.catalog.tableExists(table_name):
        logger.info(f"Bảng {table_name} đã tồn tại. Đang APPEND dữ liệu...")
        df_raw.writeTo(table_name).append()
    else:
        logger.info(f"Bảng {table_name} chưa tồn tại. Đang TẠO MỚI...")
        (
            df_raw.writeTo(table_name)
            .tableProperty("write.format.default", "parquet")
            .partitionedBy("ingestion_date") # Partition theo ngày ingest để dễ quản lý
            .create()
        )
    
    # Hiển thị thông tin bảng
    logger.info("Thông tin bảng sau khi ghi:")
    spark.sql(f"DESCRIBE TABLE {table_name}").show(truncate=False)
    

    logger.info(f"Ghi thành công dữ liệu ngày {target_date} vào bảng nessie.taxi.bronze!")
    spark.stop()

if __name__ == "__main__":
    main()
