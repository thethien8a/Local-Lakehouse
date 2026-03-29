import logging
from pyspark.sql import SparkSession
from src.bronze.bronze_ingestion import ingest_bronze_dates
from src.bronze.date_range import parse_bronze_args, validate_and_build_target_dates

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Nhận tham số ngày từ command line (Airflow sẽ truyền vào)
    args = parse_bronze_args()
    target_dates, app_name, _ = validate_and_build_target_dates(args)

    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Tạo namespace nessie.taxi nếu chưa tồn tại
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.taxi")
    
    table_name = "nessie.taxi.bronze"
    ingested_days, skipped_days, total_rows = ingest_bronze_dates(
        spark=spark,
        table_name=table_name,
        target_dates=target_dates,
    )

    if spark.catalog.tableExists(table_name):
        logger.info("Thông tin bảng sau khi ghi:")
        spark.sql(f"DESCRIBE TABLE {table_name}").show(truncate=False)
    

    logger.info(
        "Hoàn tất ingest Bronze. "
        f"Ingested days: {ingested_days}, skipped days: {skipped_days}, total rows: {total_rows:,}"
    )
    spark.stop()

if __name__ == "__main__":
    main()
