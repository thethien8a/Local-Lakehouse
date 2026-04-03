import logging
from pyspark.sql import SparkSession
from src.utils.date_utils import parse_date_args, build_app_name
from src.pipeline.bronze.bronze_ingestion import ingest_bronze_dates

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    # Parse date arguments (--start required, --end optional)
    date_range = parse_date_args("Bronze")
    app_name = build_app_name("Bronze", date_range)
    start_str, end_str = date_range.as_filter_tuple()
    
    logger.info(f"Bronze Ingestion: {start_str} -> {end_str}")
    logger.info(f"Số ngày cần xử lý: {len(date_range.target_dates)}")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Tao namespace nessie.taxi neu chua ton tai
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.taxi")
    
    table_name = "nessie.taxi.bronze"
    ingested_days, skipped_days, total_rows = ingest_bronze_dates(
        spark=spark,
        table_name=table_name,
        target_dates=date_range.target_dates,
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
