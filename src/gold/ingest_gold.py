import argparse
import time
import logging
from pyspark.sql import SparkSession
from get_data import get_silver_data
from create_dim_tables import create_dim_tables
from create_gold_tables import create_gold_tables

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=False, help="Ngày cần xử lý (YYYY-MM-DD). Bỏ trống để tự detect.")
    args = parser.parse_args()
    target_date = args.date

    if target_date:
        logger.info(f"Tổng hợp dữ liệu cho ngày: {target_date}")
    else:
        logger.info("Tổng hợp dữ liệu cho các ngày chưa được tổng hợp - tự detect")
    
    label = target_date or "auto-detect"
    logger.info("Tạo Spark Session...")
    spark = (
        SparkSession.builder
        .appName(f"Gold Transformation")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    branch_tag = target_date.replace("-", "") if target_date else "auto"
    BRANCH_NAME = f"etl_gold_{branch_tag}_{int(time.time())}"

    logger.info(f"Tạo branch '{BRANCH_NAME}' từ main...")
    spark.sql(f"CREATE BRANCH IF NOT EXISTS `{BRANCH_NAME}` IN nessie FROM main")
    spark.sql(f"USE REFERENCE `{BRANCH_NAME}` IN nessie")

    logger.info("Đang đọc dữ liệu Silver...")
    df_silver = get_silver_data(spark, target_date, BRANCH_NAME)

    logger.info("Đang tạo các bảng dimension...")
    create_dim_tables(spark)

    logger.info("Đang tạo các bảng gold aggregate...")
    create_gold_tables(spark, df_silver)

    logger.info(f"Merge branch '{BRANCH_NAME}' về main...")
    spark.sql(f"MERGE BRANCH `{BRANCH_NAME}` INTO main IN nessie")
    logger.info(f"Merge thành công! Dữ liệu Gold [{label}] đã publish lên main.")

    spark.sql("USE REFERENCE main IN nessie")
    spark.sql(f"DROP BRANCH IF EXISTS `{BRANCH_NAME}` IN nessie")

    logger.info("Hoàn tất Gold Transformation!")
    spark.stop()

if __name__ == "__main__":
    main()
