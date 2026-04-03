import time
import logging
from pyspark.sql import SparkSession
from src.utils.date_utils import parse_date_args, build_app_name, DateRange
from src.pipeline.gold.get_data import get_silver_data_by_date_range
from src.pipeline.gold.create_dim_tables import create_dim_tables
from src.pipeline.gold.create_gold_tables import create_gold_tables

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_branch_name(date_range: DateRange) -> str:
    """Tao branch name cho WAP pattern"""
    if date_range.is_single_day:
        tag = date_range.start_date.strftime("%Y%m%d")
    else:
        tag = f"{date_range.start_date.strftime('%Y%m%d')}_{date_range.end_date.strftime('%Y%m%d')}"
    return f"etl_gold_{tag}_{int(time.time())}"


def main() -> None:
    # Parse date arguments (--start required, --end optional)
    date_range = parse_date_args("Gold")
    app_name = build_app_name("Gold", date_range)
    start_str, end_str = date_range.as_filter_tuple()
    
    logger.info(f"Gold Transformation: {start_str} -> {end_str}")
    logger.info(f"Số ngày cần xử lý: {len(date_range.target_dates)}")
    
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    BRANCH_NAME = create_branch_name(date_range)

    try:
        # WAP Pattern - tao branch
        logger.info(f"Tạo branch '{BRANCH_NAME}' từ main...")
        spark.sql(f"CREATE BRANCH IF NOT EXISTS `{BRANCH_NAME}` IN nessie FROM main")
        spark.sql(f"USE REFERENCE `{BRANCH_NAME}` IN nessie")

        # Loc du lieu Silver theo date range
        logger.info("Đang đọc dữ liệu Silver...")
        df_silver = get_silver_data_by_date_range(spark, start_str, end_str, BRANCH_NAME)
        
        if df_silver is None:
            logger.warning("Không có dữ liệu Silver. Dừng lại.")
            return

        row_count = df_silver.count()
        logger.info(f"Số dòng Silver cần xử lý: {row_count:,}")

        # Tao dimension tables (chi chay 1 lan neu chua ton tai)
        logger.info("Đang tạo các bảng dimension...")
        create_dim_tables(spark)

        # Tao/update fact + aggregate tables
        logger.info("Đang tạo các bảng gold (fact + aggregate)...")
        create_gold_tables(spark, df_silver)

        # Merge branch ve main
        logger.info(f"Merge branch '{BRANCH_NAME}' về main...")
        spark.sql(f"MERGE BRANCH `{BRANCH_NAME}` INTO main IN nessie")
        logger.info(f"Merge thanh cong! Du lieu Gold [{start_str} -> {end_str}] da publish len main.")

        spark.sql("USE REFERENCE main IN nessie")
        spark.sql(f"DROP BRANCH IF EXISTS `{BRANCH_NAME}` IN nessie")
        logger.info(f"Drop branch '{BRANCH_NAME}' thành công!")

        logger.info("Hoàn tất Gold Transformation!")
        
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý: {e}")
        try:
            spark.sql("USE REFERENCE main IN nessie")
            spark.sql(f"DROP BRANCH IF EXISTS `{BRANCH_NAME}` IN nessie")
        except Exception:
            pass
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
