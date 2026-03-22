from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F
import logging

logger = logging.getLogger(__name__)

GOLD_CHECKPOINT_TABLE = "nessie.taxi.gold_checkpoint"
BRANCH_NAME = "etl_gold_1774197715"

def get_silver_data(spark: SparkSession, target_date: str, branch_name: str) -> DataFrame:
    """Lấy dữ liệu Silver theo ngày hoặc tự detect"""
    try:
        df_silver_full = spark.table("nessie.taxi.silver")

        if target_date:
            logger.info(f"Lọc dữ liệu Silver cho ngày {target_date}...")
            df_silver = df_silver_full.filter(F.col("pickup_date") == target_date)
        elif spark.catalog.tableExists(GOLD_CHECKPOINT_TABLE):
            last_gold_date = spark.sql(
                f"SELECT MAX(pickup_date) as d FROM {GOLD_CHECKPOINT_TABLE}"
            ).collect()[0]["d"]
            logger.info(f"Ngày cuối đã tổng hợp Gold: {last_gold_date}. Lấy dữ liệu mới hơn...")
            df_silver = df_silver_full.filter(F.col("pickup_date") > last_gold_date)
        else:
            logger.info("Chưa có bảng Gold. Tổng hợp toàn bộ dữ liệu Silver...")
            df_silver = df_silver_full

        if not df_silver.head(1):
            logger.warning("Không có dữ liệu Silver cần tổng hợp. Bỏ qua.")
            spark.sql("USE REFERENCE main IN nessie")
            spark.sql(f"DROP BRANCH IF EXISTS `{branch_name}` IN nessie")
            spark.stop()
            return None

        return df_silver
    except Exception as e:
        logger.error(f"Không thể đọc bảng Silver. Lỗi: {e}")
        spark.stop()
        return None