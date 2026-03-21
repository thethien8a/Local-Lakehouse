from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import logging

logger = logging.getLogger("ingest_silver")


def get_bronze_table(spark: SparkSession):
    """Đọc bảng Bronze, trả về DataFrame hoặc None nếu bảng chưa tồn tại."""
    try:
        return spark.table("nessie.taxi.bronze")
    except Exception as e:
        logger.error(f"Bảng Bronze chưa tồn tại. Lỗi: {e}")
        return None


def get_daily_data(spark: SparkSession, target_date: str):
    """Lọc dữ liệu Bronze theo ngày mục tiêu. Trả về DataFrame."""
    df_bronze = get_bronze_table(spark)
    if df_bronze is None:
        return None
    return df_bronze.filter(F.col("ingestion_date") == target_date)
