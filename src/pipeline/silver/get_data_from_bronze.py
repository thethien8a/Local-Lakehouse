from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import List
import logging

logger = logging.getLogger("ingest_silver")


def get_bronze_table(spark: SparkSession) -> DataFrame | None:
    """Doc bang Bronze, tra ve DataFrame hoac None neu bang chua ton tai."""
    try:
        return spark.table("nessie.taxi.bronze")
    except Exception as e:
        logger.error(f"Bảng Bronze chưa tồn tại. Lỗi: {e}")
        return None


def get_bronze_data_by_date_range(
    spark: SparkSession, 
    start_date: str, 
    end_date: str
) -> DataFrame | None:
    """
    Loc du lieu Bronze theo date range (inclusive).
    
    Args:
        spark: SparkSession
        start_date: Ngay bat dau (YYYY-MM-DD)
        end_date: Ngay ket thuc (YYYY-MM-DD)
    
    Returns:
        DataFrame chua du lieu trong khoang [start_date, end_date]
    """
    df_bronze = get_bronze_table(spark)
    if df_bronze is None:
        return None
    
    logger.info(f"Lọc dữ liệu Bronze từ {start_date} đến {end_date}...")
    return df_bronze.filter(
        (F.col("ingestion_date") >= start_date) & 
        (F.col("ingestion_date") <= end_date)
    )


def get_daily_data(spark: SparkSession, target_date: str) -> DataFrame | None:
    """Loc du lieu Bronze theo ngay muc tieu. Tra ve DataFrame."""
    return get_bronze_data_by_date_range(spark, target_date, target_date)
