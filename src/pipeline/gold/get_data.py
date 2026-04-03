from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F
import logging

logger = logging.getLogger(__name__)


def get_silver_data_by_date_range(
    spark: SparkSession, 
    start_date: str, 
    end_date: str,
    branch_name: str
) -> DataFrame | None:
    """
    Lay du lieu Silver theo date range (inclusive).
    
    Args:
        spark: SparkSession
        start_date: Ngay bat dau (YYYY-MM-DD)
        end_date: Ngay ket thuc (YYYY-MM-DD)
        branch_name: Ten branch hien tai (de cleanup neu loi)
    
    Returns:
        DataFrame chua du lieu trong khoang [start_date, end_date]
    """
    try:
        df_silver_full = spark.table("nessie.taxi.silver")
        
        logger.info(f"Lọc dữ liệu Silver từ {start_date} đến {end_date}...")
        df_silver = df_silver_full.filter(
            (F.col("pickup_date") >= start_date) & 
            (F.col("pickup_date") <= end_date)
        )

        if not df_silver.head(1):
            logger.warning(f"Không có dữ liệu Silver trong khoảng {start_date} -> {end_date}. Bỏ qua.")
            spark.sql("USE REFERENCE main IN nessie")
            spark.sql(f"DROP BRANCH IF EXISTS `{branch_name}` IN nessie")
            spark.stop()
            return None

        return df_silver
        
    except Exception as e:
        logger.error(f"Không thể đọc bảng Silver. Lỗi: {e}")
        spark.stop()
        return None


def get_silver_data(spark: SparkSession, target_date: str, branch_name: str) -> DataFrame | None:
    """
    Lay du lieu Silver theo ngay don le.
    Giu lai de backward compatible.
    """
    return get_silver_data_by_date_range(spark, target_date, target_date, branch_name)
