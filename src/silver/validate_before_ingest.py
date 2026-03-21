from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging

logger = logging.getLogger("ingest_silver")


def validate_silver(spark: SparkSession, table_name: str) -> bool:
    """
    Validate toàn bộ dữ liệu Silver sau khi ghi.
    Kiểm tra: có dữ liệu, không null ở pickup_datetime.
    Trả về True nếu pass, False nếu fail.
    """
    df_check = spark.table(table_name)
    silver_count = df_check.count()

    null_pickup = df_check.filter(F.col("tpep_pickup_datetime").isNull()).count()
    
    if silver_count > 0 and null_pickup == 0:
        logger.info(f"Validation OK — {silver_count:,} rows")
        return True

    logger.error(
        f"Validation FAILED — rows={silver_count}, null_pickup={null_pickup}"
    )
    return False
