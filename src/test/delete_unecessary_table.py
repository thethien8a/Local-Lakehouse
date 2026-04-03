"""
Xóa các bảng gold không cần thiết trong Nessie catalog.

Usage (chạy từ spark-master container):
    docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/src/test/delete_unecessary_table.py
"""
from pyspark.sql import SparkSession
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

GOLD_TABLES_TO_DROP = [
    "nessie.taxi.fact_trips"
]

SILVER_TABLES_TO_DROP = [
    "nessie.taxi.silver"
]

def main():
    spark = SparkSession.builder \
        .appName("Drop Unnecessary Gold Tables") \
        .getOrCreate()

    # Drop trực tiếp trên main, không cần WAP vì đây là thao tác xóa đơn giản
    spark.sql("USE REFERENCE main IN nessie")

    for table in GOLD_TABLES_TO_DROP:
        try:
            # Nessie catalog không hỗ trợ PURGE, dùng DROP TABLE thường
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            logger.info(f"Dropped: {table}")
        except Exception as e:
            logger.error(f"Failed to drop {table}: {e}")

    for table in SILVER_TABLES_TO_DROP:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            logger.info(f"Dropped: {table}")
        except Exception as e:
            logger.error(f"Failed to drop {table}: {e}")

    # Verify
    remaining = spark.sql("SHOW TABLES IN nessie.taxi").collect()
    logger.info("Remaining tables in nessie.taxi:")
    for row in remaining:
        logger.info(f"  - {row['tableName']}")

    spark.stop()


if __name__ == "__main__":
    main()
