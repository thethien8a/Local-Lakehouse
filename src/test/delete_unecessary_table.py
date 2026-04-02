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

TABLES_TO_DROP = [
    "nessie.taxi.gold_efficiency_by_timebucket",
    "nessie.taxi.gold_fare_by_date",
    "nessie.taxi.gold_od_pairs",
    "nessie.taxi.gold_revenue_by_hour",
    "nessie.taxi.gold_revenue_by_location",
    "nessie.taxi.gold_revenue_weekday_weekend",
    "nessie.taxi.gold_tip_behavior",
    "nessie.taxi.gold_traffic_by_zone",
]


def main():
    spark = SparkSession.builder \
        .appName("Drop Unnecessary Gold Tables") \
        .getOrCreate()

    # Drop trực tiếp trên main, không cần WAP vì đây là thao tác xóa đơn giản
    spark.sql("USE REFERENCE main IN nessie")

    for table in TABLES_TO_DROP:
        try:
            # Nessie catalog không hỗ trợ PURGE, dùng DROP TABLE thường
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
