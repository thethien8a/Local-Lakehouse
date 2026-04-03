import logging
from pyspark.sql import SparkSession
from src.config.path import DATA_DIR
from src.pipeline.gold.writers import write_dim_table

logger = logging.getLogger(__name__)


def create_dim_tables(spark: SparkSession) -> None:
    """Tạo các bảng dimension cho Star Schema"""

    # dim_zone: lookup tên khu vực cho bar chart Top 10 pickup locations
    if not spark.catalog.tableExists("nessie.taxi.dim_zone"):
        logger.info("Tạo dim_zone từ taxi_zone_lookup.csv...")
        try:
            df_zone = spark.read.option("header", "true").csv(
                str(DATA_DIR / "taxi_zone_lookup.csv")
            )
            write_dim_table(df_zone, "nessie.taxi.dim_zone")
        except Exception as e:
            logger.error(f"Không thể tạo dim_zone. Lỗi: {e}")
            spark.stop()
            raise

    # dim_payment: lookup tên phương thức thanh toán cho donut chart
    if not spark.catalog.tableExists("nessie.taxi.dim_payment"):
        logger.info("Tạo dim_payment...")
        df_payment = spark.createDataFrame([
            (0, "Flex Fare trip"),
            (1, "Credit card"),
            (2, "Cash"),
            (3, "No charge"),
            (4, "Dispute"),
            (5, "Unknown"),
            (6, "Voided trip"),
        ], ["payment_type", "payment_name"])
        write_dim_table(df_payment, "nessie.taxi.dim_payment")
