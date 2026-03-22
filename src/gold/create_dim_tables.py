import logging
from pyspark.sql import SparkSession
from src.config.path import DATA_DIR
from writers import write_dim_table

logger = logging.getLogger(__name__)


def create_dim_tables(spark: SparkSession) -> None:
    """Tạo các bảng dimension nếu chưa tồn tại"""

    # dim_zone: từ file CSV của NYC TLC
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

    # dim_payment: theo data dictionary NYC TLC
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

    # dim_vendor: theo data dictionary NYC TLC
    if not spark.catalog.tableExists("nessie.taxi.dim_vendor"):
        logger.info("Tạo dim_vendor...")
        df_vendor = spark.createDataFrame([
            (1, "Creative Mobile Technologies"),
            (2, "VeriFone Inc."),
            (6, "Mautro"),
            (7, "NYMT"),
        ], ["vendor_id", "vendor_name"])
        write_dim_table(df_vendor, "nessie.taxi.dim_vendor")

    # dim_ratecode: theo data dictionary NYC TLC
    if not spark.catalog.tableExists("nessie.taxi.dim_ratecode"):
        logger.info("Tạo dim_ratecode...")
        df_ratecode = spark.createDataFrame([
            (1, "Standard rate"),
            (2, "JFK"),
            (3, "Newark"),
            (4, "Nassau or Westchester"),
            (5, "Negotiated fare"),
            (6, "Group ride"),
            (99, "Unknown"),
        ], ["ratecode_id", "ratecode_name"])
        write_dim_table(df_ratecode, "nessie.taxi.dim_ratecode")
