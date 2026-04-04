import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
    
    # dim_date: pre-generated date dimension thay cho các cột đã bỏ ở silver
    if not spark.catalog.tableExists("nessie.taxi.dim_date"):
        logger.info("Tạo dim_date (2023-12-01 → 2025-01-01)...")
        try:
            df_date = (
                spark.sql(
                    "SELECT explode(sequence("
                    "  to_date('2023-12-01'), to_date('2025-01-01'), interval 1 day"
                    ")) AS pickup_date"
                )
                .withColumn("pickup_hour", F.lit(None).cast("int"))
                .withColumn("pickup_weekday", F.dayofweek(F.col("pickup_date")))
                .withColumn(
                    "is_weekend",
                    F.when(F.dayofweek(F.col("pickup_date")).isin(1, 7), True).otherwise(False),
                )
                .withColumn("day_of_month", F.dayofmonth(F.col("pickup_date")))
                .withColumn("month", F.month(F.col("pickup_date")))
                .withColumn("year", F.year(F.col("pickup_date")))
                .withColumn("quarter", F.quarter(F.col("pickup_date")))
            )

            # Expand mỗi ngày ra 24 giờ để có pickup_hour
            df_hours = spark.range(0, 24).withColumnRenamed("id", "pickup_hour_val")
            df_date = (
                df_date.crossJoin(df_hours)
                .drop("pickup_hour")
                .withColumnRenamed("pickup_hour_val", "pickup_hour")
                .withColumn(
                    "time_bucket",
                    F.when(F.col("pickup_hour").between(0, 6), "early_morning")
                    .when(F.col("pickup_hour").between(7, 12), "morning")
                    .when(F.col("pickup_hour").between(13, 18), "afternoon")
                    .otherwise("evening"),
                )
                .withColumn(
                    "is_rush_hour",
                    F.when(
                        (F.col("pickup_hour").between(7, 9))
                        | (F.col("pickup_hour").between(16, 19)),
                        True,
                    ).otherwise(False),
                )
                .select(
                    "pickup_date", "pickup_hour", "pickup_weekday",
                    "is_weekend", "time_bucket", "is_rush_hour",
                    "day_of_month", "month", "year", "quarter",
                )
            )
            write_dim_table(df_date, "nessie.taxi.dim_date")
        except Exception as e:
            logger.error(f"Không thể tạo dim_date. Lỗi: {e}")
            spark.stop()
            raise
