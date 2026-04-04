import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from src.pipeline.gold.writers import write_gold_table

logger = logging.getLogger(__name__)


def create_gold_tables(spark: SparkSession, df_silver: DataFrame) -> None:
    """Tạo fact table + aggregate table cho Gold layer (Star Schema)"""

    try:
        _create_fact_trips(df_silver)
        _create_agg_daily_summary(df_silver)
    except Exception as e:
        logger.error(f"Có bảng gold bị lỗi: {e}")
        spark.stop()
        raise


def _create_fact_trips(df_silver: DataFrame) -> None:
    """Fact table chính — mỗi row = 1 chuyến đi, dashboard tool tự aggregate"""
    logger.info("Tạo fact_trips từ silver...")
    df = df_silver.select(
        "trip_id",
        "pickup_date",
        "pulocation_id",
        "dolocation_id",
        "payment_type",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "tip_amount",
        "trip_duration_min",
    )
    write_gold_table(df, "nessie.taxi.fact_trips")


def _create_agg_daily_summary(df_silver: DataFrame) -> None:
    """Aggregate table — KPI tổng hợp theo ngày, giảm scan khi load dashboard"""
    logger.info("Tạo agg_daily_summary...")
    df = (
        df_silver.groupBy("pickup_date")
        .agg(
            F.count("*").alias("total_trips"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_revenue_per_trip"),
            F.avg("trip_distance").alias("avg_distance_per_trip"),
            F.avg("tip_amount").alias("avg_tip"),
            F.sum("fare_amount").alias("total_fare"),
        )
    )
    write_gold_table(df, "nessie.taxi.agg_daily_summary")
