import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from writers import write_gold_table

logger = logging.getLogger(__name__)


def create_gold_tables(spark: SparkSession, df_silver: DataFrame) -> None:
    """Tạo các bảng gold aggregate từ silver, JOIN dim để enrich data"""

    dim_zone = spark.table("nessie.taxi.dim_zone")
    dim_payment = spark.table("nessie.taxi.dim_payment")
    
    try:
        _revenue_by_hour(df_silver)
        _revenue_by_location(df_silver, dim_zone)
        _fare_by_date(df_silver)
        _od_pairs(df_silver, dim_zone)
        _efficiency_by_timebucket(df_silver)
        _tip_behavior(df_silver, dim_payment)
        _traffic_by_zone(df_silver, dim_zone)
        _revenue_weekday_weekend(df_silver, dim_zone)
    except Exception as e:
        logger.error(f"Có bảng gold bị lỗi: {e}")
        spark.stop()
        raise

def _revenue_by_hour(df_silver: DataFrame) -> None:
    logger.info("1. Tổng hợp doanh thu theo giờ...")
    df = (
        df_silver.groupBy("pickup_date", "pickup_hour")
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("*").alias("total_trips"),
            F.avg("total_amount").alias("avg_revenue_per_trip")
        )
    )
    write_gold_table(df, "nessie.taxi.gold_revenue_by_hour")


def _revenue_by_location(df_silver: DataFrame, dim_zone: DataFrame) -> None:
    logger.info("2. Tổng hợp doanh thu theo khu vực...")
    df = (
        df_silver.groupBy("pickup_date", "pulocation_id")
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("*").alias("total_trips"),
            F.avg("total_amount").alias("avg_revenue_per_trip"),
            F.avg("tip_amount").alias("avg_tip_amount"),
            F.avg("fare_per_mile").alias("avg_fare_per_mile")
        )
        .join(
            dim_zone,
            F.col("pulocation_id") == dim_zone["LocationID"],
            "left"
        )
        .select(
            "pickup_date", "pulocation_id",
            dim_zone["Borough"].alias("borough"),
            dim_zone["Zone"].alias("zone_name"),
            "total_revenue", "total_trips",
            "avg_revenue_per_trip", "avg_tip_amount", "avg_fare_per_mile"
        )
    )
    write_gold_table(df, "nessie.taxi.gold_revenue_by_location")


def _fare_by_date(df_silver: DataFrame) -> None:
    logger.info("3. Tổng hợp giá cả trung bình theo ngày...")
    df = (
        df_silver.groupBy("pickup_date")
        .agg(
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.count("*").alias("total_trips")
        )
    )
    write_gold_table(df, "nessie.taxi.gold_fare_by_date")


def _od_pairs(df_silver: DataFrame, dim_zone: DataFrame) -> None:
    logger.info("4. Tổng hợp cặp hành trình phổ biến nhất (OD Pairs)...")
    pu_zone = dim_zone.alias("pu_zone")
    do_zone = dim_zone.alias("do_zone")
    df = (
        df_silver.groupBy("pickup_date", "pulocation_id", "dolocation_id")
        .agg(
            F.count("*").alias("total_trips"),
            F.avg("total_amount").alias("avg_revenue"),
            F.avg("trip_duration_min").alias("avg_duration_min"),
            F.avg("tip_amount").alias("avg_tip"),
            F.avg("avg_speed_mph").alias("avg_speed_mph")
        )
        .join(pu_zone, F.col("pulocation_id") == F.col("pu_zone.LocationID"), "left")
        .join(do_zone, F.col("dolocation_id") == F.col("do_zone.LocationID"), "left")
        .select(
            "pickup_date", "pulocation_id", "dolocation_id",
            F.col("pu_zone.Borough").alias("pu_borough"),
            F.col("pu_zone.Zone").alias("pu_zone_name"),
            F.col("do_zone.Borough").alias("do_borough"),
            F.col("do_zone.Zone").alias("do_zone_name"),
            "total_trips", "avg_revenue", "avg_duration_min",
            "avg_tip", "avg_speed_mph"
        )
    )
    write_gold_table(df, "nessie.taxi.gold_od_pairs")


def _efficiency_by_timebucket(df_silver: DataFrame) -> None:
    logger.info("5. Tổng hợp hiệu suất theo khung giờ...")
    df = (
        df_silver.groupBy("pickup_date", "time_bucket", "is_rush_hour", "is_weekend")
        .agg(
            F.count("*").alias("total_trips"),
            F.avg("fare_per_min").alias("avg_fare_per_min"),
            F.avg("fare_per_mile").alias("avg_fare_per_mile"),
            F.avg("avg_speed_mph").alias("avg_speed_mph"),
            F.avg("trip_duration_min").alias("avg_duration_min"),
            F.avg("tip_amount").alias("avg_tip"),
            F.avg("total_amount").alias("avg_total_amount")
        )
    )
    write_gold_table(df, "nessie.taxi.gold_efficiency_by_timebucket")


def _tip_behavior(df_silver: DataFrame, dim_payment: DataFrame) -> None:
    logger.info("6. Tổng hợp hành vi tip...")
    df = (
        df_silver.groupBy("pickup_date", "payment_type", "pickup_weekday", "is_weekend")
        .agg(
            F.count("*").alias("total_trips"),
            F.avg("tip_amount").alias("avg_tip"),
            F.sum(F.col("is_tip_more_total").cast("int")).alias("trips_tip_exceed_fare"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.avg("fare_amount").alias("avg_fare_amount")
        )
        .join(dim_payment, "payment_type", "left")
        .select(
            "pickup_date", "payment_type",
            dim_payment["payment_name"],
            "pickup_weekday", "is_weekend",
            "total_trips", "avg_tip", "trips_tip_exceed_fare",
            "avg_total_amount", "avg_fare_amount"
        )
    )
    write_gold_table(df, "nessie.taxi.gold_tip_behavior")


def _traffic_by_zone(df_silver: DataFrame, dim_zone: DataFrame) -> None:
    logger.info("7. Tổng hợp phân tích tắc đường...")
    df = (
        df_silver.groupBy("pickup_date", "pulocation_id", "pickup_hour", "is_rush_hour")
        .agg(
            F.avg("avg_speed_mph").alias("avg_speed_mph"),
            F.avg("trip_duration_min").alias("avg_duration_min"),
            F.avg("fare_per_min").alias("avg_fare_per_min"),
            F.count("*").alias("total_trips")
        )
        .join(
            dim_zone,
            F.col("pulocation_id") == dim_zone["LocationID"],
            "left"
        )
        .select(
            "pickup_date", "pulocation_id",
            dim_zone["Borough"].alias("borough"),
            dim_zone["Zone"].alias("zone_name"),
            "pickup_hour", "is_rush_hour",
            "avg_speed_mph", "avg_duration_min", "avg_fare_per_min", "total_trips"
        )
    )
    write_gold_table(df, "nessie.taxi.gold_traffic_by_zone")


def _revenue_weekday_weekend(df_silver: DataFrame, dim_zone: DataFrame) -> None:
    logger.info("8. Tổng hợp doanh thu Weekday vs Weekend...")
    df = (
        df_silver.groupBy("pickup_date", "pulocation_id", "is_weekend")
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("*").alias("total_trips"),
            F.avg("fare_per_mile").alias("avg_fare_per_mile"),
            F.avg("tip_amount").alias("avg_tip"),
            F.avg("avg_speed_mph").alias("avg_speed_mph")
        )
        .join(
            dim_zone,
            F.col("pulocation_id") == dim_zone["LocationID"],
            "left"
        )
        .select(
            "pickup_date", "pulocation_id",
            dim_zone["Borough"].alias("borough"),
            dim_zone["Zone"].alias("zone_name"),
            "is_weekend", "total_revenue", "total_trips",
            "avg_fare_per_mile", "avg_tip", "avg_speed_mph"
        )
    )
    write_gold_table(df, "nessie.taxi.gold_revenue_weekday_weekend")
