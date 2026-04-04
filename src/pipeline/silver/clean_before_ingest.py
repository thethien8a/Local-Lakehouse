from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

logger = logging.getLogger("ingest_silver")

def filtering_df(df: DataFrame) -> DataFrame:
    """
    Lọc dữ liệu theo các điều kiện nhất định.
    """
    # Loại bỏ các dòng null ở cột thời gian
    df = df.filter(
        F.col("tpep_pickup_datetime").isNotNull()
        & F.col("tpep_dropoff_datetime").isNotNull()
    )

    # Lọc ID hợp lệ
    df = df.filter(F.col("VendorID").isin(1, 2, 6, 7))
    df = df.filter(F.col("RatecodeID").isin(1, 2, 3, 4, 5, 6, 99))
    df = df.filter(F.col("payment_type").isin(0, 1, 2, 3, 4, 5, 6))

    # Các cột bắt buộc > 0
    must_be_positive = [
        "fare_amount", "mta_tax", "improvement_surcharge",
        "total_amount", "trip_distance", "passenger_count",
    ]
    for col in must_be_positive:
        df = df.filter(F.col(col) > 0)

    # Các cột cho phép >= 0
    can_be_zero = [
        "extra", "tip_amount", "tolls_amount",
        "congestion_surcharge", "Airport_fee",
    ]
    for col in can_be_zero:
        df = df.filter(F.col(col) >= 0)

    # pickup phải <= dropoff
    df = df.filter(F.col("tpep_pickup_datetime") <= F.col("tpep_dropoff_datetime"))
    
    return df

def clean_data(df: DataFrame) -> DataFrame:
    """
    Làm sạch và feature engineering cho dữ liệu trước khi ghi vào Silver.
    Bao gồm: loại null, lọc ID hợp lệ, lọc giá trị âm, tạo cột mới, rename cột.
    """

    df = filtering_df(df)

    df = feature_engineering(df)
    
    # Rename cột cho chuẩn naming convention
    df = (
        df.withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("RatecodeID", "ratecode_id")
        .withColumnRenamed("PULocationID", "pulocation_id")
        .withColumnRenamed("DOLocationID", "dolocation_id")
        .withColumnRenamed("Airport_fee", "airport_fee")
    )

    return df

def feature_engineering(df: DataFrame) -> DataFrame:
    """
    Feature engineering cho dữ liệu Silver.
    """
    
    # Generate unique trip_id using MD5 hash of key columns
    # Include as many columns as possible to minimize hash collisions
    df = df.withColumn(
        "trip_id",
        F.md5(
            F.concat_ws(
                "|",
                F.col("VendorID").cast("string"),
                F.col("tpep_pickup_datetime").cast("string"),
                F.col("tpep_dropoff_datetime").cast("string"),
                F.col("PULocationID").cast("string"),
                F.col("DOLocationID").cast("string"),
                F.col("passenger_count").cast("string"),
                F.col("trip_distance").cast("string"),
                F.col("fare_amount").cast("string"),
                F.col("tip_amount").cast("string"),
                F.col("total_amount").cast("string"),
                F.col("payment_type").cast("string"),
            )
        )
    )
    
    # Feature Engineering — date columns chuyển sang dim_date, chỉ giữ pickup_date làm FK
    df = (
        df.withColumn(
            "is_tip_more_total",
            F.when(F.col("tip_amount") > F.col("total_amount"), True).otherwise(False),
        )
        .withColumn("pickup_date", F.to_date(F.col("tpep_pickup_datetime")))
        .withColumn(
            "trip_duration_min",
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60,
        )
        .withColumn("avg_speed_mph", F.col("trip_distance") * 60 / F.col("trip_duration_min"))
        .withColumn("fare_per_mile", F.col("total_amount") / F.col("trip_distance"))
        .withColumn("fare_per_min", F.col("total_amount") / F.col("trip_duration_min"))
    )

    return df
