import logging
from typing import List, Set, Tuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config.path import LANDING_ZONE_PATH

logger = logging.getLogger(__name__)


def _get_existing_ingestion_dates(spark: SparkSession, table_name: str) -> Set[str]:
    existing_dates = spark.sql(f"SELECT DISTINCT ingestion_date FROM {table_name}").collect()
    return {row["ingestion_date"] for row in existing_dates}


def ingest_bronze_dates(spark: SparkSession, table_name: str, target_dates: List[str]) -> Tuple[int, int, int]:
    ingested_days = 0
    skipped_days = 0
    total_rows = 0

    existing_date_set: Set[str] = set()
    table_exists = spark.catalog.tableExists(table_name)
    if table_exists:
        existing_date_set = _get_existing_ingestion_dates(spark, table_name)

    for target_date in target_dates:
        daily_file_path = f"{LANDING_ZONE_PATH}/date={target_date}"
        logger.info(f"Đang đọc dữ liệu ngày {target_date} từ: {daily_file_path}")

        try:
            df_raw = spark.read.parquet(daily_file_path)
            df_raw = df_raw.withColumn("ingestion_date", F.lit(target_date))
        except Exception as e:
            logger.warning(f"Không tìm thấy dữ liệu cho ngày {target_date}. Bỏ qua. Lỗi: {e}")
            skipped_days += 1
            continue

        if table_exists and target_date in existing_date_set:
            logger.info(f"Ngày {target_date} đã tồn tại trong bảng. Bỏ qua.")
            skipped_days += 1
            continue

        rows = df_raw.count()
        logger.info(f"Số dòng đọc được: {rows:,}")
        total_rows += rows

        if table_exists:
            logger.info(f"Bảng {table_name} đã tồn tại")
            df_raw.writeTo(table_name).append()
        else:
            logger.info(f"Bảng {table_name} chưa tồn tại. Đang TẠO MỚI...")
            (
                df_raw.writeTo(table_name)
                .tableProperty("write.format.default", "parquet")
                .partitionedBy("ingestion_date")
                .create()
            )
            table_exists = True

        existing_date_set.add(target_date)
        ingested_days += 1

    return ingested_days, skipped_days, total_rows
