import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils.date_utils import parse_date_args, build_app_name, DateRange
from src.pipeline.silver.get_data_from_bronze import get_bronze_data_by_date_range
from src.pipeline.silver.clean_before_ingest import clean_data
from src.pipeline.silver.validate_before_ingest import validate_silver

logger = logging.getLogger("ingest_silver")
logging.basicConfig(level=logging.INFO)


def create_branch_name(date_range: DateRange) -> str:
    """Tao branch name cho WAP pattern"""
    if date_range.is_single_day:
        tag = date_range.start_date.strftime("%Y%m%d")
    else:
        tag = f"{date_range.start_date.strftime('%Y%m%d')}_{date_range.end_date.strftime('%Y%m%d')}"
    return f"etl_silver_{tag}_{int(time.time())}"


def main():
    # Parse date arguments (--start required, --end optional)
    date_range = parse_date_args("Silver")
    app_name = build_app_name("Silver", date_range)
    start_str, end_str = date_range.as_filter_tuple()
    
    logger.info(f"Silver Transformation: {start_str} -> {end_str}")
    logger.info(f"Số ngày cần xử lý: {len(date_range.target_dates)}")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    silver_table = "nessie.taxi.silver"
    BRANCH_NAME = create_branch_name(date_range)

    try:
        # Lọc dữ liệu Bronze theo date range
        df_bronze = get_bronze_data_by_date_range(spark, start_str, end_str)
        if df_bronze is None:   
            logger.error("Không thể đọc dữ liệu Bronze. Dừng lại.")
            spark.stop()
            return

        row_count = df_bronze.count()
        if row_count == 0:
            logger.warning(f"Không có dữ liệu Bronze trong khoảng {start_str} -> {end_str}. Bỏ qua.")
            spark.stop()
            return
        
        logger.info(f"Số dòng Bronze cần làm sạch: {row_count:,}")

        # WAP Pattern - tao branch
        logger.info(f"Tạo branch '{BRANCH_NAME}' từ main...")
        spark.sql(f"CREATE BRANCH IF NOT EXISTS `{BRANCH_NAME}` IN nessie FROM main")
        spark.sql(f"USE REFERENCE `{BRANCH_NAME}` IN nessie")

        # Lam sach + feature engineering
        logger.info("Đang làm sạch dữ liệu...")
        df_cleaned = clean_data(df_bronze)
        
        cleaned_count = df_cleaned.count()
        logger.info(f"Số dòng sau khi làm sạch: {cleaned_count:,}")

        w = Window.partitionBy("trip_id").orderBy(F.col("tpep_pickup_datetime").desc())
        df_cleaned = (
            df_cleaned
            .withColumn("_row_num", F.row_number().over(w))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

        # Upsert vao Silver
        logger.info("Upsert dữ liệu vào bảng Silver...")
        if spark.catalog.tableExists(silver_table):
            df_cleaned.createOrReplaceTempView("temp_silver")
            spark.sql(f"""
                MERGE INTO {silver_table} AS target
                USING temp_silver AS source
                ON target.trip_id = source.trip_id
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
            """)
        else:
            df_cleaned.writeTo(silver_table) \
                .tableProperty("write.format.default", "parquet") \
                .partitionedBy("ingestion_date") \
                .create()


        # Validate -> merge hoac giu branch de debug
        if validate_silver(spark, silver_table):
            logger.info(f"Merge branch '{BRANCH_NAME}' vào main...")
            spark.sql(f"MERGE BRANCH `{BRANCH_NAME}` INTO main IN nessie")
            
            spark.sql("USE REFERENCE main IN nessie")
            logger.info("Merge thành công!")
            
            spark.sql(f"DROP BRANCH IF EXISTS `{BRANCH_NAME}` IN nessie")
            logger.info(f"Drop branch '{BRANCH_NAME}' thành công!")
        else:
            logger.error(f"Validation FAILED. KHÔNG merge. Branch '{BRANCH_NAME}' giữ nguyên để debug.")

        logger.info("Hoàn tất Silver Transformation!")
        
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý: {e}")
        logger.info("Drop nhánh hiện tại...")
        try:
            spark.sql(f"DROP BRANCH IF EXISTS `{BRANCH_NAME}` IN nessie")
        except Exception:
            pass
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
