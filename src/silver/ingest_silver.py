import argparse
import time
from pyspark.sql import SparkSession
import logging

from get_data_from_bronze import get_bronze_table, get_daily_data
from clean_before_ingest import clean_data
from validate_before_ingest import validate_silver

logger = logging.getLogger("ingest_silver")
logging.basicConfig(level=logging.INFO)

BRANCH_NAME = f"etl_silver_{int(time.time())}"

def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--date", required=False, help="Ngày cần xử lý (Format: YYYY-MM-DD)")
        args = parser.parse_args()

        spark = (
            SparkSession.builder
            .appName("Silver Transformation")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        # Đọc bảng Bronze
        df_bronze = get_bronze_table(spark)
        if df_bronze is None:
            spark.stop()
            return
        
        silver_table = "nessie.taxi.silver"
        if args.date is None:
            # Tự động xử lý các dữ liệu chưa được xử lý ở bronze
            logger.info("Tự động xử lý các dữ liệu chưa được xử lý ở bronze")
            # Kiểm tra xem có dữ liệu nào ở silver

            if spark.catalog.tableExists(silver_table):
                logger.info(f"Bảng {silver_table} đã tồn tại. Đang kiểm tra dữ ")
                
                try:
                    # Lấy ra ngày xử lý cuối cùng của silver
                    latest_date_silver = spark.sql(f"SELECT MAX(ingestion_date) as date FROM {silver_table}").collect()[0]["date"]
                    logger.info(f"Ngày xử lý cuối cùng của silver: {latest_date_silver}")
                    df_needed_clean = df_bronze.filter(df_bronze.ingestion_date >= latest_date_silver)

                except Exception as e:
                    logger.error(f"Lỗi khi lấy ngày xử lý cuối cùng của silver: {e}")
                    spark.stop()
                    return
            else:
                logger.info(f"Bảng {silver_table} chưa tồn tại. Yêu cầu transform toàn bộ dữ liệu từ bronze")
                df_needed_clean = df_bronze
        else:
            df_needed_clean = get_daily_data(spark, args.date)
            
        # WAP Pattern — tạo branch
        
        logger.info(f"Tạo branch '{BRANCH_NAME}' từ main...")
        spark.sql(f"CREATE BRANCH IF NOT EXISTS `{BRANCH_NAME}` IN nessie FROM main")

        # Switch sang branch mới
        spark.sql(f"USE REFERENCE `{BRANCH_NAME}` IN nessie")

        prev_clean_count = df_needed_clean.count()
        logger.info(f"Số dòng cần làm sạch: {prev_clean_count:,}")

        # Làm sạch + feature engineering
        logger.info("Đang làm sạch dữ liệu...")
        df_cleaned = clean_data(df_needed_clean)
        
        row_count = df_cleaned.count()
        logger.info(f"Số dòng sau khi làm sạch: {row_count:,}")

        
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

        # Validate → merge hoặc giữ branch để debug
        if validate_silver(spark, silver_table):
            logger.info(f"Merge branch '{BRANCH_NAME}' vào main...")
            spark.sql(f"MERGE BRANCH `{BRANCH_NAME}` INTO main IN nessie")
            
            logger.info("Switch sang branch main để an toàn (nếu có lỗi)...")
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
        spark.sql(f"DROP BRANCH IF EXISTS `{BRANCH_NAME}` IN nessie")

    spark.stop()

if __name__ == "__main__":
    main()
