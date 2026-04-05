#!/usr/bin/env python3
"""
Script để thực hiện expire snapshots cho các bảng Iceberg trong hệ thống Lakehouse.
Expire snapshots giúp xóa các snapshot cũ để giải phóng dung lượng lưu trữ.
"""

import argparse
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Tạo Spark session với cấu hình cho Iceberg và Nessie"""
    return (
        SparkSession.builder
        .appName(f"Lakehouse Expire Snapshots - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        .getOrCreate()
    )

def expire_snapshots_for_table(spark, table_name, days_to_keep=30, retain_last=10):
    """
    Thực hiện expire snapshots cho một bảng cụ thể
    
    Args:
        spark: Spark session
        table_name: Tên bảng cần expire snapshots (vd: 'nessie.taxi.silver')
        days_to_keep: Số ngày snapshot cần giữ lại (mặc định: 30)
        retain_last: Số lượng snapshot gần nhất luôn giữ lại (mặc định: 10)
    """
    logger.info(f"Bắt đầu expire snapshots cho bảng: {table_name}")
    logger.info(f"   - Giữ lại snapshot cũ hơn {days_to_keep} ngày")
    logger.info(f"   - Luôn giữ lại {retain_last} snapshot gần nhất")
    
    try:
        # Tính toán thời điểm cutoff
        cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).strftime('%Y-%m-%d %H:%M:%S.000')
        
        # Gọi stored procedure để expire snapshots
        sql_query = f"""
        CALL nessie.system.expire_snapshots(
            table => '{table_name}',
            older_than => TIMESTAMP '{cutoff_date}',
            retain_last => {retain_last}
        )
        """
        
        result = spark.sql(sql_query)
        
        # Lấy kết quả
        rows = result.collect()
        if rows:
            logger.info(f"Expire snapshots hoàn tất cho bảng {table_name}")
            for row in rows:
                logger.info(f"   - Deleted data files: {row.deleted_data_files_count}")
                logger.info(f"   - Deleted position delete files: {row.deleted_position_delete_files_count}")
                logger.info(f"   - Deleted equality delete files: {row.deleted_equality_delete_files_count}")
                logger.info(f"   - Deleted manifest files: {row.deleted_manifest_files_count}")
                logger.info(f"   - Deleted manifest lists: {row.deleted_manifest_lists_count}")
                logger.info(f"   - Deleted statistics files: {row.deleted_statistics_files_count}")
        else:
            logger.info(f"Không có snapshot nào bị expire cho bảng {table_name}")
            
    except Exception as e:
        logger.error(f"Lỗi khi expire snapshots bảng {table_name}: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Expire snapshots script cho Lakehouse")
    parser.add_argument("--tables", nargs="+", required=True, 
                        help="Danh sách các bảng cần expire snapshots (vd: nessie.taxi.silver nessie.taxi.gold_revenue_by_hour)")
    parser.add_argument("--days-to-keep", type=int, default=30,
                        help="Số ngày snapshot cần giữ lại (mặc định: 30)")
    parser.add_argument("--retain-last", type=int, default=10,
                        help="Số lượng snapshot gần nhất luôn giữ lại (mặc định: 10)")
    args = parser.parse_args()
    
    # Tạo Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"Bắt đầu expire snapshots cho {len(args.tables)} bảng")
    logger.info(f"   - Giữ lại snapshot cũ hơn {args.days_to_keep} ngày")
    logger.info(f"   - Luôn giữ lại {args.retain_last} snapshot gần nhất")
    
    # Thực hiện expire snapshots cho từng bảng
    for table_name in args.tables:
        try:
            expire_snapshots_for_table(spark, table_name, args.days_to_keep, args.retain_last)
        except Exception as e:
            logger.error(f"Bỏ qua bảng {table_name} do lỗi: {str(e)}")
            continue
    
    logger.info("Hoàn tất quá trình expire snapshots")
    
    # Dừng Spark session
    spark.stop()

if __name__ == "__main__":
    main()