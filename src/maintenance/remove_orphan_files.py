#!/usr/bin/env python3
"""
Script để thực hiện remove orphan files cho các bảng Iceberg trong hệ thống Lakehouse.
Remove orphan files giúp xóa các file trên MinIO không còn được tham chiếu bởi bất kỳ snapshot nào.
"""

import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Tạo Spark session với cấu hình cho Iceberg và Nessie"""
    return (
        SparkSession.builder
        .appName(f"Lakehouse Remove Orphan Files - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        .getOrCreate()
    )

def remove_orphan_files_for_table(spark, table_name):
    """
    Thực hiện remove orphan files cho một bảng cụ thể
    
    Args:
        spark: Spark session
        table_name: Tên bảng cần remove orphan files (vd: 'nessie.taxi.silver')
    """
    logger.info(f"[ORPHAN] Bắt đầu remove orphan files cho bảng: {table_name}")
    
    try:
        # Gọi stored procedure để remove orphan files
        result = spark.sql(f"CALL nessie.system.remove_orphan_files('{table_name}')")
        
        # Lấy kết quả
        rows = result.collect()
        if rows:
            logger.info(f"[ORPHAN] Remove orphan files hoàn tất cho bảng {table_name}")
            for row in rows:
                logger.info(f"   - Files removed: {row.removed_files_count}")
        else:
            logger.info(f"[ORPHAN] Không có file mồ côi nào được tìm thấy cho bảng {table_name}")
            
    except Exception as e:
        logger.error(f"[ORPHAN] Lỗi khi remove orphan files bảng {table_name}: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Remove orphan files script cho Lakehouse")
    parser.add_argument("--tables", nargs="+", required=True, 
                        help="Danh sách các bảng cần remove orphan files (vd: nessie.taxi.silver nessie.taxi.gold_revenue_by_hour)")
    args = parser.parse_args()
    
    # Tạo Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"[ORPHAN] Bắt đầu remove orphan files cho {len(args.tables)} bảng")
    
    # Thực hiện remove orphan files cho từng bảng
    for table_name in args.tables:
        try:
            remove_orphan_files_for_table(spark, table_name)
        except Exception as e:
            logger.error(f"[ORPHAN] Bỏ qua bảng {table_name} do lỗi: {str(e)}")
            continue
    
    logger.info("[ORPHAN] Hoàn tất quá trình remove orphan files")
    
    # Dừng Spark session
    spark.stop()

if __name__ == "__main__":
    main()