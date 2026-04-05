#!/usr/bin/env python3
"""
Script để thực hiện compaction cho các bảng Iceberg trong hệ thống Lakehouse.
Compaction giúp gộp các file nhỏ thành file lớn để tăng hiệu năng đọc.
"""

import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Tạo Spark session với cấu hình cho Iceberg và Nessie"""
    return (
        SparkSession.builder
        .appName(f"Lakehouse Compaction - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        .getOrCreate()
    )

def compact_table(spark, table_name):
    """
    Thực hiện compaction cho một bảng cụ thể
    
    Args:
        spark: Spark session
        table_name: Tên bảng cần compaction (vd: 'nessie.taxi.silver')
    """
    logger.info(f"Bắt đầu compaction cho bảng: {table_name}")
    
    try:
        # Gọi stored procedure để compaction
        result = spark.sql(f"CALL nessie.system.rewrite_data_files('{table_name}')")
        
        # Lấy kết quả
        rows = result.collect()
        if rows:
            logger.info(f"Compaction hoàn tất cho bảng {table_name}")
            for row in rows:
                logger.info(f"   - Files rewritten: {row.rewritten_data_files_count}")
                logger.info(f"   - Files added: {row.added_data_files_count}")
                logger.info(f"   - Bytes rewritten: {row.rewritten_bytes_count}")
                logger.info(f"   - Files failed: {row.failed_data_files_count}")
        else:
            logger.info(f"Không có file nào cần compaction cho bảng {table_name}")
            
    except Exception as e:
        logger.error(f"Lỗi khi compaction bảng {table_name}: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Compaction script cho Lakehouse")
    parser.add_argument("--tables", nargs="+", required=True, 
                        help="Danh sách các bảng cần compaction (vd: nessie.taxi.silver nessie.taxi.fact_trips)")
    args = parser.parse_args()
    
    # Tạo Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"Bắt đầu compaction cho {len(args.tables)} bảng")
    
    # Thực hiện compaction cho từng bảng
    for table_name in args.tables:
        try:
            compact_table(spark, table_name)
        except Exception as e:
            logger.error(f"Bỏ qua bảng {table_name} do lỗi: {str(e)}")
            continue
    
    logger.info("Hoàn tất quá trình compaction")
    
    # Dừng Spark session
    spark.stop()

if __name__ == "__main__":
    main()