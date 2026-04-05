#!/usr/bin/env python3
"""
Script tổng hợp để thực hiện toàn bộ quy trình bảo trì định kỳ cho hệ thống Lakehouse.
Bao gồm: compaction, expire snapshots, và remove orphan files.
"""

import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession

# Import các module từ cùng thư mục
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from compact_tables import create_spark_session, compact_table
from expire_snapshots import expire_snapshots_for_table
from remove_orphan_files import remove_orphan_files_for_table

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_full_maintenance(tables, days_to_keep=30, retain_last=10, skip_compaction=False, skip_expire=False, skip_remove_orphan=False):
    """
    Thực hiện toàn bộ quy trình bảo trì
    
    Args:
        tables: Danh sách các bảng cần bảo trì
        days_to_keep: Số ngày snapshot cần giữ lại
        retain_last: Số lượng snapshot gần nhất luôn giữ lại
        skip_compaction: Bỏ qua bước compaction
        skip_expire: Bỏ qua bước expire snapshots
        skip_remove_orphan: Bỏ qua bước remove orphan files
    """
    # Tạo Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"Bắt đầu quy trình bảo trì cho {len(tables)} bảng")
    
    # 1. Compaction
    if not skip_compaction:
        logger.info("BẮT ĐẦU COMPACT TABLES")
        for table_name in tables:
            try:
                compact_table(spark, table_name)
            except Exception as e:
                logger.error(f"Bỏ qua compaction bảng {table_name} do lỗi: {str(e)}")
                continue
    else:
        logger.info("BỎ QUA COMPACT TABLES theo yêu cầu")
    
    # 2. Expire snapshots
    if not skip_expire:
        logger.info("BẮT ĐẦU EXPIRE SNAPSHOTS")
        for table_name in tables:
            try:
                expire_snapshots_for_table(spark, table_name, days_to_keep, retain_last)
            except Exception as e:
                logger.error(f"Bỏ qua expire snapshots bảng {table_name} do lỗi: {str(e)}")
                continue
    else:
        logger.info("BỎ QUA EXPIRE SNAPSHOTS theo yêu cầu")
    
    # 3. Remove orphan files (chỉ chạy sau khi expire snapshots)
    if not skip_remove_orphan:
        logger.info("BẮT ĐẦU REMOVE ORPHAN FILES")
        for table_name in tables:
            try:
                remove_orphan_files_for_table(spark, table_name)
            except Exception as e:
                logger.error(f"Bỏ qua remove orphan files bảng {table_name} do lỗi: {str(e)}")
                continue
    else:
        logger.info("BỎ QUA REMOVE ORPHAN FILES theo yêu cầu")
    
    logger.info("HOÀN TẤT QUY TRÌNH BẢO TRÌ")
    
    # Dừng Spark session
    spark.stop()

def main():
    parser = argparse.ArgumentParser(description="Quy trình bảo trì định kỳ cho Lakehouse")
    parser.add_argument("--tables", nargs="+", required=True, 
                        help="Danh sách các bảng cần bảo trì (vd: nessie.taxi.silver nessie.taxi.gold_revenue_by_hour)")
    parser.add_argument("--days-to-keep", type=int, default=30,
                        help="Số ngày snapshot cần giữ lại (mặc định: 30)")
    parser.add_argument("--retain-last", type=int, default=10,
                        help="Số lượng snapshot gần nhất luôn giữ lại (mặc định: 10)")
    parser.add_argument("--skip-compaction", action="store_true",
                        help="Bỏ qua bước compaction")
    parser.add_argument("--skip-expire", action="store_true",
                        help="Bỏ qua bước expire snapshots")
    parser.add_argument("--skip-remove-orphan", action="store_true",
                        help="Bỏ qua bước remove orphan files")
    parser.add_argument("--mode", choices=["weekly", "monthly", "full"], default="full",
                        help="Chế độ bảo trì: weekly (chỉ compaction), monthly (compaction + expire + orphan), full (tất cả)")
    
    args = parser.parse_args()
    
    # Xác định các bước cần thực hiện dựa trên chế độ
    skip_compaction = args.skip_compaction
    skip_expire = args.skip_expire
    skip_remove_orphan = args.skip_remove_orphan
    
    if args.mode == "weekly":
        # Chỉ chạy compaction
        skip_compaction = False
        skip_expire = True
        skip_remove_orphan = True
    elif args.mode == "monthly":
        # Chạy expire và remove orphan, có thể bỏ qua compaction nếu đã chạy weekly
        skip_compaction = True  # Giả định compaction đã chạy weekly
        skip_expire = False
        skip_remove_orphan = False
    # Nếu là "full" thì sử dụng các tùy chọn riêng lẻ
    
    run_full_maintenance(
        tables=args.tables,
        days_to_keep=args.days_to_keep,
        retain_last=args.retain_last,
        skip_compaction=skip_compaction,
        skip_expire=skip_expire,
        skip_remove_orphan=skip_remove_orphan
    )

if __name__ == "__main__":
    main()