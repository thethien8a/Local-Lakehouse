import logging

logger = logging.getLogger(__name__)


def write_gold_table(df, table_name, partition_col="pickup_date"):
    """Ghi dữ liệu vào Gold table với cơ chế Overwrite Partition"""
    spark = df.sparkSession
    if spark.catalog.tableExists(table_name):
        logger.info(f"  -> Ghi đè partition ({partition_col}) vào {table_name}...")
        df.writeTo(table_name).overwritePartitions()
    else:
        logger.info(f"  -> Tạo mới bảng {table_name}...")
        df.writeTo(table_name).tableProperty("gc.enabled", "true").partitionedBy(partition_col).create()


def write_dim_table(df, table_name):
    """Ghi bảng dim — không partition, bỏ qua nếu đã tồn tại"""
    spark = df.sparkSession
    if spark.catalog.tableExists(table_name):
        logger.info(f"  -> dim '{table_name}' đã tồn tại, bỏ qua.")
    else:
        logger.info(f"  -> Tạo mới dim '{table_name}'...")
        df.writeTo(table_name).create()
