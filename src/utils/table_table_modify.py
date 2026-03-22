"""
Script để ALTER TABLE và backfill trip_id cho Silver table.

Usage:
    python src/utils/table_table_modify.py

Chức năng:
    1. Kiểm tra xem cột trip_id đã tồn tại chưa
    2. Nếu chưa: ADD COLUMN trip_id
    3. Backfill trip_id cho toàn bộ data cũ (dùng MD5 hash)
    4. Verify kết quả
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_column_exists(spark: SparkSession, table_name: str, column_name: str) -> bool:
    """Kiểm tra xem cột có tồn tại trong bảng không"""
    try:
        columns = spark.sql(f"DESCRIBE TABLE {table_name}").collect()
        column_names = [row['col_name'] for row in columns]
        return column_name in column_names
    except Exception as e:
        logger.error(f"Lỗi khi kiểm tra cột: {e}")
        return False


def add_trip_id_column(spark: SparkSession, table_name: str):
    """
    Thêm cột trip_id vào bảng nếu chưa tồn tại.
    
    Args:
        spark: SparkSession
        table_name: Tên table (e.g., 'nessie.taxi.silver')
    """
    logger.info(f"Đang kiểm tra bảng {table_name}...")
    
    # Kiểm tra bảng có tồn tại không
    if not spark.catalog.tableExists(table_name):
        logger.error(f"❌ Bảng {table_name} không tồn tại!")
        return False
    
    # Kiểm tra cột trip_id đã tồn tại chưa
    if check_column_exists(spark, table_name, 'trip_id'):
        logger.warning(f"⚠️  Cột trip_id đã tồn tại trong bảng {table_name}")
        logger.info("Bỏ qua bước ADD COLUMN, chuyển sang backfill...")
        return True
    
    # Thêm cột trip_id
    logger.info("Đang thêm cột trip_id (STRING) vào bảng...")
    try:
        spark.sql(f"""
            ALTER TABLE {table_name} 
            ADD COLUMN trip_id STRING
        """)
        logger.info("✅ Đã thêm cột trip_id thành công!")
        return True
    except Exception as e:
        logger.error(f"❌ Lỗi khi thêm cột trip_id: {e}")
        return False


def backfill_trip_id(spark: SparkSession, table_name: str, batch_size: int = 1000000):
    """
    Backfill giá trị trip_id cho toàn bộ data cũ.
    
    Cách hoạt động:
        1. Đọc bảng cũ
        2. Tạo trip_id bằng MD5 hash
        3. Overwrite lại bảng (theo partition để an toàn)
    
    Args:
        spark: SparkSession
        table_name: Tên table
        batch_size: Số rows xử lý mỗi batch (default: 1M)
    """
    logger.info(f"Đang backfill trip_id cho bảng {table_name}...")
    
    try:
        # Đọc toàn bộ bảng
        df = spark.table(table_name)
        total_rows = df.count()
        logger.info(f"Tổng số rows cần backfill: {total_rows:,}")
        
        # Kiểm tra xem đã có trip_id chưa (NULL = chưa có)
        null_trip_id_count = df.filter(F.col("trip_id").isNull()).count()
        logger.info(f"Số rows có trip_id NULL: {null_trip_id_count:,}")
        
        if null_trip_id_count == 0:
            logger.info("✅ Tất cả rows đã có trip_id. Không cần backfill!")
            return True
        
        # Generate trip_id cho toàn bộ data
        logger.info("Đang generate trip_id bằng MD5 hash...")
        
        # QUAN TRỌNG: Kiểm tra xem columns có lowercase hay uppercase
        df_schema = df.columns
        
        # Check nếu table dùng uppercase (VendorID) hay lowercase (vendor_id)
        if 'VendorID' in df_schema:
            # Case 1: Bronze layer (uppercase)
            logger.info("Phát hiện schema uppercase (Bronze layer)")
            vendor_col = 'VendorID'
            pu_col = 'PULocationID'
            do_col = 'DOLocationID'
        elif 'vendor_id' in df_schema:
            # Case 2: Silver layer (lowercase)
            logger.info("Phát hiện schema lowercase (Silver layer)")
            vendor_col = 'vendor_id'
            pu_col = 'pulocation_id'
            do_col = 'dolocation_id'
        else:
            logger.error("❌ Không tìm thấy cột VendorID hoặc vendor_id!")
            return False
        
        # Generate trip_id (dùng COALESCE để handle NULL)
        df_with_trip_id = df.withColumn(
            "trip_id",
            F.md5(
                F.concat_ws(
                    "|",
                    F.coalesce(F.col(vendor_col).cast("string"), F.lit("")),
                    F.coalesce(F.col("tpep_pickup_datetime").cast("string"), F.lit("")),
                    F.coalesce(F.col("tpep_dropoff_datetime").cast("string"), F.lit("")),
                    F.coalesce(F.col(pu_col).cast("string"), F.lit("")),
                    F.coalesce(F.col(do_col).cast("string"), F.lit("")),
                    F.coalesce(F.col("fare_amount").cast("string"), F.lit(""))
                )
            )
        )
        
        # Verify trip_id đã được tạo
        sample = df_with_trip_id.select("trip_id").limit(5).collect()
        logger.info(f"Sample trip_id: {[row['trip_id'] for row in sample]}")
        
        # Xác định partition column
        # Check partition trong table metadata
        table_info = spark.sql(f"SHOW CREATE TABLE {table_name}").collect()
        partition_col = None
        
        # Determine partition column
        if 'pickup_date' in df_schema:
            partition_col = 'pickup_date'
        elif 'ingestion_date' in df_schema:
            partition_col = 'ingestion_date'
        else:
            logger.warning("⚠️  Không tìm thấy partition column. Sẽ overwrite toàn bộ table.")
        
        # Overwrite table với dynamic partition mode
        logger.info("Đang ghi lại data với trip_id mới...")
        
        if partition_col:
            logger.info(f"Sử dụng dynamic partition overwrite theo cột: {partition_col}")
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            
            # Overwrite từng partition
            df_with_trip_id.writeTo(table_name).overwritePartitions()
        else:
            # Overwrite toàn bộ table (nguy hiểm hơn!)
            logger.warning("⚠️  CẢNH BÁO: Đang overwrite TOÀN BỘ table!")
            logger.info("Tạo backup trước khi overwrite...")
            
            # Tạo branch backup
            branch_name = f"backup_before_trip_id_{int(time.time())}"
            spark.sql(f"CREATE BRANCH IF NOT EXISTS `{branch_name}` IN nessie FROM main")
            logger.info(f"✅ Đã tạo backup branch: {branch_name}")
            
            # Overwrite table
            df_with_trip_id.writeTo(table_name).overwrite()
        
        logger.info("✅ Backfill hoàn tất!")
        
        # Verify kết quả
        verify_backfill(spark, table_name)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Lỗi khi backfill trip_id: {e}")
        import traceback
        traceback.print_exc()
        return False


def verify_backfill(spark: SparkSession, table_name: str):
    """
    Verify xem backfill đã thành công chưa.
    
    Kiểm tra:
        1. Tất cả rows đều có trip_id (không NULL)
        2. trip_id có format đúng (32 chars MD5)
        3. Uniqueness rate
    """
    logger.info("\n" + "="*60)
    logger.info("VERIFY BACKFILL RESULT")
    logger.info("="*60)
    
    df = spark.table(table_name)
    
    # Check 1: NULL count
    total_rows = df.count()
    null_count = df.filter(F.col("trip_id").isNull()).count()
    logger.info(f"1️⃣  NULL Check:")
    logger.info(f"   Total rows: {total_rows:,}")
    logger.info(f"   NULL trip_id: {null_count:,}")
    
    if null_count > 0:
        logger.error(f"   ❌ FAIL: Còn {null_count:,} rows có trip_id NULL!")
    else:
        logger.info(f"   ✅ PASS: Tất cả rows đều có trip_id")
    
    # Check 2: Format check (MD5 = 32 chars)
    invalid_format = df.filter(F.length("trip_id") != 32).count()
    logger.info(f"\n2️⃣  Format Check (MD5 = 32 chars):")
    logger.info(f"   Invalid format count: {invalid_format:,}")
    
    if invalid_format > 0:
        logger.error(f"   ❌ FAIL: {invalid_format:,} rows có trip_id sai format!")
        # Show sample
        samples = df.filter(F.length("trip_id") != 32).select("trip_id").limit(5).collect()
        logger.error(f"   Sample: {[row['trip_id'] for row in samples]}")
    else:
        logger.info(f"   ✅ PASS: Tất cả trip_id đều đúng format (32 chars)")
    
    # Check 3: Uniqueness
    unique_count = df.select("trip_id").distinct().count()
    duplicate_count = total_rows - unique_count
    uniqueness_rate = (unique_count / total_rows * 100) if total_rows > 0 else 0
    
    logger.info(f"\n3️⃣  Uniqueness Check:")
    logger.info(f"   Total rows: {total_rows:,}")
    logger.info(f"   Unique trip_id: {unique_count:,}")
    logger.info(f"   Duplicates: {duplicate_count:,}")
    logger.info(f"   Uniqueness rate: {uniqueness_rate:.4f}%")
    
    if duplicate_count > 0:
        logger.warning(f"   ⚠️  WARNING: {duplicate_count:,} duplicate trip_ids detected!")
        logger.info(f"   Đây là bình thường nếu có trips hoàn toàn giống nhau")
    else:
        logger.info(f"   ✅ PASS: 100% unique!")
    
    # Show sample trip_ids
    logger.info(f"\n4️⃣  Sample trip_ids:")
    samples = df.select("trip_id", "vendor_id" if "vendor_id" in df.columns else "VendorID") \
                .limit(5).collect()
    for i, row in enumerate(samples, 1):
        logger.info(f"   {i}. {row['trip_id']}")
    
    logger.info("\n" + "="*60)


def main():
    """Main function"""
    TABLE_NAME = "nessie.taxi.silver"
    
    logger.info("="*60)
    logger.info("BACKFILL TRIP_ID FOR SILVER TABLE")
    logger.info("="*60)
    logger.info(f"Target table: {TABLE_NAME}\n")
    
    # Khởi tạo SparkSession
    spark = (
        SparkSession.builder
        .appName("Backfill trip_id")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Step 1: Add column nếu chưa có
        if not add_trip_id_column(spark, TABLE_NAME):
            logger.error("❌ Không thể thêm cột trip_id. Dừng lại.")
            return
        
        # Step 2: Backfill data
        if not backfill_trip_id(spark, TABLE_NAME):
            logger.error("❌ Backfill thất bại. Kiểm tra logs ở trên.")
            return
        
        logger.info("\n🎉 HOÀN TẤT! trip_id đã được backfill thành công!")
        logger.info("Bây giờ bạn có thể chạy pipeline mới với MERGE statement.")
        
    except Exception as e:
        logger.error(f"❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
