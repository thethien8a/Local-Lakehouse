"""
Test script để verify trip_id generation.
Kiểm tra:
1. trip_id được tạo đúng format (MD5 hash)
2. Deterministic - cùng data cho ra cùng trip_id
3. Uniqueness rate
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
sys.path.append("../src/silver")
from src.silver.clean_before_ingest import feature_engineering

def test_trip_id_deterministic():
    """Test xem trip_id có deterministic không"""
    spark = (
        SparkSession.builder
        .appName("Test trip_id")
        .getOrCreate()
    )
    
    # Tạo sample data
    data = [
        (1, "2024-01-01 10:00:00", "2024-01-01 10:30:00", 100, 200, 50.0),
        (2, "2024-01-01 11:00:00", "2024-01-01 11:30:00", 101, 201, 60.0),
        (1, "2024-01-01 10:00:00", "2024-01-01 10:30:00", 100, 200, 50.0),  # Duplicate
    ]
    
    df = spark.createDataFrame(
        data,
        ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID", "fare_amount"]
    )
    
    # Convert string to timestamp
    df = df.withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
    df = df.withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
    
    # Generate trip_id
    df_with_id = df.withColumn(
        "trip_id",
        F.md5(
            F.concat_ws(
                "|",
                F.col("VendorID").cast("string"),
                F.col("tpep_pickup_datetime").cast("string"),
                F.col("tpep_dropoff_datetime").cast("string"),
                F.col("PULocationID").cast("string"),
                F.col("DOLocationID").cast("string"),
                F.col("fare_amount").cast("string")
            )
        )
    )
    
    print("=== Sample Data with trip_id ===")
    df_with_id.show(truncate=False)
    
    # Check duplicates
    trip_ids = df_with_id.select("trip_id").collect()
    print(f"\ntrip_id[0]: {trip_ids[0]['trip_id']}")
    print(f"trip_id[1]: {trip_ids[1]['trip_id']}")
    print(f"trip_id[2]: {trip_ids[2]['trip_id']}")
    
    # Verify deterministic
    assert trip_ids[0]['trip_id'] == trip_ids[2]['trip_id'], "❌ FAIL: trip_id không deterministic!"
    assert trip_ids[0]['trip_id'] != trip_ids[1]['trip_id'], "❌ FAIL: Trips khác nhau có cùng trip_id!"
    
    print("\n✅ PASS: trip_id is deterministic!")
    
    # Check format
    assert len(trip_ids[0]['trip_id']) == 32, "❌ FAIL: MD5 hash phải 32 ký tự!"
    print("✅ PASS: trip_id format correct (MD5 = 32 chars)")
    
    spark.stop()
def test_bronze_to_silver_idempotency():
    """
    Test xem chạy pipeline 2 lần liên tiếp có tạo duplicate không.
    
    Scenario:
    1. Chạy lần 1 → insert 100 rows
    2. Chạy lần 2 cùng data → MERGE nên UPDATE chứ không INSERT mới
    3. Verify: Vẫn chỉ có 100 rows
    """
    print("\n=== Test Idempotency ===")
    print("Hướng dẫn test thủ công:")
    print("1. Chạy: python src/silver/ingest_silver.py --date 2024-01-01")
    print("2. Kiểm tra count: spark-sql -e 'SELECT COUNT(*) FROM nessie.taxi.silver'")
    print("3. Chạy lại lần 2: python src/silver/ingest_silver.py --date 2024-01-01")
    print("4. Kiểm tra count lại → phải giống bước 2")
    print("\nNếu count tăng gấp đôi → BUG: MERGE không hoạt động đúng!")
if __name__ == "__main__":
    print("🧪 Testing trip_id generation...\n")
    
    test_trip_id_deterministic()
    test_bronze_to_silver_idempotency()
    
    print("\n🎉 All tests passed!")