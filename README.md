***

# 🏗️ Local Data Lakehouse: Spark, Iceberg, Nessie & Airflow

Dự án này triển khai một hệ thống **Data Lakehouse cục bộ (Local Deployment)** mạnh mẽ, kết hợp tính toàn vẹn của Data Warehouse và tính linh hoạt của Data Lake. Hệ thống sử dụng kiến trúc **Medallion (Bronze - Silver - Gold)** để xử lý và phân tích tập dữ liệu NYC Taxi.

---

## 🚀 Tổng quan Kiến trúc

Hệ thống được xây dựng dựa trên 4 lớp chức năng cốt lõi:

1. **Lớp Lưu trữ Đối tượng (MinIO):** Hoạt động như S3 cục bộ, lưu trữ dữ liệu thô và các tệp Parquet.
2. **Lớp Định dạng Bảng (Apache Iceberg):** Cung cấp giao dịch ACID, tiến hóa lược đồ (Schema) và phân vùng (Partition).
3. **Lớp Catalog & Quản lý Phiên bản (Project Nessie):** Đóng vai trò như "Git cho dữ liệu", hỗ trợ phân nhánh (branching) và mô hình Write-Audit-Publish (WAP).
4. **Lớp Xử lý & Điều phối (Spark & Airflow):** Spark đóng vai trò là Engine tính toán ETL (SQL/DataFrame), trong khi Airflow lập lịch và giám sát quy trình.

### Data Flow

```
Landing Zone (S3/MinIO)
    ↓
Bronze Layer (Raw Ingestion)
    ↓ Cleaning & Enrichment
Silver Layer (Validated Data)
    ↓ Aggregation & Analytics
Gold Layer (Business-Ready Tables)
```

---

## 🛠️ Công nghệ & Dịch vụ (Tech Stack)

Các dịch vụ được triển khai thông qua **Docker Compose**:

| Dịch vụ | Hình ảnh (Image) | Vai trò | Cổng (Port) |
| :--- | :--- | :--- | :--- |
| **MinIO** | `minio/minio` | Lưu trữ đối tượng S3-compatible | `9000` (API), `9001` (UI) |
| **Nessie** | `ghcr.io/projectnessie/nessie`| Catalog quản lý phiên bản dữ liệu | `19120` |
| **Spark Master** | `bitnamilegacy/spark:3.5.1` | Điều phối cụm tính toán | `7077`, `8082` (UI) |
| **Spark Worker** | `bitnamilegacy/spark:3.5.1` | Thực thi các tác vụ xử lý dữ liệu | `8081` (UI) |
| **Airflow** | `apache/airflow:2.10.3` | Điều phối quy trình tự động (DAGs)| `8080` (UI) |
| **PostgreSQL**| `postgres:15` | Lưu trữ metadata cho Airflow | `5432` |

---

## 📋 Yêu cầu Hệ thống (Prerequisites)

* **Docker & Docker Compose** đã được cài đặt.
* **RAM:** Tối thiểu 8GB - 16GB (để chạy đồng thời Airflow, Spark và MinIO).
* **Python 3.8+** (nếu muốn phát triển script local).

---

## 📂 Cấu trúc Dự án

```
lakehouse/
├── conf/                          # Cấu hình Spark
│   ├── spark-defaults.conf        # Kết nối Nessie, MinIO, Iceberg
│   └── log4j2.properties          # Log configuration
├── dags/                          # ⚠️ Airflow DAGs (CHƯA CÓ - CẦN TẠO)
├── data/                          # Dữ liệu local
│   ├── nyc_taxi_data.parquet      # File gốc (50MB)
│   └── landing/                   # Landing zone - chia theo ngày
│       └── date=YYYY-MM-DD/       # Partition theo ngày
├── jars/                          # JAR dependencies cho Spark
│   ├── iceberg-spark-runtime-3.5_2.12-1.6.0.jar
│   ├── nessie-spark-extensions-3.5_2.12-0.99.0.jar
│   ├── hadoop-aws-3.3.4.jar
│   ├── aws-java-sdk-bundle-1.12.262.jar
│   └── wildfly-openssl-1.0.7.Final.jar
├── logs/                          # Airflow logs
├── src/                           # PySpark scripts
│   ├── bronze/
│   │   └── ingest_bronze.py       # Ingest raw data → Bronze
│   ├── silver/
│   │   ├── ingest_silver.py       # Bronze → Silver (WAP pattern)
│   │   ├── get_data_from_bronze.py
│   │   ├── clean_before_ingest.py
│   │   └── validate_before_ingest.py
│   ├── gold/
│   │   └── ingest_gold.py         # Silver → Gold (8 bảng tổng hợp)
│   ├── utils/
│   │   ├── download_data.py       # Tải NYC Taxi dataset
│   │   ├── download_jars.py       # Tải dependencies JAR
│   │   └── split_data_by_day.py   # Chia data theo ngày
│   ├── config/
│   │   └── path.py                # Cấu hình đường dẫn
│   └── test/
│       ├── show_all_table.py      # Liệt kê tables
│       ├── show_namespace.py      # Liệt kê namespaces
│       └── checking_value_table.py
├── docker-compose.yml             # Định nghĩa services
└── README.md
```

---

## ⚙️ Cấu hình Cốt lõi

### Các gói JAR phụ thuộc cho Spark

Để Spark làm việc với Iceberg, Nessie và MinIO, hệ thống sử dụng **5 JAR files**:

| JAR File | Phiên bản | Chức năng |
|----------|-----------|-----------|
| `iceberg-spark-runtime-3.5_2.12` | 1.6.0 | Iceberg runtime cho Spark 3.5 |
| `nessie-spark-extensions-3.5_2.12` | 0.99.0 | Nessie catalog integration |
| `hadoop-aws` | 3.3.4 | S3A filesystem support |
| `aws-java-sdk-bundle` | 1.12.262 | AWS SDK cho MinIO |
| `wildfly-openssl` | 1.0.7.Final | OpenSSL support |

**Tải tự động:**
```bash
python src/utils/download_jars.py
```

### spark-defaults.conf

Cấu hình mẫu để kết nối Spark với Nessie và MinIO:

```properties
spark.sql.extensions = org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions
spark.sql.catalog.nessie = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.nessie.catalog-impl = org.apache.iceberg.nessie.NessieCatalog
spark.sql.catalog.nessie.uri = http://nessie:19120/api/v1
spark.sql.catalog.nessie.ref = main
spark.sql.catalog.nessie.warehouse = s3a://warehouse/
spark.sql.catalog.nessie.authentication.type = NONE

# Cấu hình MinIO (S3A)
spark.hadoop.fs.s3a.endpoint = http://minio:9000
spark.hadoop.fs.s3a.access.key = admin
spark.hadoop.fs.s3a.secret.key = password
spark.hadoop.fs.s3a.path.style.access = true
spark.hadoop.fs.s3a.impl = org.apache.hadoop.fs.s3a.S3AFileSystem
```

---

## 🗺️ Lộ trình Triển khai (Roadmap)

### Giai đoạn 1 & 2: Hạ tầng & Cấu hình ✅

1. Khởi động hệ thống: `docker-compose up -d`
2. Truy cập MinIO UI (`http://localhost:9001`), bucket `warehouse` tự động được tạo.
3. Mạng lưới volume dùng chung (`spark-events`) giữa Spark và Airflow để quản lý log.

### Giai đoạn 3: Ingestion (Bronze Layer) ✅ HOÀN THÀNH

**Mục tiêu:** Ingest dữ liệu thô từ Landing Zone vào bảng Iceberg.

**Bảng:** `nessie.taxi.bronze`  
**Partition:** `ingestion_date`  
**Script:** `src/bronze/ingest_bronze.py`

**Chạy:**
```bash
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/bronze/ingest_bronze.py \
  --date 2024-01-15
```

### Giai đoạn 4: Transformation (Silver Layer) ✅ HOÀN THÀNH

**Mục tiêu:** Làm sạch, validate và làm giàu dữ liệu với WAP Pattern.

**Bảng:** `nessie.taxi.silver`  
**Partition:** `ingestion_date`  
**Script:** `src/silver/ingest_silver.py`

**Chức năng:**
- Loại bỏ dữ liệu lỗi (fare < 0, trip_distance < 0, null values)
- Feature Engineering: `trip_duration_min`, `avg_speed_mph`, `fare_per_mile`, `fare_per_min`
- Time-based features: `pickup_hour`, `pickup_weekday`, `is_weekend`, `is_rush_hour`, `time_bucket`
- Business flags: `is_long_trip`, `is_tip_more_total`
- WAP Pattern với Nessie branching

**Chạy:**
```bash
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/silver/ingest_silver.py \
  --date 2024-01-15
```

### Giai đoạn 5: Aggregation (Gold Layer) ✅ HOÀN THÀNH

**Mục tiêu:** Tạo 8 bảng tổng hợp phục vụ Business Intelligence.

**Script:** `src/gold/ingest_gold.py`

**8 Bảng Gold:**

| Bảng | Mô tả | Dimensions | Metrics |
|------|-------|-----------|---------|
| `gold_revenue_by_hour` | Doanh thu theo giờ | pickup_date, pickup_hour | total_revenue, total_trips, avg_revenue_per_trip |
| `gold_revenue_by_location` | Doanh thu theo khu vực | pickup_date, pulocation_id | total_revenue, total_trips, avg_tip, avg_fare_per_mile |
| `gold_fare_by_date` | Giá cước trung bình theo ngày | pickup_date | avg_fare_amount, avg_total_amount, total_trips |
| `gold_od_pairs` | Cặp điểm đón-trả phổ biến | pickup_date, pulocation_id, dolocation_id | total_trips, avg_revenue, avg_duration, avg_speed |
| `gold_efficiency_by_timebucket` | Hiệu suất theo khung giờ | pickup_date, time_bucket, is_rush_hour, is_weekend | avg_fare_per_min, avg_fare_per_mile, avg_speed |
| `gold_tip_behavior` | Hành vi tip | pickup_date, payment_type, pickup_weekday, is_weekend | avg_tip, trips_tip_exceed_fare |
| `gold_traffic_by_zone` | Phân tích tắc đường | pickup_date, pulocation_id, pickup_hour, is_rush_hour | avg_speed, avg_duration, total_trips |
| `gold_revenue_weekday_weekend` | So sánh Weekday vs Weekend | pickup_date, pulocation_id, is_weekend | total_revenue, total_trips, avg_fare_per_mile |

**Chạy:**
```bash
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/gold/ingest_gold.py \
  --date 2024-01-15
```

### Giai đoạn 6: Orchestration với Airflow ⚠️ CHƯA TRIỂN KHAI

**Hiện trạng:** Folder `dags/` đang **rỗng** - chưa có DAG nào.

**Cần làm:**
1. Tạo Connection trên Airflow UI tới Spark Master (`spark://spark-master:7077`)
2. Tạo DAG để orchestrate Bronze → Silver → Gold pipeline
3. Sử dụng `SparkSubmitOperator` (KHÔNG dùng PythonOperator cho ETL nặng)
4. Cấu hình scheduling (daily/hourly)

---

## 🌿 Tính năng Nâng cao (Nessie WAP Pattern)

Dự án áp dụng mô hình **Write-Audit-Publish (WAP)** thông qua Nessie Branching để đảm bảo chất lượng dữ liệu:

### Workflow WAP

1. **Tạo nhánh cô lập:** `CREATE BRANCH etl_jan_2024 IN nessie`
2. **Ghi dữ liệu (ETL):** Spark ghi vào nhánh vừa tạo (không ảnh hưởng nhánh `main`).
3. **Kiểm định (Audit):** Chạy Data Quality Checks trên nhánh `etl_jan_2024`.
4. **Hợp nhất (Publish):** Khi dữ liệu "sạch", hợp nhất vào nhánh chính `MERGE BRANCH etl_jan_2024 INTO main IN nessie`.

**Du hành thời gian (Time Travel):**
Hệ thống cho phép truy vấn dữ liệu theo các thời điểm cụ thể trong quá khứ thông qua `AT TIMESTAMP` hoặc `AT SNAPSHOT`.

---

## 🧪 WAP Thực tế với Nessie (Production-Ready)

### Silver Layer WAP

Script `src/silver/ingest_silver.py` hỗ trợ chạy theo từng bước độc lập:

**Các bước:**
1. **Write:** Tạo branch-per-run và ghi dữ liệu Silver lên branch đó
2. **Audit:** Chạy quality gates trên branch vừa ghi
3. **Publish:** Merge branch vào `main` với retry/backoff

**Chạy đầy đủ (local test):**
```bash
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/silver/ingest_silver.py \
  --date 2024-01-15
```

**Chạy tách bước (khuyến nghị cho Airflow DAG):**

```bash
# 1) WRITE - tạo branch theo run id
docker compose exec \
  -e WAP_MODE=write \
  -e WAP_RUN_ID=manual_20260307_0900 \
  spark-master \
  spark-submit /opt/bitnami/spark/src/silver/ingest_silver.py --date 2024-01-15

# 2) AUDIT - kiểm định branch vừa ghi
docker compose exec \
  -e WAP_MODE=audit \
  -e NESSIE_WAP_BRANCH=etl_silver_manual_20260307_0900_20260307T090012Z_abc12345 \
  spark-master \
  spark-submit /opt/bitnami/spark/src/silver/ingest_silver.py --date 2024-01-15

# 3) PUBLISH - merge có retry, sau đó drop branch đã publish
docker compose exec \
  -e WAP_MODE=publish \
  -e NESSIE_WAP_BRANCH=etl_silver_manual_20260307_0900_20260307T090012Z_abc12345 \
  -e WAP_MERGE_MAX_RETRIES=5 \
  spark-master \
  spark-submit /opt/bitnami/spark/src/silver/ingest_silver.py --date 2024-01-15
```

### Gold Layer WAP

Script `src/gold/ingest_gold.py` tự động:
- Tạo branch `etl_gold_YYYYMMDD_timestamp`
- Ghi 8 bảng Gold vào branch
- Merge về `main` khi hoàn tất
- Cleanup branch sau khi publish

### Biến môi trường quan trọng cho Nessie WAP

| Biến | Mặc định | Ý nghĩa |
| :--- | :--- | :--- |
| `WAP_MODE` | `all` | Chọn mode `write/audit/publish/all`. |
| `WAP_RUN_ID` | `adhoc` | Run ID nghiệp vụ (script tự append timestamp + UUID). |
| `WAP_BRANCH_PREFIX` | `etl_silver` | Prefix cho branch WAP phục vụ cleanup theo TTL. |
| `NESSIE_WAP_BRANCH` | auto-generated | Ép dùng branch cụ thể (audit/publish). |
| `WAP_MERGE_MAX_RETRIES` | `3` | Số lần retry khi `MERGE BRANCH` gặp conflict. |
| `WAP_MERGE_BASE_BACKOFF_SEC` | `5` | Backoff cơ bản (giây) giữa các lần retry merge. |
| `WAP_CLEANUP_STALE_BRANCHES` | `true` | Bật cleanup các branch cũ theo TTL. |
| `WAP_BRANCH_RETENTION_HOURS` | `72` | TTL cho branch cũ cùng prefix. |
| `WAP_EXPIRE_SNAPSHOTS` | `false` | Bật gọi `expire_snapshots` sau khi publish. |
| `WAP_SNAPSHOT_RETENTION_DAYS` | `14` | Số ngày giữ snapshot khi cleanup. |
| `SILVER_MIN_KEEP_RATIO` | `0.20` | Ngưỡng tỷ lệ dữ liệu giữ lại sau clean. |

---

## 🔧 Scripts & Utilities

### 1. Setup Scripts (src/utils/)

#### download_data.py
Tải dataset NYC Taxi Yellow Tripdata (Tháng 1/2024, ~50MB).

```bash
python src/utils/download_data.py
```

**Output:** `data/nyc_taxi_data.parquet`

#### download_jars.py
Tải tự động 5 JAR dependencies cho Spark.

```bash
python src/utils/download_jars.py
```

**Output:** Các file JAR trong folder `jars/`

#### split_data_by_day.py
Chia file dữ liệu lớn thành các partition theo ngày (giả lập Landing Zone thực tế).

```bash
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/utils/split_data_by_day.py
```

**Output:** `data/landing/date=YYYY-MM-DD/`

### 2. Ingestion Scripts

#### Bronze Layer (src/bronze/ingest_bronze.py)
```bash
# Ingest dữ liệu một ngày cụ thể
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/bronze/ingest_bronze.py \
  --date 2024-01-15
```

**Chức năng:**
- Đọc từ Landing Zone (`data/landing/date=YYYY-MM-DD/`)
- Append vào bảng `nessie.taxi.bronze`
- Thêm column `ingestion_date` để tracking

#### Silver Layer (src/silver/)
```bash
# Transform Bronze → Silver
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/silver/ingest_silver.py \
  --date 2024-01-15
```

**Module breakdown:**
- `get_data_from_bronze.py`: Đọc dữ liệu từ Bronze table
- `clean_before_ingest.py`: Data cleaning + feature engineering
- `validate_before_ingest.py`: Data quality checks
- `ingest_silver.py`: Orchestrate full WAP pipeline

#### Gold Layer (src/gold/ingest_gold.py)
```bash
# Tạo 8 bảng aggregation
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/gold/ingest_gold.py \
  --date 2024-01-15
```

### 3. Testing & Monitoring Scripts (src/test/)

```bash
# Liệt kê tất cả namespaces
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/test/show_namespace.py

# Liệt kê tất cả tables
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/test/show_all_table.py

# Kiểm tra giá trị trong table
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/test/checking_value_table.py
```

---

## 📊 Bảng Gold Layer Chi tiết

### 1. gold_revenue_by_hour
**Mục đích:** Phân tích doanh thu theo từng giờ trong ngày

**Columns:**
- `pickup_date`: Ngày
- `pickup_hour`: Giờ (0-23)
- `total_revenue`: Tổng doanh thu
- `total_trips`: Số chuyến
- `avg_revenue_per_trip`: Doanh thu trung bình/chuyến

**Use case:** Tìm giờ peak để tối ưu fleet management

### 2. gold_revenue_by_location
**Mục đích:** Phân tích khu vực có doanh thu cao

**Columns:**
- `pickup_date`: Ngày
- `pulocation_id`: Location ID
- `total_revenue`, `total_trips`, `avg_revenue_per_trip`
- `avg_tip_amount`, `avg_fare_per_mile`

**Use case:** Xác định hot zones cho drivers

### 3. gold_fare_by_date
**Mục đích:** Xu hướng giá cước theo ngày

**Columns:**
- `pickup_date`: Ngày
- `avg_fare_amount`: Giá cước TB
- `avg_total_amount`: Tổng chi phí TB
- `total_trips`: Số chuyến

**Use case:** Pricing strategy & demand forecasting

### 4. gold_od_pairs
**Mục đích:** Phân tích cặp điểm đón-trả phổ biến (Origin-Destination)

**Columns:**
- `pickup_date`, `pulocation_id`, `dolocation_id`
- `total_trips`, `avg_revenue`, `avg_duration_min`
- `avg_tip`, `avg_speed_mph`

**Use case:** Route optimization & demand prediction

### 5. gold_efficiency_by_timebucket
**Mục đích:** Hiệu suất theo khung giờ (morning/afternoon/evening/night)

**Columns:**
- `pickup_date`, `time_bucket`, `is_rush_hour`, `is_weekend`
- `total_trips`, `avg_fare_per_min`, `avg_fare_per_mile`
- `avg_speed_mph`, `avg_duration_min`, `avg_tip`

**Use case:** Operational efficiency analysis

### 6. gold_tip_behavior
**Mục đích:** Phân tích hành vi tip theo payment type & thời gian

**Columns:**
- `pickup_date`, `payment_type`, `pickup_weekday`, `is_weekend`
- `total_trips`, `avg_tip`, `trips_tip_exceed_fare`
- `avg_total_amount`, `avg_fare_amount`

**Use case:** Payment method analysis & driver earnings

### 7. gold_traffic_by_zone
**Mục đích:** Phân tích tắc đường theo zone & giờ

**Columns:**
- `pickup_date`, `pulocation_id`, `pickup_hour`, `is_rush_hour`
- `avg_speed_mph`, `avg_duration_min`, `avg_fare_per_min`, `total_trips`

**Use case:** Traffic pattern analysis & congestion prediction

### 8. gold_revenue_weekday_weekend
**Mục đích:** So sánh performance Weekday vs Weekend

**Columns:**
- `pickup_date`, `pulocation_id`, `is_weekend`
- `total_revenue`, `total_trips`, `avg_fare_per_mile`
- `avg_tip`, `avg_speed_mph`

**Use case:** Seasonal demand analysis

---

## ✅ Hiện trạng Triển khai

### Hoàn thành ✅
- [x] **Infrastructure Setup**: Docker Compose với 8 services
- [x] **MinIO**: S3-compatible storage với auto bucket creation
- [x] **Nessie**: Git-like catalog với versioning
- [x] **Spark Cluster**: Master + Worker với 5 JAR dependencies
- [x] **Airflow**: Webserver + Scheduler (chưa có DAG)
- [x] **PostgreSQL**: Metadata database cho Airflow
- [x] **Bronze Layer**: Raw data ingestion với partition
- [x] **Silver Layer**: Full WAP pattern với validation
- [x] **Gold Layer**: 8 bảng tổng hợp business-ready
- [x] **Utilities**: Download scripts, data splitting
- [x] **Testing Scripts**: Monitoring & debugging tools

### Chưa Triển khai ⚠️
- [ ] **Airflow DAG**: Folder `dags/` rỗng - cần tạo DAG orchestrate pipeline
- [ ] **Data Quality Monitoring**: Automated quality checks
- [ ] **Alert System**: Notification khi pipeline fail
- [ ] **Documentation**: API documentation cho từng bảng Gold

### Cần Cải thiện 🔄
- [ ] **Error Handling**: Retry logic cho Bronze layer
- [ ] **Logging**: Centralized logging với ELK stack
- [ ] **Testing**: Unit tests cho transformation logic
- [ ] **Performance**: Optimize partition strategy cho Gold tables

---

## 🧹 Vận hành và Tối ưu hóa (Maintenance)

Dữ liệu Iceberg là bất biến, do đó cần thực hiện bảo trì định kỳ:

### Compaction (Nén tệp)
```sql
CALL nessie.system.rewrite_data_files('nessie.taxi.silver')
```

### Expire Snapshots (Xóa lịch sử cũ)
```sql
CALL nessie.system.expire_snapshots(
  table => 'nessie.taxi.silver',
  older_than => TIMESTAMP '2024-01-01 00:00:00.000',
  retain_last => 10
)
```

### Remove Orphan Files
Dọn dẹp tệp trên MinIO không còn được dùng:
```sql
CALL nessie.system.remove_orphan_files(
  table => 'nessie.taxi.silver'
)
```

### Recommended Schedule
- **Compaction**: Weekly (chủ nhật đêm)
- **Expire Snapshots**: Monthly (giữ 30 ngày)
- **Orphan Files**: Monthly (sau expire snapshots)

---

## ⚠️ Khắc phục sự cố (Troubleshooting)

### 1. Lỗi OOM (Out Of Memory)
**Triệu chứng:** Spark job bị kill giữa chừng

**Giải pháp:**
```yaml
# Trong docker-compose.yml, tăng memory cho worker
environment:
  - SPARK_WORKER_MEMORY=4G  # Tăng từ 2G lên 4G
```

### 2. Xung đột phiên bản JAR
**Triệu chứng:** `ClassNotFoundException` hoặc `NoSuchMethodError`

**Giải pháp:**
- Kiểm tra Compatibility Matrix:
  - Spark 3.5.x → Iceberg 1.6.0
  - Iceberg 1.6.0 → Nessie 0.99.0
- Đảm bảo JAR versions khớp với `src/utils/download_jars.py`

### 3. Lỗi quyền truy cập log (Airflow/Spark)
**Triệu chứng:** `PermissionError` khi ghi log

**Giải pháp:**
```bash
# Chạy lệnh này trên host
sudo chown -R 50000:50000 ./logs
```

### 4. MinIO connection refused
**Triệu chứng:** `Connection refused: http://minio:9000`

**Giải pháp:**
```bash
# Kiểm tra MinIO đã khởi động chưa
docker compose ps minio

# Restart nếu cần
docker compose restart minio
```

### 5. Nessie MERGE BRANCH conflict
**Triệu chứng:** `MERGE BRANCH failed: conflict detected`

**Giải pháp:**
- WAP pattern tự động retry với backoff
- Kiểm tra biến `WAP_MERGE_MAX_RETRIES` (default: 3)
- Nếu vẫn fail, manual merge:
```sql
USE REFERENCE main IN nessie;
MERGE BRANCH `etl_silver_xxx` INTO main IN nessie;
```

---

## 📚 Tài liệu Tham khảo

- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Project Nessie Documentation](https://projectnessie.org/)
- [Apache Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [NYC Taxi Dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

---

## 🎯 Quick Start Guide

### 1. Clone & Setup
```bash
# Clone repository
git clone <repo-url>
cd lakehouse

# Tải JAR dependencies
python src/utils/download_jars.py

# Tải NYC Taxi dataset
python src/utils/download_data.py
```

### 2. Khởi động Services
```bash
docker-compose up -d

# Kiểm tra services
docker-compose ps
```

### 3. Chuẩn bị Data
```bash
# Split data theo ngày
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/utils/split_data_by_day.py
```

### 4. Chạy Pipeline
```bash
# Bronze ingestion
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/bronze/ingest_bronze.py --date 2024-01-15

# Silver transformation
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/silver/ingest_silver.py --date 2024-01-15

# Gold aggregation
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/gold/ingest_gold.py --date 2024-01-15
```

### 5. Truy cập UI
- **MinIO:** http://localhost:9001 (admin/password)
- **Spark Master:** http://localhost:8082
- **Spark Worker:** http://localhost:8081
- **Airflow:** http://localhost:8080 (airflow/airflow123)
- **Nessie API:** http://localhost:19120/api/v1

---

## 📞 Support & Contribution

Nếu gặp vấn đề hoặc có đề xuất cải tiến, vui lòng:
1. Kiểm tra section **Troubleshooting** trước
2. Xem logs: `docker-compose logs <service-name>`
3. Tạo issue với đầy đủ thông tin: error logs, environment, steps to reproduce

---

*Tài liệu cập nhật dựa trên chiến lược triển khai hệ thống Local Data Lakehouse (2026).  
Phiên bản: v1.1 - Phản ánh 100% hiện trạng codebase thực tế.*
