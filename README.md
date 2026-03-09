***

# 🏗️ Local Data Lakehouse: Spark, Iceberg, Nessie & Airflow

Dự án này triển khai một hệ thống **Data Lakehouse cục bộ (Local Deployment)** mạnh mẽ, kết hợp tính toàn vẹn của Data Warehouse và tính linh hoạt của Data Lake. Hệ thống sử dụng kiến trúc **Medallion (Bronze - Silver - Gold)** để xử lý và phân tích tập dữ liệu NYC Taxi.

## 🚀 Tổng quan Kiến trúc

Hệ thống được xây dựng dựa trên 4 lớp chức năng cốt lõi:
1. **Lớp Lưu trữ Đối tượng (MinIO):** Hoạt động như S3 cục bộ, lưu trữ dữ liệu thô và các tệp Parquet.
2. **Lớp Định dạng Bảng (Apache Iceberg):** Cung cấp giao dịch ACID, tiến hóa lược đồ (Schema) và phân vùng (Partition).
3. **Lớp Catalog & Quản lý Phiên bản (Project Nessie):** Đóng vai trò như "Git cho dữ liệu", hỗ trợ phân nhánh (branching) và mô hình Write-Audit-Publish (WAP).
4. **Lớp Xử lý & Điều phối (Spark & Airflow):** Spark đóng vai trò là Engine tính toán ETL (SQL/DataFrame), trong khi Airflow lập lịch và giám sát quy trình.

---

## 🛠️ Công nghệ & Dịch vụ (Tech Stack)

Các dịch vụ được triển khai thông qua **Docker Compose**:

| Dịch vụ | Hình ảnh (Image) | Vai trò | Cổng (Port) |
| :--- | :--- | :--- | :--- |
| **MinIO** | `minio/minio` | Lưu trữ đối tượng S3-compatible | `9000` (API), `9001` (UI) |
| **Nessie** | `ghcr.io/projectnessie/nessie`| Catalog quản lý phiên bản dữ liệu | `19120` |
| **Spark Master** | `bitnamilegacy/spark:3.5.1` | Điều phối cụm tính toán | `7077`, `8080` (UI) |
| **Spark Worker** | `bitnamilegacy/spark:3.5.1` | Thực thi các tác vụ xử lý dữ liệu | `8081` (UI) |
| **Airflow** | `apache/airflow:2.10.x` | Điều phối quy trình tự động (DAGs)| `8080` (UI) |
| **PostgreSQL**| `postgres:15` | Lưu trữ metadata cho Airflow | `5432` |

---

## 📋 Yêu cầu Hệ thống (Prerequisites)

* **Docker & Docker Compose** đã được cài đặt.
* **RAM:** Tối thiểu 8GB - 16GB (để chạy đồng thời Airflow, Spark và MinIO).
* Python 3.8+ (nếu muốn phát triển script local).

---

## ⚙️ Cấu hình cốt lõi

### Các gói JAR phụ thuộc cho Spark
Để Spark làm việc được với Iceberg, Nessie và MinIO, cần đảm bảo các thư viện sau:
* `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0`
* `org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.99.0`
* `org.apache.hadoop:hadoop-aws:3.3.x` và `com.amazonaws:aws-java-sdk-bundle:1.12.x`

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

### Giai đoạn 1 & 2: Hạ tầng & Cấu hình
1. Khởi động hệ thống: `docker-compose up -d`
2. Truy cập MinIO UI (`http://localhost:9001`), tạo bucket tên `warehouse`.
3. Khởi tạo mạng lưới volume dùng chung (`spark-events`) giữa Spark và Airflow để quản lý log.

### Giai đoạn 3: Ingestion (Bronze Layer)
* **Mục tiêu:** Ingest dữ liệu `nyc_taxi_data.parquet` nguyên bản vào bảng Iceberg.
* Giữ lại dấu vết kiểm toán (audit trail).

### Giai đoạn 4: Transformation (Silver Layer)
* **Mục tiêu:** Làm sạch và làm giàu dữ liệu.
* Lọc dữ liệu lỗi (ví dụ: `fare_amount < 0`), xử lý null, chuẩn hóa tiền tệ.
* Tạo cột phái sinh: `duration = dropoff_time - pickup_time`.
* Ghi vào `nessie.taxi.silver` và phân vùng (Partitioning) theo ngày.

### Giai đoạn 5: Aggregation (Gold Layer)
* **Mục tiêu:** Bảng tóm tắt phục vụ Business/Báo cáo.
* Tạo bảng doanh thu, quãng đường trung bình theo giờ, theo khu vực (ví dụ: `revenue_by_location`).

### Giai đoạn 6: Orchestration với Airflow
* Tạo Connection trên Airflow UI tới Spark Master (`spark://spark-master:7077`).
* Sử dụng `SparkSubmitOperator` trong DAG để chạy độc lập các script PySpark. 
  *(Lưu ý: Không dùng PythonOperator để chạy logic ETL nặng trực tiếp trên Airflow Worker).*

---

## 🌿 Tính năng Nâng cao (Nessie WAP Pattern)

Dự án áp dụng mô hình **Write-Audit-Publish (WAP)** thông qua Nessie Branching để đảm bảo chất lượng dữ liệu:

1. **Tạo nhánh cô lập:** `CREATE BRANCH etl_jan_2024 IN nessie`
2. **Ghi dữ liệu (ETL):** Spark ghi vào nhánh vừa tạo (không ảnh hưởng nhánh `main`).
3. **Kiểm định (Audit):** Chạy Data Quality Checks trên nhánh `etl_jan_2024`.
4. **Hợp nhất (Publish):** Khi dữ liệu "sạch", hợp nhất vào nhánh chính `MERGE BRANCH etl_jan_2024 INTO main IN nessie`.

**Du hành thời gian (Time Travel):**
Hệ thống cho phép truy vấn dữ liệu theo các thời điểm cụ thể trong quá khứ thông qua `AT TIMESTAMP` hoặc `AT SNAPSHOT`.

---

## 🧪 WAP Thực tế với Nessie (Production-Friendly)

Script `src/silver/ingest_silver.py` hiện hỗ trợ chạy theo từng bước độc lập để bám đúng mô hình orchestrator thực tế:

1. `write`: Tạo branch-per-run và ghi dữ liệu Silver lên branch đó.
2. `audit`: Chạy quality gates trên chính branch vừa ghi.
3. `publish`: Audit lại + merge branch vào `main` với retry/backoff.
4. `all`: Chạy full pipeline write → audit → publish (hợp cho local test nhanh).

### Chạy đúng môi trường (trong Spark container)

```bash
docker compose exec spark-master spark-submit /opt/bitnami/spark/src/silver/ingest_silver.py
```

### Chạy tách bước (khuyến nghị cho Airflow DAG)

```bash
# 1) WRITE - tạo branch theo run id
docker compose exec \
  -e WAP_MODE=write \
  -e WAP_RUN_ID=manual_20260307_0900 \
  spark-master \
  spark-submit /opt/bitnami/spark/src/silver/ingest_silver.py

# 2) AUDIT - kiểm định branch vừa ghi
docker compose exec \
  -e WAP_MODE=audit \
  -e NESSIE_WAP_BRANCH=etl_silver_manual_20260307_0900_20260307T090012Z_abc12345 \
  spark-master \
  spark-submit /opt/bitnami/spark/src/silver/ingest_silver.py

# 3) PUBLISH - merge có retry, sau đó drop branch đã publish
docker compose exec \
  -e WAP_MODE=publish \
  -e NESSIE_WAP_BRANCH=etl_silver_manual_20260307_0900_20260307T090012Z_abc12345 \
  -e WAP_MERGE_MAX_RETRIES=5 \
  spark-master \
  spark-submit /opt/bitnami/spark/src/silver/ingest_silver.py
```

### Biến môi trường quan trọng cho Nessie WAP

| Biến | Mặc định | Ý nghĩa |
| :--- | :--- | :--- |
| `WAP_MODE` | `all` | Chọn mode `write/audit/publish/all`. |
| `WAP_RUN_ID` | `adhoc` | Run ID nghiệp vụ (script tự append thêm UTC timestamp + UUID để luôn unique). |
| `WAP_BRANCH_PREFIX` | `etl_silver` | Prefix cho branch WAP phục vụ cleanup theo TTL. |
| `NESSIE_WAP_BRANCH` | auto-generated | Ép dùng branch cụ thể (audit/publish). |
| `WAP_MERGE_MAX_RETRIES` | `3` | Số lần retry khi `MERGE BRANCH` gặp conflict/lỗi tạm thời. |
| `WAP_MERGE_BASE_BACKOFF_SEC` | `5` | Backoff cơ bản (giây) giữa các lần retry merge. |
| `WAP_CLEANUP_STALE_BRANCHES` | `true` | Bật cleanup các branch cũ theo TTL. |
| `WAP_BRANCH_RETENTION_HOURS` | `72` | TTL cho branch cũ cùng prefix `etl_silver_*`. |
| `WAP_EXPIRE_SNAPSHOTS` | `false` | Bật gọi `expire_snapshots` sau khi publish thành công. |
| `WAP_SNAPSHOT_RETENTION_DAYS` | `14` | Số ngày giữ snapshot khi cleanup. |
| `SILVER_MIN_KEEP_RATIO` | `0.20` | Ngưỡng tỷ lệ dữ liệu giữ lại sau clean (`silver/bronze`). |

## 🧹 Vận hành và Tối ưu hóa (Maintenance)

Dữ liệu Iceberg là bất biến, do đó cần thực hiện bảo trì định kỳ qua các lệnh Spark:
* **Compaction (Nén tệp):** `CALL nessie.system.rewrite_data_files('db.table')`
* **Expire Snapshots (Xóa lịch sử cũ):** `CALL nessie.system.expire_snapshots(...)`
* **Remove Orphan Files (Dọn rác vật lý):** Dọn dẹp tệp trên MinIO không còn được dùng đến.

---

## ⚠️ Khắc phục sự cố (Troubleshooting)

* **Lỗi OOM (Out Of Memory):** Hệ thống này "ngốn" khá nhiều RAM. Hãy điều chỉnh tham số memory cho executor của Spark và cấu hình Docker Compose nếu bị crash.
* **Xung đột phiên bản JAR:** Là lỗi phổ biến nhất. Hãy kiểm tra kỹ *Compatibility Matrix* giữa Iceberg, Nessie và Spark.
* **Lỗi quyền truy cập log (Airflow/Spark):** Sử dụng dịch vụ `airflow-init` để chạy lệnh `chown -R` (UID 50000) đảm bảo user airflow có quyền ghi vào thư mục volume ánh xạ local.

---
*Tài liệu dựa trên chiến lược triển khai hệ thống Local Data Lakehouse (2026).*
