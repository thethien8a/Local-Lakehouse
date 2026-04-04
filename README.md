# Local Data Lakehouse 

Dự án triển khai hệ thống **Data Lakehouse** chạy local theo kiến trúc **Medallion**, sử dụng bộ công nghệ hiện đại để xử lý và phân tích tập dữ liệu NYC Taxi.

---

## Kiến trúc Hệ thống
- **Storage**: MinIO (S3-compatible) lưu trữ Parquet files.
- **Table Format**: Apache Iceberg (ACID, Schema Evolution).
- **Catalog & Versioning**: Project Nessie (Git-for-Data, WAP pattern).
- **Compute**: Apache Spark 3.5.1 (PySpark).
- **Orchestration**: Apache Airflow 2.10.3.
- **Monitoring**: Prometheus & Grafana (Planned) - Giám sát tài nguyên và hiệu suất pipeline.
- **Query & BI**: Trino (SQL Engine) & Metabase (Dashboard).

---

## Cấu trúc Medallion
- **Bronze**: Dữ liệu thô từ Landing Zone (`ingest_bronze.py`).
- **Silver**: Làm sạch, chuẩn hóa và Feature Engineering với WAP pattern (`ingest_silver.py`).
- **Gold**: 3 bảng dim và 1 bảng fact phục vụ phân tích, kết hợp thêm 1 bảng agg để tối ưu hiệu năng truy vấn theo ngày.

---

## Cài đặt nhanh

### 1. Chuẩn bị môi trường
```bash
# Tải JAR dependencies cho Spark
python src/utils/download_jars.py

# Tải bộ dữ liệu NYC Taxi mẫu
python src/utils/download_data.py

# Khởi động toàn bộ services (Docker)
docker-compose up -d
```

### 2. Khởi tạo dữ liệu
```bash
# Chia nhỏ dữ liệu theo ngày vào Landing Zone
docker compose exec spark-master spark-submit /opt/bitnami/spark/src/utils/split_data_by_day.py
```

### 3. Chạy Pipeline (Manual)
```bash
# Chạy Bronze -> Silver -> Gold cho một ngày cụ thể
docker compose exec spark-master spark-submit /opt/bitnami/spark/src/pipeline/bronze/ingest_bronze.py --start 2024-01-15
docker compose exec spark-master spark-submit /opt/bitnami/spark/src/pipeline/silver/ingest_silver.py --start 2024-01-15
docker compose exec spark-master spark-submit /opt/bitnami/spark/src/pipeline/gold/ingest_gold.py --start 2024-01-15
```

---

## Giao diện quản lý
| Dịch vụ | URL | Credentials |
| :--- | :--- | :--- |
| **Airflow** | http://localhost:8080 | `airflow` / `airflow123` |
| **MinIO** | http://localhost:9001 | `admin` / `password` |
| **Spark Master** | http://localhost:8082 | - |
| **Metabase** | http://localhost:3000 | (Setup khi chạy lần đầu) |
| **Nessie API** | http://localhost:19120 | - |
| **Grafana** | http://localhost:3001 | (Coming Soon - Chưa cấu hình) |
| **Prometheus** | http://localhost:9090 | (Coming Soon - Chưa cấu hình) |

---

## Tính năng nổi bật
- **WAP (Write-Audit-Publish)**: Sử dụng Nessie branching để kiểm soát chất lượng dữ liệu trước khi merge vào `main`.
- **Maintenance**: Tự động dọn dẹp snapshot, nén file (`run_maintenance.py`).
- **Orchestration**: Toàn bộ quy trình được tự động hóa qua Airflow DAGs.

---
*Cập nhật: 04/2026*
