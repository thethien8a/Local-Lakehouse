# NYC Taxi Data Lakehouse

> Hệ thống Data Lakehouse end-to-end xử lý dữ liệu chuyến taxi vàng New York City sử dụng kiến trúc Medallion (Bronze → Silver → Gold).

## 📖 Documentation Website

👉 **[Click vào đây để xem toàn bộ hệ thống](https://thethien8a.github.io/Local-Lakehouse/)**

Website documentation được deploy trên GitHub Pages, mô tả chi tiết:
- Kiến trúc Medallion (Bronze → Silver → Gold)
- Data Pipeline & Feature Engineering
- Star Schema Design & Dashboard Mapping
- Airflow DAGs & Orchestration
- Prometheus + Grafana Monitoring
- Docker Services & Ports
- Iceberg Table Maintenance
- Quick Start Guide

## 🏗️ Tech Stack

| Component | Technology |
|-----------|-----------|
| Table Format | Apache Iceberg |
| Processing | Apache Spark 3.5 |
| Orchestration | Apache Airflow 2.10 |
| Catalog | Project Nessie |
| Storage | MinIO (S3-compatible) |
| Query Engine | Trino |
| BI Dashboard | Metabase |
| Monitoring | Prometheus + Grafana |
| Database | PostgreSQL 15 |

## 🚀 Quick Start

```bash
# 1. Clone repo
git clone https://github.com/thethien8a/Local-Lakehouse.git
cd Local-Lakehouse

# 2. Cấu hình môi trường
cp .env.example .env
# Điền credentials vào .env

# 3. Tải dữ liệu NYC Taxi và các JAR dependencies
python -m src.utils.download_data
python -m src.utils.download_jars

# 4. Khởi chạy
docker compose up -d

# 5. Chia dữ liệu theo ngày (chạy trong Spark container)
docker exec -it spark-master spark-submit /opt/bitnami/spark/src/utils/split_data_by_day.py

# 6. Trigger ETL pipeline
# Vào Airflow UI: http://localhost:8080
# Trigger DAG bronze_ingestion với params date_from, date_to
```

## 📁 Cấu trúc

```
lakehouse/
├── conf/              # Cấu hình services (Spark, Prometheus, Grafana, Trino...)
├── dashboard/         # Grafana dashboard JSON + Metabase docs
├── data/              # Dữ liệu thô + landing zone
├── docker/            # Custom Dockerfiles
├── src/
│   ├── airflow/dags/  # Airflow DAGs (5 DAGs)
│   ├── pipeline/      # ETL code (bronze/, silver/, gold/)
│   ├── maintenance/   # Iceberg maintenance scripts
│   ├── utils/         # Utility functions
│   └── config/        # Path config
├── docs/              # Website documentation (GitHub Pages)
├── index.html         # Documentation entry point
└── docker-compose.yml # 13 services orchestration
```

## 🌐 Service Ports

| Port | Service |
|------|---------|
| 8080 | Airflow Web UI |
| 8082 | Spark Master UI |
| 8083 | Trino UI |
| 3000 | Metabase |
| 3001 | Grafana |
| 9000 | MinIO API |
| 9001 | MinIO Console |
| 9090 | Prometheus |
| 19120 | Nessie API |

## 📄 License

This project is for educational purposes.
