# Dashboard 1: Lakehouse Overview

> Cái nhìn tổng quan toàn bộ stack — mở lên là biết ngay hệ thống khỏe hay đang có vấn đề.

## Thông tin chung

- **Dashboard name:** Lakehouse Overview
- **Datasource:** Prometheus
- **Auto-refresh:** 30s
- **Time range mặc định:** Last 1 hour

## Panels

### Row 1: Service Health (Stat panels — Green/Red)

| # | Panel | Type | Query | Thresholds |
|---|-------|------|-------|------------|
| 1 | Spark Master | Stat | `up{job="spark-master"}` | 1 = green, 0 = red |
| 2 | Spark Worker | Stat | `up{job="spark-worker"}` | 1 = green, 0 = red |
| 3 | Airflow | Stat | `up{job="airflow"}` | 1 = green, 0 = red |
| 4 | MinIO | Stat | `up{job="minio"}` | 1 = green, 0 = red |
| 5 | PostgreSQL | Stat | `up{job="postgresql"}` | 1 = green, 0 = red |
| 6 | Prometheus | Stat | `up{job="prometheus"}` | 1 = green, 0 = red |

### Row 2: Key Metrics (Stat panels)

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 7 | Airflow Task Success Rate | Gauge | `sum(airflow_ti_successes_total) / (sum(airflow_ti_successes_total) + sum(airflow_ti_failures_total)) * 100` | % task thành công. Thresholds: >95% green, 80-95% yellow, <80% red |
| 8 | DAG Import Errors | Stat | `airflow_dag_processing_import_errors` | Phải = 0. Thresholds: 0 = green, >0 = red |
| 9 | Spark Active Workers | Stat | `spark_master_workers` | Số worker connected đến master |
| 10 | Spark Active Apps | Stat | `spark_master_apps` | Số Spark application đang chạy |
| 11 | MinIO Disk Usage % | Gauge | `(1 - minio_cluster_capacity_usable_free_bytes / minio_cluster_capacity_usable_total_bytes) * 100` | Thresholds: <70% green, 70-85% yellow, >85% red |
| 12 | PostgreSQL Connections | Stat | `sum(pg_stat_activity_count{datname="airflow"})` | Active connections đến Airflow DB |

### Row 3: Time Series Overview

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 13 | Airflow Task Throughput | Time series | `rate(airflow_ti_successes_total[5m])` + `rate(airflow_ti_failures_total[5m])` | 2 series: success (green) và failed (red) |
| 14 | Spark JVM Heap (All nodes) | Time series | `jvm_memory_used_bytes{area="heap", job=~"spark.*"}` | Memory pressure tổng quan |
| 15 | Request/Second to MinIO | Time series | `sum(rate(minio_s3_requests_total[5m]))` | Throughput API calls |
| 16 | PostgreSQL Transactions/s | Time series | `rate(pg_stat_database_xact_commit{datname="airflow"}[5m])` | DB throughput |

## Ghi chú
- Dashboard này dùng để xem nhanh, mỗi panel chỉ hiển thị metric quan trọng nhất
- Click vào từng service sẽ link sang dashboard chi tiết tương ứng (dùng Grafana Data Links)
