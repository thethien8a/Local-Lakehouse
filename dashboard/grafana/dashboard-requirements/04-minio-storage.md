# Dashboard 4: MinIO Object Storage

> Giám sát S3-compatible storage — dung lượng, throughput, API latency.

## Thông tin chung

- **Dashboard name:** MinIO Storage
- **Datasource:** Prometheus
- **Auto-refresh:** 30s
- **Time range mặc định:** Last 3 hours
- **Variables (dropdown filter):**
  - `bucket`: label_values từ `minio_bucket_usage_total_bytes`
  - `api`: label_values từ `minio_s3_requests_total` (GetObject, PutObject, ListObjects, ...)

## Panels

### Row 1: Cluster Capacity (Stat/Gauge panels)

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 1 | Total Capacity | Stat | `minio_cluster_capacity_usable_total_bytes` | Tổng dung lượng (format: bytes → GB/TB) |
| 2 | Used Capacity | Stat | `minio_cluster_capacity_usable_total_bytes - minio_cluster_capacity_usable_free_bytes` | Đã dùng bao nhiêu |
| 3 | Free Capacity | Stat | `minio_cluster_capacity_usable_free_bytes` | Còn trống bao nhiêu |
| 4 | Disk Usage % | Gauge | `(1 - minio_cluster_capacity_usable_free_bytes / minio_cluster_capacity_usable_total_bytes) * 100` | Thresholds: <70% green, 70-85% yellow, >85% red |
| 5 | MinIO Uptime | Stat | `time() - minio_node_process_starttime_seconds` | Thời gian uptime (format: duration) |

### Row 2: Bucket Usage

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 6 | Bucket Size | Bar gauge | `minio_bucket_usage_total_bytes` by `bucket` | Dung lượng mỗi bucket (warehouse, ...) |
| 7 | Object Count | Bar gauge | `minio_bucket_usage_object_total` by `bucket` | Số object mỗi bucket |
| 8 | Bucket Size Over Time | Time series | `minio_bucket_usage_total_bytes{bucket=~"$bucket"}` by `bucket` | Trend tăng trưởng dung lượng |

### Row 3: S3 API Performance

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 9 | Total Request Rate | Time series | `sum(rate(minio_s3_requests_total[5m]))` | Tổng API calls/s |
| 10 | Request Rate by API | Time series | `rate(minio_s3_requests_total{api=~"$api"}[5m])` by `api` | Breakdown theo API: Get, Put, List, Delete, Head |
| 11 | Error Rate | Time series | `rate(minio_s3_requests_errors_total[5m])` by `api` | Lỗi API. Phải gần 0 |
| 12 | Error Rate % | Time series | `rate(minio_s3_requests_errors_total[5m]) / rate(minio_s3_requests_total[5m]) * 100` | % request bị lỗi. >1% = có vấn đề |

### Row 4: Latency

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 13 | API Latency p50 | Time series | `histogram_quantile(0.50, rate(minio_s3_requests_duration_seconds_bucket[5m]))` by `api` | Median latency |
| 14 | API Latency p95 | Time series | `histogram_quantile(0.95, rate(minio_s3_requests_duration_seconds_bucket[5m]))` by `api` | p95 — 95% requests nhanh hơn giá trị này |
| 15 | API Latency p99 | Time series | `histogram_quantile(0.99, rate(minio_s3_requests_duration_seconds_bucket[5m]))` by `api` | p99 — tail latency, ảnh hưởng UX |

### Row 5: Network Traffic

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 16 | Data Sent (TX) | Time series | `rate(minio_s3_traffic_sent_bytes_total[5m])` | Bandwidth ra (GetObject, downloads) |
| 17 | Data Received (RX) | Time series | `rate(minio_s3_traffic_received_bytes_total[5m])` | Bandwidth vào (PutObject, uploads) |
| 18 | Total Data Transferred | Stat | `minio_s3_traffic_sent_bytes_total + minio_s3_traffic_received_bytes_total` | Tổng data đã truyền từ khi khởi động |

## Alerts gợi ý
- **Disk usage > 85%:** Sắp hết dung lượng, cần cleanup hoặc mở rộng
- **Error rate > 1%:** API lỗi bất thường, kiểm tra logs MinIO
- **p99 latency > 5s:** Storage chậm, có thể do disk I/O hoặc network
