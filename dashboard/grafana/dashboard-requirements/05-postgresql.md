# Dashboard 5: PostgreSQL (Airflow Metadata DB)

> Giám sát database — connections, query performance, cache hit, table health.

## Thông tin chung

- **Dashboard name:** PostgreSQL
- **Datasource:** Prometheus
- **Auto-refresh:** 30s
- **Time range mặc định:** Last 3 hours
- **Metrics source:** postgres-exporter (port 9187)

## Panels

### Row 1: Database Health (Stat/Gauge panels)

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 1 | PostgreSQL Status | Stat | `up{job="postgresql"}` | 1 = green, 0 = red |
| 2 | Database Size | Stat | `pg_database_size_bytes{datname="airflow"}` | Kích thước DB (format: bytes → MB/GB) |
| 3 | Active Connections | Stat | `sum(pg_stat_activity_count{datname="airflow"})` | Tổng connections |
| 4 | Connection Usage % | Gauge | `sum(pg_stat_activity_count) / pg_settings_max_connections * 100` | Thresholds: <70% green, 70-85% yellow, >85% red |
| 5 | Cache Hit Ratio | Gauge | `pg_stat_database_blks_hit{datname="airflow"} / (pg_stat_database_blks_hit{datname="airflow"} + pg_stat_database_blks_read{datname="airflow"}) * 100` | PHẢI >99%. <95% = cần tăng shared_buffers |

### Row 2: Connection Details

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 6 | Connections by State | Time series (stacked) | `pg_stat_activity_count{datname="airflow"}` by `state` | Breakdown: active, idle, idle in transaction, disabled |
| 7 | Max Connections | Stat | `pg_settings_max_connections` | Giới hạn max_connections đang set |

### Row 3: Transaction Throughput

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 8 | Commits/s | Time series | `rate(pg_stat_database_xact_commit{datname="airflow"}[5m])` | Transaction commit rate |
| 9 | Rollbacks/s | Time series | `rate(pg_stat_database_xact_rollback{datname="airflow"}[5m])` | Rollback rate — tăng = có lỗi trong Airflow |
| 10 | Commit vs Rollback | Time series (stacked) | Cả 2 query trên gộp lại | So sánh trực quan |

### Row 4: Query Performance

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 11 | Rows Fetched/s | Time series | `rate(pg_stat_database_tup_fetched{datname="airflow"}[5m])` | Rows thực sự trả về cho client |
| 12 | Rows Returned/s | Time series | `rate(pg_stat_database_tup_returned{datname="airflow"}[5m])` | Rows DB quét qua. Nếu >> fetched = thiếu index |
| 13 | Fetch Efficiency | Time series | `rate(pg_stat_database_tup_fetched[5m]) / rate(pg_stat_database_tup_returned[5m]) * 100` | % rows hữu ích. Thấp = full table scan, cần thêm index |

### Row 5: Write Operations

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 14 | Rows Inserted/s | Time series | `rate(pg_stat_database_tup_inserted{datname="airflow"}[5m])` | INSERT rate |
| 15 | Rows Updated/s | Time series | `rate(pg_stat_database_tup_updated{datname="airflow"}[5m])` | UPDATE rate — Airflow update task state nhiều |
| 16 | Rows Deleted/s | Time series | `rate(pg_stat_database_tup_deleted{datname="airflow"}[5m])` | DELETE rate |

### Row 6: Table Health

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 17 | Dead Tuples (Top 10) | Bar gauge | `topk(10, pg_stat_user_tables_n_dead_tup)` by `relname` | Bảng nào có nhiều dead rows nhất. Nhiều = cần VACUUM |
| 18 | Live vs Dead Tuples | Table | `pg_stat_user_tables_n_live_tup`, `pg_stat_user_tables_n_dead_tup` by `relname` | Bảng chi tiết live/dead ratio |
| 19 | Last Vacuum | Table | `pg_stat_user_tables_last_vacuum` hoặc `pg_stat_user_tables_last_autovacuum` by `relname` | Lần VACUUM cuối mỗi bảng |

### Row 7: Locks & Long Queries

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 20 | Locks by Mode | Time series | `pg_locks_count` by `mode` | AccessShareLock, RowExclusiveLock, ExclusiveLock, ... |
| 21 | Longest Running Query | Stat | `pg_stat_activity_max_tx_duration_seconds{datname="airflow"}` | Query chạy lâu nhất. >60s = có thể cần kill |

## Alerts gợi ý
- **Connection usage > 85%:** Gần hết connections, tăng max_connections hoặc dùng pgbouncer
- **Cache hit ratio < 95%:** Quá nhiều disk read, tăng shared_buffers
- **Rollback rate tăng đột biến:** Airflow đang gặp lỗi hàng loạt
- **Dead tuples > 10000 trên 1 bảng:** Cần chạy VACUUM ANALYZE
- **Longest query > 300s:** Query bị treo, cần kiểm tra và kill
