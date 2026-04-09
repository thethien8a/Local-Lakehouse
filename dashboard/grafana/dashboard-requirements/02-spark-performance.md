# Dashboard 2: Apache Spark Performance

> Theo dõi chi tiết hiệu năng Spark Master/Worker — JVM, memory, GC, executor, shuffle.

## Thông tin chung

- **Dashboard name:** Spark Performance
- **Datasource:** Prometheus
- **Auto-refresh:** 15s
- **Time range mặc định:** Last 1 hour
- **Variables (dropdown filter):**
  - `job`: `spark-master`, `spark-worker` (multi-select)

## Panels

### Row 1: Cluster Status (Stat panels)

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 1 | Master Status | Stat | `up{job="spark-master"}` | 1 = green, 0 = red |
| 2 | Worker Status | Stat | `up{job="spark-worker"}` | 1 = green, 0 = red |
| 3 | Active Workers | Stat | `spark_master_workers` | Số worker connected |
| 4 | Active Apps | Stat | `spark_master_apps` | Spark apps đang chạy |
| 5 | Worker Cores Used | Gauge | `spark_worker_coresused / spark_worker_cores * 100` | % CPU cores đang dùng. >90% = cần thêm worker |
| 6 | Worker Memory Used | Gauge | `spark_worker_memused_mb / spark_worker_memtotal_mb * 100` | % RAM đang dùng. >85% = nguy hiểm |

### Row 2: JVM Memory

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 7 | JVM Heap Used (Master) | Time series | `jvm_memory_bytes_used{job="spark-master", area="heap"}` | Heap memory master over time |
| 8 | JVM Heap Used (Worker) | Time series | `jvm_memory_bytes_used{job="spark-worker", area="heap"}` | Heap memory worker over time |
| 9 | JVM Non-Heap (Master) | Time series | `jvm_memory_bytes_used{job="spark-master", area="nonheap"}` | Metaspace, code cache |
| 10 | JVM Non-Heap (Worker) | Time series | `jvm_memory_bytes_used{job="spark-worker", area="nonheap"}` | Metaspace, code cache |

### Row 3: Garbage Collection

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 11 | GC Pause Time Rate | Time series | `rate(jvm_gc_collection_seconds_sum{job=~"spark.*"}[5m])` by `gc, instance` | Thời gian GC pause/giây. Nếu >0.1s/s = memory pressure |
| 12 | GC Count Rate | Time series | `rate(jvm_gc_collection_seconds_count{job=~"spark.*"}[5m])` by `gc, instance` | Tần suất GC. Spike = allocation pressure |
| 13 | JVM Threads | Time series | `jvm_threads_current{job=~"spark.*"}` by `instance` | Số thread active. Tăng liên tục = thread leak |

### Row 4: Spark Application Metrics

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 14 | DAGScheduler Active Jobs | Time series | `spark_dag_scheduler_activejobs` | Jobs đang chạy trong DAGScheduler |
| 15 | DAGScheduler Active Stages | Time series | `spark_dag_scheduler_stages` | Stages đang active |
| 16 | Executor Total Tasks | Time series | `rate(spark_executor_totaltasks[5m])` | Task throughput — đơn vị: tasks/s |
| 17 | Executor Failed Tasks | Time series | `rate(spark_executor_failedtasks[5m])` | Task failures — phải gần 0 |

### Row 5: BlockManager & Shuffle

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 18 | BlockManager Memory Used | Time series | `spark_block_manager_memory_memused_mb` | Cache memory (RDD, DataFrame) |
| 19 | BlockManager Remaining | Time series | `spark_block_manager_memory_remainingmem_mb` | Bộ nhớ cache còn trống |
| 20 | Shuffle Read | Time series | `rate(spark_shuffle_shuffle_read_bytes[5m])` | Data shuffle đọc giữa executors |
| 21 | Shuffle Write | Time series | `rate(spark_shuffle_shuffle_write_bytes[5m])` | Data shuffle ghi giữa executors |

## Alerts gợi ý
- **GC pause > 2s/min:** JVM đang chật, cần tăng heap hoặc giảm workload
- **Worker memory > 90%:** Gần hết RAM, task mới có thể bị reject
- **Executor failed tasks > 0:** Có task lỗi, cần kiểm tra logs
