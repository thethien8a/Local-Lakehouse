# Dashboard 3: Airflow Orchestration

> Giám sát pipeline DAG — thành công/thất bại, scheduler health, pool capacity.

## Thông tin chung

- **Dashboard name:** Airflow Orchestration
- **Datasource:** Prometheus
- **Auto-refresh:** 30s
- **Time range mặc định:** Last 6 hours
- **Variables (dropdown filter):**
  - `dag_id`: regex `.*`, label_values từ `airflow_dagrun_duration_success_seconds`
  - `pool`: label_values từ `airflow_pool_open_slots`

## Panels

### Row 1: DAG Run Overview (Stat panels)

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 1 | Task Success Total | Stat | `airflow_ti_successes_total` | Tổng task thành công |
| 2 | Task Failure Total | Stat | `airflow_ti_failures_total` | Tổng task thất bại. Threshold: 0 = green, >0 = red |
| 3 | DAGBag Size | Stat | `airflow_dagbag_size` | Số DAG được load |
| 4 | Import Errors | Stat | `airflow_dag_processing_import_errors` | Lỗi khi parse DAG files. PHẢI = 0 |
| 5 | DAG Parse Time | Stat | `airflow_dag_processing_total_parse_time_seconds` | Tổng thời gian parse. >30s = DAG quá phức tạp |

### Row 2: DAG Run Duration

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 6 | DAG Duration (Success) | Time series | `airflow_dagrun_duration_success_seconds{dag_id=~"$dag_id"}` by `dag_id` | Thời gian chạy mỗi DAG. Tăng dần = performance regression |
| 7 | DAG Duration (Failed) | Time series | `airflow_dagrun_duration_failed_seconds{dag_id=~"$dag_id"}` by `dag_id` | Thời gian trước khi DAG fail |

### Row 3: Task Instance Metrics

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 8 | Task Finish by State | Time series (stacked) | `rate(airflow_ti_finish_total{dag_id=~"$dag_id"}[5m])` by `state` | Breakdown: success, failed, skipped, upstream_failed |
| 9 | Task Start Rate | Time series | `rate(airflow_ti_start_total{dag_id=~"$dag_id"}[5m])` by `dag_id, task_id` | Tần suất task được trigger |

### Row 4: Scheduler Health

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 10 | Scheduler Heartbeat | Time series | `rate(airflow_scheduler_heartbeat_total[5m])` | Nếu = 0 → scheduler chết. Alert ngay |
| 11 | Scheduler Loop Duration | Time series | `airflow_scheduler_loop_duration_seconds` | Mỗi vòng lặp mất bao lâu. >5s = scheduler quá tải |
| 12 | Critical Section Duration | Time series | `airflow_scheduler_critical_section_duration_seconds` | Lock time trong scheduler. Cao = bottleneck |
| 13 | Critical Section Query | Time series | `airflow_scheduler_critical_section_query_duration_seconds` | DB query time trong critical section |
| 14 | Tasks Running | Stat | `airflow_executor_running_tasks` | Tasks đang chạy |
| 15 | Tasks Starving | Stat | `airflow_scheduler_tasks_starving` | Tasks không có slot. >0 = cần tăng pool/executor |
| 16 | Orphaned Tasks Cleared | Time series | `rate(airflow_scheduler_orphaned_tasks_cleared_total[5m])` | Tasks bị mồ côi — thường do worker crash |

### Row 5: Executor & Pool Capacity

| # | Panel | Type | Query | Mô tả |
|---|-------|------|-------|-------|
| 17 | Executor Slots | Time series (stacked) | `airflow_executor_open_slots`, `airflow_executor_queued_tasks`, `airflow_executor_running_tasks` | 3 series stack: open (green), running (blue), queued (yellow) |% slot đã dùng mỗi pool |
| 19 | Pool Detail | Table | `airflow_pool_open_slots`, `airflow_pool_queued_slots`, `airflow_pool_running_slots`, `airflow_pool_deferred_slots` by `pool` | Bảng chi tiết từng pool |

## Alerts gợi ý
- **Scheduler heartbeat = 0 trong 2 phút:** Scheduler chết, cần restart
- **Import errors > 0:** DAG file bị lỗi syntax/import
- **Tasks starving > 0 trong 5 phút:** Không đủ slot, cần tăng pool size
- **Scheduler loop > 10s:** Scheduler quá tải, cần tối ưu DAG hoặc tăng tài nguyên
