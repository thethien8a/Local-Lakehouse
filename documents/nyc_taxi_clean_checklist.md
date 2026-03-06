# NYC Taxi Data Prep + Data Quality Checklist (Data Engineer)

Checklist này chốt theo scope của project:

- Mục tiêu chính: **Data Quality**
- Phạm vi thời gian: **từ 2024-01-01 trở đi**
- Đầu ra mong muốn: **dữ liệu sạch + bộ feature hữu ích cho phân tích**

## 1) Scope Và Acceptance Criteria

- [ ] Lọc dữ liệu với điều kiện `tpep_pickup_datetime >= '2024-01-01'`.
- [ ] Khóa rõ input dataset version (tránh chạy lại ra kết quả khác).
- [ ] Định nghĩa "clean" trước khi xử lý:
  - [ ] Không duplicate theo khóa business.
  - [ ] Không bản ghi thời gian âm.
  - [ ] Không giá trị tiền/quãng đường vô lý.
  - [ ] Có rule xử lý null rõ ràng theo từng cột.

## 2) Data Readiness (Chuẩn Bị Dữ Liệu)

- [ ] Xác định đường dẫn nguồn (raw parquet/csv) và kiểm tra file tồn tại.
- [ ] Kiểm tra schema drift giữa các file/tháng trước khi union.
- [ ] Chuẩn hóa timezone cho pickup/dropoff nếu nguồn không đồng nhất.
- [ ] Chốt data contract input (tên cột, type, mandatory columns).
- [ ] Tạo bảng metadata ingest: `source_file`, `ingest_ts`, `row_count_raw`.

## 3) Data Profiling Ban Đầu

- [ ] Tổng số record sau khi filter từ 2024-01-01.
- [ ] Distinct count cho các cột mã (`VendorID`, `RatecodeID`, `payment_type`).
- [ ] Null count và null ratio cho toàn bộ cột.
- [ ] Min/Max/Percentile cho các cột numeric chính.
- [ ] Min/Max datetime và timezone consistency.

## 4) Schema Và Type Validation

- [ ] `tpep_pickup_datetime`, `tpep_dropoff_datetime` đúng kiểu datetime.
- [ ] Cột numeric cast về kiểu phù hợp (`passenger_count`, `trip_distance`, các cột amount).
- [ ] Cột mã cast kiểu integer/categorical nhất quán.
- [ ] Chuẩn hóa tên cột và thứ tự cột theo chuẩn pipeline.

## 5) Rule Kiểm Tra Chất Lượng (Hard Rules)

- [ ] `tpep_dropoff_datetime >= tpep_pickup_datetime`.
- [ ] `trip_distance > 0`.
- [ ] `fare_amount >= 0`.
- [ ] `total_amount >= 0`.
- [ ] `passenger_count > 0`.
- [ ] `VendorID` thuộc tập mã hợp lệ.
- [ ] `payment_type` thuộc tập mã hợp lệ.

## 6) Rule Kiểm Tra Chất Lượng (Soft Rules)

- [ ] `trip_duration_min` không vượt ngưỡng quá bất thường (ví dụ p99.9 hoặc ngưỡng business).
- [ ] `avg_speed_mph` nằm trong dải hợp lý (lọc extreme outlier).
- [ ] `tip_amount` hợp lý theo `fare_amount` (flag nếu quá cao bất thường).
- [ ] `total_amount` gần bằng tổng thành phần phí (cho phép sai số nhỏ do làm tròn).

## 7) Duplicate Và Primary Key Strategy

- [ ] Chọn khóa dedup rõ ràng, ví dụ:
  - [ ] `VendorID`
  - [ ] `tpep_pickup_datetime`
  - [ ] `tpep_dropoff_datetime`
  - [ ] `PULocationID`
  - [ ] `DOLocationID`
  - [ ] `trip_distance`
  - [ ] `fare_amount`
- [ ] Đếm số duplicate trước và sau dedup.
- [ ] Lưu lại số bản ghi bị loại do duplicate để audit.

## 8) Missing Data Handling

- [ ] Xác định cột critical (nếu null thì loại record).
- [ ] Xác định cột cho phép null (giữ nguyên hoặc fill theo rule).
- [ ] Tài liệu hóa rõ từng rule fill/drop.
- [ ] Đo tác động của việc drop/fill lên quy mô dữ liệu.

## 9) Outlier Handling

- [ ] Áp dụng IQR hoặc percentile clipping cho `trip_distance`, `duration`, `fare_amount`, `total_amount`.
- [ ] Không xóa outlier "mù"; phải gán nhãn lý do loại/giữ.
- [ ] So sánh thống kê trước và sau xử lý outlier.

## 10) Feature Engineering (Phục Vụ Phân Tích)

- [ ] Tạo feature thời gian:
  - [ ] `pickup_date`
  - [ ] `pickup_hour`
  - [ ] `pickup_weekday`
  - [ ] `is_weekend`
  - [ ] `time_bucket` (early_morning/morning/afternoon/evening/night)
- [ ] Tạo feature vận hành:
  - [ ] `trip_duration_min`
  - [ ] `avg_speed_mph`
  - [ ] `is_airport_trip` (dựa trên zone/airport fee)
  - [ ] `is_rush_hour`
- [ ] Tạo feature doanh thu/phí:
  - [ ] `tip_pct`
  - [ ] `base_plus_surcharge` (tổng các phí trừ tip)
  - [ ] `fare_per_mile`
  - [ ] `fare_per_min`
- [ ] Tạo feature địa lý:
  - [ ] map `PULocationID` -> `pu_zone`, `pu_borough`
  - [ ] map `DOLocationID` -> `do_zone`, `do_borough`
  - [ ] `od_pair` = `pu_zone || '->' || do_zone`

## 11) Data Quality Report (Bắt Buộc)

- [ ] Tạo bảng rule results: `rule_name`, `failed_rows`, `failed_ratio`.
- [ ] Tạo bảng reconciliation:
  - [ ] Input rows
  - [ ] Rows removed by time filter
  - [ ] Rows removed by hard rules
  - [ ] Rows removed by dedup
  - [ ] Final clean rows
- [ ] Ghi nhận các warning còn tồn tại (soft rule violations).

## 12) Output Dataset Contract (Dữ Liệu Sạch + Feature-Ready)

- [ ] Output path/table được cố định (ví dụ `silver/nyc_taxi_clean`).
- [ ] Partition theo `pickup_date`.
- [ ] Đảm bảo schema ổn định cho downstream jobs.
- [ ] Bổ sung metadata cột:
  - [ ] `ingest_ts`
  - [ ] `processing_ts`
  - [ ] `dq_pass_flag`
  - [ ] `dq_issue_codes` (nếu cần lưu lỗi mềm)

## 13) Validation Trước Khi Chốt

- [ ] Re-run toàn bộ notebook từ đầu để đảm bảo reproducible.
- [ ] Kiểm tra lại sample records ở cả đầu và cuối pipeline.
- [ ] So sánh số lượng record với kỳ vọng business (không tụt bất thường).
- [ ] Export 1 file summary markdown/csv để team review nhanh.

## 14) Definition Of Done

- [ ] Có bảng/partition clean cho dữ liệu từ 2024-01-01 trở đi.
- [ ] Có data quality report kèm tỷ lệ lỗi theo rule.
- [ ] Có log/audit trail cho số record bị loại theo từng bước.
- [ ] Có bộ feature tối thiểu để analyst có thể vào phân tích ngay.
- [ ] Downstream đọc được dữ liệu mà không cần sửa schema hoặc fix null thêm.
