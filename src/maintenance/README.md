# Scripts Bảo trì Lakehouse

Tập hợp các script để thực hiện bảo trì định kỳ cho hệ thống Lakehouse, bao gồm:

- Compaction: Gộp các file nhỏ thành file lớn để tăng hiệu năng đọc
- Expire Snapshots: Xóa các snapshot cũ để giải phóng dung lượng
- Remove Orphan Files: Xóa các file không còn được tham chiếu

## Danh sách Scripts

### 1. `compact_tables.py`
Thực hiện compaction cho các bảng Iceberg.

**Usage:**
```bash
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/maintenance/compact_tables.py \
  --tables nessie.taxi.silver nessie.taxi.gold_revenue_by_hour
```

### 2. `expire_snapshots.py`
Xóa các snapshot cũ hơn số ngày quy định.

**Usage:**
```bash
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/maintenance/expire_snapshots.py \
  --tables nessie.taxi.silver nessie.taxi.gold_revenue_by_hour \
  --days-to-keep 30 \
  --retain-last 10
```

### 3. `remove_orphan_files.py`
Xóa các file không còn được tham chiếu bởi bất kỳ snapshot nào.

**Usage:**
```bash
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/maintenance/remove_orphan_files.py \
  --tables nessie.taxi.silver nessie.taxi.gold_revenue_by_hour
```

### 4. `run_maintenance.py`
Script tổng hợp thực hiện toàn bộ quy trình bảo trì.

**Usage:**
```bash
# Chế độ hàng tuần (chỉ compaction)
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/maintenance/run_maintenance.py \
  --mode weekly \
  --tables nessie.taxi.silver nessie.taxi.gold_revenue_by_hour

# Chế độ hàng tháng (expire + remove orphan)
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/maintenance/run_maintenance.py \
  --mode monthly \
  --tables nessie.taxi.silver nessie.taxi.gold_revenue_by_hour \
  --days-to-keep 30 \
  --retain-last 10

# Chế độ đầy đủ (tất cả các bước)
docker compose exec spark-master spark-submit \
  /opt/bitnami/spark/src/maintenance/run_maintenance.py \
  --mode full \
  --tables nessie.taxi.silver nessie.taxi.gold_revenue_by_hour \
  --days-to-keep 30 \
  --retain-last 10
```

## Lịch trình khuyến nghị

| Tác vụ | Tần suất | Thời điểm | Thứ tự |
|--------|----------|-----------|--------|
| Compaction | Hàng tuần | Chủ nhật đêm (2:00 AM) | 1 |
| Expire Snapshots | Hàng tháng | Cuối tháng (3:00 AM) | 2 |
| Remove Orphan Files | Hàng tháng | Sau expire (4:00 AM) | 3 |

## Crontab Example

Để tự động hóa bảo trì, bạn có thể thêm các dòng sau vào crontab:

```bash
# Hàng tuần: Chủ nhật lúc 2:00 AM - Compaction
0 2 * * 0 cd /opt/bitnami/spark && docker compose exec spark-master spark-submit /opt/bitnami/spark/src/maintenance/run_maintenance.py --mode weekly --tables nessie.taxi.silver nessie.taxi.gold_revenue_by_hour nessie.taxi.gold_revenue_by_location nessie.taxi.gold_fare_by_date nessie.taxi.gold_od_pairs nessie.taxi.gold_efficiency_by_timebucket nessie.taxi.gold_tip_behavior nessie.taxi.gold_traffic_by_zone nessie.taxi.gold_revenue_weekday_weekend

# Hàng tháng: Ngày 1 hàng tháng lúc 3:00 AM - Expire snapshots và Remove orphan files
0 3 1 * * cd /opt/bitnami/spark && docker compose exec spark-master spark-submit /opt/bitnami/spark/src/maintenance/run_maintenance.py --mode monthly --tables nessie.taxi.silver nessie.taxi.gold_revenue_by_hour nessie.taxi.gold_revenue_by_location nessie.taxi.gold_fare_by_date nessie.taxi.gold_od_pairs nessie.taxi.gold_efficiency_by_timebucket nessie.taxi.gold_tip_behavior nessie.taxi.gold_traffic_by_zone nessie.taxi.gold_revenue_weekday_weekend --days-to-keep 30 --retain-last 10
```

## Lưu ý quan trọng

1. **Backup trước khi expire**: Nếu bạn cần time travel, hãy backup trước
2. **Chạy ngoài giờ cao điểm**: Bảo trì tốn CPU/IO, nên chạy đêm
3. **Theo dõi logs**: Kiểm tra logs để đảm bảo không có lỗi
4. **Test trước**: Chạy trên môi trường test trước khi chạy production