# Star Schema Design - NYC Taxi Data Lakehouse

> Tài liệu này mô tả kiến trúc Star Schema cho hệ thống Data Lakehouse NYC Taxi.

---

## 📊 Tổng quan Kiến trúc

```
┌─────────────────────────────────────────────────────────────────┐
│                        FACT TABLE                                │
│                    nessie.taxi.fact_trips                        │
├─────────────────────────────────────────────────────────────────┤
│  trip_id (PK)  │  pickup_date  │  pickup_hour  │  pickup_weekday │
│  pulocation_id │  dolocation_id│  payment_type  │  trip_distance  │
│  fare_amount   │  total_amount │  tip_amount    │  trip_duration  │
│  ...           │               │               │                 │
└─────────────────────────────────────────────────────────────────┘
         │                    │                    │
         │                    │                    │
         ▼                    ▼                    ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  dim_zone    │    │ dim_payment  │    │ dim_time     │
│  (Location)  │    │ (Payment)    │    │ (Time)       │
└──────────────┘    └──────────────┘    └──────────────┘
```

---

## 🎯 Fact Table

### `nessie.taxi.fact_trips`

**Mô tả:** Bảng sự kiện chính - mỗi dòng đại diện cho một chuyến taxi

**Partition:** `pickup_date`

**Primary Key:** `trip_id` (MD5 hash)

| Column | Data Type | Description | Source |
|--------|-----------|-------------|--------|
| `trip_id` | STRING | Unique identifier cho mỗi chuyến đi (MD5 hash) | Generated |
| `pickup_date` | DATE | Ngày đón khách | Silver |
| `pickup_hour` | INT | Giờ đón khách (0-23) | Silver |
| `pickup_weekday` | INT | Thứ trong tuần (1=Sunday, 7=Saturday) | Silver |
| `time_bucket` | STRING | Bucket theo khung giờ (Morning/Afternoon/Evening/Night) | Silver |
| `is_weekend` | BOOLEAN | Có phải cuối tuần không | Silver |
| `pulocation_id` | INT | Mã khu vực đón khách (FK → dim_zone) | Silver |
| `dolocation_id` | INT | Mã khu vực trả khách (FK → dim_zone) | Silver |
| `payment_type` | INT | Loại thanh toán (FK → dim_payment) | Silver |
| `trip_distance` | DOUBLE | Quãng đường (dặm) | Silver |
| `fare_amount` | DOUBLE | Cước phí chính | Silver |
| `total_amount` | DOUBLE | Tổng tiền | Silver |
| `tip_amount` | DOUBLE | Tiền boa | Silver |
| `trip_duration_min` | DOUBLE | Thời gian chuyến đi (phút) | Silver |

**Grain:** Một dòng = một chuyến taxi

---

## 📐 Dimension Tables

### 1. `nessie.taxi.dim_zone`

**Mô tả:** Bảng lookup thông tin khu vực taxi zone của NYC

**Source:** `data/taxi_zone_lookup.csv`

| Column | Data Type | Description | Key |
|--------|-----------|-------------|-----|
| `LocationID` | INT | Mã khu vực (PK) | PK |
| `Borough` | STRING | Quận (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR) | - |
| `Zone` | STRING | Tên khu vực cụ thể | - |
| `service_zone` | STRING | Vùng dịch vụ (Yellow Zone, Green Zone, Boro Zone, Airport) | - |

**Relationship:**
- `fact_trips.pulocation_id` → `dim_zone.LocationID`
- `fact_trips.dolocation_id` → `dim_zone.LocationID`

**Sử dụng cho:**
- Top 10 pickup locations bar chart
- OD pairs analysis
- Traffic by zone analysis

---

### 2. `nessie.taxi.dim_payment`

**Mô tả:** Bảng lookup phương thức thanh toán

**Source:** Hardcoded trong [`create_dim_tables.py`](../src/gold/create_dim_tables.py:28-36)

| Column | Data Type | Description | Key |
|--------|-----------|-------------|-----|
| `payment_type` | INT | Mã loại thanh toán (PK) | PK |
| `payment_name` | STRING | Tên phương thức thanh toán | - |

**Values:**
| payment_type | payment_name |
|--------------|--------------|
| 0 | Flex Fare trip |
| 1 | Credit card |
| 2 | Cash |
| 3 | No charge |
| 4 | Dispute |
| 5 | Unknown |
| 6 | Voided trip |

**Relationship:**
- `fact_trips.payment_type` → `dim_payment.payment_type`

**Sử dụng cho:**
- Payment type donut/pie chart
- Tipping behavior analysis

---

### 3. `nessie.taxi.dim_time` (Implicit)

**Mô tả:** Thông tin thời gian được lưu trực tiếp trong fact table

**Columns trong fact_trips:**
- `pickup_date` - Ngày
- `pickup_hour` - Giờ (0-23)
- `pickup_weekday` - Thứ (1-7)
- `time_bucket` - Bucket (Morning/Afternoon/Evening/Night)
- `is_weekend` - Có phải cuối tuần

**Sử dụng cho:**
- Heatmap (trips theo giờ & thứ)
- Line chart (trips theo ngày)
- Weekday vs Weekend analysis
- Rush hour analysis

---

## 📈 Aggregate Tables

### `nessie.taxi.agg_daily_summary`

**Mô tả:** Bảng tổng hợp KPI theo ngày - giảm scan khi load dashboard

**Partition:** `pickup_date`

| Column | Data Type | Description | Formula |
|--------|-----------|-------------|---------|
| `pickup_date` | DATE | Ngày đón khách | - |
| `total_trips` | BIGINT | Tổng số chuyến | COUNT(*) |
| `total_revenue` | DOUBLE | Tổng doanh thu | SUM(total_amount) |
| `avg_revenue_per_trip` | DOUBLE | Doanh thu trung bình/trip | AVG(total_amount) |
| `avg_distance_per_trip` | DOUBLE | Quãng đường trung bình/trip | AVG(trip_distance) |
| `avg_tip` | DOUBLE | Tiền boa trung bình | AVG(tip_amount) |
| `total_fare` | DOUBLE | Tổng cước phí | SUM(fare_amount) |

**Sử dụng cho:**
- KPI tổng quan (Big Numbers)
- Line chart (trips theo ngày)
- Daily trend analysis

---

## 🔗 Relationships

```
┌─────────────────────────────────────────────────────────────────┐
│                    nessie.taxi.fact_trips                        │
├─────────────────────────────────────────────────────────────────┤
│  trip_id (PK)                                                    │
│  pickup_date                                                     │
│  pickup_hour                                                     │
│  pickup_weekday                                                  │
│  time_bucket                                                     │
│  is_weekend                                                      │
│  pulocation_id ─────────────────────────────────────┐           │
│  dolocation_id ─────────────────────────────────────┤           │
│  payment_type ───────────────────────────────────────┤           │
│  trip_distance                                            │       │
│  fare_amount                                              │       │
│  total_amount                                             │       │
│  tip_amount                                               │       │
│  trip_duration_min                                         │       │
└─────────────────────────────────────────────────────────────────┘
         │                                                    │       │
         │                                                    │       │
         ▼                                                    ▼       ▼
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│ dim_zone         │    │ dim_payment      │    │ dim_time (implicit)│
│ LocationID (PK)  │    │ payment_type (PK)│    │ pickup_date       │
│ Borough          │    │ payment_name     │    │ pickup_hour       │
│ Zone             │    │                  │    │ pickup_weekday    │
│ service_zone     │    │                  │    │ time_bucket       │
└──────────────────┘    └──────────────────┘    │ is_weekend        │
                                                        │
                                                        ▼
                                              ┌──────────────────┐
                                              │ agg_daily_summary │
                                              │ pickup_date (PK) │
                                              │ total_trips      │
                                              │ total_revenue    │
                                              │ avg_revenue...   │
                                              └──────────────────┘
```

---

## 📊 Dashboard Mapping

### Tầng 1: Filters & KPI tổng quan

| Component | Data Source | Table(s) |
|-----------|-------------|----------|
| **Ngày trong tháng** | `pickup_date` | fact_trips, agg_daily_summary |
| **Loại thanh toán** | `payment_type` | fact_trips + dim_payment |
| **Khung giờ** | `time_bucket` | fact_trips |
| **Total Trips** | COUNT(*) | agg_daily_summary |
| **Total Revenue** | SUM(total_amount) | agg_daily_summary |
| **Avg. Revenue / Trip** | AVG(total_amount) | agg_daily_summary |
| **Avg. Distance / Trip** | AVG(trip_distance) | agg_daily_summary |

### Tầng 2: Phân tích hành vi vận hành

| Component | Data Source | Table(s) |
|-----------|-------------|----------|
| **Heatmap (Trips theo giờ & thứ)** | COUNT(*) GROUP BY pickup_hour, pickup_weekday | fact_trips |
| **Line chart (Trips theo ngày)** | COUNT(*) GROUP BY pickup_date | agg_daily_summary |

### Tầng 3: Phân tích không gian & doanh thu

| Component | Data Source | Table(s) |
|-----------|-------------|----------|
| **Top 10 pickup locations** | COUNT(*) GROUP BY pulocation_id | fact_trips + dim_zone |
| **Scatter plot (Distance vs Fare)** | trip_distance, fare_amount | fact_trips |
| **Payment type share** | COUNT(*) GROUP BY payment_type | fact_trips + dim_payment |