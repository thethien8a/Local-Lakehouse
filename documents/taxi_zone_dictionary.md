# NYC Taxi Zone Dictionary - Từ điển Khu vực Taxi

> Tài liệu này mô tả ý nghĩa các cột trong file `taxi_zone_lookup.csv` - bảng tra cứu thông tin khu vực taxi zone của NYC.
> Nguồn: [NYC TLC Taxi Zone Map](https://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)

---

## 📋 Tổng quan các cột

| STT | Tên cột | Mô tả ngắn |
|-----|---------|------------|
| 1 | LocationID | Mã định danh duy nhất cho khu vực taxi zone |
| 2 | Borough | Quận (Borough) của NYC |
| 3 | Zone | Tên cụ thể của khu vực taxi zone |
| 4 | service_zone | Loại vùng dịch vụ taxi |

---

## 🔍 Chi tiết từng cột

### 1. `LocationID`
- **Mô tả**: Mã định danh duy nhất cho từng khu vực taxi zone
- **Kiểu dữ liệu**: Số nguyên (INT)
- **Phạm vi**: 1 - 265
- **Sử dụng**: Liên kết với các cột `PULocationID` (điểm đón) và `DOLocationID` (điểm trả) trong dataset taxi
- **Ví dụ**: 
  - `1` = Newark Airport
  - `132` = JFK Airport
  - `138` = LaGuardia Airport
  - `186` = Penn Station/Madison Sq West

### 2. `Borough`
- **Mô tả**: Quận (Borough) của New York City nơi khu vực taxi zone nằm
- **Kiểu dữ liệu**: Chuỗi (STRING)
- **Giá trị**:

| Borough | Mô tả |
|---------|-------|
| Manhattan | Quận trung tâm, khu vực kinh doanh chính |
| Brooklyn | Quận đông dân nhất, phía đông nam |
| Queens | Quận lớn nhất về diện tích, phía đông |
| Bronx | Quận phía bắc |
| Staten Island | Quận đảo phía nam |
| EWR | Newark Airport (New Jersey - ngoài NYC) |
| Unknown | Không xác định |
| N/A | Ngoài phạm vi NYC |

### 3. `Zone`
- **Mô tả**: Tên cụ thể của khu vực taxi zone
- **Kiểu dữ liệu**: Chuỗi (STRING)
- **Ví dụ**:
  - "Central Park"
  - "Times Sq/Theatre District"
  - "JFK Airport"
  - "LaGuardia Airport"
  - "East Village"
  - "Financial District North"

### 4. `service_zone`
- **Mô tả**: Loại vùng dịch vụ taxi được phép hoạt động trong khu vực
- **Kiểu dữ liệu**: Chuỗi (STRING)
- **Giá trị**:

| service_zone | Mô tả | Ghi chú |
|--------------|-------|---------|
| **Yellow Zone** | Khu vực taxi vàng (Yellow Taxi) | Chủ yếu ở Manhattan |
| **Boro Zone** | Khu vực taxi quận (Boro Taxi/Green Taxi) | Các khu vực ngoài Manhattan |
| **Airports** | Khu vực sân bay | JFK, LaGuardia |
| **EWR** | Sân bay Newark | Newark, New Jersey |
| **N/A** | Không áp dụng | Ngoài NYC hoặc không xác định |

---

## 📊 Thống kê theo Borough

| Borough | Số lượng Zone |
|---------|---------------|
| Manhattan | ~70 zones |
| Brooklyn | ~60 zones |
| Queens | ~65 zones |
| Bronx | ~35 zones |
| Staten Island | ~25 zones |
| Đặc biệt (EWR, Unknown, N/A) | 3 zones |

---

## 📊 Thống kê theo Service Zone

| service_zone | Mô tả |
|--------------|-------|
| Yellow Zone | ~65 zones - Khu vực Manhattan và trung tâm |
| Boro Zone | ~195 zones - Các khu vực ngoài Manhattan |
| Airports | 2 zones - JFK Airport (132), LaGuardia Airport (138) |
| EWR | 1 zone - Newark Airport (1) |
| N/A | 2 zones - Unknown (264), Outside of NYC (265) |

---

## 💡 Ví dụ tra cứu

### Ví dụ 1: LocationID = 186
```
LocationID:     186
Borough:        Manhattan
Zone:           Penn Station/Madison Sq West
service_zone:   Yellow Zone
```
**Ý nghĩa**: Khu vực Penn Station/Madison Square West thuộc Manhattan, taxi vàng được phép hoạt động.

### Ví dụ 2: LocationID = 132
```
LocationID:     132
Borough:        Queens
Zone:           JFK Airport
service_zone:   Airports
```
**Ý nghĩa**: Sân bay JFK thuộc Queens, là khu vực sân bay đặc biệt.

### Ví dụ 3: LocationID = 43
```
LocationID:     43
Borough:        Manhattan
Zone:           Central Park
service_zone:   Yellow Zone
```
**Ý nghĩa**: Central Park thuộc Manhattan, taxi vàng được phép hoạt động.

---

## 🔗 Liên kết với Dataset Taxi

File `taxi_zone_lookup.csv` được sử dụng để tra cứu thông tin cho hai cột trong dataset taxi:

- **`PULocationID`**: Mã khu vực nơi đón khách (Pickup Location)
- **`DOLocationID`**: Mã khu vực nơi trả khách (Dropoff Location)

### Ví dụ phân tích chuyến đi:
```
PULocationID:  186  → Penn Station/Madison Sq West, Manhattan (Yellow Zone)
DOLocationID:  79   → East Village, Manhattan (Yellow Zone)
```

---

## 📚 Tài liệu tham khảo

- [NYC TLC Trip Record Data](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)
- [NYC TLC Taxi Zone Map](https://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)
- [NYC TLC Data Dictionary PDF](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
