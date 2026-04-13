# NYC Yellow Taxi Dataset - Data Dictionary

> Tài liệu này mô tả ý nghĩa các cột trong bộ dataset NYC Yellow Taxi Trip Records.
> Nguồn: [NYC TLC Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)

---

## 📋 Tổng quan các cột

| STT | Tên cột | Mô tả ngắn |
|-----|---------|------------|
| 1 | VendorID | Mã nhà cung cấp dịch vụ TPEP |
| 2 | tpep_pickup_datetime | Thởi gian đón khách |
| 3 | tpep_dropoff_datetime | Thởi gian trả khách |
| 4 | passenger_count | Số hành khách |
| 5 | trip_distance | Quãng đường (dặm) |
| 6 | RatecodeID | Mã loại cước phí |
| 7 | store_and_fwd_flag | Cờ lưu tạm dữ liệu |
| 8 | PULocationID | Mã khu vực đón khách |
| 9 | DOLocationID | Mã khu vực trả khách |
| 10 | payment_type | Phương thức thanh toán |
| 11 | fare_amount | Cước phí chính |
| 12 | extra | Phụ phí khác |
| 13 | mta_tax | Thuế MTA |
| 14 | tip_amount | Tiền boa |
| 15 | tolls_amount | Phí cầu đường |
| 16 | improvement_surcharge | Phí cải thiện |
| 17 | total_amount | Tổng tiền |
| 18 | congestion_surcharge | Phí tắc nghẽn |
| 19 | Airport_fee | Phí sân bay |

---

## 🔍 Chi tiết từng cột

### 1. Thông tin chuyến đi cơ bản

#### `VendorID`
- **Mô tả**: Mã nhà cung cấp dịch vụ TPEP (Taxicab Passenger Enhancement Program)
- **Giá trị**:
  - `1` = Creative Mobile Technologies, LLC
  - `2` = Curb Mobility, LLC
  - `6` = Myle Technologies Inc
  - `7` = Helix

#### `tpep_pickup_datetime`
- **Mô tả**: Thởi gian đồng hồ tính tiền bắt đầu (lên xe)
- **Định dạng**: YYYY-MM-DD HH:MM:SS

#### `tpep_dropoff_datetime`
- **Mô tả**: Thởi gian đồng hồ tính tiền kết thúc (xuống xe)
- **Định dạng**: YYYY-MM-DD HH:MM:SS

#### `passenger_count`
- **Mô tả**: Số lượng hành khách trong xe
- **Kiểu dữ liệu**: Số thực (thường là 1.0, 2.0, ...)

#### `trip_distance`
- **Mô tả**: Quãng đường di chuyển tính bằng dặm (mile)
- **Kiểu dữ liệu**: Số thực

---

### 2. Loại cước phí và cờ báo hiệu

#### `RatecodeID`
- **Mô tả**: Mã loại cước áp dụng tại thởi điểm kết thúc chuyến đi
- **Giá trị**:
  - `1` = Standard rate (Giá chuẩn)
  - `2` = JFK (Sân bay JFK)
  - `3` = Newark (Sân bay Newark)
  - `4` = Nassau or Westchester
  - `5` = Negotiated fare (Cước thỏa thuận)
  - `6` = Group ride (Đi nhóm)
  - `99` = Null/unknown

#### `store_and_fwd_flag`
- **Mô tả**: Cờ báo hiệu chuyến đi có bị lưu tạm trong bộ nhớ xe trước khi gửi đến máy chủ không (do mất kết nối)
- **Giá trị**:
  - `Y` = Store and forward trip (Có lưu tạm)
  - `N` = Not a store and forward trip (Không lưu tạm)

---

### 3. Vị trí đón/trả khách

#### `PULocationID`
- **Mô tả**: Mã khu vực TLC (Taxi Zone) nơi đồng hồ được bật (đón khách)
- **Lưu ý**: Đây là mã vùng theo bản đồ Taxi Zone của NYC, không phải tọa độ GPS

#### `DOLocationID`
- **Mô tả**: Mã khu vực TLC (Taxi Zone) nơi đồng hồ tắt (trả khách)
- **Lưu ý**: Đây là mã vùng theo bản đồ Taxi Zone của NYC

> 📍 **Tham khảo**: Bản đồ Taxi Zone của NYC có thể tìm thấy tại [NYC TLC website](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)

---

### 4. Thanh toán và chi phí

#### `payment_type`
- **Mô tả**: Phương thức thanh toán của hành khách
- **Giá trị**:
  - `0` = Flex Fare trip
  - `1` = Credit card (Thẻ tín dụng)
  - `2` = Cash (Tiền mặt)
  - `3` = No charge (Miễn phí)
  - `4` = Dispute (Tranh chấp)
  - `5` = Unknown (Không xác định)
  - `6` = Voided trip (Chuyến đi bị hủy)

#### `fare_amount`
- **Mô tả**: Cước phí theo thởi gian và quãng đường do đồng hồ tính
- **Đơn vị**: Đô la Mỹ (USD)

#### `extra`
- **Mô tả**: Các khoản phụ phí khác (phí đêm, phí cao điểm,...)
- **Đơn vị**: Đô la Mỹ (USD)

#### `mta_tax`
- **Mô tả**: Thuế MTA (Metropolitan Transportation Authority) tự động tính theo cước
- **Đơn vị**: Đô la Mỹ (USD)

#### `tip_amount`
- **Mô tả**: Tiền boa
- **Lưu ý**: Chỉ ghi nhận tiền boa từ thẻ tín dụng. **Tiền boa bằng tiền mặt không được bao gồm**
- **Đơn vị**: Đô la Mỹ (USD)

#### `tolls_amount`
- **Mô tả**: Tổng phí cầu đường trong chuyến đi
- **Đơn vị**: Đô la Mỹ (USD)

#### `improvement_surcharge`
- **Mô tả**: Phụ phí cải thiện được áp dụng khi bắt đầu chuyến đi
- **Lưu ý**: Bắt đầu áp dụng từ năm 2015
- **Đơn vị**: Đô la Mỹ (USD)

#### `congestion_surcharge`
- **Mô tả**: Phụ phí tắc nghẽn của bang New York (NYS)
- **Đơn vị**: Đô la Mỹ (USD)

#### `Airport_fee`
- **Mô tả**: Phí sân bay
- **Lưu ý**: Chỉ áp dụng khi đón khách tại sân bay LaGuardia hoặc John F. Kennedy
- **Đơn vị**: Đô la Mỹ (USD)

#### `total_amount`
- **Mô tả**: Tổng số tiền tính cho hành khách
- **Lưu ý**: **KHÔNG bao gồm tiền boa bằng tiền mặt**
- **Đơn vị**: Đô la Mỹ (USD)

---

## 💡 Ví dụ phân tích

### Dòng dữ liệu mẫu:

```
VendorID:              2
Tpep_pickup_datetime:  2024-01-01 00:57:55
Tpep_dropoff_datetime: 2024-01-01 01:17:43
Passenger_count:       1.0
Trip_distance:         1.72
RatecodeID:            1.0
Store_and_fwd_flag:    N
PULocationID:          186
DOLocationID:          79
Payment_type:          2
Fare_amount:           17.7
Extra:                 1.0
Mta_tax:               0.5
Tip_amount:            0.00
Tolls_amount:          0.0
Improvement_surcharge: 1.0
Total_amount:          22.70
Congestion_surcharge:  2.5
Airport_fee:           0.0
```

### Phân tích:
- **Nhà cung cấp**: Curb Mobility (VendorID=2)
- **Thởi gian**: Từ 00:57:55 đến 01:17:43 (~20 phút)
- **Hành khách**: 1 ngườii
- **Quãng đường**: 1.72 dặm
- **Tuyến đường**: Từ khu vực 186 đến khu vực 79
- **Thanh toán**: Tiền mặt (payment_type=2)
- **Tổng tiền**: $22.70
  - Cước chính: $17.7
  - Phụ phí: $1.0
  - Thuế MTA: $0.5
  - Phí cải thiện: $1.0
  - Phí tắc nghẽn: $2.5
  - Tiền boa: $0 (do thanh toán tiền mặt, không ghi nhận)

---

## 📚 Tài liệu tham khảo

- [NYC TLC Trip Record Data](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)
- [NYC TLC Data Dictionary PDF](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
- [NYC Taxi Fare Information](https://www.nyc.gov/site/tlc/passengers/taxi-fare.page)
