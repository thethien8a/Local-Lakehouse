# Phân tích dữ liệu taxi New York

## Khán giả
- Quản lý vận hành đội xe
- Điều hành viên

## Loại dashboard
- Operational dashboard

## Câu hỏi kinh doanh
- **Weekday vs Weekend**: Doanh thu và thời gian đi lại khác nhau thế nào?
- **Tipping behavior**: Tip có tăng theo quãng đường không, hay theo khung giờ (đêm vs ngày)?
- **Yếu tố thời tiết (bonus insight)**: Tháng 1 ở New York là mùa đông lạnh và có tuyết. Nếu có vài ngày số chuyến giảm mạnh, hãy đối chiếu sự kiện thời tiết (ví dụ bão tuyết) để tăng chất lượng insight.

## Bố cục dashboard

### Tầng 1: Filters & KPI tổng quan

**Bộ lọc (Slicers)**
- **Ngày trong tháng**: 1-31
- **Loại thanh toán**: Tiền mặt / Thẻ
- **Khung giờ**: Sáng / Trưa / Chiều / Tối

**KPI (Big Numbers)**
- **Total Trips**: Tổng số chuyến (khối lượng công việc)
- **Total Revenue**: Tổng doanh thu (Fare + Extra + Tips...)
- **Avg. Revenue / Trip**: Doanh thu trung bình mỗi chuyến
- **Avg. Distance / Trip**: Quãng đường trung bình mỗi chuyến

### Tầng 2: Phân tích hành vi vận hành (trọng tâm theo tháng)

Mục tiêu: Trả lời câu hỏi **“Tài xế nên ra đường vào lúc nào?”**

**Biểu đồ 1: Heatmap / Matrix (Trips theo giờ & thứ)**
- **Y**: Thứ 2 đến Chủ nhật
- **X**: 0h-23h
- **Màu**: Total Trips
- **Ý nghĩa**: Nhìn nhanh “điểm nóng” theo khung giờ (đi làm, đi chơi, về khuya...).

**Biểu đồ 2: Line chart (Trips theo ngày trong tháng)**
- **X**: 1-31
- **Y**: Total Trips
- **Ý nghĩa**: Biến động theo ngày; có thể phát hiện ngày đặc biệt (ví dụ 1/1).

### Tầng 3: Phân tích không gian & doanh thu

**Biểu đồ 3: Horizontal bar chart (Top 10 pickup locations)**
- **Ý nghĩa**: Trả lời câu hỏi “Nên điều xe tập trung ở khu vực nào?”

**Biểu đồ 4: Scatter plot (Distance vs Fare Amount)**
- **Ý nghĩa**: Quan sát tương quan, phát hiện điểm bất thường (fare cao bất thường / fare thấp bất thường).

**Biểu đồ 5: Donut/Pie (Payment Type share)**
- **Ý nghĩa**: Tỷ lệ trả thẻ vs tiền mặt (lưu ý: tip thường được ghi nhận rõ hơn với thanh toán thẻ).