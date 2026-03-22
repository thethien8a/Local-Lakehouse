
from src.config.path import DATA_DIR
import urllib.request
import os

def download_nyc_taxi_data():
    """
    Tải file dữ liệu NYC Taxi Yellow Tripdata (tháng 1/2024) dạng Parquet.
    Đây là file dữ liệu chuẩn, kích thước khoảng ~50MB.
    """
    # URL tải file của tháng 1/2024
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    
    # Thư mục đích (thư mục data mà Spark đã mount)
    output_file = DATA_DIR / "nyc_taxi_data.parquet"
    
    # Kiểm tra xem thư mục data đã tồn tại chưa
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        print(f"Đã tạo thư mục: {DATA_DIR}")
        
    print(f"Đang tải dữ liệu từ {url}...")
    print("Vui lòng đợi một chút, file nặng khoảng 50MB...")
    
    try:
        urllib.request.urlretrieve(url, output_file)
        print(f"Tải thành công! File đã được lưu tại: {output_file}")
    except Exception as e:
        print(f"ERROR: Lỗi khi tải file: {e}")

if __name__ == "__main__":
    download_nyc_taxi_data()
