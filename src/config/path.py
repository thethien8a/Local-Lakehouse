from pathlib import Path

ROOT_DIR = Path(__file__).parents[2] # root của project

DATA_DIR = ROOT_DIR / "data"

# Đường dẫn file dữ liệu thô trong container Spark
RAW_DATA_PATH = "/opt/bitnami/spark/data/nyc_taxi_data.parquet"