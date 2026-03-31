from airflow.datasets import Dataset
from datetime import datetime, timedelta

# Spark connection và configuration
SPARK_CONN_ID = "spark_default"
SPARK_MASTER = "spark://spark-master:7077"

# Datasets cho cross-DAG dependencies (nếu muốn dùng multiple DAGs)
BRONZE_DATASET = Dataset("s3://warehouse/nessie/taxi/bronze")
SILVER_DATASET = Dataset("s3://warehouse/nessie/taxi/silver")
GOLD_DATASET = Dataset("s3://warehouse/nessie/taxi/gold")

DEFAULT_ARGS = {
    "owner": "lakehouse-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}