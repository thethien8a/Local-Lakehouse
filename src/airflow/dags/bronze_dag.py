from airflow import DAG
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from dags_conf import BRONZE_DATASET, SPARK_CONN_ID, DEFAULT_ARGS

with DAG(
    dag_id="bronze_ingestion",
    default_args=DEFAULT_ARGS,
    description="Ingest dữ liệu thô vào Bronze layer",
    schedule="0 1 * * *",
    catchup=False,
    tags=["lakehouse", "bronze"],
    params={
        "date_from": Param(
            default="",
            type="string",
            description="Ngày bắt đầu (YYYY-MM-DD). Để trống = dùng execution date.",
        ),
        "date_to": Param(
            default="",
            type="string",
            description="Ngày kết thúc (YYYY-MM-DD). Để trống = dùng execution date.",
        ),
    },
) as bronze_dag:
    bronze_task = SparkSubmitOperator(
        task_id="ingest_bronze",
        application="/opt/bitnami/spark/src/bronze/ingest_bronze.py",
        name="bronze_ingestion",
        conn_id=SPARK_CONN_ID,
        application_args=[
            "--date_from", "{{ params.date_from if params.date_from else ds }}",
            "--date_to", "{{ params.date_to if params.date_to else ds }}",
        ],
        env_vars={"PYTHONPATH": "/opt/bitnami/spark"},
        outlets=[BRONZE_DATASET],
    )
