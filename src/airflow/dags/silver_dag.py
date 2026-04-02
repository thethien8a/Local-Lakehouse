from airflow import DAG
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from dags_conf import BRONZE_DATASET, SILVER_DATASET, SPARK_CONN_ID, DEFAULT_ARGS

with DAG(
    dag_id="silver_transformation",
    default_args=DEFAULT_ARGS,
    description="Transform dữ liệu từ Bronze sang Silver",
    schedule=[BRONZE_DATASET],
    catchup=False,
    tags=["lakehouse", "silver"],
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
) as silver_dag:
    silver_task = SparkSubmitOperator(
        task_id="ingest_silver",
        application="/opt/bitnami/spark/src/silver/ingest_silver.py",
        name="silver_transformation",
        conn_id=SPARK_CONN_ID,
        application_args=[
            "--date_from", "{{ params.date_from if params.date_from else ds }}",
            "--date_to", "{{ params.date_to if params.date_to else ds }}",
        ],
        env_vars={"PYTHONPATH": "/opt/bitnami/spark"},
        outlets=[SILVER_DATASET],
    )
