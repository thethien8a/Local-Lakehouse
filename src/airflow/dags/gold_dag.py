from airflow import DAG
from airflow.models.param import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from dags_conf import SILVER_DATASET, GOLD_DATASET, SPARK_CONN_ID, DEFAULT_ARGS

with DAG(
    dag_id="gold_transformation",
    default_args=DEFAULT_ARGS,
    description="Tạo Gold tables từ Silver",
    schedule=[SILVER_DATASET],
    catchup=False,
    tags=["lakehouse", "gold"],
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
) as gold_dag:
    gold_task = SparkSubmitOperator(
        task_id="ingest_gold",
        application="/opt/bitnami/spark/src/gold/ingest_gold.py",
        name="gold_transformation",
        conn_id=SPARK_CONN_ID,
        application_args=[
            "--date_from", "{{ params.date_from if params.date_from else ds }}",
            "--date_to", "{{ params.date_to if params.date_to else ds }}",
        ],
        env_vars={"PYTHONPATH": "/opt/bitnami/spark"},
        conf={
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
        },
        outlets=[GOLD_DATASET],
    )
