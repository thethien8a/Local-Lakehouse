from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from dags_conf import BRONZE_DATASET, SPARK_CONN_ID, SPARK_MASTER, DEFAULT_ARGS

with DAG(
    dag_id="bronze_ingestion",
    default_args=DEFAULT_ARGS,
    description="Ingest dữ liệu thô vào Bronze layer",
    schedule="0 1 * * *",
    catchup=False,
    tags=["lakehouse", "bronze"],
) as bronze_dag:
    bronze_task = SparkSubmitOperator(
        task_id="ingest_bronze",
        application="/opt/bitnami/spark/src/bronze/ingest_bronze.py",
        name="bronze_ingestion",
        conn_id=SPARK_CONN_ID,
        application_args=["--date", "{{ ds }}"],
        conf={
            "spark.master": SPARK_MASTER,
            "spark.submit.deployMode": "client",
            "spark.jars": "/opt/bitnami/spark/custom-jars/*",
        },
        outlets=[BRONZE_DATASET],
    )
