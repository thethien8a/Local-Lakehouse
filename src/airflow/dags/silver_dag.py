from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from dags_conf import BRONZE_DATASET, SILVER_DATASET, SPARK_CONN_ID, SPARK_MASTER, DEFAULT_ARGS

with DAG(
    dag_id="silver_transformation",
    default_args=DEFAULT_ARGS,
    description="Transform dữ liệu từ Bronze sang Silver",
    schedule=[BRONZE_DATASET],
    catchup=False,
    tags=["lakehouse", "silver"],
) as silver_dag:
    silver_task = SparkSubmitOperator(
        task_id="ingest_silver",
        application="/opt/bitnami/spark/src/silver/ingest_silver.py",
        name="silver_transformation",
        conn_id=SPARK_CONN_ID,
        conf={
            "spark.master": SPARK_MASTER,
            "spark.submit.deployMode": "client",
            "spark.jars": "/opt/bitnami/spark/custom-jars/*",
        },
        outlets=[SILVER_DATASET],
    )
