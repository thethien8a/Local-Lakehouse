from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from dags_conf import SILVER_DATASET, GOLD_DATASET, SPARK_CONN_ID, SPARK_MASTER, DEFAULT_ARGS

with DAG(
    dag_id="gold_transformation",
    default_args=DEFAULT_ARGS,
    description="Tạo Gold tables từ Silver",
    schedule=[SILVER_DATASET],
    catchup=False,
    tags=["lakehouse", "gold"],
) as gold_dag:
    gold_task = SparkSubmitOperator(
        task_id="ingest_gold",
        application="/opt/bitnami/spark/src/gold/ingest_gold.py",
        name="gold_transformation",
        conn_id=SPARK_CONN_ID,
        conf={
            "spark.master": SPARK_MASTER,
            "spark.submit.deployMode": "client",
            "spark.jars": "/opt/bitnami/spark/custom-jars/*",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
        },
        outlets=[GOLD_DATASET],
    )
