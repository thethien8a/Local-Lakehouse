from airflow import DAG
from airflow.models.param import Param
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dags_conf import SSH_CONN_ID, SPARK_SUBMIT, DEFAULT_ARGS

with DAG(
    dag_id="bronze_ingestion",
    default_args=DEFAULT_ARGS,
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
    bronze_task = SSHOperator(
        task_id="ingest_bronze",
        ssh_conn_id=SSH_CONN_ID,
        command=(
            f"{SPARK_SUBMIT}"
            "/opt/bitnami/spark/src/pipeline/bronze/ingest_bronze.py "
            '--date_from {{ params.date_from if params.date_from else ds }} '
            '--date_to {{ params.date_to if params.date_to else ds }}'
        ),
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="silver_transformation",
        conf={
            "date_from": "{{ params.date_from if params.date_from else ds }}",
            "date_to": "{{ params.date_to if params.date_to else ds }}",
        },
        wait_for_completion=False,
        reset_dag_run=True,
    )

    bronze_task >> trigger_silver
