from airflow import DAG
from airflow.models.param import Param
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dags_conf import SSH_CONN_ID, SPARK_SUBMIT, DEFAULT_ARGS, SSH_CMD_TIMEOUT, SSH_CONN_TIMEOUT

with DAG(
    dag_id="silver_transformation",
    default_args=DEFAULT_ARGS,
    schedule=None,
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
    silver_task = SSHOperator(
        task_id="ingest_silver",
        ssh_conn_id=SSH_CONN_ID,
        command=(
            f"{SPARK_SUBMIT}"
            "/opt/bitnami/spark/src/pipeline/silver/ingest_silver.py "
            '--start {{ dag_run.conf.get("date_from", ds) }} '
            '--end {{ dag_run.conf.get("date_to", ds) }}'
        ),
        cmd_timeout=SSH_CMD_TIMEOUT,
        conn_timeout=SSH_CONN_TIMEOUT,
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_dag_id="gold_aggregation",
        conf={
            "date_from": "{{ dag_run.conf.get('date_from', ds) }}",
            "date_to": "{{ dag_run.conf.get('date_to', ds) }}",
        },
        wait_for_completion=False,
        reset_dag_run=True,
    )

    silver_task >> trigger_gold
