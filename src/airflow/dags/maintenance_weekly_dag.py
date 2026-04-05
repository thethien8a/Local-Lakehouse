from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from dags_conf import (
    SSH_CONN_ID, SPARK_SUBMIT, DEFAULT_ARGS,
    SSH_CMD_TIMEOUT, SSH_CONN_TIMEOUT,
    MAINTENANCE_TABLES, MAINTENANCE_SCRIPT,
)

TABLES_ARG = " ".join(MAINTENANCE_TABLES)

with DAG(
    dag_id="maintenance_weekly",
    default_args=DEFAULT_ARGS,
    description="Compaction hàng tuần — gộp small files để tăng hiệu năng đọc",
    schedule="0 2 * * 0",  # Chủ nhật 2:00 AM
    catchup=False,
    tags=["lakehouse", "maintenance"],
) as dag:
    compact_tables = SSHOperator(
        task_id="compact_tables",
        ssh_conn_id=SSH_CONN_ID,
        command=(
            f"{SPARK_SUBMIT}"
            f"{MAINTENANCE_SCRIPT} "
            f"--mode weekly "
            f"--tables {TABLES_ARG}"
        ),
        cmd_timeout=SSH_CMD_TIMEOUT,
        conn_timeout=SSH_CONN_TIMEOUT,
    )
