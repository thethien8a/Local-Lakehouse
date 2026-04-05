from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from dags_conf import (
    SSH_CONN_ID, SPARK_SUBMIT, DEFAULT_ARGS,
    SSH_CMD_TIMEOUT, SSH_CONN_TIMEOUT,
    MAINTENANCE_TABLES, MAINTENANCE_SCRIPT,
)

TABLES_ARG = " ".join(MAINTENANCE_TABLES)
DAYS_TO_KEEP = 30
RETAIN_LAST = 10

with DAG(
    dag_id="maintenance_monthly",
    default_args=DEFAULT_ARGS,
    description="Expire snapshots + remove orphan files hàng tháng — giải phóng storage",
    schedule="0 3 1 * *",  # Ngày 1 hàng tháng, 3:00 AM
    catchup=False,
    tags=["lakehouse", "maintenance"],
) as dag:
    expire_and_cleanup = SSHOperator(
        task_id="expire_and_remove_orphans",
        ssh_conn_id=SSH_CONN_ID,
        command=(
            f"{SPARK_SUBMIT}"
            f"{MAINTENANCE_SCRIPT} "
            f"--mode monthly "
            f"--tables {TABLES_ARG} "
            f"--days-to-keep {DAYS_TO_KEEP} "
            f"--retain-last {RETAIN_LAST}"
        ),
        cmd_timeout=SSH_CMD_TIMEOUT,
        conn_timeout=SSH_CONN_TIMEOUT,
    )
