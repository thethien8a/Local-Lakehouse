from datetime import datetime, timedelta

SSH_CONN_ID = "spark_ssh"

# Spark jobs can run for a long time — disable SSH command timeout
SSH_CMD_TIMEOUT = None
SSH_CONN_TIMEOUT = 30

SPARK_SUBMIT = (
    "cd /opt/bitnami/spark && "
    "PYTHONPATH=/opt/bitnami/spark "
    "/opt/bitnami/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
)

DEFAULT_ARGS = {
    "owner": "lakehouse-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

MAINTENANCE_TABLES = [
    "nessie.taxi.bronze",
    "nessie.taxi.silver",
    "nessie.taxi.fact_trips",
    "nessie.taxi.agg_daily_summary",
]

MAINTENANCE_SCRIPT = "/opt/bitnami/spark/src/maintenance/run_maintenance.py"
