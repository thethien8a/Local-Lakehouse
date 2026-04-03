from datetime import datetime, timedelta

SSH_CONN_ID = "spark_ssh"

SPARK_SUBMIT = (
    "spark-submit "
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
