from datetime import datetime, timedelta

SSH_CONN_ID = "spark_ssh"

# Spark jobs can run for a long time — disable SSH command timeout
SSH_CMD_TIMEOUT = None
SSH_CONN_TIMEOUT = 30

JMX_AGENT = (
    "-javaagent:/opt/bitnami/spark/custom-jars/"
    "jmx_prometheus_javaagent-1.0.1.jar=9092:"
    "/opt/bitnami/spark/jmx-exporter.yml"
)

DRIVER_JAVA_OPTS = (
    "-Divy.message.logger.level=4 "
    "-Dlog4j.configurationFile=/opt/bitnami/spark/conf/log4j2.properties "
    f"{JMX_AGENT}"
)

SPARK_SUBMIT = (
    "cd /opt/bitnami/spark && "
    "PYTHONPATH=/opt/bitnami/spark "
    "/opt/bitnami/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    f"--conf 'spark.driver.extraJavaOptions={DRIVER_JAVA_OPTS}' "
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
