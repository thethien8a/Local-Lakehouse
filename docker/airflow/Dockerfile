FROM apache/airflow:2.10.3

USER root

# Cài Java (Spark cần JRE để chạy spark-submit)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Tải và cài Spark (chỉ cần binary, không cần Hadoop built-in vì dùng custom jars)
ENV SPARK_VERSION=3.5.1
ENV SPARK_HOME=/opt/spark

# Nếu có mạng, dùng curl. Nếu không, tải manual rồi COPY.
COPY spark-${SPARK_VERSION}-bin-hadoop3.tgz /tmp/spark.tgz
RUN tar -xzf /tmp/spark.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} \
    && rm /tmp/spark.tgz

ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==4.11.1
