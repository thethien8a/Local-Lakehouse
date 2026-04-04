FROM bitnamilegacy/spark:3.5.1

USER root

# SSH server để Airflow có thể remote submit spark jobs
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssh-server gettext-base \
    && mkdir -p /run/sshd \
    && sed -i 's/#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config \
    && sed -i 's/#PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config \
    && ssh-keygen -A \
    && echo "JAVA_HOME=/opt/bitnami/java" >> /etc/environment \
    && echo "SPARK_HOME=/opt/bitnami/spark" >> /etc/environment \
    && echo "PYSPARK_PYTHON=/opt/bitnami/python/bin/python3" >> /etc/environment \
    && echo "PYTHONPATH=/opt/bitnami/spark" >> /etc/environment \
    && echo "PATH=/opt/bitnami/python/bin:/opt/bitnami/java/bin:/opt/bitnami/spark/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" >> /etc/environment \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 22
