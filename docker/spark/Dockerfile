FROM bitnamilegacy/spark:3.5.1

USER root

# SSH server để Airflow có thể remote submit spark jobs
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssh-server \
    && mkdir -p /run/sshd \
    && echo 'root:spark' | chpasswd \
    && sed -i 's/#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 22
