FROM bitnamilegacy/spark:3.5.1

# Override entrypoint mặc định của Bitnami để tránh lỗi chroot
ENTRYPOINT []

# SSH server để Airflow có thể remote submit spark jobs
RUN apt-get update \
    && apt-get install -y --no-install-recommends openssh-server \
    && mkdir -p /run/sshd \
    && mkdir -p /var/run/sshd \
    && echo 'root:spark' | chpasswd \
    && sed -i 's/#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config \
    && sed -i 's/#PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config \
    && echo "UsePAM no" >> /etc/ssh/sshd_config \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy entrypoint script
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 22

# Chạy entrypoint khi container start
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
