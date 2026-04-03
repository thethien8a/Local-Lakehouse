#!/bin/bash
set -e

# Start SSH daemon in background
/usr/sbin/sshd

# Start Spark Master directly (bypass Bitnami script to avoid chroot error)
/opt/bitnami/spark/sbin/start-master.sh

# Keep container running
tail -f /dev/null
