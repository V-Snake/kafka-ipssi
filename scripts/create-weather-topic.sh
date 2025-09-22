#!/usr/bin/env bash
set -euo pipefail
topic="${1:-weather_stream}"

# Empêche Git Bash de convertir /opt/... en C:\Program Files\Git\opt\...
MSYS_NO_PATHCONV=1 docker exec kafka bash -lc "/opt/bitnami/kafka/bin/kafka-topics.sh \
  --create --if-not-exists \
  --topic '${topic}' \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1"
