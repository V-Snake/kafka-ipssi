#!/usr/bin/env bash
set -euo pipefail
topic="${1:-weather_stream}"

docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
  --create --if-not-exists \
  --topic "$topic" \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
