param([string]$Topic = "weather_stream")

docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh `
  --create --if-not-exists `
  --topic $Topic `
  --bootstrap-server localhost:9092 `
  --partitions 1 --replication-factor 1
