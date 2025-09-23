param(
  [string[]]$Topics = @("weather_stream","weather_transformed","weather_aggregates"),
  [string]$Bootstrap = "localhost:9092"
)

foreach ($t in $Topics) {
  docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh `
    --create --if-not-exists `
    --topic $t `
    --bootstrap-server $Bootstrap `
    --partitions 1 --replication-factor 1
}

docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server $Bootstrap
