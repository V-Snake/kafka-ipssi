param(
  [string]$City="Paris",
  [string]$CC="FR",
  [int]$Loops=8,
  [float]$Interval=1
)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSCommandPath -Parent) | Out-Null
Set-Location ..

Write-Host "🧱 Infra Docker (Kafka + HDFS)..."
docker compose up -d kafka hdfs-namenode hdfs-datanode
Start-Sleep -Seconds 5
docker exec -it hdfs-namenode hdfs dfs -mkdir -p /hdfs-data

Write-Host "🧵 Topics..."
.\scripts\create-topics.ps1 | Out-Null

Write-Host "⚡ Spark alerts (détaché)..."
.\scripts\run-spark-alerts.ps1 -Fresh -Detach | Out-Null

Write-Host "🌦️ Producer météo -> weather_stream..."
$py = ".\.venv\Scripts\python.exe"; if (-not (Test-Path $py)) { $py = "python" }
& $py .\producer_weather.py --city-name $City --country $CC --loops $Loops --interval $Interval --topic weather_stream

Write-Host "🗃️ Consumer HDFS (attaché, timeout si plus de flux)..."
& $py .\consumer_hdfs.py --topic weather_transformed --bootstrap localhost:9092 --hdfs-url http://localhost:9870 --hdfs-base-path /hdfs-data --group-id smoke_exo7 --timeout-ms 4000

Write-Host "🔍 Vérif HDFS:"
docker exec -it hdfs-namenode hdfs dfs -ls -R /hdfs-data
