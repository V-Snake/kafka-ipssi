param(
  [string]$InTopic = "weather_transformed",
  [string]$OutTopic = "weather_aggregates",
  [string]$Bootstrap = "127.0.0.1:9092",
  [string]$Checkpoint,
  [string]$Window = "1 minute",
  [string]$Slide = "30 seconds",
  [switch]$Console,
  [switch]$Fresh
)

if (-not $Checkpoint) { $Checkpoint = Join-Path $PWD "chk\exo05" }
if ($Fresh) { Remove-Item -Recurse -Force $Checkpoint -ErrorAction SilentlyContinue }

$env:HADOOP_HOME = 'C:\hadoop-3.3.6'
$env:PATH = "C:\hadoop-3.3.6\bin;$env:PATH"
$py = Join-Path $PWD ".venv\Scripts\python.exe"
$env:PYSPARK_PYTHON = $py
$env:PYSPARK_DRIVER_PYTHON = $py
$env:JAVA_TOOL_OPTIONS = '-Djava.net.preferIPv4Stack=true'

# Spark <-> Kafka package
$packages = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"

# Options console
$consoleFlag = @()
if ($Console) { $consoleFlag = @("--console") }

spark-submit.cmd `
  --conf "spark.pyspark.python=$py" `
  --conf "spark.pyspark.driver.python=$py" `
  --conf "spark.driver.host=127.0.0.1" `
  --conf "spark.driver.bindAddress=127.0.0.1" `
  --conf "spark.hadoop.io.native.lib.available=false" `
  --packages $packages .\spark_aggregates.py `
  --in-topic $InTopic `
  --out-topic $OutTopic `
  --bootstrap-server $Bootstrap `
  --checkpoint $Checkpoint `
  --window "$Window" `
  --slide "$Slide" `
  $consoleFlag
