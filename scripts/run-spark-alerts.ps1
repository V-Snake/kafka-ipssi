param(
  [double]$WindMax = 60,
  [double]$TempMin = 0,
  [string]$Bootstrap = "127.0.0.1:9092",
  [string]$Topic = "weather_stream",
  [string]$Checkpoint,
  [switch]$Fresh # si présent -> reset checkpoint
)

# Dossier projet = repo kafka-ipssi/kafka-ipssi
if (-not $Checkpoint) { $Checkpoint = Join-Path $PWD "chk\ex04" }
if ($Fresh) { Remove-Item -Recurse -Force $Checkpoint -ErrorAction SilentlyContinue }

$env:HADOOP_HOME = 'C:\hadoop-3.3.6'
$env:PATH = "C:\hadoop-3.3.6\bin;$env:PATH"
$py = Join-Path $PWD ".venv\Scripts\python.exe"
$env:PYSPARK_PYTHON = $py
$env:PYSPARK_DRIVER_PYTHON = $py
$env:JAVA_TOOL_OPTIONS = '-Djava.net.preferIPv4Stack=true'

spark-submit.cmd `
  --conf "spark.pyspark.python=$py" `
  --conf "spark.pyspark.driver.python=$py" `
  --conf "spark.driver.host=127.0.0.1" `
  --conf "spark.driver.bindAddress=127.0.0.1" `
  --conf "spark.driver.extraJavaOptions=-Dhadoop.home.dir=C:/hadoop-3.3.6 -Djava.net.preferIPv4Stack=true" `
  --conf "spark.executor.extraJavaOptions=-Dhadoop.home.dir=C:/hadoop-3.3.6 -Djava.net.preferIPv4Stack=true" `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 .\spark_alerts.py `
  --bootstrap $Bootstrap --topic $Topic `
  --starting-offsets latest `
  --wind-max $WindMax --temp-min $TempMin `
  --checkpoint $Checkpoint
