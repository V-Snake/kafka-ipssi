param(
  [double]$WindMax = 60,
  [double]$TempMin = 0,
  [string]$Bootstrap = "127.0.0.1:9092",
  [string]$Topic = "weather_stream",
  [string]$Checkpoint,
  [switch]$Fresh,      # si présent -> reset checkpoint
  [switch]$Detach,     # lance dans une nouvelle fenêtre (détaché)
  [string]$LogFileBase # préfixe commun pour les logs si -Detach (sinon autogénéré)
)

$ErrorActionPreference = 'Stop'

if (-not $Checkpoint) { $Checkpoint = Join-Path $PWD "chk\ex04" }
if ($Fresh) { Remove-Item -Recurse -Force $Checkpoint -ErrorAction SilentlyContinue }

$env:HADOOP_HOME = 'C:\hadoop-3.3.6'
$env:PATH = "C:\hadoop-3.3.6\bin;$env:PATH"

$py = Join-Path $PWD ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) { $py = "python" }
$env:PYSPARK_PYTHON = $py
$env:PYSPARK_DRIVER_PYTHON = $py
$env:JAVA_TOOL_OPTIONS = '-Djava.net.preferIPv4Stack=true'

if ($Detach) {
  New-Item -ItemType Directory -Force -Path ".\logs" | Out-Null
  New-Item -ItemType Directory -Force -Path ".\pids" | Out-Null
  $ts = Get-Date -Format 'yyyyMMdd-HHmmss'
  if ($LogFileBase) {
    $logOut = "$LogFileBase.out.log"
    $logErr = "$LogFileBase.err.log"
  } else {
    $logOut = ".\logs\run-spark-alerts-$ts.out.log"
    $logErr = ".\logs\run-spark-alerts-$ts.err.log"
  }

  $args = @(
    '-NoProfile','-ExecutionPolicy','Bypass','-File', $PSCommandPath,
    '-Bootstrap', $Bootstrap,
    '-Topic', $Topic,
    '-Checkpoint', $Checkpoint,
    '-WindMax', $WindMax,
    '-TempMin', $TempMin
  )
  if ($Fresh) { $args += '-Fresh' }

  $p = Start-Process -FilePath 'powershell' -ArgumentList $args `
        -RedirectStandardOutput $logOut -RedirectStandardError $logErr `
        -WindowStyle Minimized -PassThru
  $p.Id | Set-Content .\pids\alerts.pid
  Write-Host "✅ Spark alerts détaché (PID $($p.Id))."
  Write-Host "   Logs: $logOut  (stdout)"
  Write-Host "         $logErr  (stderr)"
  exit 0
}

# ----- Exécution *attachée* (courante) -----
$packages = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"

spark-submit.cmd `
  --conf "spark.pyspark.python=$py" `
  --conf "spark.pyspark.driver.python=$py" `
  --conf "spark.driver.host=127.0.0.1" `
  --conf "spark.driver.bindAddress=127.0.0.1" `
  --conf "spark.driver.extraJavaOptions=-Dhadoop.home.dir=C:/hadoop-3.3.6 -Djava.net.preferIPv4Stack=true" `
  --conf "spark.executor.extraJavaOptions=-Dhadoop.home.dir=C:/hadoop-3.3.6 -Djava.net.preferIPv4Stack=true" `
  --packages $packages .\spark_alerts.py `
  --bootstrap $Bootstrap --topic $Topic `
  --starting-offsets latest `
  --wind-max $WindMax --temp-min $TempMin `
  --checkpoint $Checkpoint
