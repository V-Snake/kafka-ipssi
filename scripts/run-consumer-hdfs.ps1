param(
  [string]$Topic = "weather_transformed",
  [string]$Bootstrap = "localhost:9092",
  [string]$HdfsUrl = "http://localhost:9870",
  [string]$HdfsBasePath = "/hdfs-data",
  [string]$GroupId = "exo7_hdfs_sink",
  [int]$TimeoutMs = 4000,
  [int]$MaxMessages = 0,      # 0 = illimité
  [switch]$Detach,            # lance dans une nouvelle fenêtre
  [string]$LogFileBase        # préfixe commun pour les logs si -Detach (sinon autogénéré)
)
$ErrorActionPreference = 'Stop'

$py = Join-Path $PWD ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) { $py = "python" }

$common = @(
  ".\consumer_hdfs.py",
  "--topic", $Topic,
  "--bootstrap", $Bootstrap,
  "--hdfs-url", $HdfsUrl,
  "--hdfs-base-path", $HdfsBasePath,
  "--group-id", $GroupId,
  "--timeout-ms", $TimeoutMs,
  "--max-messages", $MaxMessages
)

if ($Detach) {
  New-Item -ItemType Directory -Force -Path ".\logs" | Out-Null
  New-Item -ItemType Directory -Force -Path ".\pids" | Out-Null
  $ts = Get-Date -Format 'yyyyMMdd-HHmmss'
  if ($LogFileBase) {
    $logOut = "$LogFileBase.out.log"
    $logErr = "$LogFileBase.err.log"
  } else {
    $logOut = ".\logs\consumer-hdfs-$ts.out.log"
    $logErr = ".\logs\consumer-hdfs-$ts.err.log"
  }

  $p = Start-Process -FilePath $py -ArgumentList $common `
        -RedirectStandardOutput $logOut -RedirectStandardError $logErr `
        -WindowStyle Minimized -PassThru
  $p.Id | Set-Content .\pids\consumer_hdfs.pid
  Write-Host "✅ Consumer HDFS détaché (PID $($p.Id))."
  Write-Host "   Logs: $logOut  (stdout)"
  Write-Host "         $logErr  (stderr)"
  exit 0
}

# Exécution attachée
& $py @common
