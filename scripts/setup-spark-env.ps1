param(
  [string]$HadoopHome = 'C:\hadoop-3.3.6',
  [switch]$Persist,
  [switch]$Quiet
)
$ErrorActionPreference = 'Stop'

if (-not (Test-Path $HadoopHome)) { throw "HADOOP_HOME introuvable: $HadoopHome" }

# Session env
$env:HADOOP_HOME = $HadoopHome
if (-not ($env:PATH -split ';' | Where-Object { $_ -eq "$HadoopHome\bin" })) {
  $env:PATH = "$HadoopHome\bin;$env:PATH"
}

$projectRoot = Resolve-Path (Join-Path $PSScriptRoot '..')
$venvPy = Join-Path $projectRoot '.venv\Scripts\python.exe'
if (-not (Test-Path $venvPy)) { Write-Warning "Python du venv non trouvé à $venvPy"; $venvPy='python' }
$env:PYSPARK_PYTHON = $venvPy
$env:PYSPARK_DRIVER_PYTHON = $venvPy

# winutils + C:\tmp
New-Item -ItemType Directory -Force -Path C:\tmp | Out-Null
& "$HadoopHome\bin\winutils.exe" chmod 777 C:\tmp | Out-Null

# Persistant (optionnel)
if ($Persist) {
  [Environment]::SetEnvironmentVariable('HADOOP_HOME', $HadoopHome, 'User')
  # Nettoie toute vieille valeur foireuse
  [Environment]::SetEnvironmentVariable('SPARK_SUBMIT_OPTS', $null, 'User')
}

if (-not $Quiet) {
  Write-Host "HADOOP_HOME = $env:HADOOP_HOME"
  Write-Host "PYSPARK_PYTHON = $env:PYSPARK_PYTHON"
  & "$HadoopHome\bin\winutils.exe" ls C:\ | Out-Null
  Write-Host "winutils OK"
}
