# Equivalente de run.sh para Windows (PowerShell):   .\run.ps1   o   .\run.ps1 --sin-carga
$ErrorActionPreference = "Continue"
Set-Location $PSScriptRoot
New-Item -ItemType Directory -Force resultados | Out-Null
Start-Transcript -Path "resultados\run_log.txt" -Force | Out-Null
docker compose up -d --build --wait
if ($LASTEXITCODE -ne 0) { throw "docker compose up fallo" }
if (-not (Test-Path "venv\Scripts\python.exe")) { python -m venv venv }
& .\venv\Scripts\python.exe -m pip install -q -r requirements.txt
$env:PYTHONUTF8 = "1"
& .\venv\Scripts\python.exe run_all.py @args
$code = $LASTEXITCODE
Stop-Transcript | Out-Null
exit $code
