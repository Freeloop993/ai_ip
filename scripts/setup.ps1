$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$project = Split-Path -Parent $root

Write-Host "[setup] creating data directory"
New-Item -ItemType Directory -Force -Path (Join-Path $project "data") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $project "plugins\\coze-bridge") | Out-Null

if (-not (Test-Path (Join-Path $project ".env"))) {
  Write-Host "[setup] writing .env from .env.example"
  Copy-Item (Join-Path $project ".env.example") (Join-Path $project ".env")
}

Write-Host "[setup] done"
Write-Host "Run server with:"
Write-Host '$env:PYTHONPATH = "' + (Join-Path $project "src") + '"; py -3 -m mvp_pipeline.server'
