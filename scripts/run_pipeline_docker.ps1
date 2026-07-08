# Lance le pipeline complet dans Docker (conteneur Spark)
# Usage: .\scripts\run_pipeline_docker.ps1 [-Mode sample|full] [-Step all|extract|...]

param(
    [ValidateSet("sample", "full")]
    [string]$Mode = "sample",
    [ValidateSet("all", "extract", "bronze", "silver", "quality", "gold", "warehouse")]
    [string]$Step = "all"
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$env:DATA_MODE = $Mode

Write-Host "=============================================="
Write-Host " CDC Pipeline (Docker / Spark)"
Write-Host " Mode: $Mode | Step: $Step"
Write-Host "=============================================="

$bashArgs = @($Mode, $Step)
if ($IsWindows -or $env:OS -match "Windows") {
    bash "$PSScriptRoot/run_pipeline_docker.sh" @bashArgs
} else {
    & "$PSScriptRoot/run_pipeline_docker.sh" @bashArgs
}
