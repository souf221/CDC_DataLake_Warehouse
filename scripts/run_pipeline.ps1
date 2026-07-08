# Script PowerShell pour lancer le pipeline CDC Lakehouse sur Windows
# Usage: .\scripts\run_pipeline.ps1 [-Mode sample|full] [-Step all|extract|bronze|silver|quality|gold|warehouse]

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
$env:PYTHONPATH = "$ProjectRoot\src"

Write-Host "=============================================="
Write-Host " CDC Data Lakehouse Pipeline"
Write-Host " Mode: $Mode | Step: $Step"
Write-Host "=============================================="

function Run-Extract { python -m ingestion.extract }
function Run-Bronze { python -m bronze.bronze_loader }
function Run-Silver { python -m silver.silver_transform }
function Run-Quality { python -m quality.validator }
function Run-Gold { python -m gold.gold_kpis }
function Run-Warehouse { python -m gold.warehouse_loader }

switch ($Step) {
    "extract"   { Run-Extract }
    "bronze"    { Run-Bronze }
    "silver"    { Run-Silver }
    "quality"   { Run-Quality }
    "gold"      { Run-Gold }
    "warehouse" { Run-Warehouse }
    "all" {
        Run-Extract
        Run-Bronze
        Run-Silver
        Run-Quality
        Run-Gold
        Run-Warehouse
    }
}

Write-Host ">>> Pipeline terminé avec succès !" -ForegroundColor Green
