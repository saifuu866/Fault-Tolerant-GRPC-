<#
.SYNOPSIS
    Starts the full Fault-Tolerant RPC cluster (servers + dashboard).
.EXAMPLE
    .\scripts\run_cluster.ps1
    .\scripts\run_cluster.ps1 -SeparateWindows
#>
[CmdletBinding()]
param(
    [switch]$SeparateWindows,
    [switch]$ServersOnly
)

$RootDir = Split-Path -Parent $PSScriptRoot
Set-Location $RootDir

Write-Host "===================================================" -ForegroundColor Cyan
Write-Host "Starting Fault-Tolerant Distributed RPC Cluster" -ForegroundColor Cyan
Write-Host "===================================================" -ForegroundColor Cyan

$VenvPython = Join-Path $RootDir "venv\Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    $VenvPython = "python"
}

$Arguments = @("run.py")
if ($SeparateWindows) { $Arguments += "--separate-windows" }
if ($ServersOnly) { $Arguments += "--servers-only" }

& $VenvPython $Arguments
