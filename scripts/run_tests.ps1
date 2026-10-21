<#
.SYNOPSIS
    Runs the 17 unit tests for the Fault-Tolerant RPC framework.
.EXAMPLE
    .\scripts\run_tests.ps1
#>
$RootDir = Split-Path -Parent $PSScriptRoot
Set-Location $RootDir

Write-Host "===================================================" -ForegroundColor Cyan
Write-Host "Running Fault-Tolerance Framework Test Suite" -ForegroundColor Cyan
Write-Host "===================================================" -ForegroundColor Cyan

$VenvPython = Join-Path $RootDir "venv\Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    $VenvPython = "python"
}

& $VenvPython -m unittest discover -s tests -v
