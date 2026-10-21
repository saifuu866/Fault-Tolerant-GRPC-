<#
.SYNOPSIS
    Compiles proto/service.proto into Python gRPC stubs in generated/.
.EXAMPLE
    .\scripts\compile_protos.ps1
#>
$RootDir = Split-Path -Parent $PSScriptRoot
Set-Location $RootDir

Write-Host "===================================================" -ForegroundColor Cyan
Write-Host "Compiling Protocol Buffers (proto/service.proto)" -ForegroundColor Cyan
Write-Host "===================================================" -ForegroundColor Cyan

$VenvPython = Join-Path $RootDir "venv\Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    $VenvPython = "python"
}

& $VenvPython generate_protos.py
