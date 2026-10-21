<#
.SYNOPSIS
    Sends sample demo client requests demonstrating load balancing, retries, and errors.
.EXAMPLE
    .\scripts\demo_client.ps1
#>
$RootDir = Split-Path -Parent $PSScriptRoot
Set-Location $RootDir

Write-Host "===================================================" -ForegroundColor Cyan
Write-Host "Running Sample Demo Client Requests" -ForegroundColor Cyan
Write-Host "===================================================" -ForegroundColor Cyan

$VenvPython = Join-Path $RootDir "venv\Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    $VenvPython = "python"
}

Write-Host "`n[1/4] Sending 6 echo requests (Round-robin demonstration)..." -ForegroundColor Yellow
& $VenvPython client/rpc_client.py --operation echo --payload "distributed-rpc" --count 6

Write-Host "`n[2/4] Sending 3 sum requests..." -ForegroundColor Yellow
& $VenvPython client/rpc_client.py --operation sum --payload "10,20,30,40" --count 3

Write-Host "`n[3/4] Sending flaky request (Retry mechanism demonstration)..." -ForegroundColor Yellow
& $VenvPython client/rpc_client.py --operation flaky --payload "1" --count 1

Write-Host "`n[4/4] Sending permanent error request (Boom - no retry demonstration)..." -ForegroundColor Yellow
& $VenvPython client/rpc_client.py --operation boom --count 1
