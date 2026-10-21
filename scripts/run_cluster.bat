@echo off
setlocal
cd /d "%~dp0\.."
echo ===================================================
echo Starting Fault-Tolerant Distributed RPC Cluster
echo ===================================================

if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

python run.py %*
endlocal
