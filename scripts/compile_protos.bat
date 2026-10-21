@echo off
setlocal
cd /d "%~dp0\.."
echo ===================================================
echo Compiling Protocol Buffers (proto/service.proto)
echo ===================================================

if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

python generate_protos.py
endlocal
