@echo off
setlocal
cd /d "%~dp0\.."
echo ===================================================
echo Running Fault-Tolerance Framework Test Suite
echo ===================================================

if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

python -m unittest discover -s tests -v
endlocal
