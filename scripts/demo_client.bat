@echo off
setlocal
cd /d "%~dp0\.."
echo ===================================================
echo Running Sample Demo Client Requests
echo ===================================================

if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

echo [1/4] Sending 6 echo requests (Round-robin demonstration)...
python client/rpc_client.py --operation echo --payload "distributed-rpc" --count 6

echo.
echo [2/4] Sending 3 sum requests...
python client/rpc_client.py --operation sum --payload "10,20,30,40" --count 3

echo.
echo [3/4] Sending flaky request (Retry mechanism demonstration)...
python client/rpc_client.py --operation flaky --payload "1" --count 1

echo.
echo [4/4] Sending permanent error request (Boom - no retry demonstration)...
python client/rpc_client.py --operation boom --count 1

endlocal
