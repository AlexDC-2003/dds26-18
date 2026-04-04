@echo off
setlocal enabledelayedexpansion
set passes=0
set failures=0

for /l %%i in (1,1,30) do (
    echo.
    echo === Run %%i / 30 ===
    python "%~dp0fault_tolerance_test.py"
    if !errorlevel! equ 0 (
        set /a passes+=1
        echo PASS
    ) else (
        set /a failures+=1
        echo FAIL
    )
    echo Restarting sentinels to clear stale master state...
    docker compose -f "%~dp0..\docker-compose.yml" restart sentinel-1 sentinel-2 sentinel-3 >nul 2>&1
    timeout /t 5 /nobreak >nul
    echo Waiting for all services to be writable before next run...
    :waitloop
    curl -sf http://127.0.0.1:8000/stock/find/0 >nul 2>&1
    if !errorlevel! neq 0 ( timeout /t 3 /nobreak >nul & goto waitloop )
    curl -sf http://127.0.0.1:8000/payment/find_user/0 >nul 2>&1
    if !errorlevel! neq 0 ( timeout /t 3 /nobreak >nul & goto waitloop )
    curl -sf -X POST http://127.0.0.1:8000/orders/create/healthcheck >nul 2>&1
    if !errorlevel! neq 0 ( timeout /t 3 /nobreak >nul & goto waitloop )
    echo Services ready.
)

echo.
echo === Results: !passes! / 30 passed, !failures! failed ===
