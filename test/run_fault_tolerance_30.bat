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
)

echo.
echo === Results: !passes! / 30 passed, !failures! failed ===
