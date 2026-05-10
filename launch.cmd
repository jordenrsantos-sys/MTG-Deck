@echo off
setlocal

set ROOT=%~dp0
set MODE=%~1

if "%MODE%"=="" set MODE=dev

if /I not "%MODE%"=="dev" if /I not "%MODE%"=="prod" (
    echo ERROR: mode must be dev or prod.
    echo Usage: launch.cmd [dev^|prod]
    exit /b 1
)

if exist "%ROOT%.venv\Scripts\python.exe" (
    "%ROOT%.venv\Scripts\python.exe" "%ROOT%launch.py" --mode %MODE%
) else (
    python "%ROOT%launch.py" --mode %MODE%
)

endlocal
