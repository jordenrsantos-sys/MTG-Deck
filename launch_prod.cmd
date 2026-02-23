@echo off

set ROOT=%~dp0

if exist "%ROOT%.venv\Scripts\python.exe" (
    "%ROOT%.venv\Scripts\python.exe" "%ROOT%launch_prod.py"
) else (
    python "%ROOT%launch_prod.py"
)

pause
