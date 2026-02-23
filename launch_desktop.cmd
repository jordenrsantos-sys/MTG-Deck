@echo off

set ROOT=%~dp0
set DIST_INDEX=%ROOT%ui_harness\dist\index.html
set DESKTOP_DIR=%ROOT%desktop
set RELEASE_EXE_A=%DESKTOP_DIR%\src-tauri\target\release\mtg_engine_desktop.exe
set RELEASE_EXE_B=%DESKTOP_DIR%\src-tauri\target\release\MTG Engine Desktop.exe

if exist "%RELEASE_EXE_A%" (
    start "" "%RELEASE_EXE_A%"
    exit /b 0
)

if exist "%RELEASE_EXE_B%" (
    start "" "%RELEASE_EXE_B%"
    exit /b 0
)

if not exist "%DIST_INDEX%" (
    echo UI not built. Run the following first:
    echo   cd ui_harness
    echo   npm run build
    pause
    exit /b 1
)

pushd "%DESKTOP_DIR%"

if not exist "node_modules" (
    echo Desktop dependencies not installed. Run:
    echo   cd desktop
    echo   npm install
    popd
    pause
    exit /b 1
)

call npm run tauri:dev
set EXIT_CODE=%ERRORLEVEL%

popd
pause
exit /b %EXIT_CODE%
