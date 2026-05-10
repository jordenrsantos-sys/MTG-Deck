@echo off
REM verify_release.bat — Windows-native fallback for verify_release.sh.
REM Phase 6 Stage 9 pre-release verification.
REM
REM Runs the v1.0 ship-readiness gate checks. See verify_release.sh for the
REM canonical bash version + full per-check rationale.
REM
REM Usage (run from repo/):
REM   scripts\verify_release.bat
REM
REM Exits 0 when every check passes; 1 otherwise.

setlocal ENABLEDELAYEDEXPANSION

REM Repo root = parent of scripts/
set "REPO_DIR=%~dp0.."
pushd "%REPO_DIR%" 1>nul 2>&1
if errorlevel 1 (
  echo FATAL: cannot cd to repo dir %REPO_DIR%
  exit /b 2
)

REM Baselines (v1.0 — update with each release)
set "EXPECTED_PYTEST_MIN=747"
set "EXPECTED_VITEST_MIN=244"
set "EXPECTED_BUNDLE_MAX_BYTES=512000"
set "EXPECTED_MANIFEST_SHA=a63ae1feec8b9e3a"
set "EXPECTED_CORPUS_SHA=6d5c6af51ff7b"

set "PASS_COUNT=0"
set "FAIL_COUNT=0"

echo ===== verify_release.bat — Phase 6 Stage 9 v1.0 ship-readiness gate =====
echo Repo: %REPO_DIR%
echo.

REM ----- Check 1: pytest -----
echo [1/7] pytest baseline (expect ^>=%EXPECTED_PYTEST_MIN%)...
python -m pytest --tb=no -q > "%TEMP%\pytest_out.txt" 2>&1
for /f %%C in ('python -c "import re; m=re.search(r'(\d+) passed', open(r'%TEMP%\pytest_out.txt').read()); print(m.group(1) if m else 0)"') do set "PYTEST_COUNT=%%C"
if "!PYTEST_COUNT!"=="" set "PYTEST_COUNT=0"
if !PYTEST_COUNT! GEQ %EXPECTED_PYTEST_MIN% (
  set /a PASS_COUNT+=1
  echo   PASS  pytest  !PYTEST_COUNT! passed
) else (
  set /a FAIL_COUNT+=1
  echo   FAIL  pytest  !PYTEST_COUNT! passed ^(^< %EXPECTED_PYTEST_MIN%^) ^-^- see %TEMP%\pytest_out.txt
)

REM ----- Check 2: vitest -----
echo [2/7] vitest baseline (expect ^>=%EXPECTED_VITEST_MIN%)...
if exist "ui_harness\node_modules" (
  pushd ui_harness 1>nul
  call npx vitest run > "%TEMP%\vitest_out.txt" 2>&1
  popd 1>nul
  for /f %%C in ('python -c "import re; t=open(r'%TEMP%\vitest_out.txt',encoding='utf-8',errors='replace').read(); t=re.sub(r'\x1b\[[0-9;]*m','',t); nums=[int(n) for n in re.findall(r'(\d+)\s+passed',t)]; print(max(nums) if nums else 0)"') do set "VITEST_COUNT=%%C"
  if "!VITEST_COUNT!"=="" set "VITEST_COUNT=0"
  if !VITEST_COUNT! GEQ %EXPECTED_VITEST_MIN% (
    set /a PASS_COUNT+=1
    echo   PASS  vitest  !VITEST_COUNT! passed
  ) else (
    set /a FAIL_COUNT+=1
    echo   FAIL  vitest  !VITEST_COUNT! passed ^(^< %EXPECTED_VITEST_MIN%^) ^-^- see %TEMP%\vitest_out.txt
  )
) else (
  set /a FAIL_COUNT+=1
  echo   FAIL  vitest  ui_harness\node_modules missing — run 'npm install' first
)

REM ----- Check 3: Vite production build -----
echo [3/7] Vite production build (expect exit 0 + bundle ^<%EXPECTED_BUNDLE_MAX_BYTES% bytes)...
if exist "ui_harness\node_modules" (
  pushd ui_harness 1>nul
  call npm run build > "%TEMP%\vite_build_out.txt" 2>&1
  set "VITE_EXIT=!ERRORLEVEL!"
  popd 1>nul
  if !VITE_EXIT! NEQ 0 (
    set /a FAIL_COUNT+=1
    echo   FAIL  vite-build  exit code !VITE_EXIT! — see %TEMP%\vite_build_out.txt
  ) else (
    for %%F in (ui_harness\dist\assets\index-*.js) do set "BUNDLE_FILE=%%F"
    if "!BUNDLE_FILE!"=="" (
      set /a FAIL_COUNT+=1
      echo   FAIL  vite-build  exit 0 but no dist\assets\index-*.js produced
    ) else (
      for %%S in ("!BUNDLE_FILE!") do set "BUNDLE_SIZE=%%~zS"
      if !BUNDLE_SIZE! LEQ %EXPECTED_BUNDLE_MAX_BYTES% (
        set /a PASS_COUNT+=1
        echo   PASS  vite-build  exit 0; bundle !BUNDLE_SIZE! bytes
      ) else (
        set /a FAIL_COUNT+=1
        echo   FAIL  vite-build  bundle !BUNDLE_SIZE! exceeds %EXPECTED_BUNDLE_MAX_BYTES%
      )
    )
  )
) else (
  set /a FAIL_COUNT+=1
  echo   FAIL  vite-build  ui_harness\node_modules missing
)

REM ----- Check 4: Manifest SHA -----
echo [4/7] Manifest SHA-256 (expect prefix %EXPECTED_MANIFEST_SHA%)...
set "MANIFEST_FILE=api\engine\data\packs\curated_pack_manifest_v1.json"
if exist "%MANIFEST_FILE%" (
  for /f "delims=" %%S in ('python -c "import hashlib; print(hashlib.sha256(open(r'%MANIFEST_FILE%','rb').read()).hexdigest())"') do (
    set "MANIFEST_SHA=%%S"
  )
  echo !MANIFEST_SHA! | findstr /B /C:"%EXPECTED_MANIFEST_SHA%" >nul
  if !ERRORLEVEL! EQU 0 (
    set /a PASS_COUNT+=1
    echo   PASS  manifest-sha  prefix matches
  ) else (
    set /a FAIL_COUNT+=1
    echo   FAIL  manifest-sha  expected prefix %EXPECTED_MANIFEST_SHA%
  )
) else (
  set /a FAIL_COUNT+=1
  echo   FAIL  manifest-sha  manifest file missing
)

REM ----- Check 5: Corpus SHA -----
echo [5/7] Calibration corpus SHA-256 (expect prefix %EXPECTED_CORPUS_SHA%)...
set "CORPUS_FILE=api\engine\data\calibration\external_decks_v1.json"
if exist "%CORPUS_FILE%" (
  for /f "delims=" %%S in ('python -c "import hashlib; print(hashlib.sha256(open(r'%CORPUS_FILE%','rb').read()).hexdigest())"') do (
    set "CORPUS_SHA=%%S"
  )
  echo !CORPUS_SHA! | findstr /B /C:"%EXPECTED_CORPUS_SHA%" >nul
  if !ERRORLEVEL! EQU 0 (
    set /a PASS_COUNT+=1
    echo   PASS  corpus-sha  prefix matches
  ) else (
    set /a FAIL_COUNT+=1
    echo   FAIL  corpus-sha  expected prefix %EXPECTED_CORPUS_SHA%
  )
) else (
  set /a FAIL_COUNT+=1
  echo   FAIL  corpus-sha  corpus file missing
)

REM ----- Check 6: Perf-audit -----
echo [6/7] Perf-timing audit (expect exit 0)...
python -m tools.perf.run_perf_timing_audit --iterations 10 > "%TEMP%\perf_audit_out.txt" 2>&1
set "PERF_EXIT=!ERRORLEVEL!"
if !PERF_EXIT! EQU 0 (
  set /a PASS_COUNT+=1
  echo   PASS  perf-audit  exit 0
) else (
  set /a FAIL_COUNT+=1
  echo   FAIL  perf-audit  exit code !PERF_EXIT! — see %TEMP%\perf_audit_out.txt
)

REM ----- Check 7: npm deps capture -----
echo [7/7] Capture npm top-level deps (documentation only)...
if exist "ui_harness\node_modules" (
  pushd ui_harness 1>nul
  call npm ls --depth=0 > "%TEMP%\npm_ls_top.txt" 2>&1
  popd 1>nul
  set /a PASS_COUNT+=1
  echo   PASS  npm-deps-capture  captured to %TEMP%\npm_ls_top.txt
) else (
  set /a FAIL_COUNT+=1
  echo   FAIL  npm-deps-capture  ui_harness\node_modules missing
)

REM ----- Summary -----
echo.
echo ===== Summary =====
echo PASS=!PASS_COUNT!  FAIL=!FAIL_COUNT!
if !FAIL_COUNT! GTR 0 (
  echo RELEASE GATE: FAIL — do not ship
  popd 1>nul
  exit /b 1
) else (
  echo RELEASE GATE: PASS — ready to ship
  popd 1>nul
  exit /b 0
)
