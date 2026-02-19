$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$workspaceRoot = Split-Path -Parent $repoRoot

$candidates = @(
    (Join-Path $repoRoot ".venv\Scripts\python.exe"),
    (Join-Path $repoRoot "venv\Scripts\python.exe"),
    (Join-Path $workspaceRoot ".venv\Scripts\python.exe"),
    (Join-Path $workspaceRoot "venv\Scripts\python.exe")
)

$PY = $null
foreach ($candidate in $candidates) {
    if (Test-Path -LiteralPath $candidate -PathType Leaf) {
        $PY = $candidate
        break
    }
}

if (-not $PY) {
    Write-Error ("No repo venv python.exe found. Checked: {0}" -f ($candidates -join ", "))
    exit 1
}

Write-Host ("Using Python: {0}" -f $PY)
& $PY -V
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

$env:PYTHONDONTWRITEBYTECODE = "1"

$files = @(
    "engine\db_tags.py",
    "taxonomy\\__init__.py",
    "taxonomy\\exporter.py",
    "taxonomy\\loader.py",
    "snapshot_build\\index_build.py",
    "snapshot_build\\tag_snapshot.py",
    "api\\engine\\pipeline_build.py",
    "api\\main.py",
    "tests\\test_taxonomy_compiler.py",
    "tests\\test_runtime_tags.py"
)

Push-Location $repoRoot
try {
    $compileArgs = @("-B", "-m", "py_compile") + $files
    Write-Host ("Running: `"{0}`" {1}" -f $PY, ($compileArgs -join " "))
    & $PY @compileArgs
    $exitCode = $LASTEXITCODE

    if ($exitCode -ne 0) {
        Write-Warning ("Direct invocation failed with exit code {0}. Retrying via PowerShell -NoProfile wrapper." -f $exitCode)
        $fileArgs = ($files | ForEach-Object { "'$_'" }) -join " "
        $fallbackCommand = "& '$PY' -B -m py_compile $fileArgs"
        powershell -NoProfile -Command $fallbackCommand
        $exitCode = $LASTEXITCODE
    }

    if ($exitCode -ne 0) {
        exit $exitCode
    }

    Write-Host "Syntax check passed."
    exit 0
}
finally {
    Pop-Location
}
