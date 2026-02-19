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

Push-Location $repoRoot
try {
    $testArgs = @("-B", "-m", "unittest", "tests.test_taxonomy_compiler", "tests.test_runtime_tags", "-v")
    Write-Host ("Running: `"{0}`" {1}" -f $PY, ($testArgs -join " "))
    & $PY @testArgs
    $exitCode = $LASTEXITCODE

    if ($exitCode -ne 0) {
        Write-Warning ("Direct invocation failed with exit code {0}. Retrying via PowerShell -NoProfile wrapper." -f $exitCode)
        $fallbackCommand = "& '$PY' -B -m unittest tests.test_taxonomy_compiler tests.test_runtime_tags -v"
        powershell -NoProfile -Command $fallbackCommand
        $exitCode = $LASTEXITCODE
    }

    if ($exitCode -ne 0) {
        exit $exitCode
    }

    Write-Host "Tests passed."
    exit 0
}
finally {
    Pop-Location
}
