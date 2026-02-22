param(
    [string]$DbPath = "",
    [switch]$BuildUI
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir
$uiRoot = Join-Path $repoRoot "ui_harness"

$ApiPort = 8000
$UiPort = 5173

function Get-ExitCodeLabel {
    param(
        [System.Diagnostics.Process]$Process
    )

    if ($null -eq $Process) {
        return "unknown"
    }

    try {
        $Process.Refresh()
    }
    catch {
        # Ignore refresh errors and fall back to best effort exit code access.
    }

    try {
        if ($Process.HasExited) {
            $code = $Process.ExitCode
            if ($null -ne $code) {
                return [string]$code
            }
        }
    }
    catch {
        # Ignore access errors and fall through to unknown.
    }

    return "unknown"
}

function Resolve-AbsolutePath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PathValue,
        [Parameter(Mandatory = $true)]
        [string]$BasePath
    )

    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        return [System.IO.Path]::GetFullPath($PathValue)
    }

    return [System.IO.Path]::GetFullPath((Join-Path $BasePath $PathValue))
}

function Wait-HttpReady {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Url,
        [Parameter(Mandatory = $true)]
        [string]$ServiceName,
        [int]$TimeoutSeconds = 30,
        [System.Diagnostics.Process]$RelatedProcess = $null,
        [ScriptBlock]$Validator = $null,
        [System.Diagnostics.Process[]]$MustStayAliveProcesses = @(),
        [ScriptBlock]$OnMustStayAliveExit = $null
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    while ((Get-Date) -lt $deadline) {
        if ($null -ne $RelatedProcess -and $RelatedProcess.HasExited) {
            throw ("{0} process exited before readiness check passed (exit code {1})." -f $ServiceName, (Get-ExitCodeLabel -Process $RelatedProcess))
        }

        foreach ($requiredProcess in $MustStayAliveProcesses) {
            if ($null -ne $requiredProcess -and $requiredProcess.HasExited) {
                if ($null -ne $OnMustStayAliveExit) {
                    & $OnMustStayAliveExit $requiredProcess
                }
                throw ("Required process exited before {0} became ready (exit code {1})." -f $ServiceName, (Get-ExitCodeLabel -Process $requiredProcess))
            }
        }

        try {
            $response = Invoke-WebRequest -Uri $Url -Method Get -TimeoutSec 3 -UseBasicParsing
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 400) {
                if ($null -eq $Validator) {
                    return
                }

                if (& $Validator $response) {
                    return
                }
            }
        }
        catch {
            # Expected while process is still starting.
        }

        Start-Sleep -Milliseconds 400
    }

    throw ("Timed out waiting for {0} at {1} after {2} seconds." -f $ServiceName, $Url, $TimeoutSeconds)
}

function Stop-ChildProcess {
    param(
        [System.Diagnostics.Process]$Process,
        [string]$Name
    )

    if ($null -eq $Process) {
        return
    }

    try {
        $Process.Refresh()
    }
    catch {
        # Ignore refresh errors; best effort shutdown still applies.
    }

    if (-not $Process.HasExited) {
        Write-Host ("Stopping {0} (PID {1})..." -f $Name, $Process.Id)
        $null = & taskkill /PID $Process.Id /T /F 2>$null

        try {
            $Process.Refresh()
        }
        catch {
            # Ignore refresh errors and continue fallback.
        }

        if (-not $Process.HasExited) {
            Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
        }
    }
}

if (-not (Test-Path -LiteralPath $uiRoot -PathType Container)) {
    throw ("UI folder not found: {0}" -f $uiRoot)
}

if ([string]::IsNullOrWhiteSpace($DbPath)) {
    if (-not [string]::IsNullOrWhiteSpace($env:MTG_ENGINE_DB_PATH)) {
        $DbPath = $env:MTG_ENGINE_DB_PATH
    }
    else {
        $DbPath = "e:\mtg-engine\data\mtg.sqlite"
    }
}

$dbPathResolved = Resolve-AbsolutePath -PathValue $DbPath -BasePath $repoRoot
if (-not (Test-Path -LiteralPath $dbPathResolved -PathType Leaf)) {
    throw ("MTG_ENGINE_DB_PATH was not found: {0}" -f $dbPathResolved)
}

$pythonCandidates = @(
    (Join-Path $repoRoot ".venv\Scripts\python.exe"),
    (Join-Path $repoRoot "venv\Scripts\python.exe")
)
$pythonExe = $null
foreach ($candidate in $pythonCandidates) {
    if (Test-Path -LiteralPath $candidate -PathType Leaf) {
        $pythonExe = $candidate
        break
    }
}
if ($null -eq $pythonExe) {
    $pythonExe = "python"
}

$apiBaseUrl = "http://127.0.0.1:{0}" -f $ApiPort
$uiUrl = "http://127.0.0.1:{0}" -f $UiPort

$env:MTG_ENGINE_DB_PATH = $dbPathResolved
$env:MTG_ENGINE_DEV_CORS = "1"
$env:VITE_API_BASE_URL = $apiBaseUrl

$backendProcess = $null
$uiProcess = $null

try {
    Write-Host "=== MTG Engine Client Launcher ==="
    Write-Host ("Repo root: {0}" -f $repoRoot)
    Write-Host ("DB path:   {0}" -f $env:MTG_ENGINE_DB_PATH)
    Write-Host ("API URL:   {0}" -f $apiBaseUrl)
    Write-Host ("UI URL:    {0}" -f $uiUrl)

    $distPath = Join-Path $uiRoot "dist"
    if ($BuildUI -or -not (Test-Path -LiteralPath $distPath -PathType Container)) {
        Write-Host "Building UI bundle (npm run build)..."
        Push-Location $uiRoot
        try {
            & npm.cmd run build
            if ($LASTEXITCODE -ne 0) {
                throw ("UI build failed with exit code {0}." -f $LASTEXITCODE)
            }
        }
        finally {
            Pop-Location
        }
    }
    else {
        Write-Host "Using existing ui_harness/dist (pass -BuildUI to rebuild)."
    }

    Write-Host "Starting backend API (uvicorn)..."
    $backendCommand = "`"{0}`" -m uvicorn api.main:app --host 127.0.0.1 --port {1} 2>&1" -f $pythonExe, $ApiPort
    $backendProcess = Start-Process -FilePath "cmd.exe" -ArgumentList @("/c", $backendCommand) -WorkingDirectory $repoRoot -PassThru -NoNewWindow
    $null = $backendProcess.Handle

    Start-Sleep -Seconds 3
    if ($backendProcess.HasExited) {
        $startupFailureMessage = "Backend process exited within 3 seconds (exit code {0})." -f (Get-ExitCodeLabel -Process $backendProcess)
        Write-Host $startupFailureMessage -ForegroundColor Red
        throw $startupFailureMessage
    }

    Wait-HttpReady -Url ("{0}/health" -f $apiBaseUrl) -ServiceName "backend API" -TimeoutSeconds 20 -RelatedProcess $backendProcess -Validator {
        param($response)
        try {
            $body = $response.Content | ConvertFrom-Json
            return $body.ok -eq $true
        }
        catch {
            return $false
        }
    }
    Write-Host "Backend API is ready."

    Write-Host "Starting UI preview server..."
    $uiArgs = @("vite", "preview", "--host", "127.0.0.1", "--port", [string]$UiPort, "--strictPort")
    $uiProcess = Start-Process -FilePath "npx.cmd" -ArgumentList $uiArgs -WorkingDirectory $uiRoot -PassThru -NoNewWindow
    $null = $uiProcess.Handle

    Wait-HttpReady -Url $uiUrl -ServiceName "UI server" -TimeoutSeconds 30 -RelatedProcess $uiProcess -MustStayAliveProcesses @($backendProcess) -OnMustStayAliveExit {
        param($process)
        Write-Host "Backend process exited. Shutting down UI." -ForegroundColor Red
        Stop-ChildProcess -Process $uiProcess -Name "UI server"
    }
    Write-Host "UI server is ready."

    Start-Process $uiUrl | Out-Null
    Write-Host "Browser opened. Paste your decklist into the UI."
    Write-Host "Press Ctrl+C to stop both backend and UI."

    while ($true) {
        Start-Sleep -Seconds 1

        if ($backendProcess.HasExited) {
            Write-Host "Backend process exited. Shutting down UI." -ForegroundColor Red
            Stop-ChildProcess -Process $uiProcess -Name "UI server"
            throw ("Backend API exited unexpectedly with code {0}." -f (Get-ExitCodeLabel -Process $backendProcess))
        }
        if ($uiProcess.HasExited) {
            throw ("UI server exited unexpectedly with code {0}." -f (Get-ExitCodeLabel -Process $uiProcess))
        }
    }
}
finally {
    Stop-ChildProcess -Process $uiProcess -Name "UI server"
    Stop-ChildProcess -Process $backendProcess -Name "backend API"
}
