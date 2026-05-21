<#
.SYNOPSIS
  Mega-task v3 Phase 1 — install (or remove) the daily Scryfall set-release watcher
  as a Windows scheduled task.

.DESCRIPTION
  Creates a Windows Task Scheduler entry "MTGEngine.NewSetWatcher" that runs
  tools/check_new_sets.py daily at 06:03 UTC (local 06:03 if your machine's
  TZ is UTC; otherwise adjust the trigger time). The task is OS-level and
  persists across reboots / Claude sessions.

  This script is the substrate-level equivalent of the kickoff's
  "scheduled-tasks MCP" call — the MCP isn't available in this environment,
  so we use schtasks.exe directly. Idempotent: re-running --install replaces
  the existing entry.

.PARAMETER Install
  Create or replace the scheduled task.

.PARAMETER Remove
  Delete the scheduled task.

.PARAMETER PythonPath
  Path to python.exe. Defaults to "python" on PATH.

.PARAMETER RepoRoot
  Path to the mtg-engine/repo directory. Defaults to the parent of this script.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File tools/install_set_watcher_schedule.ps1 -Install

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File tools/install_set_watcher_schedule.ps1 -Remove
#>
param(
  [switch]$Install,
  [switch]$Remove,
  [string]$PythonPath = "python",
  [string]$RepoRoot = (Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path))
)

$TaskName = "MTGEngine.NewSetWatcher"
$ScriptPath = Join-Path $RepoRoot "tools\check_new_sets.py"

if ($Remove) {
  Write-Host "Removing scheduled task: $TaskName"
  schtasks /Delete /TN $TaskName /F
  exit $LASTEXITCODE
}

if (-not $Install) {
  Write-Host "Usage: -Install (create) or -Remove (delete)."
  Write-Host ""
  Write-Host "Current status of $TaskName"":"
  schtasks /Query /TN $TaskName 2>$null
  exit 0
}

if (-not (Test-Path $ScriptPath)) {
  Write-Error "Watcher script not found at: $ScriptPath"
  exit 2
}

# Action: run the watcher; if exit code is 1 (new sets detected), the
# parent scheduler logs the event. Phase 2's ingestion CLI is triggered
# manually by the user OR by extending this Action to chain into
# ingest_new_set.py. For v3 Phase 1 the watcher just detects + reports.
$Action = "$PythonPath `"$ScriptPath`""

# Daily at 06:03 local (off-the-hour per kickoff's distributed-fleet hint)
$Trigger = "06:03"

Write-Host "Installing scheduled task $TaskName..."
Write-Host "  Action: $Action"
Write-Host "  Trigger: Daily at $Trigger"
Write-Host "  Script: $ScriptPath"

schtasks /Create /TN $TaskName /TR $Action /SC DAILY /ST $Trigger /F
if ($LASTEXITCODE -ne 0) {
  Write-Error "schtasks failed with code $LASTEXITCODE"
  exit $LASTEXITCODE
}

Write-Host ""
Write-Host "Installed. Verify with:"
Write-Host "  schtasks /Query /TN $TaskName /V /FO LIST"
