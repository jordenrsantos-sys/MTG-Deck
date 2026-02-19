$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$uiHarnessDir = (Resolve-Path (Join-Path $scriptDir "..")).Path
Set-Location $uiHarnessDir

Write-Host "Working directory: $uiHarnessDir"

$nodeCmd = Get-Command node -ErrorAction SilentlyContinue
$npmCmd = Get-Command npm -ErrorAction SilentlyContinue

if (-not $nodeCmd -or -not $npmCmd) {
  Write-Host "Node.js and npm are required but were not found on PATH."
  Write-Host "Install Node.js LTS, then re-open your terminal and re-run this script."
  Write-Host "Download: https://nodejs.org/en/download"
  Write-Host "Windows (PowerShell, optional): winget install OpenJS.NodeJS.LTS"
  exit 1
}

$nodeVersion = node -v
$npmVersion = npm -v
Write-Host "node: $nodeVersion"
Write-Host "npm:  $npmVersion"

npm install
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

npm run build
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

npm run dev
exit $LASTEXITCODE
