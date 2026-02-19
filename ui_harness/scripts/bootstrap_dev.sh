#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UI_HARNESS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$UI_HARNESS_DIR"

echo "Working directory: $UI_HARNESS_DIR"

if ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1; then
  echo "Node.js and npm are required but were not found on PATH."
  echo "Install Node.js LTS, then re-open your terminal and re-run this script."
  echo "Download: https://nodejs.org/en/download"
  echo "macOS (Homebrew, optional): brew install node@lts"
  exit 1
fi

echo "node: $(node -v)"
echo "npm:  $(npm -v)"

npm install
npm run build
npm run dev
