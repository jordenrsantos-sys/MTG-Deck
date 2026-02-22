# MTG Engine One-Click Client Launcher

This launcher starts the backend API and a locally served UI bundle, then opens the UI in your browser.

## Prerequisites

- Python and dependencies installed for this repo
- Node/npm available for `ui_harness`
- A valid SQLite DB file

Default DB path used by the launcher:

- `e:\mtg-engine\data\mtg.sqlite`

## Run the launcher

From File Explorer, double-click:

- `scripts\Run MTG Engine Client.bat`

Equivalent command:

- `powershell -NoProfile -ExecutionPolicy Bypass -File scripts\run_client.ps1`

Optional flags:

- `-DbPath "E:\path\to\mtg.sqlite"` to override `MTG_ENGINE_DB_PATH`
- `-BuildUI` to force rebuild of `ui_harness/dist`
- `-ApiPort 8000 -UiPort 5173` to override fixed ports

## Environment variables set by launcher

- `MTG_ENGINE_DB_PATH` (resolved absolute path)
- `MTG_ENGINE_DEV_CORS=1`
- `VITE_API_BASE_URL=http://127.0.0.1:<API_PORT>`

## Create a desktop shortcut

1. Right-click `scripts\Run MTG Engine Client.bat`
2. Select **Send to -> Desktop (create shortcut)**
3. Double-click the desktop shortcut to launch backend + UI

## Stop the launcher

- Use `Ctrl+C` in the launcher console window.
- The script will stop backend and UI child processes.
