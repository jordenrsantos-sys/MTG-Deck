# New-Set Watcher (mega-task v3 Phase 1) - runbook

The watcher polls Scryfall's `/sets` endpoint daily and reports any
set codes not yet in our corpus ledger. On detection, the parent
scheduler should invoke `tools/ingest_new_set.py <code>` (Phase 2)
to pull and ingest the cards.

## Components

- `api/engine/integrations/scryfall_sets_watcher_v1.py` — Scryfall
  client + new-set detector + known-codes ledger.
- `api/engine/data/scripts/known_set_codes_v1.json` — the ledger.
  Seeded once from `cards_raw` via `--init-from-corpus`.
- `tools/check_new_sets.py` — CLI. Exit 1 on detect; 0 on no-op.
- `tools/install_set_watcher_schedule.ps1` — Windows Task Scheduler
  install (the "scheduled-tasks MCP" called out in the kickoff isn't
  available; the OS scheduler is the locally-available equivalent).

## Install

```powershell
# One-time: seed the ledger from your already-ingested corpus.
python tools\check_new_sets.py --init-from-corpus

# One-time: install the daily scheduled task (06:03 local).
powershell -ExecutionPolicy Bypass -File tools\install_set_watcher_schedule.ps1 -Install

# Verify
schtasks /Query /TN MTGEngine.NewSetWatcher /V /FO LIST

# Manual run
python tools\check_new_sets.py
python tools\check_new_sets.py --json
```

## Uninstall

```powershell
powershell -ExecutionPolicy Bypass -File tools\install_set_watcher_schedule.ps1 -Remove
```

## Exit codes

- `0` — no new sets
- `1` — one or more new sets detected
- `2` — error (Scryfall unreachable, ledger corrupt, etc.)

## Rate-limit policy

Per Scryfall's published guidelines: max 10 req/s; the client uses
exponential backoff (1s, 2s, 4s) on 429 / 5xx and `User-Agent:
mtg-engine-mega-task-v3-watcher/1.0`.
