# UI Harness (Phase 1)

Desktop-first UI harness for viewing MTG Engine `/build` payloads using `UI_CONTRACT_v1.md`.

## What this harness includes

- File mode (loads fixture JSON from `./ui_harness/fixtures/build_result.json`)
- API mode (calls local FastAPI `/build`)
- Header chips + analysis status bar
- Optional v2 panels:
  - `commander_dependency_v2`
  - `engine_coherence_v2`
  - `stress_transform_engine_v2`
  - graph bounds caps-hit indicators
- Canonical/debug slots panel
- Fast local card suggest search (`/cards/suggest`):
  - starts after 2 chars
  - 60ms debounce
  - max 20 results
  - keyboard navigation (arrow keys, Enter, Tab autocomplete)
  - hover preview with 150ms delay and fade-in
  - offline-safe art behavior (local URI only; otherwise placeholder)

## Run

From `repo/ui_harness`:

```bash
npm install
npm run dev
```

Open the URL shown by Vite (default `http://localhost:5173`).

## API mode setup (optional)

1. Run the backend locally (default expected base: `http://localhost:8000`).
2. In the harness, switch to **API mode**.
3. Confirm API base URL in the control panel.
4. Fill snapshot/profile/bracket/commander and run `/build`.

You can also set:

```bash
VITE_API_BASE_URL=http://localhost:8000
```

in `ui_harness/.env`.

## Fixture notes

- The included fixture file is: `ui_harness/fixtures/build_result.json`
- It is a real build payload extracted from a local repro bundle.
