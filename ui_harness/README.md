# UI Harness (Phase 1)

Desktop-first UI harness for viewing MTG Engine `/build` payloads using `UI_CONTRACT_v1.md`.

## What this harness includes

- File mode (loads fixture JSON from `./ui_harness/fixtures/build_result.json`)
- API mode (calls local FastAPI `/build`)
- Deck input + build runner panel:
  - commander autocomplete via `/cards/suggest`
  - decklist paste + deterministic normalized preview (`1 Card Name` lines sorted by card name)
  - supports `1 Card`, `1x Card`, and `Card` input
  - ignores blank lines and full-line comments starting with `#` or `//`
- Header chips + analysis status bar
- Optional v2 panels:
  - `commander_dependency_v2`
  - `engine_coherence_v2`
  - `stress_transform_engine_v2`
  - graph bounds caps-hit indicators
- Canonical/debug slots panel
- Primitive Explorer (scaffold, read-only)
- Fast local card suggest search (`/cards/suggest`):
  - starts after 2 chars
  - 60ms debounce
  - max 20 results
  - keyboard navigation (arrow keys, Enter, Tab autocomplete)
  - hover preview with 150ms delay and fade-in
  - offline-safe art behavior (backend local image cache only; otherwise placeholder)

## Run

From `repo/ui_harness`:

```bash
npm install
npm run dev
```

Open the URL shown by Vite (default `http://127.0.0.1:5173`).

## API mode setup (optional)

1. Run the backend locally (default expected base: `http://127.0.0.1:8000`).
2. In the harness, switch to **API mode**.
3. Confirm API base URL in the control panel.
4. Fill snapshot/profile/bracket/commander and run `/build`.

You can also set:

```bash
VITE_API_BASE_URL=http://127.0.0.1:8000
```

in `ui_harness/.env`.

## Deck input + build runner quick use

1. Set **Data mode** to **API mode**.
2. In **Deck input + build runner**, fill commander + decklist and adjust snapshot/profile/bracket if needed.
3. Click **Run build**.
4. On success, the returned build JSON is loaded into the existing Phase 1 panels automatically.

Notes:
- In file mode, Snapshot ID defaults to the fixture snapshot.
- In API mode, if Snapshot ID is blank, the harness resolves latest snapshot from `/snapshots?limit=1` before posting `/build`.

## Local image cache behavior

- Hover art previews are served only from local backend cache endpoint:
  - `GET /cards/image/{oracle_id}?size=normal`
- Cache directory is resolved from `MTG_ENGINE_IMAGE_CACHE_DIR` (default: `./data/card_images`).
- The UI never renders remote `https://...` image links directly.
- If an image is not cached, `/cards/image/...` returns `MISSING_IMAGE` and the UI keeps a placeholder (`Not cached in local image cache.`).

## Update Mode prefetch (explicit opt-in)

Prefetch image binaries into local cache from URLs already stored in your local DB snapshot metadata:

```bash
python -m snapshot_build.prefetch_card_images --db ./data/mtg.sqlite --snapshot_id <id> --out ./data/card_images --size normal --limit 500
```

Notes:
- Update Mode only (network allowed for the script).
- Runtime API/UI remain local-cache-only and do not fetch remote images.

## Fixture notes

- The included fixture file is: `ui_harness/fixtures/build_result.json`
- It is a real build payload extracted from a local repro bundle.
- Primitive Explorer scaffold requires build outputs containing primitive index fields (`primitive_index_by_slot` and/or `slot_ids_by_primitive`).
