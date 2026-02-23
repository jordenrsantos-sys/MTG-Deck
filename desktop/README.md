# MTG Engine Desktop Wrapper (Tauri v2)

This desktop app wraps the existing production serve mode.
It starts the local backend if needed, waits for it to become ready, opens a desktop window, and shuts the backend down on close when the desktop app launched it.

## Prerequisites

1. Rust toolchain + Cargo
2. Tauri CLI (Node-based):
   - from `desktop/`: `npm install`
3. Python environment used by this repo (prefer `repo/.venv`)

## Production prerequisites

The desktop app relies on backend production static serving (`launch_prod.py` -> FastAPI serves UI dist), but UI files are bundled into desktop resources for packaging.
It also ships a baseline SQLite DB snapshot (`mtg.sqlite`) as a bundled resource.

Build UI first:

```bash
cd ui_harness
npm run build
```

Sync desktop resources (UI dist + baseline DB):

```bash
cd ../desktop
npm run sync:resources
```

No Vite server is used at runtime.

## Build packaged desktop app

From repo root:

```bash
cd desktop
npm install
npm run tauri:build
```

`tauri:build` runs resource sync first, then bundles:
- `desktop/resources/ui_dist`
- `desktop/resources/ui_dist_version.txt`
- `desktop/resources/mtg.sqlite`

At runtime, the app extracts bundled UI into app data (`<app_data>/ui_dist`) and copies baseline DB to `<app_data>/mtg.sqlite` on first run only.
Then it launches backend with:
- `MTG_ENGINE_UI_DIST_DIR=<app_data>/ui_dist`
- `MTG_ENGINE_DB_PATH=<app_data>/mtg.sqlite`

This makes installed desktop UI/DB serving independent from repo-relative paths.

### App data location

- Windows (typical): `%APPDATA%\com.mtgengine.desktop\`
- Runtime files of interest:
  - `%APPDATA%\com.mtgengine.desktop\ui_dist\...`
  - `%APPDATA%\com.mtgengine.desktop\mtg.sqlite`
  - `%APPDATA%\com.mtgengine.desktop\card_images\normal\...`
  - `%APPDATA%\com.mtgengine.desktop\card_images\small\...`

### Image cache behavior

- Desktop startup bootstraps image cache folders:
  - `<app_data>/card_images/normal`
  - `<app_data>/card_images/small`
- Backend is launched with `MTG_ENGINE_IMAGE_CACHE_DIR=<app_data>/card_images`.
- The app never deletes or overwrites existing cached image files.

### Optional: prefetch images into desktop cache (advanced)

If you want to prefill the desktop cache from a known snapshot, run from repo root:

```bat
set MTG_ENGINE_IMAGE_CACHE_DIR=%APPDATA%\com.mtgengine.desktop\card_images
python -m snapshot_build.prefetch_card_images --db "%APPDATA%\com.mtgengine.desktop\mtg.sqlite" --snapshot_id <SNAPSHOT_ID> --source card_images --out "%MTG_ENGINE_IMAGE_CACHE_DIR%" --sizes normal,small --workers 4 --resume --progress 100
```

This is optional maintenance/setup work and not part of normal offline runtime.

> Existing `%APPDATA%\...\mtg.sqlite` is never overwritten automatically.

### Advanced: replace DB with a newer snapshot

1. Close the desktop app.
2. Backup current app-data DB.
3. Replace `%APPDATA%\com.mtgengine.desktop\mtg.sqlite` with your newer SQLite snapshot.
4. Start the desktop app.

The app keeps your existing DB file and does not overwrite it on startup.

## Desktop dev run

From repo root:

```bash
cd desktop
npm install
npm run tauri:dev
```

Or use the convenience launcher on Windows:

```bat
launch_desktop.cmd
```

## Offline behavior

- Local-first/offline runtime only.
- WebView target is `http://127.0.0.1:8000/`.
- No runtime internet calls are introduced by this wrapper.

## Troubleshooting

### Port 8000 already in use

The wrapper checks port `8000` on startup.
If it is already open, it assumes backend is already running and does not spawn/kill backend.

### UI not built (`ui_harness/dist` missing)

If UI build/sync resources are missing, run:

```bash
cd ui_harness
npm run build
cd ../desktop
npm run sync:resources
```

Then restart desktop app.
