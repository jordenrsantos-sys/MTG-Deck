# Ops Considerations

What you should know before the project sees real users. Local-first today; the considerations below scale across both desktop and (deferred) hosted modes.

## Storage growth

### `mtg.sqlite`

Currently ~750 MB. Read-only at runtime. Growth drivers:

- **New MTG sets**: ~50-100 MB per set release (every 3-4 months — 3-4 standard sets/year + Commander/Modern Horizons cycles).
- **Theme/primitive pack expansion**: KB-scale per pack revision (`themes_v1_5.json`, `primitive_index_v1.json`, etc.).
- **Snapshot rotation**: 3-snapshot retention (current + 2 prior) at ~750 MB each = ~2.25 GB if all kept on-disk.

**Mitigation:**
- Tauri bundles only the active snapshot (~750 MB). Older snapshots live in cloud storage or release artifacts.
- Hosted mode (4.12c) keeps snapshots server-side; per-user storage is the saved-decks index only (negligible).

### Image cache (`data/card_images/`)

- Per-image: ~50-200 KB JPEG.
- Per-100 distinct oracle_ids cached: ~10-50 MB.
- Top-1000 commanders + their staple cards: ~400 MB.
- Long-running cache: can grow to 2-5 GB without eviction.

**Mitigation:**
- LRU eviction tooling lives at `tools/cards/cache_eviction.py` (planned; not yet wired). Manual: `du -sh data/card_images/` periodically; `find data/card_images -atime +90 -delete` for 90-day eviction.
- Tauri release: ship a pre-seeded subset; populate the rest on first hover (~80ms per image fetch from Scryfall via the engine's proxy).

## Backup strategy

### What to back up
1. **`api/engine/data/`** — curated packs (calibration / themes / synergy / etc.). The manifest pins SHAs; backup is git-managed for the pack files.
2. **`data/snapshots/<id>/`** — active snapshot. ~750 MB per snapshot.
3. **User data**: localStorage under `mtgdb:*` namespaces. Not in git; user's responsibility on desktop. Hosted mode (4.12c) backs up server-side.
4. **`Mtg deck building brain/`** — Obsidian vault (design layer). Git + Obsidian Sync.

### What NOT to back up
- `data/card_images/` — re-fetchable from Scryfall via `/cards/image/<oracle_id>` (the cache rebuilds on demand).
- `node_modules/`, `.venv/`, `__pycache__/`, `dist/` — all reproducible from git + lock files.

### Recovery

- **Pack corruption**: re-fetch from git OR re-run the pack-build tooling (`tools/pack_build/<pack_name>.py`). Manifest hash mismatch surfaces immediately at startup.
- **Snapshot missing**: structured error in `BuildResponse` per Phase 6 Stage 6 hardening. User can restore the snapshot from backup OR rebuild against the current snapshot.
- **mtg.sqlite corrupt**: replace from the most recent backup. The DB is read-only, so corruption is rare (filesystem-level only).

## Performance

### Engine `/build` budget

- **Cold start**: 1.4 sec (Phase 4.6 latency probe verified). Driven by initial pack loading + module import. The UI's `useHealthPrewarm` (Phase 4.10) fires `GET /health` on App mount to amortize this.
- **Warm `/build`**: 5-7 ms / TestClient + in-process measurement (Phase 4 BUNDLE close-out). Wall-clock through HTTP: 50-200 ms typical.
- **Per-layer budget** (Phase 6 Stage 6 instrumentation, planned): <500 ms per layer warm; <8 sec total. If a layer regresses past 500 ms, surface in the per-layer timing report at `05_VALIDATION/PERF_TIMING_REPORT_<date>.md`.

### `/build/partial`

For seed-builder responsiveness. 200 ms debounce per autonomous_repair_log #4. Triggers `seed_synergy_detection_v1 + commander_recommendation_v1` only — full pipeline still runs (the partial endpoint runs the full pipeline then filters the response; a true short-circuiting executor is a Phase 6 follow-up).

### UI bundle

- Current: ~376 kB JS (Phase 4.14 post-GroupedDeckList tree-shake), ~87 kB CSS. Under the 450 kB lazy-load threshold.
- Largest contributors: the workspace tree (DeckEditorPanel + DeckPanel + WorkspaceView wrappers) + the playtest module (`lib/goldfishState` + `lib/usePlaytest` + 6 zone components).
- If bundle grows past 450 kB, lazy-load via `React.lazy` candidates: GoldfishView, SettingsView, LandingView (each is independently routed).

## Monitoring (when hosted-mode lands)

- **`/health` latency** — useHealthPrewarm pings on every App mount; success ≤200 ms.
- **`/build` p50 / p95 / p99 wall-clock** — alert on p95 >2 sec.
- **`/import/url` success rate per source** — Archidekt should sit ≥95%; Moxfield + EDHREC are DEFERRED (always return `status:"DEFERRED"`).
- **404 rate on `/cards/image/<oracle_id>`** — high rate → image cache eviction is too aggressive OR the cache is genuinely cold (re-seed considered).
- **5xx rate per route** — alert on any sustained 5xx (engine layer error is structured per Stage 6 hardening; non-zero rate means cards table missing OR snapshot missing OR a real bug).

## Concurrency

- **Snapshot rotation race**: while a build is in-flight, replacing the active snapshot can race. Mitigation: snapshot updates lock the active-snapshot symlink atomically (`os.replace`); in-flight builds either complete with the prior snapshot OR fail with a structured error and retry.
- **Multi-user (hosted-mode 4.12c)**: per-user state isolation is the storage adapter's responsibility. The engine itself is stateless per request; the snapshot + packs are shared read-only.

## Calibration corpus

- `api/engine/data/calibration/external_decks_v1.json` is the curated external-deck corpus (SHA `6d5c6af51ff7b...` per Phase 5a verification).
- `playtest_changelog_v1.json` documents per-deck dispositions (auto-accepted / user-overridden / disputed).
- Refreshes happen via `tools/update_external_deck_corpus.py`. Each refresh changes the corpus SHA; pinned tests must be updated alongside.
- The runtime engine never reads these — they're flagged `calibration_only:true` and skipped by `iter_runtime_pack_entries`.

## Security posture

- **Closed-world rule**: no runtime network calls, no oracle-text parsing at runtime, no inventing data. Reduces attack surface — the engine accepts user input (deck text + URL imports), validates against curated packs, returns structured output. No code-eval paths.
- **`/import/url`** (Engine-4A): server-side fetch acts as CORS proxy; SSRF surface limited to Archidekt's API host (whitelisted in `import_url_v1.py`'s parser registry). No arbitrary URL fetch.
- **No auth in v1.0** (4.12c deferred). Local-first; users own their localStorage.

## Logging

- Engine: `print` to stdout (uvicorn captures). Structured-log migration planned for Phase 6.1+ if hosted-mode lands.
- UI: `lib/devLog` wraps console. DEV-only; production builds drop the calls (`import.meta.env.DEV` gate).

## When something goes wrong

1. Check `99_META/CHANGELOG.md` newest entry — what shipped most recently?
2. Check `control/TASK_BOARD.md` — STATUS state + EXECUTION LOG.
3. Reproduce against `mtg.sqlite` snapshot ID + the request's `build_hash_v1` — same inputs → byte-identical output. If output diverges, non-determinism leaked in (halt-and-surface).
4. `validate_manifest_hashes` — pack drift surfaces here.
5. `99_META/ROLLBACK_PLAN.md` for the safe reversion procedure.

## Future considerations

- **Phase 4.12c**: hosted mode + auth + per-user persistence. Gated on user direction re: auth provider + DB host.
- **Phase 6.1+**: structured logging, perf instrumentation, automated cache eviction, snapshot rotation tooling.
- **Phase 7+**: native mobile (React Native / Expo) — long-term horizon.
