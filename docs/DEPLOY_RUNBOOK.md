# Deploy Runbook

Operational procedures for shipping the MTG Engine. Covers Tauri desktop bundle (current ship target) + hosted-mode prereqs (gated on Phase 4.12c) + image cache management + snapshot rotation.

## Pre-deploy checklist

1. **Pre-flight regression green.**
   - Pytest: 711 pass + 3 skip + 2 pre-existing failures (716 total). The 2 pre-existing failures (`test_pipeline_profile_bracket_enforcement_v1` + `test_bracket_gc_limits_v1::test_b4_and_b5_are_unlimited`) are documented; new failures are halt-and-surface.
   - Vitest: 244 across 20 files (post-Phase 4.14) green.
   - Vite build: green; bundle ≤450 kB JS, ≤90 kB CSS.
2. **`validate_manifest_hashes` clean.**
   - All curated packs match their pinned SHA-256 in `api/engine/data/packs/curated_pack_manifest_v1.json`.
3. **Engine SHAs preserved** at the milestone-locked values where unchanged this release. `profile_bracket_enforcement_v1.py` SHA `5a98e4d14f3f4980...` (preserved across 18+ landings) is a canary.
4. **Live-test pass complete.** Engine + UI driven end-to-end:
   - Import an Archidekt URL → workspace populates with the imported deck.
   - Click `1. Complete deck` → 99-card deck completes (~3 sec).
   - Click `2. Build` → SufficiencyDashboard / SwapSuggestionsList / CommanderRecommendationPanel populate.
   - Save deck → entries appear in `#decks` (saved-decks view).
   - Navigate to `#playtest` → goldfish opens with the active deck.
   - Hover any card → image + type + oracle text shown (or "Image not cached." when missing).
5. **CHANGELOG entry added** at the top of `99_META/CHANGELOG.md` describing the release.
6. **Release notes drafted** at `99_META/RELEASE_NOTES_v<version>.md`.

## Tauri desktop bundle (default ship target)

The UI ships as a Tauri-wrapped binary by default per OQ-1 (local-first). Hosted-web-mode is gated on Phase 4.12c (auth + server-DB; deferred).

Build:

```bash
cd repo/ui_harness
npm install
npm run build           # Vite build (output: dist/)

# Tauri (when wired)
cd ../..
# Tauri scaffolding sits alongside the engine; see project_phase_4_v1_shipped
# for the desktop-app architecture pattern. Tauri build target:
#   - Windows: .msi installer
#   - macOS: .dmg
#   - Linux: .AppImage
```

The Tauri bundle includes:
- The Vite-built `dist/` (UI)
- A bundled Python interpreter + the engine package + `mtg.sqlite` (read-only)
- The image cache directory (`data/card_images/`) optionally pre-seeded OR fetched on first run

The Tauri shell intentionally does not terminate externally-started backends. If port 8000 is bound by another process when the user launches the desktop app, the workspace surfaces an `external_backend_warning_mode` banner with retry instructions (existing 4.x behavior).

## Hosted-web-mode prereqs (Phase 4.12c — DEFERRED)

When auth-provider + DB-host decisions land, the lift is:

1. Choose auth provider: Supabase / Auth0 / Firebase / custom JWT. Each adds a small dep.
2. Choose DB host: Supabase / Neon / RDS / etc. for `mtg.sqlite`'s online sibling.
3. Implement `lib/storage/serverAdapter.ts` against the chosen backend (the `StorageAdapter` interface from Phase 4.12a is already in place; only the impl + factory env-flag swap is needed).
4. Wire OAuth callback route. AppRouter currently has 7 hash routes (`#/`, `#workspace*`, `#import`, `#playtest`, `#decks`, `#settings`, `#diagnostics`); auth needs `#auth-callback` + per-user state hydration.
5. Server-side mtg.sqlite + image-cache hosting decisions.
6. Migration: existing localStorage users keep working; opt-in sign-up moves their `mtgdb:decks:*` to the server adapter.

Until 4.12c lands, hosted mode is intentionally not built.

## Image cache management

Card images live at `data/card_images/<oracle_id>.<size>.<ext>`. Sizes: `normal` (default), `small`, `large`. Format: `.jpg` or `.png` per Scryfall's served format.

- **Cache miss handling**: the UI surfaces "Image not cached." (Phase 4.14 Stage 4 copy). The engine's `/cards/image/{oracle_id}?size=<size>` endpoint returns 404 when missing.
- **Pre-seeding**: a Tauri release CAN bundle a pre-seeded cache subset — typical bundle size for top-1000 commanders + their staple cards is ~400 MB. Decide per-release whether to bundle or fetch-on-first-run.
- **Cache size monitoring**: `data/card_images/` grows ~10-50 MB per 100 distinct oracle_ids cached. For long-lived deployments, periodic LRU eviction is reasonable (not currently automated).

## Snapshot rotation

The engine consumes "snapshot" inputs at `data/snapshots/<snapshot_id>/`. Each snapshot represents a set-cycle export of card data + primitive tags + theme assignments.

- **Active snapshot**: latest under `data/snapshots/`. The UI surfaces the active snapshot ID in the `db_snapshot_id` field of every BuildResponse.
- **Adding a new snapshot**: extract via the existing snapshot-build tooling (`tools/snapshot/`), validate via `validate_manifest_hashes`, drop into `data/snapshots/<new_id>/`, retire the old one (keep N=3 for rollback).
- **Snapshot mismatch handling**: if a saved deck references a snapshot_id no longer present, the workspace surfaces a "snapshot not found" structured error. User can either rebuild against the current snapshot OR restore the older snapshot from backup.

## mtg.sqlite

Read-only at runtime. Currently ~750 MB. Growth driven by:
- New card sets (each ~50-100 MB)
- Theme/primitive expansion (smaller — KB scale per pack)

For deploys: bundle the file directly (Tauri); for hosted mode (4.12c), serve via read-only volume mount or embed via volume mount.

## Rollback

See `99_META/ROLLBACK_PLAN.md` for the per-release reversion procedure. Quick version:

1. `git checkout <prior-release-tag>`
2. `validate_manifest_hashes` clean against the prior release's manifest.
3. Re-run Tauri build (or revert Vite-built `dist/`).
4. Restore `data/snapshots/<id>/` if the rollback predates a snapshot rotation.
5. Document the rollback reason in `99_META/CHANGELOG.md` (newest entry).

## Monitoring suggestions (when hosted-mode lands)

- `/health` endpoint latency (UI's `useHealthPrewarm` already pings on App mount).
- `/build` p50 / p95 / p99 wall-clock. Engine target: <8 sec warm; cold start ~1.4 sec.
- `/import/url` success rate by source (Archidekt = ACTIVE; Moxfield + EDHREC = DEFERRED).
- localStorage quota errors (workspace logs them via `devLog` in dev; silent in production).
- 4xx / 5xx rate per route.

## Contact

Engine + UI architecture: `Mtg deck building brain/99_META/Home.md` is the entry point to the design vault.
