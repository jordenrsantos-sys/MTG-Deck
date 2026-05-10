# Onboarding

New-contributor guide. Assumes you've read the project `README.md` and `repo/README.md`.

## Mental model in 5 minutes

The codebase has three layers, in directionally-strict order:

1. **Calibration** (offline). `tools/playtest/` runs simulations against external deck corpora; outputs feed user-reviewed curation; the curated result lands in `api/engine/data/calibration/*.json` flagged `calibration_only:true`.
2. **Curated packs** (on-disk). All inputs the engine consumes are JSON packs registered in `api/engine/data/packs/curated_pack_manifest_v1.json` with SHA pins. The runtime loader (`api/engine/curated_pack_manifest_v1.py`) skips packs flagged `calibration_only:true`.
3. **Runtime engine** (online). FastAPI app at `api/main.py` exposes `POST /build` (full pipeline), `POST /build/partial` (named layers only), `POST /import/url`, plus card-data endpoints. The pipeline orchestrator lives at `api/engine/pipeline_build.py`; per-stage layers live at `api/engine/layers/<layer_id>.py`.

The UI is a React workspace (`ui_harness/`) that consumes `BuildResponse` directly. It never calls calibration tooling.

## Your first task

A typical task looks like this in `control/TASK_BOARD.md`:

```
- Description: <what to do + why>
- Pre-implementation discovery (Stage 0): <what to read first>
- Files to update: <explicit list>
- HARD safeties: <halt-and-surface conditions>
- SOFT safeties: <autonomous-repair allowed>
- Halt-and-surface conditions per stage
- Resumability
- Out of scope
- Expected close-out
```

Per `control/AUTOMATION_RULES.md`, every task kickoff carries standing requirements unless explicitly waived:

1. Read all four control files (`PROJECT_CONTEXT.md`, `TASK_BOARD.md`, `AUTOMATION_RULES.md`, `OBSIDIAN_API.md`).
2. Pre-flight regression (targeted pytest suite) + `validate_manifest_hashes` clean.
3. Sanity-import any new/modified Python module via the dataclass-quirk-aware pattern (`importlib.util.spec_from_file_location` + `sys.modules[name] = module` + `spec.loader.exec_module`).
4. Pre-existing failures stay out of scope unless the task addresses one.
5. Frozen contracts (`ui_contract_v1` / `structural_snapshot_v1` / `graph_v1`) don't widen without explicit halt-and-ask.
6. Calibration boundary discipline.
7. Closed-world rule.
8. Halt-and-surface discipline — set STATUS=blocked rather than invent workarounds.

## Testing strategy

Three layers, each catches different bugs:

- **Pytest** (`tests/`): engine layer correctness + pack shape + manifest integrity + frozen-snapshot drift.
- **Vitest** (`ui_harness/src/**/__tests__/`): pure helpers + reducer transitions + adapter logic. Vitest config uses `environment: "node"` (no jsdom; no `@testing-library/react` dep). Component-level tests render via direct call; integration coverage extracts pure helpers (`extractSufficiencySummary`, `buildWorkspacePillText`, etc.) that the React shell consumes.
- **Live test** (manual): start the engine + UI, drive a real flow end-to-end, observe browser DevTools + the engine's logs. **Live test catches what unit tests miss** — race conditions, prop-pass-through gaps, copy-text bugs, hover-cache misses.

The integration tests (`workspaceIntegration.test.ts` etc.) cover the wiring contract: given a BuildResponse, do the extracted adapters correctly project to component props? They don't render React — they test the regression surface that allowed Phase 4 BUNDLE Integration's panel-orphaned bug.

## Common pitfalls

- **DeckEditorPanel + DeckPanel + CardHoverPreview internals are locked.** They're the canonical visual deck-rendering components. Don't touch their internals; wire AROUND them via the existing prop interface.
- **DeckInputPanel adapter prop SHA `18ecdac40880f36d...`** must stay BYTE-IDENTICAL. The 4.3 contract is a hard invariant.
- **Don't write to `mtg.sqlite`.** Read-only at runtime.
- **Don't add runtime network calls.** No Scryfall fetches, no oracle-text scrapes. The closed-world rule is non-negotiable.
- **Don't widen frozen contracts** without explicit halt-and-ask.
- **Use the storage adapter** for new persistence. `lib/storage/` is the abstraction; new keys land under `mtgdb:*` namespaces.
- **Reducer for shared deck state.** `lib/workspaceDeckState.ts` is the canonical state machine. New deck-related fields extend the reducer, not parallel useState.
- **Stable JSON serialization** for any pack you author: `sort_keys=True, ensure_ascii=False, separators=(",", ":"), single trailing newline`.

## Running the offline sim framework

The Phase 5b playtest framework (`tools/playtest/`) is calibration-only. Don't import it from runtime code (it's enforced — the runtime loader skips `calibration_only:true` packs).

Typical use: drift sweeps. Run `tools/playtest/run_<sweep>.py` against a corpus, inspect the report at `05_VALIDATION/PLAYTEST_DRIFT_REPORT_<date>.md`, queue items for user review, write the curated outcome into `api/engine/data/calibration/external_decks_v1.json` + `playtest_changelog_v1.json`.

## Editing the vault

The Obsidian vault at `../Mtg deck building brain/` is the design layer. When a task implementation surfaces a finding of lasting value (architecture fact, data shape surprise, decision made), write it into the relevant vault page AND add a one-line entry to `99_META/CHANGELOG.md`. Ephemeral chatter stays in `control/TASK_BOARD.md` EXECUTION LOG.

When the Obsidian REST API is reachable (check `OBSIDIAN_API_KEY` env var), prefer it over filesystem writes for vault edits — see `OBSIDIAN_API.md` for endpoints. Otherwise fall back to file tools.

## Close-out attestation

Every EXECUTION LOG entry ends with a structured attestation block:

```
=== CLOSE-OUT ATTESTATION ===
files_touched:
  - path: <repo-relative path>
    sha256: <hex>
    operation: <created|modified|deleted>
targeted_regression: <count>/<count> PASSED
validate_manifest_hashes: <clean|FAIL details>
sanity_import: <clean|FAIL details>
pre_existing_failures_unchanged: <yes|list>
=== END ATTESTATION ===
```

This is the source of truth for what changed. The Cowork bash sandbox verifies each path + sha256 against the live filesystem; mismatches almost always mean bash mount lag (trust the attestation).

## Asking for help

`control/TASK_BOARD.md` is the conversation. New tasks land there with HARD safeties + SOFT auto-repair conditions. Halt-and-surface when blocked — set `STATUS=blocked` with a one-line `STATUS.Notes` naming the blocker, append the partial work to EXECUTION LOG, stop.
