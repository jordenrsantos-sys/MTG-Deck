# Mega-task v5 progress log

Append-only timestamped record of phase execution. Same format as v1-v4 logs.

## Phase 0 — Pre-flight + progress log scaffold

**Started**: 2026-05-21 (kickoff initiation)

### Environment

- Python 3.10.11 ✓
- ANTHROPIC_API_KEY set ✓
- VOYAGE_API_KEY set ✓
- Git HEAD: `e97589870` ("Phase 14 (mega-task v4): final regression + report + memory update")
- Untracked at pre-flight: `mega_task_v5_kickoff.md`, `primitives/llm_supplement_audit_v1.json`, `engine_path_test.md`
- MCPs available: mtg-engine, obsidian, figma
- **MCP gap noted**: chrome-devtools-mcp is NOT in the available MCP list. Phases 5 + 13 require it for live UI validation per kickoff. If unavailable at Phase 5, that triggers hard halt #7 unless a non-MCP fallback (manual user verification, Playwright-via-bash, etc.) is acceptable. Will diagnose at Phase 5 entry.

### Test baselines (measured this session)

- pytest collected: **1402** tests (kickoff baseline expected 1377 + new tests; +25 over v4 final report; no new commits between HEAD and v4 ship, so the +25 reflects revision of count between v4 report writing and HEAD).
- vitest collected: **713** tests (kickoff baseline 711 + 2 known failures = matches).
- Pre-existing failures expected: ~8 pytest + 2 vitest. Will verify against `pytest -q` once at Phase 0 commit and after each subsequent phase.

### Files reviewed (the 9 required by kickoff)

1. ✓ `memory/project_iter_6_prep_notes_2026-05-21.md` — locked iter 6 priorities
2. ✓ `memory/project_mega_task_v4_shipped_2026-05-21.md` — v4 ship state
3. ✓ `memory/project_graduated_playtest_spec_2026-05-21.md` — Stage 1 detail
4. ✓ `memory/feedback_pool_score_does_not_drive_llm_picking.md` — autonomous CC lesson
5. ✓ `repo/api/engine/data/agent/mega_task_v4_final_report.md` — v4 hand-off priorities
6. ⏸ `pillar_d_iteration_5_validation_report.md` — deferred read until needed (metrics same as v4 final report table)
7. ⏸ `repo/api/engine/layers/agent_build_deck_v1.py` — 4536 lines; will read targeted sections at Phase 3 / 6 / 8
8. ✓ `repo/ui_harness/src/views/AIBuildView.tsx` — 766 lines; reviewed in full
9. ⏸ `repo/api/main.py` — 3103 lines; will read targeted sections at Phase 1 / 3

### Key context for execution

- The actual uvicorn launch lives in `repo/launch.py::_start_api_process` (not `launch_dev.cmd`). Phase 1 will modify launch.py to add `--workers N`.
- Cowork memory path: `C:\Users\jorde\AppData\Local\Packages\Claude_pzs8sxrjxfjjc\LocalCache\Roaming\Claude\local-agent-mode-sessions\9f2d68e4-6579-41dd-a8ca-3462c3f52398\a461a706-2a03-44fd-8292-3267addb5d29\spaces\d463abef-278c-4a7e-b5e3-34c83dad7ccc\memory\` — MEMORY.md not yet located but individual memory files findable by name.
- Untracked `engine_path_test.md` and `primitives/llm_supplement_audit_v1.json` predate this session; not touching.

### Phase 0 commit

`74abcc8f8` — "Phase 0 (mega-task v5): pre-flight + progress log scaffold". 2 files, 634 insertions.

---

## Phase 1 — uvicorn workers ≥ 2 (BLOCKING)

**Started**: 2026-05-21 (immediately after Phase 0 commit)

### Implementation

Kickoff said "Update `launch_dev.cmd`" but `launch_dev.cmd` is a thin wrapper that delegates to `launch.cmd` → `launch.py`. The actual uvicorn invocation lives in `launch.py::_start_api_process`. Updated launch.py instead:

- New module-level constants `DEFAULT_API_WORKERS = 2` + `API_WORKERS_ENV = "MTG_ENGINE_API_WORKERS"` for env-var override.
- New helper `_resolve_api_workers()` resolves env-var → default with int validation + clamping to ≥1.
- `_start_api_process()` appends `--workers N` to the uvicorn argv when N > 1.

### Verification

- **Boot smoke**: `python -m uvicorn api.main:app --host 127.0.0.1 --port 8099 --workers 2` — parent process PID 35924 spawned two worker processes (44804, 38224); both reported "Application startup complete." within ~5s.
- **Concurrent /health smoke**: 5 parallel `curl /health` requests via xargs → all 200 in 20-44ms each. 30 parallel requests via Python ThreadPoolExecutor → mean 45ms, max 69ms, min 22ms, 100% 200.
- **Unit tests**: 7 new tests in `tests/test_launch_worker_resolution.py` covering default, int override, garbage falls back, whitespace falls back, zero/negative clamp to 1.

### Regression baselines (after Phase 1)

- **pytest**: 1384 passed (1377 prior + 7 new), 8 pre-existing failures (matches v4 baseline), 17 skipped. 104s.
- **vitest**: 711 passed, 2 pre-existing failures (matches v4 baseline). 1.66s.

### Notes

The full "/health is fast WHILE a build is in flight" property is implied by:
1. Two worker processes confirmed booted independently.
2. /health responsive under burst load (rules out single-worker bottleneck on the static endpoint).
3. The agent_build_deck_v1 endpoint is `async def` wrapping a sync 110-150s call; on a single worker it would block the event loop, but with two workers a non-build request lands on the idle worker. This will be live-validated during Phase 5 chrome-devtools walk with a real build in flight.

`launch_dev.cmd` itself was untouched — it remains a 1-line wrapper around `launch.cmd`. The intended behavior is satisfied by the launch.py change (and the env-var override gives an escape hatch).

### Phase 1 commit

`3e5ff5f07` — "Phase 1 (mega-task v5): uvicorn multi-worker for concurrent request handling".

---

## Phase 2 — Auto-default snapshot_id + field placeholder/help text

**Started**: 2026-05-21 (immediately after Phase 1 commit)

### Implementation

**Backend** (`api/main.py`):
- New `GET /snapshots/active` endpoint near the existing `/snapshots` and `/snapshot/preflight/{id}` routes. Returns `{"snapshot_id": <str>}` from the existing `_latest_snapshot_id()` helper. Returns empty string (not 404) when the snapshot list is empty so the UI can fall back to disabled-Build.

**UI** (`ui_harness/src/views/AIBuildView.tsx`):
- New `useEffect` on mount fetches `/snapshots/active` and populates `snapshotId` state automatically. Silent fallback on fetch failure — user can still set manually via Advanced.
- New `snapshotAutoLoaded` boolean drives an "(auto-populated from active snapshot)" hint next to the Snapshot ID label when the auto-fetch succeeded.
- Snapshot ID input moved into a `<details data-testid="advanced-options">` collapsible (collapsed by default via `useState(false)`). The default form is now Commander → Bracket → Theme hints → Must-includes → Build.
- Improved placeholder text per kickoff spec:
  - Commander: `"e.g., Edgar Markov"` (was `"e.g. Edgar Markov"`)
  - Theme hints: `"e.g., aristocrats, graveyard recursion (optional — agent infers from cards)"` (was `"e.g. TYPAL_VAMPIRES"`)
  - Must-includes: `"e.g., Vito, Thorn of the Dusk Rose"` (was `"e.g. Vito, Thorn of the Dusk Rose"`)
  - Snapshot ID: `"e.g., 20260217_190902_tagpass_20260222"` (was `"e.g. SCRYFALL_2026_03_15"` — that format never matched real snapshots)

### Tests

- **Backend**: `tests/test_snapshots_active_endpoint.py` — 2 tests (latest available + empty snapshot list).
- **UI**: `ui_harness/src/views/__tests__/AIBuildViewPhase2UX.test.ts` — 11 tests covering: useEffect mount fetch, snapshotAutoLoaded flag, silent fallback, Advanced details block presence, Snapshot ID inside Advanced (not at top level), collapsed-by-default state, all 4 placeholder strings, and an inverse check that Snapshot ID is NOT in the top-level form area.

### Regression baselines (after Phase 2)

- **pytest**: 1386 passed (1384 prior + 2 new). 8 pre-existing failures unchanged.
- **vitest**: 722 passed (711 prior + 11 new). 2 pre-existing failures unchanged.

### Phase 2 commit pending

---
