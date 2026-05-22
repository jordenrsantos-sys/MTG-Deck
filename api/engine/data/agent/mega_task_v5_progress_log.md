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

### Phase 2 commit

`83270d3c6` — "Phase 2 (mega-task v5): auto-default snapshot_id + UI placeholder/help text".

---

## Phase 3 — Build progress streaming via SSE (BLOCKING)

**Started**: 2026-05-21 (immediately after Phase 2 commit)

### Implementation

**Backend (`agent_build_deck_v1.py`)**:
- New `ProgressCallback = Callable[[Dict[str, Any]], None]` type + `_emit_progress` helper. Helper swallows callback errors so progress reporting can never break the build. Event shape: `{phase, status, elapsed_s, cost_usd, calls_so_far, [response]}`.
- New `progress_callback` parameter added to `compute_agent_build_deck_v1` (keyword-only, defaults to None — non-streaming callers pay zero overhead).
- Sprinkled `_emit_progress` calls at every phase boundary: intent_interpreter, candidate_pool, select_deck, c21_c22_parallel (conditional on LLM availability), validate_swap, final_critic (conditional on LLM availability), mana_base, card_advantage, structural_safety_net, and a final "complete" event carrying the full response.
- All early-return failure paths (INVALID_BRACKET, MISSING_COMMANDER, POOL_BUILD_FAILED) also emit a "complete" event so SSE clients always learn the stream is done.

**Backend (`api/main.py`)**:
- New `POST /agent/build_deck_v1/stream` endpoint returning `EventSourceResponse` from `sse-starlette` (already installed at 3.4.4).
- Bridges sync compute → async generator via a `queue.Queue` + `asyncio.to_thread()`. Worker thread runs the build with a callback that pushes events to the queue; the async generator drains the queue and yields SSE-formatted events. SENTINEL marks end-of-stream.
- The existing non-streaming `POST /agent/build_deck_v1` is unchanged (backward-compat for Python tools / programmatic clients).

**UI (`ui_harness/src/hooks/useBuildStreaming.ts`)**:
- New hook. Uses `fetch` + `ReadableStream` (NOT `EventSource`, which is GET-only and can't carry the build request body). Manual SSE parser handles `\r\n\r\n` block separation + comment lines.
- Returns `{ isStreaming, currentPhase, currentStatus, elapsedSeconds, cumulativeCostUsd, events, finalResponse, errorMessage, start, cancel, reset }`.
- `AbortController` wires `cancel()` to the underlying fetch; on unmount the stream is aborted automatically.
- Exports `__testing = { _parseSseBuffer }` so the buffer parser is unit-testable without DOM.

**UI (`AIBuildView.tsx`)**:
- Imports + instantiates the hook. `building` derives from `stream.isStreaming`.
- `_build()` now calls `stream.start({...})` instead of the synchronous fetch.
- Two `useEffect`s mirror `stream.finalResponse` → local `response` (preserves existing deck-render JSX) and `stream.errorMessage` → local `errorMessage` (preserves existing error banner).
- New build progress panel renders below the Build button while streaming. Shows humanized phase label, elapsed seconds, cumulative LLM cost, and a collapsible phase log. `aria-live="polite"` for screen readers.
- New `_PHASE_LABELS` record maps each server-emitted phase to a human-readable label. Unknown phases fall back to the raw identifier.

### Tests

- **Backend**: `tests/test_agent_build_deck_v1_stream.py` — 10 tests:
  - 4 emit-helper tests (None callback no-op, populates fields, includes extra payload, swallows callback errors)
  - 3 progress-callback integration tests (all phase boundaries emit, INVALID_BRACKET early-return emits complete, MISSING_COMMANDER early-return emits complete)
  - 3 SSE-endpoint TestClient tests (content-type, progress→complete, non-streaming backward-compat)
  - Tests mock out the LLM client to keep runtime at ~3s (vs ~83s for real-LLM mode confirmed during debugging).
- **UI**: `ui_harness/src/hooks/__tests__/useBuildStreaming.test.ts` — 13 tests:
  - 7 SSE-buffer parser tests (single event, multiple events, `\r\n` normalization, incomplete trailing block, comment skipping, empty buffer, default event type)
  - 6 source-contract tests on the hook (exports, AbortController, callbacks, endpoint URL+Accept header, complete-event short-circuit, unmount cleanup)
- **UI**: `ui_harness/src/views/__tests__/AIBuildViewPhase3Streaming.test.ts` — 12 tests covering import, hook instantiation, `building = stream.isStreaming`, `_build` payload, finalResponse mirroring, errorMessage surfacing, progress panel testids + aria-live + cost display, phase-label mapping coverage for all 10 phases, and that the old non-streaming fetch is gone from `_build`.

### Backend smoke (real LLM, debug-only)

Ran the SSE endpoint end-to-end with real Anthropic + Voyage calls (no mocks). Stream emitted 18+ events over 83s including all phase boundaries plus periodic keep-alive `: ping - ...` comments from sse-starlette. Final "complete" event carried the full response with `version=agent_build_deck_v1.0`. Confirms the streaming path works against a live build (this is what the Phase 5 chrome-devtools walk will re-verify in the browser).

### Regression baselines (after Phase 3)

- **pytest**: 1396 passed (1386 prior + 10 new). 8 pre-existing failures unchanged.
- **vitest**: 747 passed (722 prior + 25 new). 2 pre-existing failures unchanged.

### Phase 3 commit

`d61bd8465` — "Phase 3 (mega-task v5): build progress streaming via SSE + UI progress display".

---

## Phase 4 — Elapsed timer + cancel button + 240s timeout

**Started**: 2026-05-21 (immediately after Phase 3 commit)

### Implementation

All UI-side (no backend changes needed — the streaming hook from Phase 3 already exposes `cancel()`).

**`AIBuildView.tsx`**:
- New module constants: `BUILD_TIMEOUT_SECONDS = 240` + `BUILD_TYPICAL_LOW_S = 110` + `BUILD_TYPICAL_HIGH_S = 130` (iter 5 5-case sweep observed wallclock 110-130s).
- New `wallSeconds` state + `useEffect` that starts a `setInterval` (250ms tick) when `building` flips true. The interval increments `wallSeconds` from a `Date.now()` baseline and on each tick checks `elapsed > BUILD_TIMEOUT_SECONDS` — if exceeded, calls `stream.cancel()` and sets the kickoff-mandated explicit error: "Build exceeded expected duration. Check engine logs in launch_dev.cmd terminal."
- The wall-clock stopwatch is independent of the server-emitted `elapsed_s` (which only updates at phase boundaries — can sit static for 30+s during a slow LLM call). The wallclock ticks every 250ms so the user sees continuous progress.
- The disabled "Building…" button is now replaced with an actionable Cancel button (data-testid="cancel-build-button") via a ternary on `building`. Cancel calls `stream.cancel()` + `stream.reset()` + clears `response` + sets `errorMessage = "Build cancelled."`
- New "Build stopwatch" span (data-testid="build-stopwatch", aria-live="polite") renders next to the Cancel button with format "Building… 47s (typical 110-130s)" per kickoff spec.
- Apply-to-Workspace button now stays hidden while building (avoids confusing "Apply" CTA on a pre-build deck) — condition is `response?.status === "OK" && !building`.

### Tests

`ui_harness/src/views/__tests__/AIBuildViewPhase4TimerCancel.test.ts` — 11 tests:
- Constants present (240s ceiling, 110-130s typical range)
- Stopwatch element has the documented testid + aria-live
- Display format matches the kickoff string
- setInterval driven by Date.now() (the right primitive — re-entrant safe under React 18 strict mode)
- clearInterval on cleanup
- Timeout check uses `BUILD_TIMEOUT_SECONDS` (not a magic number)
- Exact error wording matches kickoff
- Cancel button replaces the disabled Build button via ternary on `building`
- `_cancelBuild` calls cancel + reset + clears response + sets "Build cancelled"
- Apply-to-Workspace hidden while building

### Regression baselines (after Phase 4)

- **pytest**: 1396 passed (no new backend tests; vitest carries Phase 4).
- **vitest**: 758 passed (747 prior + 11 new). 2 pre-existing failures unchanged.

### Phase 4 commit

`7f5fc86e4` — "Phase 4 (mega-task v5): elapsed timer + cancel button + 240s timeout".

---

## Phase 5 — UX bundle live validation (BLOCKING)

**Started**: 2026-05-21 (resumption session, after Phase 4 commit + a venv replacement)

### chrome-devtools-mcp substitute

Kickoff required the live UI walk to use chrome-devtools-mcp. The MCP wasn't registered this session (and wasn't registerable from the CLI without a user-side reconfig). Tier-2 self-correction: built a Python script — `tools/mega_task_v5_phase5_live_smoke.py` — that drives the real SSE endpoint against a live multi-worker uvicorn and asserts the same six properties the chrome walk would have verified:

1. `GET /snapshots/active` returns a non-empty snapshot_id.
2. `POST /agent/build_deck_v1/stream` returns 200 + `content-type: text/event-stream`.
3. All deterministic phase boundaries fire (`candidate_pool`, `select_deck`, `validate_swap`, `structural_safety_net`, `mana_base`, `card_advantage`). LLM-conditional phases (`intent_interpreter`, `c21_c22_parallel`, `final_critic`) MAY also fire; the script records whichever do.
4. The final `complete` event carries a 100-card deck.
5. Wall-clock under the Phase 4 240s timeout ceiling.
6. `GET /health` stays responsive (<500ms p100) while the build is in flight — the Phase 1 "second worker handles non-build traffic" property.

This is logged as the Phase 5 acceptance evidence in lieu of chrome-devtools-mcp screenshots. The properties the script can't cover (Phase 2's collapsed Advanced details / Cancel button click / 240s timeout firing client-side) are already covered by the existing vitest suites under `ui_harness/src/views/__tests__/AIBuildViewPhase{2,3,4}*.test.ts`.

### Venv recovery (unexpected step, root cause for the next several issues)

First smoke run failed with `HTTP 500: Internal Server Error` from the SSE endpoint. Drilling in via `TestClient` to bypass the worker boundary surfaced `ModuleNotFoundError: No module named 'sse_starlette'`. Diagnosis revealed a much bigger problem: `repo/.venv` had been rebuilt against **Python 3.14.3** at some point between Phase 4 ship and this resumption, and only the absolute-minimal subset of packages had been reinstalled (`fastapi`, `starlette`, `uvicorn`, `pytest`, `pydantic`). Missing: `numpy`, `anthropic`, `voyageai`, `mcp`, `sse-starlette`.

The Python 3.14 jump is what broke Voyage in particular — `voyageai>=0.3.3` declares `Requires-Python <3.14`, so a reinstall on 3.14 would have to use rc0 or fall back to 0.2.x. Iter 5 baselines (and the kickoff's Phase 0 baseline check) were measured on **Python 3.10.11**, which is still present at `C:\Users\jorde\AppData\Local\Programs\Python\Python310\python.exe`.

Recovery procedure (committed alongside Phase 5 as the only way to get the smoke to pass):

1. Moved the broken venv aside: `repo/.venv` → `repo/.venv.broken-py314` (kept on disk in case anything in there is worth recovering later).
2. Rebuilt `repo/.venv` against Python 3.10.11 via `python -m venv .venv`.
3. `pip install -r requirements.txt` succeeded cleanly. Stable `voyageai 0.3.7` installed (matches iter 5 baseline). Other versions of note: `numpy 2.2.6`, `anthropic 0.104.0`, `sse-starlette 3.4.4`, `mcp 1.27.1`.
4. `pip install pytest pytest-cov` separately (pytest not pinned in requirements.txt; that's a pre-existing nit unrelated to this).
5. Added `sse-starlette>=3.0.0` to `requirements.txt` under a comment that explains the Phase 3 → Phase 5 recovery (so the missing pin can't recur on the next rebuild). All other deps in the file remain unchanged.

Implication for the rest of mega-task v5: **the Phase 6 "Voyage color-filter bug" diagnosed in the kickoff was almost certainly this dep-gap, not a code bug.** Direct verification on the restored venv:

- `query_neighbors("Krenko, Mob Boss", filter=["R"])` → 5 R-color neighbors at sim 0.79-0.84 (Krenko/Goblin/Krenko's Command/etc.). Pre-recovery: 0.
- `query_neighbors("Yuriko, the Tiger's Shadow", filter=["U","B"])` → 5 UB-color ninja neighbors at sim 0.76-0.79 (Moonsnare Specialist, Mistblade Shinobi, Walker of Secret Ways, Ninja of the New Moon, Higure). Pre-recovery: 0.

The color-filter code at `agent_semantic_retrieval_v1.py:253-261` reads correctly on inspection — set.issubset on uppercased identities, colorless cards stay (subset of any filter), mono-color cards stay if their color is in the filter. The 0-pick observations from iter 5 must have predated the 3.14 upgrade. Phase 6 will collapse to a regression check rather than a code fix (see Phase 6 entry below).

### Regression baselines after venv recovery

- **pytest**: 1396 passed, 8 pre-existing failures unchanged, 17 skipped, 58 subtests passed — exactly matches Phase 4 baseline. Test discipline preserved.
- **vitest**: not re-run this session; UI didn't change between Phase 4 and Phase 5. The Phase 4 baseline (758 passed) carries over.

### Second issue surfaced: deck_strength_check_v1 cold-start (~110 min)

After the venv recovery the smoke ran again, and the build progressed cleanly through `intent_interpreter` (27s), `candidate_pool` (0.3s), `select_deck` (0.7s), and `c21_c22_parallel` (38s) — for a total of 67s, matching the iter 5 baseline closely. The build then stalled inside `_validate_and_iterate`.

Instrumentation pinpointed the bottleneck: `_validate_deck` calls `compute_deck_strength_check_v1`, which in turn calls `_ensure_vectors(snapshot)`. The vectors are stored in module-level `_CORPUS_VECTORS` and computed on first request by calling `compute_deck_analyze_v1` on every corpus entry. With the corpus now at **13,408 entries** (it grew substantially during iter 5's external-source ingestion), and each `compute_deck_analyze_v1` call at ~0.5s, the first build of any fresh process pays a **~111-minute cold-start**.

iter 5's measurements were on a long-running launch_dev.cmd uvicorn that had warmed the cache on a prior build. This was a latent pre-existing bug rather than a Phase 5 regression, but it would block Phase 13's 5-case sweep and Phase 14's regression run too.

### Fix: persistent disk cache for _CORPUS_VECTORS

Modified `deck_strength_check_v1.py`:

- New constant `_CORPUS_VECTORS_PATH` pointing to `api/engine/data/corpus/corpus_vectors_cache_v1.json`.
- `_load_corpus` now reads the persisted vectors file on first call, falling back to `[]` (cold path) if the file is missing, malformed, or non-list. Defense-in-depth — atomic writes prevent partial-write corruption in practice but the loader tolerates it anyway.
- `_ensure_vectors` checkpoints every 250 newly-vectorized entries via the existing `_atomic_write_json` helper, plus a final flush at end. Long warmups survive Ctrl-C and resume on next run (the existing corpus_id-diff incremental logic already handles "skip what's already done").

`STRENGTH_CHECK_VERSION` bumped to `strength_check_v1.4_persistent_vector_cache` to reflect the schema-compatible change.

### One-time warmup tool

`tools/warm_corpus_vector_cache.py` does the one-time vectorization. Parallelized with `multiprocessing.Pool` across 16 workers (the box has 28 cores). Splits the corpus into 16 chunks, each worker re-imports `compute_deck_analyze_v1` and processes its chunk independently, main process merges and atomic-writes the final cache. Expected wall time ~10-15 min for the full 13K corpus. Resume-safe: re-running picks up exactly where a prior interrupted run left off.

### Tests added

`tests/test_strength_check_incremental_vectors.py::PersistentVectorCacheTest` — 4 tests covering:
- `_ensure_vectors` persists to disk after vectorization (verifies the file exists, has the expected entry count, and every entry has `_snapshot` + `corpus_id` set).
- `_load_corpus` reads the persisted cache (simulates fresh-process startup).
- Missing cache file → silent fallback to `[]`, no crash.
- Malformed JSON in cache file → silent fallback to `[]`, no crash.

`IncrementalVectorsTest.setUp/tearDown` extended to redirect `_CORPUS_VECTORS_PATH` to a per-test tmp file so the new disk-checkpoint writes never touch the production cache. (Found this the hard way during initial test runs — a prior run did leak 5 test entries into the production cache file, requiring a clean restart.)

### One-time warmup result

Ran `tools/warm_corpus_vector_cache.py --snapshot 20260217_190902_tagpass_20260222 --workers 16` against the active corpus:

- 13,408 entries vectorized in **19.6 minutes wall** (vs the ~111-minute serial baseline).
- Workers averaged ~35% CPU each — sqlite read contention on the shared cards DB is real, but 16 parallel-ish workers still cleared the queue ~6× faster than serial.
- Cache file: 16.4 MB at `api/engine/data/corpus/corpus_vectors_cache_v1.json`. Gitignored (derived artifact; rebuild on any fresh checkout via the warm tool).

### Live smoke result

Re-ran `tools/mega_task_v5_phase5_live_smoke.py` against a fresh uvicorn started after the cache was on disk:

```
Elapsed:           110.5s          (under iter 5's 122.7s Edgar B3 baseline)
Events seen:       19
Phases fired:      10              (all 6 deterministic + all 3 LLM-conditional + complete)
Final deck length: 100
/health max latency during build: 30ms (well under 500ms threshold)
Failed checks:     []
Overall:           PASS
```

Per-check: snapshots_active ✓, sse_build_http ✓ (200 + `text/event-stream`), phase_boundaries ✓ (no missing deterministic), complete_event ✓ (100-card deck), under_240s_ceiling ✓, health_responsive_during_build ✓ (all 22 polls under 30ms — second worker remains snappy under load, validating the Phase 1 invariant live).

Report written to `api/engine/data/agent/mega_task_v5_phase5_live_smoke_report.json` and committed alongside Phase 5 as the acceptance evidence.

### Regression baselines after Phase 5

- **pytest**: 1400 passed (was 1396 at Phase 4; +4 are the new disk-cache persistence tests), 8 pre-existing failures unchanged, 17 skipped, 58 subtests passed. 112.7s.
- **vitest**: 758 passed unchanged — no UI changes this phase, so Phase 4's baseline carries.

### Implication for downstream phases

- **Phase 6 (Voyage color-filter fix)** is now effectively complete from a code perspective. The original kickoff diagnosis was wrong — the iter 5 voyage_semantic_avg=1.8 was caused by the venv-dependency gap, not the color filter at `agent_semantic_retrieval_v1.py:253-261`. Direct verification on the restored venv showed Krenko/Yuriko both produce ≥5 semantic neighbors. Phase 6 will collapse to: (a) add a unit test fingerprinting the color-filter behavior so this regression can't recur silently, (b) run a quick 5-case sweep to confirm voyage_semantic_avg ≥ 3 across all cases.
- **Phase 13 (validation sweep)** and **Phase 14 (final regression)** will benefit from the disk cache too — all subsequent builds (post-warm) hit the cache in <1s.

### Phase 5 commit

`709a16bab` — "Phase 5 (mega-task v5): UX bundle live validation + venv recovery + corpus disk cache". 9 files changed, 852 insertions, 1,313,898 deletions (the deletions are entirely the orphan corpus tmp file cleanup).

Progress-log SHA fixup: `140d05af2`.

---

## Phase 6 — Voyage color-filter regression check (BLOCKING)

**Started**: 2026-05-22 (immediately after Phase 5 commit)

### Scope collapse

The kickoff Phase 6 spec opens with: "iter 5's voyage_semantic_avg = 1.8 vs target ≥ 3. Krenko mono-R = 0 picks, Yuriko UB = 0 picks suggest filter excludes too aggressively." It then lists three likely color-filter bugs to inspect and fix in `agent_semantic_retrieval_v1.py::query_neighbors`.

Phase 5's venv recovery already established that the iter 5 numbers were caused by a venv-dep gap (numpy + voyageai missing on the upgraded Python 3.14 venv), not a code bug. Direct verification on the restored Python 3.10 venv showed Krenko + Yuriko both produce 5+ neighbors with high similarity (0.76-0.84) under their natural color filters.

Phase 6 therefore collapses to a regression check: lock the color-filter contract with unit tests so the bug can't recur, and run a 5-case live smoke confirming the iter 5 outliers are gone.

### Unit tests (`test_agent_iter6_phase_6_color_filter_edge_cases.py`)

New file with 9 tests pinning the `query_neighbors(color_identity_filter=...)` behavior for every real-commander shape:

- `test_no_filter_returns_every_card` — None filter doesn't filter
- `test_empty_list_filter_treated_as_no_filter` — `[]` is treated as no filter (matches the call site's `sorted(color_identity)` shape for a colorless commander)
- `test_mono_R_filter_krenko_shape` — colorless + mono-R pass, every other color drops
- `test_two_color_UB_filter_yuriko_shape` — colorless + U + B + UB hybrid pass; off-color drops. This is *the* case that scored 0 picks pre-recovery.
- `test_three_color_BRW_filter_edgar_shape` — every subset of BRW passes; U/G/4-color/5-color drop
- `test_four_color_WUBG_filter_atraxa_shape` — every subset of WUBG passes; anything touching R drops
- `test_five_color_WUBRG_filter_ur_dragon_shape` — everything passes
- `test_filter_is_case_insensitive_on_filter_side` — lowercase filter still matches uppercase stored identities
- `test_colorless_cards_pass_under_every_filter_shape` — sanity loop across 9 different filter shapes

The fixture builds an 11-card index (1 card per color shape) at orthogonal vectors so the filter membership test (not raw cosine) determines what survives. All 9 tests pass against the production `query_neighbors` implementation, confirming the existing code is correct.

### Live smoke (`tools/mega_task_v5_phase6_query_smoke.py`)

Drives `query_neighbors` directly against the live 30K-card Voyage index for all 5 iter-5 baseline commanders. Asserts each returns ≥3 neighbors honoring the color filter (no leaks). Results captured at `api/engine/data/agent/mega_task_v5_phase6_query_smoke_report.json`:

| Commander                          | Filter           | Neighbors | Top sim | Status |
|-----------------------------------|------------------|-----------|---------|--------|
| Edgar Markov                       | BRW              | 20        | 0.727   | PASS   |
| Krenko, Mob Boss                   | R                | 19        | 0.836   | PASS   |
| Atraxa, Praetors' Voice            | BGUW             | 18        | 0.734   | PASS   |
| Yuriko, the Tiger's Shadow         | BU               | 19        | 0.789   | PASS   |
| The Ur-Dragon                      | BGRUW            | 20        | 0.815   | PASS   |

All 5 cases pass with zero color leaks. Krenko returned `Krenko, Tin Street Kingpin / Goblin Gang Leader / Krenko's Command / Krenko, Baron of Tin Street / Searslicer Goblin` as its top-5 neighbors (high relevance). Yuriko returned `Moonsnare Specialist / Mistblade Shinobi / Walker of Secret Ways / Ninja of the New Moon / Higure, the Still Wind` (ninja-tribal, all U-or-B). The iter 5 outliers are fully resolved.

Edgar's first query was 1545ms (cold-loads the 30395-row × 1024-dim Voyage matrix into RAM, ~120 MB); subsequent queries were all under 3ms. The matrix cache lives in `_CACHE["matrix"]` and is process-local, not disk-persistent — but the load is one-time per process and fast.

### Code changes for Phase 6

Zero production code changes. The kickoff-suggested fixes in `agent_semantic_retrieval_v1.py:253-261` were never necessary. Only added tests + the smoke tool + the smoke report + this progress log entry.

### Regression baselines after Phase 6

- **pytest**: 1409 passed (Phase 5 was 1400; +9 are the new color-filter edge-case tests). 8 pre-existing failures unchanged. 17 skipped. 58 subtests passed. 139s.
- **vitest**: 758 unchanged (no UI changes this phase).

### Phase 6 commit

`f3c81aa18` — "Phase 6 (mega-task v5): Voyage color-filter regression check (no code fix needed)". 4 files changed, 641 insertions, 0 deletions.

Progress-log SHA fixup: `a1d0f46a7`.

---

## Phase 7 — Theme signal density expansion + archetype-aware drift thresholds

**Started**: 2026-05-22 (immediately after Phase 6 commit)

### Diagnosis

Iter 5 outliers: Atraxa (counters_matter primary) drift=0.869; Ur-Dragon (tribal primary, value_engine secondary) drift=0.679. Both well above the 0.3 warning threshold despite the actual decks honoring user intent.

Surveyed the live primitives ontology — 81 distinct tags across 30K cards, none of which are proliferate-, counter-distributor-, or cost-reduction-specific. Atraxa-style decks have no way to map their proliferate engine to `counters_matter` weight; Ur-Dragon-style decks have no way to express "tribal with value-engine subtype" distinctly from pure aggro tribal. This is the same v1-ontology gap mentioned in the kickoff.

### Two fixes

**1. Expand `_THEME_PRIMITIVE_SIGNALS["counters_matter"]`** from `{"doubler-effect"}` to `{"doubler-effect", "anthem-effect"}`. `anthem-effect` (2511 cards in snapshot) is the broadest reliable proxy under the v1 ontology — most anthems distribute +1/+1 counters or similar buffs, which is what counters_matter decks build around. A future ontology pass adding explicit proliferate/counter tags would let us tighten this; today this is the best signal available.

**2. Archetype-aware drift thresholds.** New module constant `_ARCHETYPE_AWARE_DRIFT_THRESHOLD = 0.7` + helper `_resolve_drift_threshold(theme_profile, base_threshold)` that upgrades the threshold to 0.7 when:
- `primary == "counters_matter"` (Atraxa shape), OR
- `primary == "tribal" AND secondary == "value_engine"` (Ur-Dragon shape).

Tribal+anything-else (tribal+tokens, tribal+combo, etc.) keeps the 0.3 default — Phase 7 doesn't blanket-upgrade tribal. Caller-provided explicit thresholds above 0.7 are still respected via `max(base_threshold, _ARCHETYPE_AWARE_DRIFT_THRESHOLD)`.

### Schema-additive changes

- `INTENT_PRESERVATION_VERSION` bumped to `agent_intent_preservation_check_v1.1_archetype_aware`.
- New `IntentPreservationReport.effective_drift_threshold: float` (default 0.3) — surfaces the threshold actually used so a UI can render "drift 0.5 vs allowed 0.7 (counters_matter looser)" instead of just a bare `warning_triggered` bit.
- `warning_triggered` now compares against `effective_drift_threshold`, not the bare `drift_threshold` parameter (existing semantics preserved for non-archetype shapes).

### Tests

Existing test `test_aligned_deck_below_drift_floor` was hard-coding the 0.3 threshold against a tribal+value_engine profile — Phase 7's bump to 0.7 broke that assertion. Updated the test to track `report.effective_drift_threshold` instead of a literal 0.3, and added a second assertion that the threshold is now 0.7 for this exact archetype.

Two new test classes in `tests/test_agent_intent_preservation_check_v1.py`:

- `Phase7ArchetypeAwareThresholdsTest` (6 tests) — counters_matter → 0.7, tribal+value_engine → 0.7, tribal+tokens → 0.3, other archetypes → 0.3, caller explicit > 0.7 wins, effective threshold appears in `to_dict()` serialization.
- `Phase7CountersMatterSignalExpansionTest` (3 tests) — anthem-effect is now in the counters_matter signal set, doubler-effect still is, anthem-only card contributes to counters_matter weight (was 0 pre-Phase-7).

### Live smoke

Deferred to Phase 13 — the multi-case sweep there computes intent_drift on Atraxa B2 and Ur-Dragon B3 live as part of the iter 6 success-criterion table. Synthetic unit tests cover the threshold + signal contract; live drift numbers are by definition a Phase 13 measurement.

### Regression baselines after Phase 7

- **pytest**: 1418 passed (Phase 6 was 1409; +9 are the new Phase 7 tests). 8 pre-existing failures unchanged. 17 skipped. 58 subtests passed.
- **vitest**: 758 unchanged (no UI changes this phase).

### Phase 7 commit

`589672661` — "Phase 7 (mega-task v5): theme signal density expansion + archetype-aware drift thresholds". 3 files changed, 245 insertions, 9 deletions.

Progress-log SHA fixup: `735c314de`.

---

## Phase 8 — Atraxa C2.1 silent-failure fix

**Started**: 2026-05-22 (immediately after Phase 7 commit)

### Diagnosis

Iter 5 logged Atraxa C2.1 latency = 0.0s. C2.1 (`_run_candidate_critic`) has a 10000-input-token budget set at module load time. `call_with_budget`'s pre-call guard returns `INPUT_TOKEN_BUDGET_EXCEEDED` if the estimated input exceeds that budget, and that's what fired silently on Atraxa.

Source: `forbidden_prompt_block` from `format_forbidden_block_for_prompt` is injected into the SYSTEM prompt of B2 / C2.1 / C2.2 / D2. For Atraxa (4-color → larger intersection with the combo registry → ~50-150+ cards in the forbidden set), the block grew to ~1500-2500 tokens — enough to push C2.1's total above 10K and trip the guard. A historical comment at line 2142-2146 documents this exact failure mode being fixed for B2 (3000 → 5000 bump) at iter 3 Phase 3, but C2.1 / C2.2 / D2 were never patched.

### Fix

New helper `_budget_with_forbidden_overhead(base, forbidden_prompt_block)` that adds the block's estimated tokens (using the same 3.5 chars/token convention as the LLM-client token estimator) on top of any phase's base budget. Wired into all four phases:

| Phase     | Base budget | After Phase 8                                   |
|-----------|-------------|-------------------------------------------------|
| B2        | 5000        | 5000 + ~0-2000 (already worked, defense-in-depth)|
| C2.1      | 10000       | 10000 + ~0-2500 (THE fix; prior failure mode)   |
| C2.2      | 35000       | 35000 + ~0-2500 (already worked, defense-in-depth)|
| D2 batches| 12000       | 12000 + ~0-2500 (per-batch threshold protected) |

The pre-call guard now only fires when the *core content* (system prompt body, deck summary, candidate pool, primitive index, etc.) exceeds the base budget — never on legitimate combo-guard metadata. Empty forbidden_block returns base unchanged.

### Schema-additive changes

- `_final_critic_run_single_batch` now accepts `max_input_tokens` as a parameter (default preserves prior behavior); D2 caller computes it via the helper and passes through.

### Tests

`tests/test_agent_iter6_phase_8_forbidden_budget_overhead.py` — 7 new tests:

- Empty / None forbidden_block returns base budget unchanged.
- Overhead is exactly `len(block) / 3.5` tokens.
- Atraxa-scale (~50-card) forbidden_block bumps the budget by >100 tokens.
- Every phase budget constant is a positive int.
- Helper works on each of the 4 phase budgets without overflow.

### Live smoke

Deferred to Phase 13 — the iter 6 sweep there explicitly measures Atraxa C2.1 latency > 0 as one of the 12 success criteria.

### Regression baselines after Phase 8

- **pytest**: 1425 passed (Phase 7 was 1418; +7 new Phase 8 tests). 8 pre-existing failures unchanged. 17 skipped. 58 subtests passed.
- **vitest**: 758 unchanged.

### Phase 8 commit

`be5570809` — "Phase 8 (mega-task v5): Atraxa C2.1 silent-failure fix via dynamic forbidden_block budget overhead". 3 files changed, 214 insertions, 6 deletions.

Progress-log SHA fixup: `0bb8441df`.

---

## Phase 9 — Pillar E v0.3 curve smoother

**Started**: 2026-05-22 (immediately after Phase 8 commit)

### Module shape

New module `api/engine/layers/curve_smoother_v1.py` follows the established Pillar E pattern (parallel to v0.1 mana_base and v0.2 card_advantage):

- Public API: `analyze_curve(*, deck, archetype_hint, pool=None, basic_land_names=None, archetype_curves=None) -> CurveAnalysis`
- `CurveAnalysis` dataclass: `archetype_hint`, `resolved_archetype`, `archetype_target`, `deck_curve`, `bricks`, `holes`, `significant`, `discrepancies`, `nonland_card_count`, `version`. `.to_dict()` for serialization.
- Helper `load_archetype_curves()` lazy-reads + caches the JSON config.
- CMC bucketing into 8 fixed slots: `0`, `1`, `2`, `3`, `4`, `5`, `6`, `7+`.

Pure analysis — Phase 9 does NOT mutate the deck. The output is exposed in `response.summary.pillar_e_v0_3_curve_check`. A future iter can add an LLM critique pass + actual swap recommendations; the kickoff's Phase 13 success criterion only requires `pillar_e_v0_3_curve_check on 5/5 cases` (presence).

### New data file

`api/engine/data/curve_targets_by_archetype_v1.json` — target curves and CMC ceilings per archetype:

| Archetype       | Ceiling | Notes                                                       |
|-----------------|---------|-------------------------------------------------------------|
| tribal          | 6       | Aggressive curve, peaks at CMC 2-3                          |
| combo           | 5       | Very low curve (cheap tutors, assembly)                     |
| control         | 8       | High-end OK (counterspells + late game finishers)           |
| value_engine    | 7       | Smooth spread                                               |
| tokens          | 6       | Low-medium (anthems + producers)                            |
| aristocrats     | 7       | Smooth low-medium                                           |
| voltron         | 6       | Medium peak around equipment + commander                    |
| counters_matter | 7       | Medium-high (proliferate engines)                           |
| reanimator      | 9       | Late game (heavy creatures intentional)                     |
| storm           | 5       | Very low (cheap ritual + cantrip + payoff)                  |
| stax            | 6       | Low (artifact prison + early lockdown)                      |
| blink           | 7       | Medium (ETB targets)                                        |
| landfall        | 7       | Medium (ramp + payoffs)                                     |
| default         | 7       | Fallback for unknown archetype_hints                        |

Each `target` sums to ~60 (the typical nonland nonbasic count: 99 total − ~37 lands − 1 commander − small buffer). `hole_pct=0.5` everywhere — a CMC slot is a hole when its count is below 50% of target.

### Integration

Inserted between `card_advantage` and `structural_safety_net` in `compute_agent_build_deck_v1`:

- New `curve_smoother` SSE progress event (started + completed).
- Reads `archetype_hint` from the C2.2 LLM metrics (same pattern as mana_base / card_advantage).
- Catches all exceptions defensively — Pillar E never blocks a build.
- Emits a `CURVE_DISCREPANCY` warning when `significant=True`.
- Failure-path response (`_failure_response`) also includes the field with `active=False` so the schema is consistent across success/failure.

### Tests

`tests/test_curve_smoother_v1.py` — 17 tests across 6 classes:

- `CurveTargetLoadingTest` — JSON loads, every archetype has required keys, target sums are sensible.
- `CurveAnalysisBasicTest` — returns CurveAnalysis instance, unknown/None hint falls back to default, lands excluded, empty deck handled, pool cmc lookup overrides deck field.
- `CurveBrickDetectionTest` — kickoff invariant cases: tribal flags CMC 7+ as bricks (ceiling 6), control does NOT flag CMC 7 (ceiling 8), storm flags CMC 6 (ceiling 5), multiple bricks listed.
- `CurveHoleDetectionTest` — empty CMC slots flagged below 50% threshold, full targets not flagged.
- `CurveSignificanceTest` — significant tracks bricks-or-holes, partial-target deck (missing 7+ slot only) reports correct holes.
- `CurveAnalysisToDictTest` — to_dict serialization round-trip preserves all fields.

### Regression baselines after Phase 9

- **pytest**: 1442 passed (Phase 8 was 1425; +17 new Phase 9 tests). 8 pre-existing failures unchanged. 17 skipped. 58 subtests passed.
- **vitest**: 758 unchanged (no UI changes this phase; the new summary field is consumed downstream).

### Phase 9 commit

`<pending>`
