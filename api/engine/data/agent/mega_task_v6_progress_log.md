# Mega-task v6 progress log

Source-of-truth running log. Append-only, timestamped per phase. Atomic commit per phase.

Substrate baseline at v6 start: `95d06c2d9` (Coherence Sweep #3 ship). pytest 1489 / vitest 758.

---

## Phase 0 — Pre-flight + memory sync — 2026-05-22

**Env confirmed:**
- Python 3.10.11 (`E:\MTG Root\mtg-engine\venv`)
- ANTHROPIC_API_KEY set, VOYAGE_API_KEY set
- Disk: 1.1 TB free on E:
- Working tree NOT clean at start: partial Phase 1 SSE-fix attempt is uncommitted in `api/main.py` (synthetic `complete` event with deck payload) + `ui_harness/src/views/AIBuildView.tsx` (timeout bump 240s → 480s). These are intentionally preserved for Phase 1 (the SSE diagnosis must build on the partial attempt to determine why explicit `complete` emission did not reach the browser).
- Untracked: `api/engine/data/agent/mega_task_v6_kickoff.md` (this kickoff), `api/engine/data/primitives/llm_supplement_audit_v1.json` (prior LLM-supplement audit artifact), `engine_path_test.md` (stale scratch).
- Last 3 commits visible: `95d06c2d9` Sweep #3 Phase 10 synthesis, `7f98820ae` Sweep #3 Phase 9, `a00fad37e` Sweep #3 Phase 8.

**Reference files read:**
- `repo/api/engine/data/agent/pillar_d_iteration_6_validation_report.md` — 9/12 pass, 3 failing criteria documented (voyage_semantic_avg=1.4 needs ≥3; intent_drift per-case 2/5 needs ≥4/5; pillar_e_v0_4_interaction_within=0/5 needs ≥4/5).
- `repo/api/engine/data/agent/coherence_sweep_3_health_report.md` — substrate "in good shape"; 4 items queued for iter 7 (voyage_downgrade_pass wiring, voyage_rules_embedding at-scale, ENGINE_API_GUIDE overhaul, 8 pre-existing test failures triage). 8 pre-existing failures enumerated.
- `repo/api/engine/data/agent/mega_task_v5_final_report.md` — v5 14 phases on top of v4 (e97589870), ship at 4cee4a287; pytest 1489 / vitest 758 baselines; iter 6 → iter 7 hand-off priorities #1-#3 match this kickoff's Phase 2/3/4.
- `repo/api/engine/data/agent/mega_task_v6_kickoff.md` — this mega-task spec.

**Cowork memory files at `spaces/.../memory/` (project_iter_7_prep_notes, project_coherence_sweep_3_shipped, project_mega_task_v5_shipped, feedback_pool_score_does_not_drive_llm_picking, project_5_pillar_forward_plan) are NOT present on local disk.** Memory directory at `C:/Users/jorde/.claude/projects/E--MTG-Root/memory/` is empty. The substantive content from those memories is covered transitively by the 4 reference files read above (the iter 7 prep notes' priorities #1-#3 are documented in mega_task_v5_final_report.md hand-off section; the feedback_pool_score_does_not_drive memory's load-bearing learning is documented in v5 report's hand-off paragraph for priority #1). Proceeding.

**Test baselines:** pytest run dispatched in background. Will record exact pass count in Phase 0 commit message after it returns.

**Decisions / open items:**
- Will preserve the in-flight `api/main.py` + `AIBuildView.tsx` modifications for Phase 1; Phase 0 commits only this progress log scaffold.

**Commit:** `Phase 0 (mega-task v6): pre-flight + progress log scaffold` — `d7c91ab51`.

**Test baselines confirmed (post-commit, repo/.venv Python 3.10.11):**
- pytest: **1489 passed, 8 failed, 17 skipped** — matches kickoff baseline exactly.
- 8 known pre-existing failures: 1× `test_bracket_gc_limits_v1`, 5× `test_complete_bracket_violations_v1::TestHttpEndpointWiring`, 1× `test_no_random_imports`, 1× `test_pipeline_profile_bracket_enforcement_v1`. Queue for Phase 7 triage.

---

## Phase 1 — SSE UI end-to-end fix + browser verification — 2026-05-22 (BLOCKING)

**Root cause identified:** `useBuildStreaming.ts` had a React 18 StrictMode `mountedRef` regression. In React 18 dev mode, every component mounts → fires cleanup → re-mounts. The hook's cleanup ran `mountedRef.current = false`, and the useEffect body never reset it on re-mount. So after StrictMode's mount-unmount-remount cycle, `mountedRef.current` was permanently `false`. Inside the stream loop, every `setState` was gated by `if (!mountedRef.current) return;` — so events arrived on the wire but never updated React state. UI stayed at `INITIAL_STATE` until the 480s timer fired. Python tools (urllib, httpx) and curl never reproduced because they have no React lifecycle.

**Diagnostic evidence (server + wire format are perfect; bug is UI-only):**
- curl with `Origin: http://localhost:5173` (mimics browser cross-origin POST): 200 OK, `content-type: text/event-stream; charset=utf-8`, `access-control-allow-origin: http://localhost:5173`, `x-accel-buffering: no`, `Transfer-Encoding: chunked`. Events flow real-time at every phase boundary including `complete` with the full 100-card deck. First event at ~15s. ping comments every 15s.
- `tools/mega_task_v6_phase1_browser_simulation.py` (httpx async stream + bit-for-bit equivalent of `_parseSseBuffer`): **OK**, 13 phases fired (intent_interpreter, candidate_pool, select_deck, c21_c22_parallel, validate_swap, final_critic, curve_smoother, interaction_designer, mana_base, card_advantage, structural_safety_net, graduated_playtest, complete), deck_len=100, complete_event.present=true.
- v5 `tools/mega_task_v5_phase5_live_smoke.py` continues to pass (cross-checked the existing report json — still represents reality).

**Fix delivered:**
- `ui_harness/src/hooks/useBuildStreaming.ts`: useEffect body now sets `mountedRef.current = true;` on every mount (1-line fix with the v6 P1 explanation comment). This makes the hook idempotent across StrictMode's mount-cleanup-remount cycle.
- `ui_harness/src/hooks/__tests__/useBuildStreaming.test.ts`: added regression grep test "(v6 P1 regression) mount effect resets mountedRef=true on every mount" that fails if a future refactor removes the reset.
- `tests/test_agent_build_deck_v1_stream_e2e.py`: new Python e2e test using `httpx.AsyncClient + httpx.ASGITransport` to consume the SSE stream incrementally with the exact same parser logic as `useBuildStreaming._parseSseBuffer`. Asserts ≥6 distinct (phase,status) tuples + a final `complete` event with a populated `response.version`. Also asserts CORS `access-control-allow-origin` matches the request `Origin`. Both tests pass.
- `tools/mega_task_v6_phase1_browser_simulation.py`: new browser-equivalent SSE consumer used as the chrome-devtools-mcp substitute for Phase 11's UI verification.

**Defense-in-depth fixes:**
- `ui_harness/vite.config.ts`: added `/agent`, `/deck`, `/commander`, `/theme`, `/card`, `/playtest`, `/corpus` proxy entries. Removes cross-origin as a moving variable for dev — if a dev runs Vite on a port outside the engine's CORS allowlist (e.g., 5175 when 5173/5174 are taken), the proxy keeps the UI working same-origin instead of silently breaking. The CORS path was verified clean for the standard 5173/5174 layout, but the proxy entries make the dev workflow robust regardless of port choice.

**Cleanup:**
- `api/main.py::_run_build`: removed the redundant synthetic `complete` event re-emission that the live debug attempt added based on a misdiagnosis. `compute_agent_build_deck_v1` already emits a `complete` event via `progress_callback` at the end of every build (verified in 4 emission sites + the existing v5 tests). Comment explains the misdiagnosis so future maintainers don't reintroduce it.
- `ui_harness/src/views/AIBuildView.tsx`: reverted `BUILD_TIMEOUT_SECONDS` 240→480 bump. The bump was based on the misread that builds were exceeding 240s; with events arriving in real time the 110-130s normal build comfortably fits 240s with headroom for Pillar E v0.5/v0.6 + semantic injection (each ~10-20s).

**Tests (post-fix):**
- pytest: **1491 passed, 8 failed (same pre-existing)** — +2 new e2e tests.
- vitest: **759 passed, 2 failed (pre-existing metricPillHeader source-grep drift on WorkspaceView)** — +1 new regression grep test.
- The 2 pre-existing vitest failures are queued for Phase 7 triage alongside the 8 pytest failures.

**Browser verification:** chrome-devtools-mcp not available in this session. Verified end-to-end via `tools/mega_task_v6_phase1_browser_simulation.py` (httpx async streaming, bit-for-bit equivalent of `useBuildStreaming._parseSseBuffer`). This proves the server's wire format, headers, CORS, and event delivery are correct for any browser-equivalent fetch consumer. The StrictMode fix is the load-bearing UI-side correction; Phase 11 will re-verify with a fresh end-to-end run.

**Commit:** `Phase 1 (mega-task v6): SSE UI end-to-end fix — React 18 StrictMode mountedRef regression + browser-equivalent regression coverage` — `d0ef37fdd`.

---

## Phase 2 — Semantic-injection guarantee — 2026-05-22 (BLOCKING)

**Goal:** close iter 6 success criterion #6 (voyage_semantic_avg=1.4-2.0 vs target ≥3) via the post-hoc deterministic injection layer mandated by the kickoff and the `feedback_pool_score_does_not_drive_llm_picking` cowork memory learning.

**Built:**
- `api/engine/layers/agent_semantic_injection_v1.py` — new module. Public API: `inject_semantic_picks(deck, anchor_cards, color_identity, *, n_target, forbidden_set=None, query_neighbors=None) -> (modified_deck, swap_log)`. Bracket-aware `_DEFAULT_N_TARGETS`: B1/B2=2, B3/B4=3, B5=4. Queries Voyage for top-30 neighbors per anchor (commander + must-includes + creative outliers from C2.2). Filters by color identity (delegated to `query_neighbors`), de-duplicates anchors + in-deck cards + forbidden set. Identifies low-priority swap targets via the `_SWAPPABLE_SOURCE_SUBSTRINGS` (only C2.2 wild-discovery picks; never commander / must-includes / mana base / C2.1 picks / archetype staples). Returns the modified deck + a swap log. Graceful fallback when Voyage is offline or no neighbors are available (returns unmodified deck + empty swap log).
- `api/engine/layers/agent_build_deck_v1.py` integration: inserted between `validate_swap` completion (line 433) and `final_critic` start so D2 rewrites rationales for the post-injection composition. Anchor list = commander + must_include_cards + every deck card with `wild_combo_discovery` or `creative_outlier` in `source`. `n_target` derived from bracket. Swap log surfaces in `response.summary.semantic_injection = {"count": <int>, "swap_log": [...]}`. Failures non-fatal: a `SEMANTIC_INJECTION_FAILED` warning is appended and the build continues unchanged.
- `tests/test_agent_semantic_injection_v1.py` — 13 unit tests covering: bracket-aware target resolution; full-pool injection (none semantic yet); partial-pool (some already semantic); all-anchors-overlap edge case; color-identity filter forwarding; forbidden_set blocking; protection of commander/must-includes/C2.1/mana_base/archetype staples; no-swappable-wild-picks edge case; Voyage-backend-exception graceful fallback; already-at-target no-op; version constant.

**Tests:**
- pytest: **1504 passed**, 8 pre-existing failed, 17 skipped, 58 subtests passed (1489 baseline + 2 Phase 1 e2e + 13 Phase 2 = 1504 expected ✓).

**Surface:**
- New build response field: `summary.semantic_injection = {"count": <N>, "swap_log": [{"removed", "added", "anchor", "similarity"}, ...]}`. Phase 11's iter-7 sweep metric reads `count` directly.
- New warning code on success: `SEMANTIC_INJECTION_APPLIED` (informational, names the version + count).

**Decisions / open items:**
- `_SWAPPABLE_SOURCE_SUBSTRINGS` is intentionally conservative — only swaps `C2_2_wild_combo_discovery_added` / `wild_combo_discovery` tags. If a future case has no swappable picks and the injection layer no-ops, that's acceptable (the build doesn't regress; Phase 11 will tell us if widening the swap pool is needed).
- Smoke test deferred to Phase 11's iter-7 sweep — running 5 live Edgar/Krenko/Atraxa/Yuriko/Ur-Dragon builds at this phase would cost ~$1.50 + ~10 min; Phase 11 needs to do that anyway and we get the same signal with one combined spend.

**Commit:** `Phase 2 (mega-task v6): semantic-injection guarantee — post-hoc N-card injection with bracket-aware target`.

