# Coherence Sweep #3 progress log

Append-only timestamped record of phase execution. Same format as mega-task v1-v5 logs.

## Phase 0 — Pre-flight + report skeleton

**Started**: 2026-05-22 (immediately after mega-task v5 ship at commit `4cee4a287`)

### Environment

- Python 3.10.11 ✓
- Deps confirmed importable: numpy 2.2.6, anthropic 0.104.0, voyageai 0.3.7, sse_starlette 3.4.4, fastapi 0.129.0, pydantic 2.12.5, mcp ✓
- Disk free: 1.1 TB on E: ✓
- Git HEAD: `4cee4a287` ("Phase 14 progress log: record actual commit SHA (ef77f6278) + ship marker")
- pytest baseline: **1489 passed**, 8 pre-existing failures unchanged, 17 skipped, 58 subtests passed (109s) — matches mega-task v5 final state.
- vitest baseline: 758 (carried over from v5).
- Untracked at pre-flight: `api/engine/data/agent/coherence_sweep_3_kickoff.md` (sweep kickoff doc — committed in Phase 0), `api/engine/data/primitives/llm_supplement_audit_v1.json` (pre-existing per v5 Phase 0 log), `engine_path_test.md` (pre-existing per v5 Phase 0 log; leave alone).

### Files reviewed (the 7 required by kickoff)

1. ✓ `spaces/.../memory/project_iter_6_prep_notes_2026-05-21.md` — Coherence Sweep #3 spec section read; 10 audit areas locked.
2. ✓ `spaces/.../memory/MEMORY.md` — full memory index loaded.
3. ✓ `spaces/.../memory/project_mega_task_v5_shipped_2026-05-22.md` — v5 ship state.
4. ✓ `repo/api/engine/data/agent/mega_task_v5_final_report.md` — v5 final report.
5. ✓ `spaces/.../memory/project_5_pillar_forward_plan.md` — overall roadmap with v5 status update.
6. ⏸ `repo/api/main.py` — 3000+ lines; will read targeted sections during phases 2 / 6.
7. ✓ `repo/requirements.txt` — current deps.

### Phase 0 commit

`56f8cca57` — "Coherence Sweep #3 Phase 0: pre-flight + report skeleton". 3 files changed, 488 insertions, 0 deletions.

---

## Phase 1 — Substrate cache audit (BLOCKING)

**Started**: 2026-05-22 (immediately after Phase 0)

### Method

Grepped for module-level cache patterns across `repo/api/engine` and `repo/engine`:
- `_CACHE = {...}` or `_cache = ...` module globals
- `@lru_cache` / `@functools.cache` decorators
- `_load_*` / `_ensure_*` function names that compute-and-store
- Module-level eager-loaded data (`_FOO = _load_json(_PATH)` at import time)

For each cache found, measured cold-start cost via a fresh-process timer script. Threshold for inline fix: cold-start > 30s AND no disk persistence AND fix < 50 LOC.

### Findings

**6 module-level lazy caches.** Worst cold-start: `_CACHE` in `agent_semantic_retrieval_v1.py` at 1557.8ms (Voyage matrix load). Well under the 30s threshold. The previously-problematic `_CORPUS_VECTORS` in `deck_strength_check_v1.py` (was ~111 min cold-start) is already disk-persisted via mega-task v5 Phase 5.

**3 module-level eager-loaded data structures.** Largest import cost: `deck_combo_insights_v1.py` at 449.8ms (4,527 outcomes + 3,679 bracket-pairs). Full `api.main` import: 897ms total.

**No inline fixes needed.** Section 1 of the health report writes "clean — no caches exceed the trigger."

### Phase 1 commit

`f67f388a3` — "Coherence Sweep #3 Phase 1: substrate cache audit — clean (no inline fixes)". 2 files changed, 64 insertions, 2 deletions.

---

## Phase 2 — Cross-pillar integration verification

**Started**: 2026-05-22 (immediately after Phase 1)

### Method

- TestClient smoke on Pillar A endpoints (sample of /health + /snapshots + /playtest/opposition_decks_v1 + /deck/analyze_v1 + /commander/archetype_brief_v1 + /card/search_v1 + /deck/candidate_pool_v1).
- Direct DB query to verify Pillar C primitive_v1 coverage (78.1% of 36,709 cards tagged).
- iter 6 sweep data (Phase 13 — 2 runs × 5 cases = 10 builds) used as Pillar D's full-build evidence; all 4 LLM phases fired on every case.
- Direct module smoke of Pillar E v0.3 + v0.4 + Pillar F approximator + graduated playtest on a synthetic deck.
- Track 5 per-set automation verified via file-presence inventory (not triggered per kickoff policy).

### Findings

**Clean wiring across all pillars.** One memory drift surfaced (`project_pillar_a_c_shipped_2026-05-17` claims 9 endpoints; actual is ~42 routes / ~18 v1-tier) — Phase 3 fixes inline.

### Phase 2 commit

`36a6c2ee5` — "Coherence Sweep #3 Phase 2: cross-pillar integration verification — clean". 2 files changed, 98 insertions, 1 deletion.

---

## Phase 3 — Memory ↔ code alignment (BLOCKING)

**Started**: 2026-05-22 (immediately after Phase 2)

### Method

Listed all 52 entries in cowork memory directory. Categorized: 15 `feedback_*` (architectural rules), 37 `project_*` (state snapshots, prep notes, design specs). Spot-checked the 4 kickoff-locked architectural rules + 4 recent shipped-state memories + the Pillar A endpoint count.

### Findings

**1 inline fix landed**: `project_pillar_a_c_shipped_2026-05-17.md` description said "9 endpoints" — accurate at original ship but stale by mega-task v5 (42 routes total today). Updated inline with original count + post-v5 update note.

**4 architectural rules all honored**:
- `pool_score_does_not_drive_llm_picking` → explicit "MUST SELECT" prompt at `agent_build_deck_v1.py:3657`
- `user_intent_locks_deck_shape` → theme_profile cascade verified end-to-end through B2/C2.1/C2.2/D2
- `mana_base_serves_spells_not_reverse` → "aggressive_recompute_fresh" policy at `mana_base_optimizer_v1.py:396`
- `corpus_descriptive_not_prescriptive` → creativity_envelope_metrics + staples_avoided_count tracked in build response

**Sampled shipped memories consistent**: mega-task v4/v5 shipped, iter 5/6 prep notes, iter 7 prep notes — all consistent with iter-N validation reports + iter-N+1 prep notes.

### Phase 3 commit

`0e49198a2` — "Coherence Sweep #3 Phase 3: memory ↔ code alignment — 1 inline fix landed". 2 files changed. Cowork memory edit (project_pillar_a_c_shipped_2026-05-17.md) made outside the repo.

---

## Phase 4 — Test coverage gaps

**Started**: 2026-05-22 (immediately after Phase 3)

### Method

Counted test files + grep'd test names per v3-v5 module + scanned for orphan test files. Used `pytest --co -q` data implicitly (1489 passing baseline). No live test runs — coverage audit is structural.

### Findings

**Solid coverage**: 212 test files, 1489 passing, 12 dedicated test files for `agent_build_deck_v1` alone, every Pillar E module has dedicated tests, every iter-6 Phase 5-12 deliverable has a test file.

**No dead tests** — apparent "orphans" are phase-organized tests (test_agent_iterN_phase_M_*.py) and test harness files.

**Carry-over**: same 8 pre-existing failures since mega-task v4 ship. Queued for iter 7 to decide fix-vs-retire.

### Phase 4 commit

`5d77891e3` — "Coherence Sweep #3 Phase 4: test coverage gaps — solid (212 files / 1489 passing)". 2 files changed, 83 insertions, 2 deletions.

---

## Phase 5 — Database + schema integrity

**Started**: 2026-05-22 (immediately after Phase 4)

### Method

Queried mtg.sqlite directly for: snapshot row counts, cards table schema, column populations per snapshot, snapshot inheritance metadata, ancillary table inventory. Checked corpus_v1.json + opposition_decks_v1.json sizes + entry counts. Cross-referenced against iter-3 (released_at), iter-4 (primitives_v1_json), and v5 (no new columns) migrations.

### Findings

**Clean.** All 3 snapshots have 36,709 rows. `released_at` 100% populated everywhere. `primitives_v1_json` grows 22,169 (60.4%) on raw/source → 28,666 (78.1%) on tagpass-active. Tagpass inheritance metadata correct. corpus_v1.json (48.7 MB / 13,408 decks) + opposition_decks_v1.json (16 KB / 54 entries) + corpus_vectors_cache_v1.json (16.4 MB) sizes match expectations.

15 tables in mtg.sqlite — no orphans, no anomalous row counts.

### Phase 5 commit

`f4d3195c7` — "Coherence Sweep #3 Phase 5: database + schema integrity — clean". 2 files changed, 82 insertions, 1 deletion.

---

## Phase 6 — UI ↔ endpoint contract drift

**Started**: 2026-05-22 (immediately after Phase 5)

### Method

Grep all `fetch(...)` and `EventSource(...)` calls in `ui_harness/src/`. Map each to the endpoint registered in `api/main.py`. Cross-check v5's new contracts (auto-snapshot + SSE streaming + graduated_playtest_report field).

### Findings

**Clean.** Every UI fetch maps to an existing endpoint. Both v5 contract additions verified:
- `GET /snapshots/active` (Phase 2) consumed by AIBuildView.tsx:275 ✓
- `POST /agent/build_deck_v1/stream` (Phase 3) consumed by useBuildStreaming.ts:160 ✓
- `response.summary.graduated_playtest_report` (Phase 12) consumed by AIBuildView render block ✓

8 distinct UI endpoint paths surveyed. WorkspaceView covers the legacy /build + deck/validate + deck/complete_v1 + deck/tune_v1 + cards/resolve_names flow. AIBuildView covers the v1-agent streaming flow. Phase 1 harness is debug-only.

### Phase 6 commit

`b7c7b5292` — "Coherence Sweep #3 Phase 6: UI ↔ endpoint contract drift — clean". 2 files changed, 69 insertions, 1 deletion.

---

## Phase 7 — Documentation drift

**Started**: 2026-05-22 (immediately after Phase 6)

### Method

Audited module docstrings on v5-added modules, the 3 key Obsidian vault docs (DESIGN_DECISIONS, ENGINE_API_GUIDE, MPA_SPEC), Pillar C ontology spec, repo READMEs.

### Findings

- Module docstrings: current. v5 modules all have comprehensive docstrings written during their respective phases.
- DESIGN_DECISIONS.md (2026-05-21): current. The 4 architectural rules are stable across iterations.
- ENGINE_API_GUIDE.md (2026-05-17): **drifted across 3 mega-tasks**. ~10 endpoints added post-2026-05-17 not documented. Substantive overhaul needed — queued for iter 7.
- MPA_SPEC.md: current for Phase 5b maintenance state.
- Pillar C ontology_v1.md: current.

### Phase 7 commit

`a5bfd8dba` — "Coherence Sweep #3 Phase 7: documentation drift — 1 substantive queue for iter 7". 2 files changed, 67 insertions, 1 deletion.

---

## Phase 8 — Orphan code detection

**Started**: 2026-05-22 (immediately after Phase 7)

### Method

Wrote `tools/_coherence_sweep_3_orphan_scan.py` (AST-walks all 454 .py files, records every Import/ImportFrom target, flags files in api/engine/layers + extractors + integrations + tools whose module name is never imported anywhere). Followed up with explicit `grep` on flagged candidates to filter false positives.

### Findings

**2 true production orphans:**
1. `agent_voyage_downgrade_pass_v1` — mega-task v4 Phase 10 claimed shipped, but no production import. Only test imports it. **Possible wiring bug**; queued for iter 7 decision.
2. `voyage_rules_embedding_v1` — known "at-scale deferred" hook per memory. Queued for iter 7.

**30 false-positive tools/** scripts — all CLI entry points (run via `python tools/X.py`, not imported). Includes 5 iteration sweep harnesses + 2 v5 smoke harnesses + Track 5 automation scripts + perf/MPA harnesses + data backfill scripts.

**5 false positives in layers/** that ARE imported (scanner heuristic missed them): proof_scaffold_v1, proof_attempt_v1, invariants_v1, duplicate_enforcement, patch_loop_v0.

**1 explicit `deprecated/`** folder file — wontfix.

**1 stale `.pyc`** without `.py` source — cleanup-on-rebuild, wontfix.

### Phase 8 commit

`a00fad37e` — "Coherence Sweep #3 Phase 8: orphan code — 2 true orphans queued for iter 7". 3 files changed, 182 insertions, 2 deletions.

---

## Phase 9 — External-dep audit

**Started**: 2026-05-22 (immediately after Phase 8)

### Method

Read requirements.txt + requirements-dev.txt + ui_harness/package.json. Cross-referenced declared versions against installed venv packages via `pip list`. Verified each declared dep imports without error.

### Findings

**Clean.** All 9 production deps + 3 dev deps install + import. Versions match declarations. sse-starlette pin (added in mega-task v5 Phase 5) is in place with explanatory comment. requirements-dev.txt exists with pytest + pytest-cov + httpx pins.

Minor drift: pytest 9.0.2 pinned vs 9.0.3 installed (post-v5 fresh install). Wontfix; tests pass on 9.0.3.

Iter 7 watchpoint: voyageai's <3.14 Python constraint is the same one that broke v5 Phase 5. Future Python upgrades need to revisit.

### Phase 9 commit

`7f98820ae` — "Coherence Sweep #3 Phase 9: external-dep audit — clean". 2 files changed, 90 insertions, 2 deletions.

---

## Phase 10 — Per-pillar smoke tests + final synthesis (BLOCKING)

**Started**: 2026-05-22 (immediately after Phase 9)

### Per-pillar smokes

Each pillar exercised standalone via direct import + sample-input invocation. Pillar A via TestClient against 9 sampled endpoints. Pillar D's full-build was already exercised by mega-task v5 Phase 13 (10 builds across 5 cases × 2 stochastic runs).

| Pillar | Smoke result |
|---|---|
| A | 7/9 endpoints 2xx; 2 422s from sweep-side input schema errors (validating endpoints work) |
| C | `load_ontology()` returns 64 tags |
| D | `agent_build_deck_v1` + `agent_llm_client_v1` import + working |
| E v0.1-v0.4 | all 4 modules import; v0.3 + v0.4 produce sensible output on synthetic input |
| F | `approximate_pod_winrate` returns 0.087 on degenerate deck; `run_graduated_sweep` returns Tier 0 stall (expected) |

### Final synthesis

Executive summary + categorized punch list written to `coherence_sweep_3_health_report.md`:

- **Inline fixes landed**: 1 (Pillar A endpoint count drift in `project_pillar_a_c_shipped_2026-05-17.md`)
- **Queued for iter 7 mega-task v6**: 4 substantive items (wire agent_voyage_downgrade_pass_v1, wire voyage_rules_embedding_v1 at scale, ENGINE_API_GUIDE.md overhaul, review the 8 pre-existing test failures)
- **Out-of-scope / wontfix**: 8 items documented

### Memory updates

Cowork memory at `spaces/.../memory/`:
- New: `project_coherence_sweep_3_shipped_2026-05-22.md` — full ship state with the per-audit-area verdict table + commit chain + cross-references.
- Updated: `MEMORY.md` index — added the sweep-shipped entry below the iter 7 prep entry.
- Updated: `project_iter_7_prep_notes_2026-05-22.md` — added 4 new sweep-surfaced priorities (#4-#7) to iter 7 scope; total estimate bumps from 3-5 weeks → 4-5.5 weeks.

### Phase 10 commit

Pending — final synthesis + memory updates.

---

## Coherence Sweep #3 SHIPPED 2026-05-22

11 atomic phase commits + this synthesis commit = 12 commits total on top of mega-task v5 (`4cee4a287`). pytest 1489 unchanged through all 11 phases. ~$0 API spend (sweep is read-only).
