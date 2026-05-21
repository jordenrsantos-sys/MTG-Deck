# Mega-task v5: Pillar D iter 6 — UX unblocker + CC hand-off + Pillar E v0.3-v0.4 + Graduated Playtest Stage 1

Single self-contained kickoff. You are authorized to run autonomously from start to finish without further user interaction unless a hard halt condition triggers. Self-correct using the tiered escalation. Atomic commit per phase. Maintain a running progress log throughout. Cross-phase regression check at every phase boundary.

---

## What this mega-task delivers

The agent stack shipped 5 iterations of capability in mega-tasks v1-v4 but the production UI path is currently UNUSABLE despite the engine working perfectly (Python tool sweeps confirm 5/5 cases run cleanly in 109-130s). The browser UI gives zero feedback during the 110-150s build, making "running" indistinguishable from "stuck." User abandoned a Jin Sakai build at 180s in today's live evaluation thinking it was hung when it was almost certainly 30-60s from finishing.

**Priority #1 in this mega-task is fixing the UX so the agent becomes USABLE not just CAPABLE.** Then the architectural work: CC's three iter 5 hand-off items, Pillar E v0.3-v0.4 structural fundamentals (the user's "general stuff land base draw recursion interaction" concern), and Graduated Playtest Stage 1 (tiered opposition framework via Pillar F v0.1 statistical approximator).

Read these files at Phase 0:

1. `spaces/.../memory/project_iter_6_prep_notes_2026-05-21.md` (locked iter 6 priorities — source of truth)
2. `spaces/.../memory/project_mega_task_v4_shipped_2026-05-21.md` (v4 ship state)
3. `spaces/.../memory/project_graduated_playtest_spec_2026-05-21.md` (Priority #4 detail)
4. `spaces/.../memory/feedback_pool_score_does_not_drive_llm_picking.md` (autonomous CC lesson from iter 5)
5. `repo/api/engine/data/agent/mega_task_v4_final_report.md` (v4 final report + hand-off section)
6. `repo/api/engine/data/agent/pillar_d_iteration_5_validation_report.md` (iter 5 metrics)
7. `repo/api/engine/layers/agent_build_deck_v1.py` (main agent endpoint)
8. `repo/ui_harness/src/views/AIBuildView.tsx` (current UI implementation)
9. `repo/api/main.py` (uvicorn + endpoint registration; CORS middleware)

---

## Substrate state

Mega-task v4 shipped 2026-05-21 (final commit `e97589870`, 15 commits on top of v3's `74d9dcfd1`). Iter 5 final sweep: 9/11 criteria pass (most recent measurement, slightly improved from the 8/11 Phase 13 commit due to Patch 1's Yuriko structural fix holding cleanly). Test baselines: pytest 1377 / vitest 711.

**Iter 5 baseline you must not regress:**

- iter1 structural pass: 5/5
- mean creativity_delta: 35.2
- mean novel_combo: 5.6
- mean cost: $0.30
- mean wallclock: 118.0s
- voyage_semantic_avg: 1.8 (KNOWN ISSUE — fix in Phase 6)
- pillar_c coverage_v1: 93.0%
- intent_drift: 0.592 (KNOWN ISSUE — fix in Phase 7)
- pillar_f_ordering preserved (Yuriko > Krenko > Edgar ≈ Ur-Dragon > Atraxa)
- theme_profile structured 5/5

**Architectural assets shipped through iter 5:**

- B2 structured weighted theme profile (4 modes: cards-only / hint-led / hybrid / bare-commander)
- Theme profile cascades through C2.1 / C2.2 / D2
- Theme-aware Pillar E target counts (18-theme matrix + blender)
- User-intent-preservation drift validation
- Pillar C ontology v1 with rules_modifiers dimension (17 new tags, 93% coverage)
- Voyage rules-embedding pipeline (at-scale deferred — Phase 4 hook in place)
- Combo registry merger + 12 curated external entries
- Aggressive mana-base reconciliation (any-delta gate per spell-base-first principle)
- Mana-cost-aware Voyage downgrade pass
- Functional-diversity prompt-engineering with Pillar E target awareness
- C2.1 prompt trim (Yuriko C2.1 latency 51s→38s)
- Outer-chain parallelization (C2.1 || C2.2) verified saving 22s/case
- Per-set automation pipeline operational (Scryfall watcher + atomic ingestion + Sonnet report)

**File layout iter 6 will work in:**

- `repo/api/main.py` — uvicorn entry, CORS middleware, endpoint registration (Phase 1 worker config + SSE endpoint adds)
- `repo/api/engine/layers/agent_build_deck_v1.py` — main agent endpoint (Phase 3 SSE event emission + Phase 6-8 fixes)
- `repo/api/engine/layers/agent_semantic_retrieval_v1.py` — Voyage queries (Phase 6 color-filter fix)
- `repo/api/engine/layers/agent_c22_prompt_templates_v1.py` — Phase 7 theme signal density expansion
- `repo/api/engine/layers/mana_base_optimizer_v1.py` — Pillar E v0.1 (referenced by Phase 9)
- `repo/api/engine/layers/card_advantage_optimizer_v1.py` — Pillar E v0.2 pattern (Phase 9-10 new optimizers follow)
- `repo/api/engine/layers/agent_statistical_approximator_v1.py` — Pillar F v0.1 (Phase 11-12 graduated playtest)
- `repo/api/engine/data/playtest/opposition_decks_v1.json` — opposition registry (Phase 11 tier expansion)
- `repo/ui_harness/src/views/AIBuildView.tsx` — UI (Phase 2-5 UX bundle)
- `repo/ui_harness/src/hooks/` — likely existing hooks for fetch; new `useBuildStreaming` hook in Phase 3
- `repo/launch_dev.cmd` — Phase 1 uvicorn worker config

---

## Authority and scope

You are AUTHORIZED to:

- Run all 14 phases (0-13) autonomously without halting for user direction except on the hard halt conditions below.
- Self-correct using the tiered escalation when a validation gate fails.
- Make atomic commits per phase: `git commit -m "Phase X (mega-task v5): <description>"`.
- Modify any file in `repo/api/`, `repo/ui_harness/`, `repo/tests/`, `repo/tools/`, `repo/launch_dev.cmd`, `repo/requirements.txt`.
- Add new dependencies via pip if needed (e.g., `sse-starlette` if not already present for Phase 3 SSE; nothing else exotic expected).
- Read and write Cowork memory at `spaces/.../memory/` for material findings only.
- Use chrome-devtools-mcp for live UI verification during Phase 5 (UX bundle validation) and Phase 13 (final sweep).
- Use the mtg-engine MCP and obsidian MCP for endpoint calls + reports.

You are NOT authorized to:

- Upgrade the Anthropic model from Sonnet 4.6 to Opus. Stay on Sonnet for all LLM calls.
- Modify Pillar A endpoints. All changes in agent layer + Pillar C/E/F extensions + UI.
- Modify iter 1-5 baseline test cases in ways that change their behavior.
- Roll back any commit. Forward fixes only.
- Touch the Phase 5b MPA substrate (`mpa_*.py`).
- Modify `combo_brackets_v1.json` directly. Use the additive `combo_brackets_v1_external_sources.json` pattern from iter 5.
- Touch the per-set automation pipeline from v3 unless a phase here specifically requires it.
- Add web-fetching beyond Anthropic SDK + Voyage embeddings API + Scryfall API.

---

## Hard halt conditions (NARROW — halt only on these)

1. **Validation gate fails 3 times in a row** in any single phase after exhausting tiered self-correction. Write current state + diagnosis to progress log and halt.
2. **Critical regression**: any iter 1-5 success criterion (under revised targets) breaks at any phase boundary. Halt immediately with diff that broke it.
3. **Resource exhaustion**: API spend reaches $100 (v5 ceiling) or disk usage exceeds 95% on E: drive. Graceful checkpoint and halt.
4. **Architectural contradiction**: a phase spec turns out to be impossible to implement without contradicting a prior phase's output. Write the contradiction inline in the progress log with both sides and halt.
5. **Phase 13 final validation fails on >= 3 of 12 success criteria.** Halt; don't proceed to Phase 14 final regression on a broken iter 6.
6. **Cumulative test suite regression** at any phase: pytest baseline drops below 1377 OR vitest baseline drops below 711 + new tests this mega-task adds. Halt and diagnose.
7. **Phase 5 UX validation fails**: the live UI walk-through (chrome-devtools-mcp) cannot complete a build from form-submit to deck-render. Halt — UX bundle is broken and must be fixed before proceeding to architectural work.

You do NOT halt for:

- Single test failures you can fix on the next attempt.
- Minor metric drifts.
- Token budget overruns on a single call.
- Linting warnings, deprecation notices.
- Implementation choices with multiple valid paths.
- Voyage / Anthropic API transient errors (retry with exponential backoff).

---

## Self-correction protocol (tiered escalation)

**Tier 1** — Re-read phase spec + relevant memory + iter 5 final report. Try alternate implementation path. Up to 3 attempts.

**Tier 2** — Search codebase for similar patterns. Adapt existing patterns. Up to 2 attempts.

**Tier 3** — Add known-gap note to progress log; skip remaining work for phase and continue. Only allowed for non-blocking phases.

**Tier 4** — Halt for user direction.

**Blocking phases that cannot Tier-3-skip:**

- Phase 1 (uvicorn workers) — foundational for subsequent UX phases
- Phase 3 (build progress streaming) — the critical UX unlock
- Phase 5 (UX validation) — must confirm the bundle works
- Phase 6 (Voyage color-filter fix) — iter 5's most reported architectural gap
- Phase 11 (graduated playtest tiered registry) — foundation for Phase 12
- Phase 13 (final validation) — must pass before Phase 14
- Phase 14 (final regression + memory) — must pass to declare done

Phases 2, 4, 7, 8, 9, 10, 12 are non-blocking.

---

## Progress log

Write to `repo/api/engine/data/agent/mega_task_v5_progress_log.md` from Phase 0. Same format as v1-v4 — append-only, timestamped sections per phase.

Update at: every commit, every Tier-N escalation, every halt event, every hour of long phases.

---

## Resource budget

- **Total API spend ceiling: $100.** Alarm at $80; hard halt at $100. Expected actual: $30-70.
- **Per-phase rough budget**: $3-15 for development LLM calls; $1.50 for validation sweeps; $0.30 for smoke tests.
- **Wall-clock budget**: 24-72 hours of wall-clock. Phases 9-10 (Pillar E v0.3-v0.4) are the longest.

---

## Test discipline

Run after EVERY commit:

```bash
cd "E:\MTG Root\mtg-engine\repo"
pytest -q
cd "E:\MTG Root\mtg-engine\repo\ui_harness"
npm test -- --run
```

Both must pass. Baselines: pytest 1377 + v5's new tests; vitest 711 + v5's new tests.

Phase 5 UX validation: live UI walk via chrome-devtools-mcp. Open `localhost:5173/#ai-build`, run Edgar test case, watch progress streaming, verify Cancel button works mid-build, verify timeout fires if mocked >240s, verify final deck renders.

Phase 13 iter 6 validation sweep: 5-case Python sweep + 5 UI cases via chrome-devtools-mcp, capture 12 success criteria.

Phase 14 final regression: full pytest + vitest + 5-case sweep + UI sanity + graduation report card sanity on each case.

---

## Phases

### Phase 0 — Pre-flight + memory sync

Read the 9 files listed in "What this mega-task delivers." Confirm env (Python 3.10+, git clean, pytest 1377 baseline, vitest 711 baseline, ANTHROPIC_API_KEY + VOYAGE_API_KEY set, MCPs connected). Create `repo/api/engine/data/agent/mega_task_v5_progress_log.md`. Commit: "Phase 0 (mega-task v5): pre-flight + progress log scaffold".

---

### Phase 1 — uvicorn workers ≥ 2 (BLOCKING)

Fix the foundational issue where single-worker uvicorn blocks all requests during builds.

**Implementation:**

1. Update `launch_dev.cmd` to launch uvicorn with `--workers 2` (or `--workers 4` if memory headroom permits — check with a smoke test).
2. Verify the engine can handle 2+ concurrent requests by hitting `/health` mid-build (should respond quickly even with a build in flight).
3. If FastAPI/uvicorn has worker startup issues on Windows (it sometimes does), document the actual command that works.

**Tests:**

- Concurrent-request smoke test: kick off a build via Python tool, then hit `/health` from another shell — `/health` should respond in <100ms despite the build running.

**Smoke test:** Run a 1-case Edgar build, simultaneously hit `/health` 5 times in another shell. All 5 should return 200 immediately.

**Commit**: "Phase 1 (mega-task v5): uvicorn multi-worker for concurrent request handling".

---

### Phase 2 — Auto-default snapshot_id + field placeholder/help text

**Implementation:**

1. New endpoint `GET /snapshots/active` returns the latest snapshot_id (`20260217_190902_tagpass_20260222` currently). Cached per engine startup.
2. In `AIBuildView.tsx`:
   - On mount, fetch `/snapshots/active` and set the snapshotId state to the response.
   - Hide the Snapshot ID input by default. Move it under an "Advanced options" toggle (collapsed by default).
   - Add placeholder text to all visible inputs: Commander ("e.g., Edgar Markov"), Theme hints ("e.g., aristocrats, graveyard recursion (optional — agent infers from cards)"), Must-includes ("e.g., Vito, Thorn of the Dusk Rose").

**Tests:**

- Component test: mount AIBuildView, verify snapshotId is fetched and populated automatically.
- Snapshot endpoint unit test.

**Smoke test:** Open `localhost:5173/#ai-build`, verify Commander field is the first visible input, Snapshot ID is hidden under Advanced, all fields have placeholder text.

**Commit**: "Phase 2 (mega-task v5): auto-default snapshot_id + UI placeholder/help text".

---

### Phase 3 — Build progress streaming (BLOCKING)

The critical UX unlock. Server-sent events from agent endpoint to UI.

**Implementation:**

1. Add SSE support: install `sse-starlette` if not present. Add SSE endpoint variant `POST /agent/build_deck_v1/stream` that returns event-stream content type.
2. In `agent_build_deck_v1.py`, refactor so the build flow can emit progress events at phase boundaries:
   - `{phase: "intent_interpreter", status: "started", elapsed_s: 0.0}`
   - `{phase: "intent_interpreter", status: "completed", elapsed_s: 24.1, cost_usd: 0.020}`
   - `{phase: "candidate_critic", status: "started", elapsed_s: 24.1}`
   - ... and so on for C2.1, C2.2, D2 (per batch), Pillar E (mana + card advantage), Pillar F approximator
   - Final event: `{phase: "complete", status: "completed", elapsed_s: 118.0, total_cost_usd: 0.30, response: <full deck JSON>}`
3. In `AIBuildView.tsx`:
   - Add `useBuildStreaming` hook that connects to the SSE endpoint and emits state updates.
   - Display current phase + elapsed time + cost-to-date during the build.
   - Final deck renders from the `complete` event.
4. Maintain backward compatibility: the original `POST /agent/build_deck_v1` (non-streaming) still works for Python tool / programmatic clients.

**Tests:**

- SSE endpoint unit tests with mocked event stream.
- UI hook test: stream of mocked events updates UI state correctly.
- Backward compat: non-streaming endpoint still functions.

**Smoke test:** Open `localhost:5173/#ai-build`, run a build, watch the progress display update with each phase. Verify all expected phases appear in order with non-zero elapsed times.

**Commit**: "Phase 3 (mega-task v5): build progress streaming via SSE + UI progress display".

---

### Phase 4 — Elapsed timer + cancel button + timeout

**Implementation:**

1. In `AIBuildView.tsx`:
   - Add a client-side stopwatch component shown next to the Build button during builds. Format: "Building... 47s (typical 110-130s)".
   - Replace the grayed-out "Building..." button with an actionable Cancel button. On click, aborts the fetch/EventSource and resets state.
   - Add a client-side timeout: if elapsed >240s, abort with explicit error: "Build exceeded expected duration. Check engine logs in launch_dev.cmd terminal."

**Tests:**

- Stopwatch component test with mocked timers.
- Cancel button test (mocked fetch abort).
- Timeout test (mocked 240s elapsed).

**Smoke test:** Start a build, watch the stopwatch increment, click Cancel mid-build, verify state resets and no deck renders. Separately, mock a hung build and verify the 240s timeout fires.

**Commit**: "Phase 4 (mega-task v5): elapsed timer + cancel button + 240s timeout".

---

### Phase 5 — UX bundle validation (BLOCKING)

Live UI walk via chrome-devtools-mcp validates the bundle works end-to-end.

**Implementation:**

1. Use chrome-devtools-mcp to:
   - Open `localhost:5173/#ai-build`
   - Verify all Phase 2 surface changes (auto-populated snapshot_id, placeholder text, hidden advanced option)
   - Submit an Edgar build (Commander: "Edgar Markov", bracket: B3, must-includes: "Vito, Thorn of the Dusk Rose" + "Cordial Vampire", no theme hints)
   - Verify Phase 3 progress display updates with each phase
   - Verify Phase 4 stopwatch increments correctly
   - Verify Cancel button works mid-build (test once, then re-run to completion)
   - Verify final deck renders after ~120s
2. Screenshot key states for the final report.

**Halt condition:** If the full flow doesn't work end-to-end (any phase missing, UI doesn't render, timeout fires unexpectedly), halt and diagnose. Cannot proceed to architectural phases on a broken UX bundle.

**Commit**: "Phase 5 (mega-task v5): UX bundle live validation".

---

### Phase 6 — Voyage color-filter gap diagnosis + fix (BLOCKING)

Iter 5's voyage_semantic_avg = 1.8 vs target ≥3. Krenko mono-R = 0 picks, Yuriko UB = 0 picks suggest filter excludes too aggressively.

**Implementation:**

1. Audit `agent_semantic_retrieval_v1.py::query_neighbors` — find the color-identity filter logic.
2. Compare against the actual cards in the Voyage index — what subset comparison is happening, what shape does `color_identity_filter` expect, what shape do the index entries have?
3. Most likely bugs:
   - Subset comparison using strings/lists where sets are needed
   - color_identity stored as JSON string but compared as list
   - Empty color_identity (colorless cards) being excluded when they should be included
   - Multi-color cards being excluded by an "exact match" instead of "subset"
4. Fix the bug. Re-run on Krenko + Yuriko test cases — both should now produce ≥3 semantic neighbors each.

**Tests:**

- Unit tests for color-identity subset comparison with all edge cases (mono / 2-color / 3-color / 4-color / 5-color / colorless / hybrid mana).
- Integration test: Krenko (R) query returns ≥10 R-color neighbors. Yuriko (UB) query returns ≥10 U-or-B-or-UB-or-colorless neighbors.

**Smoke test:** Run Krenko + Yuriko builds; each should produce ≥3 semantic picks in the final deck.

**Commit**: "Phase 6 (mega-task v5): Voyage color-identity-filter fix for low-color anchors".

---

### Phase 7 — Theme signal density expansion

Iter 5's intent_drift = 0.592 (Atraxa 0.869 + Ur-Dragon 0.679 outliers). Classifier vocab too tight for multi-theme archetypes.

**Implementation:**

1. Audit `_THEME_PRIMITIVE_SIGNALS` (or equivalent) in `agent_c22_prompt_templates_v1.py` / the theme classifier.
2. Expand signals for the outlier archetypes:
   - `counters_matter` (Atraxa): add proliferate-specific signals, multi-counter signals, +1/+1 distribution signals
   - `tribal` with value-engine subtype (Ur-Dragon): add cost-reduction, big-creature-aggro, ETB-trigger signals
3. Adjust drift threshold to be archetype-aware: if primary theme is `counters_matter` or `tribal-with-value-engine`, allow drift up to 0.7 (these naturally produce broader expressions).
4. Re-run on Atraxa + Ur-Dragon — drift should drop below 0.5 (or below the archetype-aware threshold).

**Tests:**

- Unit tests for expanded signal sets.
- Drift computation tests for Atraxa and Ur-Dragon should pass under new logic.

**Smoke test:** Atraxa + Ur-Dragon builds produce intent_drift < threshold per archetype.

**Commit**: "Phase 7 (mega-task v5): theme signal density expansion + archetype-aware drift thresholds".

---

### Phase 8 — Atraxa C2.1 silent-failure investigation + fix

Iter 5 reports Atraxa C2.1 latency = 0.0s. Budget-guard short-circuit firing on its larger forbidden-set prompt.

**Implementation:**

1. Add logging to C2.1's budget guard so we can see WHY it's short-circuiting on Atraxa.
2. Likely cause: Atraxa has 4 colors → larger forbidden_set from Phase 2 combo-anchor guard → exceeds C2.1 prompt's budget.
3. Fix options (pick one):
   - Compress the forbidden_set serialization in the C2.1 prompt (just card names, not full descriptions)
   - Split forbidden_set into per-color sections, only include the colors relevant to the candidate being evaluated
   - Bump C2.1 budget for high-forbidden-set cases
4. Re-run Atraxa — C2.1 should now execute and contribute to wallclock + creativity_delta normally.

**Tests:**

- Unit test: Atraxa case with 4-color forbidden_set doesn't trigger budget guard.
- Integration: Atraxa wallclock contribution from C2.1 is non-zero.

**Smoke test:** Atraxa build's C2.1 phase reports non-zero latency.

**Commit**: "Phase 8 (mega-task v5): Atraxa C2.1 silent-failure fix".

---

### Phase 9 — Pillar E v0.3 curve smoother

The user's structural-fundamentals concern from today. Identifies curve bricks (cards above natural ceiling) + holes (missing CMC slots).

**Implementation:**

1. New module `repo/api/engine/layers/curve_smoother_v1.py`:
   - Function `analyze_curve(deck, archetype_hint) -> CurveAnalysis`
   - Computes the deck's mana-cost distribution; compares to archetype-typical curves (per a new `curve_targets_by_archetype_v1.json` data file).
   - Identifies bricks (CMC > deck's natural ceiling by archetype) and holes (CMC slots with too few cards for the curve).
   - Returns recommendations: swap brick A → cheaper alternative B, fill hole at CMC 2 with one of [...].
2. Integration: runs after Pillar E v0.2 card advantage reconciliation; LLM critique pass on swap suggestions.

**Tests:**

- Unit tests for curve analysis with 5+ deck shapes.
- Reference: a tribal-aggro deck flags 7-CMC cards as bricks; a control deck flags them as fine.

**Smoke test:** 5-case sweep — Edgar/Krenko/Atraxa decks should have smoother curves post-Phase 9 than pre-Phase 9. No regression in other metrics.

**Commit**: "Phase 9 (mega-task v5): Pillar E v0.3 curve smoother".

---

### Phase 10 — Pillar E v0.4 interaction designer

The big one. Currently LLM-judgment-only; this adds structural enforcement.

**Implementation:**

1. New module `repo/api/engine/layers/interaction_designer_v1.py`:
   - Function `compute_interaction_targets(commander_color_identity, bracket, archetype_hint) -> InteractionTargets`
   - Returns target counts: counterspells (U only), targeted-creature-removal, targeted-artifact-removal, targeted-enchantment-removal, mass-removal (board wipes), graveyard-interaction.
2. Per-bracket policy:
   - B1/B2: 8-10 total interaction, 70% sorcery-speed, 1-2 mass-removal
   - B3/B4: 10-12 total, 50% sorcery-speed, 2-3 mass-removal
   - B5 (cEDH): 12-15 total, ~80% instant-speed (counterspells dominate), 0-1 sorcery-speed mass-removal
3. Color-policy: counterspells only counted if in U; black gets edicts + targeted; white gets exiles + wraths; red gets damage-based.
4. Integration: runs after Pillar E v0.3 curve smoother; LLM critique pass on additions/swaps.

**Tests:**

- Unit tests for targets across 10+ commander color combinations + brackets.
- Reference values: Atraxa B2 (4-color) gets ~10 interaction with 4 counterspells (U included). Krenko B4 (mono-R) gets ~10 interaction with 0 counterspells, 3 damage-based removal, 2 mass-damage.

**Smoke test:** 5-case sweep — all decks have interaction counts within ±2 of target by bracket+colors.

**Commit**: "Phase 10 (mega-task v5): Pillar E v0.4 interaction designer".

---

### Phase 11 — Graduated playtest Stage 1: tiered opposition registry (BLOCKING)

Per `project_graduated_playtest_spec_2026-05-21`.

**Implementation:**

1. Expand `opposition_decks_v1.json` (currently 19 entries) into tiered registry. Per bracket B1-B5:
   - Tier 0 (precons): 3 official precon decklists. Source: Scryfall data or Wizards' precon publications.
   - Tier 1 (mid-tier): 3 community-standard decks. Source: EDHREC top decks per archetype (already in corpus from Phase 5a).
   - Tier 2 (high-tier): 3 cEDH-tier decks (B5 only, top cEDH-decklist-database lists).
2. New schema field: `opposition_tier: 0 | 1 | 2`. Maintain backward compat with existing 19 entries.
3. Populate the tiered registry: 3 × 5 brackets × 3 tiers = potentially 45 decks. Reasonable start: focus on B2/B3/B4/B5 (drop B1 if precons aren't well-distinguished), giving ~36 decks. Some can be shared across tiers if they're the same archetype at different power levels.

**Tests:**

- Schema validation tests.
- Integration: registry loads, tiers parseable, all decks have valid card lists.

**Smoke test:** Load the tiered registry; verify each bracket has 3 decks per tier (or documented gaps).

**Commit**: "Phase 11 (mega-task v5): tiered opposition deck registry (Tier 0 / 1 / 2 per bracket)".

---

### Phase 12 — Graduated playtest Stage 1: graduation logic + report card

**Implementation:**

1. New module `repo/api/engine/layers/agent_graduated_playtest_v1.py`:
   - Function `run_graduated_sweep(deck, bracket) -> GraduationReport`
   - Runs Pillar F v0.1 statistical approximator against Tier 0 pod (commander + 3 precons of bracket). If predicted_winrate ≥ 0.55, advance to Tier 1. If passes, advance to Tier 2.
   - Returns per-tier predicted winrates + graduation status + suggested tweaks (placeholder for Stage 3).
2. Wire into agent build flow: after Pillar E reconciliation, before final response, run graduated sweep. Surface in response as `graduated_playtest_report`.
3. UI displays graduation report card: "✓ Tier 0 (78%), ✓ Tier 1 (62%), ✗ Tier 2 (31%)" with tier-tier labels (Precons / Mid-tier / High-tier).

**Tests:**

- Unit tests for graduation logic with mocked Pillar F approximator outputs.
- Threshold sensitivity test.
- Integration: 5-case sweep produces graduation reports, all decks at least pass Tier 0 (precons) sanity.

**Smoke test:** Edgar build returns graduation_playtest_report with all 3 tier predictions.

**Commit**: "Phase 12 (mega-task v5): graduated playtest Stage 1 — graduation logic + report card UI".

---

### Phase 13 — Iter 6 final validation sweep + report (BLOCKING)

**Capture per case (5 iter-2 sweep cases via Python tool + 5 UI cases via chrome-devtools-mcp):**

All iter 5 metrics + UX-bundle live confirmation + Phase 6-8 fix metrics + Phase 9-10 Pillar E v0.3-v0.4 outputs + Phase 12 graduation reports.

**Iter 6 success criteria (12 total, must hit at least 10):**

1. `iter1_structural_pass_5_of_5`
2. `mean_creativity_delta >= 35`
3. `mean_novel_combo >= 5`
4. `mean_cost <= $0.45`
5. `mean_wallclock <= 120s`
6. `voyage_semantic_avg >= 3` (Phase 6 fix should close to 3+)
7. `intent_drift_mean < 0.5` (Phase 7 fix should close)
8. `atraxa_c2_1_latency > 0` (Phase 8 fix)
9. `pillar_e_v0_3_curve_check on 5/5 cases` (Phase 9)
10. `pillar_e_v0_4_interaction_count_within_target on 4/5 cases` (Phase 10)
11. `graduated_playtest_report present on 5/5 cases with Tier 0+ predictions` (Phase 12)
12. `ui_live_walk_all_5_cases_complete_via_chrome_devtools` (Phase 5 + chrome-devtools verification)

Write report to `repo/api/engine/data/agent/pillar_d_iteration_6_validation_report.md`. Include iter 6 → iter 7 hand-off section.

**Halt condition:** if >= 3 of 12 success criteria fail, halt for user direction.

**Commit**: "Phase 13 (mega-task v5): iter 6 final validation sweep + report".

---

### Phase 14 — Final regression + report + memory update (BLOCKING)

**Run:**

1. Full pytest + vitest (must pass).
2. 5-case Python sweep (re-validate iter 6 metrics).
3. Live UI sanity via chrome-devtools-mcp (one Edgar build start-to-deck-render).
4. Pillar E v0.3 + v0.4 standalone smokes.
5. Graduated playtest end-to-end on a sample deck.

**Write final report** to `repo/api/engine/data/agent/mega_task_v5_final_report.md`.

**Update memory:**

- New memory file `spaces/.../memory/project_mega_task_v5_shipped_<date>.md`
- Update MEMORY.md index
- Update `project_5_pillar_forward_plan.md` to reflect what shipped + push remaining Pillar E v0.5-v0.6 + at-scale Voyage rules + live combo extractors to iter 7

**Commit**: "Phase 14 (mega-task v5): final regression + report + memory update".

---

## Mega-task v5 success criteria

Mega-task is "done" when ALL hold:

1. All 14 phases committed and Phase 14 final regression passes.
2. Phase 13 sweep meets ≥10 of 12 success criteria.
3. UX bundle (Phases 1-5) live-validated end-to-end via chrome-devtools-mcp.
4. Voyage color-filter gap fixed (Phase 6): Krenko + Yuriko produce ≥3 semantic picks each.
5. Theme signal density expanded (Phase 7): Atraxa + Ur-Dragon intent_drift below archetype-aware thresholds.
6. Atraxa C2.1 silent-failure fixed (Phase 8): non-zero C2.1 latency.
7. Pillar E v0.3 curve smoother + v0.4 interaction designer shipped + 5-case sweep clean.
8. Graduated playtest Stage 1 operational: tiered opposition registry + graduation logic + UI report card.
9. pytest + vitest baselines preserved + new tests pass.
10. Total API spend under $100.

---

## What NOT to do

- Don't upgrade to Opus.
- Don't break iter 1-5 baseline test cases.
- Don't modify `combo_brackets_v1.json` directly.
- Don't touch Phase 5b MPA substrate.
- Don't modify Pillar A endpoints (changes in agent layer + new modules + UI).
- Don't churn memory.
- Don't pad; ship simpler when spec allows.
- Don't disable the v3 per-set automation scheduled task.
- Don't try to ship Pillar E v0.5-v0.6 in this mega-task (iter 7 work).
- Don't try Stage 2 graduated playtest (game simulation requires Pillar F v0.2 substrate — multi-month).

---

## Iter 6 → iter 7 hand-off questions (your Phase 14 final report must answer)

1. Did Phase 1-5 UX bundle ship end-to-end with live UI walk validating each piece? Where did it fall short?
2. Did Phase 6 Voyage color-filter fix close the voyage_semantic_avg gap to ≥3 across all 5 cases?
3. Did Phase 7 theme signal density expansion close Atraxa + Ur-Dragon intent_drift outliers?
4. Did Phase 9 curve smoother and Phase 10 interaction designer produce noticeably better structural quality on the 5-case sweep? Sample 3 cases pre/post for comparison.
5. Did Phase 12 graduated playtest reports look sensible? Sample 5 cases' Tier 0/1/2 predictions and assess plausibility.
6. What's the most plausible iter 7 priority? Options:
   - Pillar E v0.5 win-condition coherence checker + v0.6 anti-meta hate optimizer (~2-3 weeks)
   - Pillar F v0.2 game engine substrate (multi-month — replaces Stage 1 statistical with actual game simulation)
   - At-scale Voyage rules + Scryfall rulings embedding (Phase 4 of v4 deferred at-scale)
   - Multi-deck cross-pollination + reverse-engineering target decks (iter 5 prep deferred items)
   - Bracket-partitioned corpus

---

## You are go for launch

Run from Phase 0 to Phase 14 autonomously. Halt only on the narrow hard-halt conditions. Self-correct. Atomic commits. Log progress.

When you hit Phase 14's final report, paste the executive summary inline.

Expected total wall-clock: 24-72 hours. Expected total API spend: $30-70.

Begin with Phase 0 pre-flight.
