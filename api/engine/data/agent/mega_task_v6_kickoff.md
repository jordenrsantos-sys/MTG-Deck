# Mega-task v6: Iter 7 — SSE UI fix + 3 architectural fixes + Coherence Sweep punch-list + Pillar E v0.5/v0.6

Single self-contained kickoff. You are authorized to run autonomously from start to finish without further user interaction unless a hard halt condition triggers. Self-correct using the tiered escalation. Atomic commit per phase. Maintain a running progress log throughout.

---

## What this mega-task delivers

**CRITICAL NEW PHASE 1 (added 2026-05-22 after live UI evaluation):** the v5-shipped SSE streaming endpoint at `POST /agent/build_deck_v1/stream` has an end-to-end delivery bug. Server-side build completes cleanly (POST returns 200 OK in uvicorn access log) but the browser UI receives ZERO events. Multiple live test attempts confirmed the issue: the UI's `useBuildStreaming` hook's state stays at `INITIAL_STATE` (currentPhase=null, elapsed_s=0.0, cost_usd=0.0) even after the server has streamed phase events and closed the connection. Result: builds complete server-side, deck data is produced, but the UI never renders the deck and eventually fires its 480s client-side timeout. The agent is fully functional via Python tool calls but unusable via the production UI. Until this is fixed, all of v5's UX bundle work is wasted. Phase 1 is BLOCKING — no architectural phases run until the SSE path delivers events to the browser and produces a rendered deck end-to-end, verified via chrome-devtools-mcp.

After mega-task v5 shipped the UX bundle + Pillar E v0.3/v0.4 + Graduated Playtest Stage 1, three iter-6 sweep criteria remained failing — each tagged as a real architectural issue rather than tuning. Coherence Sweep #3 then surfaced 4 substantive findings about wiring, dep scale-up, doc drift, and test-failure triage. This mega-task closes ALL of those, plus ships Pillar E v0.5 (win-con coherence) and v0.6 (anti-meta hate) — the final two structural-fundamentals optimizers in the 5-pillar plan.

After v6 ships, the agent will have: a working semantic-neighbor selection mechanism (replacing the failed score-boost + explicit-prompt approaches), real counter/proliferate primitives in the ontology (replacing the anthem-effect proxy that caused tribal dilution), an accurate iter-validation eval pipeline (no more first-match undercount artifacts), six complete Pillar E structural-fundamentals optimizers running on every build, and a verified-clean substrate after the sweep follow-ups land.

Read these files at Phase 0:

1. `spaces/.../memory/project_iter_7_prep_notes_2026-05-22.md` (locked iter 7 priorities — source of truth)
2. `spaces/.../memory/project_coherence_sweep_3_shipped_2026-05-22.md` (sweep ship state + queued items)
3. `spaces/.../memory/project_mega_task_v5_shipped_2026-05-22.md` (v5 ship state)
4. `repo/api/engine/data/agent/coherence_sweep_3_health_report.md` (sweep health report — read all 10 audit sections)
5. `repo/api/engine/data/agent/mega_task_v5_final_report.md` (v5 final report + iter 6 → iter 7 hand-off)
6. `repo/api/engine/data/agent/pillar_d_iteration_6_validation_report.md` (iter 6 metrics — what's failing and why)
7. `spaces/.../memory/feedback_pool_score_does_not_drive_llm_picking.md` (foundational architectural learning for Phase 1)
8. `spaces/.../memory/project_5_pillar_forward_plan.md` (overall roadmap; update at Phase 12)

---

## Substrate state

Coherence Sweep #3 shipped 2026-05-22 (11 atomic commits on top of mega-task v5's `4cee4a287`). Substrate verdict: "in good shape." Test baselines: pytest 1489 / vitest 758. Total CC API spend across mega-tasks v1-v5 + Sweep #3: ~$18-20.

**Iter 6 baseline you must not regress (9/12 criteria pass):**

- iter1 structural pass: 5/5
- mean creativity_delta: 37.2
- mean novel_combo: 5.6
- mean cost: $0.31
- mean wallclock: 113.8s
- pillar_c_coverage_v1: 93%+
- pillar_f_ordering: sane
- theme_profile_structured: 5/5
- ui_equivalent_build_path: 5/5
- Hellkite Charger absent on Ur-Dragon
- Atraxa C2.1 latency > 0 (BLOCKING bug from iter 5 stays fixed)
- **3 known failures iter 7 must close:**
  - voyage_semantic_avg = 2.0 (need ≥3) — Phase 1 fixes
  - intent_drift_mean = 0.614 (per-case 2/5 below threshold; need ≥4/5) — Phase 2 fixes
  - pillar_e_v0_4_interaction_within = 0/5 (need ≥4/5) — Phase 3 fixes

**Architectural rules locked in feedback memories (must be honored):**

- Corpus is descriptive not prescriptive
- User intent locks deck shape — corpus optimum is not the target
- Mana base serves spells, computed last not locked first
- Pool ranking score does not drive LLM picking — Phase 1's design REPLACES the failed score-boost mechanism

**File layout iter 7 will work in:**

- `repo/api/engine/layers/agent_build_deck_v1.py` — main agent endpoint (Phase 1 semantic-injection insertion, Phase 8 win-con coherence integration, Phase 9 anti-meta integration)
- `repo/api/engine/layers/agent_wide_candidate_pool_v1.py` — Phase 1 semantic-injection mechanism
- `repo/api/engine/layers/agent_semantic_retrieval_v1.py` — referenced by Phase 1
- `repo/api/engine/extractors/primitive_extractor_v2.py` — Phase 2 ontology v2 extraction
- `repo/api/engine/data/primitives/ontology_v1.md` → `ontology_v2.md` — Phase 2 expansion
- `repo/api/engine/layers/agent_c22_prompt_templates_v1.py` — Phase 2 archetype mapping
- `repo/api/engine/layers/agent_voyage_downgrade_pass_v1.py` — Phase 4 wiring decision
- `repo/api/engine/layers/voyage_rules_embedding_v1.py` — Phase 5 at-scale activation
- `repo/tools/test_pillar_d_iteration_6.py` → `test_pillar_d_iteration_7.py` — Phase 3 eval rewrite
- `repo/api/engine/layers/win_con_coherence_v1.py` (new) — Phase 8
- `repo/api/engine/layers/anti_meta_hate_v1.py` (new) — Phase 9
- `Mtg deck building brain/13_AI_AGENT_SURFACE/ENGINE_API_GUIDE.md` — Phase 7 overhaul

---

## Authority and scope

You are AUTHORIZED to:

- Run all 13 phases (0-12) autonomously without halting except on hard halt conditions.
- Self-correct using tiered escalation.
- Make atomic commits per phase: `git commit -m "Phase X (mega-task v6): <description>"`.
- Modify any file in `repo/api/`, `repo/ui_harness/`, `repo/tests/`, `repo/tools/`, `repo/launch_dev.cmd`, `repo/requirements.txt`, and the Obsidian vault.
- Add new dependencies via pip if needed (nothing exotic expected).
- Read and write Cowork memory at `spaces/.../memory/` for material findings.
- Use mtg-engine MCP + obsidian MCP for verifications.
- Use chrome-devtools-mcp for UI validation in Phase 10.

You are NOT authorized to:

- Upgrade the Anthropic model from Sonnet 4.6.
- Modify Pillar A endpoints (changes in agent layer + new Pillar E modules + ontology + extractor).
- Modify iter 1-6 baseline test cases in ways that change their behavior.
- Roll back any commit. Forward fixes only.
- Touch the Phase 5b MPA substrate.
- Modify `combo_brackets_v1.json` directly (use additive `combo_brackets_v1_external_sources.json` pattern).
- Touch the per-set automation pipeline scheduled task.
- Add web-fetching beyond Anthropic SDK + Voyage embeddings API + Scryfall API.
- Re-extract primitives across all 110k cards more than once (Phase 2 backfill — do it once, verify, don't churn).

---

## Hard halt conditions (NARROW — halt only on these)

1. **Validation gate fails 3 times in a row** in any single phase after exhausting tiered self-correction.
2. **Critical regression**: any iter 1-6 success criterion breaks at any phase boundary. Halt with diff.
3. **Resource exhaustion**: API spend reaches $100 (v6 ceiling). Hard halt.
4. **Architectural contradiction**: a phase spec turns out to be impossible without contradicting prior phase output.
5. **Phase 11 final validation fails on >= 3 of 14 success criteria**.
6. **Cumulative test suite regression**: pytest drops below 1489 OR vitest drops below 758 + new tests. Halt.
7. **Phase 2 primitive backfill catastrophically wrong**: if the new v2 ontology produces <80% coverage OR breaks more cards than it fixes, halt before proceeding to dependent phases.

---

## Self-correction protocol (tiered escalation)

**Tier 1** — Re-read phase spec + relevant memory + iter 6 final report + sweep health report. Try alternate implementation path. Up to 3 attempts.

**Tier 2** — Search codebase for similar patterns. Up to 2 attempts.

**Tier 3** — Add known-gap note to progress log; skip remaining work for phase and continue. Only allowed for non-blocking phases.

**Tier 4** — Halt for user direction.

**Blocking phases that cannot Tier-3-skip:**

- Phase 1 (SSE UI fix + browser verification) — UI is unusable until this works; no architectural work matters if no one can use the agent
- Phase 2 (semantic-injection guarantee) — closes iter 6 criterion 6, the most-reported architectural gap
- Phase 3 (ontology v2 + extraction backfill) — closes iter 6 criterion 7 + foundational for Pillar F
- Phase 4 (eval-script multi-primitive counting fix) — must work for Phase 11 sweep validation
- Phase 11 (iter 7 final validation) — must pass before Phase 12
- Phase 12 (final regression + memory) — must pass to declare done

Phases 5, 6, 7, 8, 9, 10 are non-blocking and can Tier-3-skip if they fail unrecoverably.

---

## Progress log

Write to `repo/api/engine/data/agent/mega_task_v6_progress_log.md` from Phase 0. Append-only, timestamped sections per phase.

---

## Resource budget

- **Total API spend ceiling: $100.** Alarm at $80; hard halt at $100. Expected actual: $20-50.
- **Per-phase rough budget**: Phase 2 is the most expensive (~$15-20 for ontology v2 LLM-assisted extraction backfill on ~110k cards using batch processing). Phase 10 sweep ~$2-3. Other phases mostly code work with minimal LLM spend.
- **Wall-clock budget**: aim 36-72 hours. Phase 2 (re-extraction + backfill) is the longest single phase.

---

## Test discipline

Run after EVERY commit:

```bash
cd "E:\MTG Root\mtg-engine\repo"
pytest -q
cd "E:\MTG Root\mtg-engine\repo\ui_harness"
npm test -- --run
```

Both must pass. Baselines: pytest 1489 + v6's new tests; vitest 758 + v6's new tests.

---

## Phases

### Phase 0 — Pre-flight + memory sync

Read the 8 files in "What this mega-task delivers." Confirm env (Python 3.10, ANTHROPIC_API_KEY + VOYAGE_API_KEY set, MCPs connected, pytest 1489 + vitest 758 baselines, git clean, disk >10GB).

Create `repo/api/engine/data/agent/mega_task_v6_progress_log.md`. Commit: "Phase 0 (mega-task v6): pre-flight + progress log scaffold".

---

### Phase 1 — SSE UI end-to-end fix + browser verification (BLOCKING — highest priority)

**Symptom:** server-side build at `POST /agent/build_deck_v1/stream` completes cleanly (uvicorn logs 200 OK after stream closes). UI's `useBuildStreaming` hook in `repo/ui_harness/src/hooks/useBuildStreaming.ts` connects, but receives ZERO events. The UI display stays at `INITIAL_STATE` (currentPhase=null, elapsed_s=0.0, cost_usd=0.0) the entire time. Eventually the 480s client-side timer fires and the user sees the "Build exceeded expected duration" error. This is reproducible across multiple commanders, brackets, and fresh-process restarts. Verified 2026-05-22 in live evaluation.

**A partial fix was attempted live 2026-05-22:** the `_run_build` function in `repo/api/main.py` was modified to emit an explicit `{"phase": "complete", "status": "completed", "response": result}` event with the deck payload before SENTINEL. This DID NOT resolve the issue — the UI still receives no events. So the problem is not just the missing complete event; it's that NO events reach the browser at all.

**Investigation approach:**

1. *Curl the streaming endpoint directly* (from a shell, bypassing the browser) to see the raw SSE wire format the server emits. If curl receives events properly, the server is fine and the issue is browser-side fetch reader / parsing. If curl ALSO receives nothing, the issue is server-side event delivery.

2. *Inspect the SSE response format.* Compare what `sse_starlette.EventSourceResponse` is emitting against what `useBuildStreaming.ts::_parseSseBuffer` expects. Look for mismatched event types (`event: progress` vs `event: message`), data format (`data: <json>` per line vs multiple `data:` lines), terminator (`\n\n` vs `\r\n\r\n`), or content-type headers.

3. *Check for browser response buffering.* If the browser is buffering the entire SSE response and only delivering after the connection closes, the UI parser should still process all events at once when it does eventually run. The fact that the UI shows `INITIAL_STATE` even AFTER the engine has logged POST 200 OK (meaning the connection has closed) rules out simple buffering — events should be delivered at close. So something else is wrong.

4. *Look at the integration test for SSE.* `tests/test_agent_build_deck_v1_stream.py` reportedly passes 10/10 per CC's v5 Phase 3 commit messages. Check what that test actually verifies — likely it tests the server-side endpoint in isolation, but does NOT test the UI hook's parsing of the server's output. If so, the test is unit-level + miss-integration. Add a real integration test that simulates a fetch + ReadableStream + the UI's `_parseSseBuffer` against actual server output.

**Fix delivery requirement:**

- Use chrome-devtools-mcp to LIVE-VERIFY the fix. Open `localhost:5173/#ai-build`, fill in Edgar Markov + B3, click Build deck, watch the network tab + the UI progress display. Pass condition: phase events visible in the network tab's EventStream view + UI progress display updates with each phase (B2 → C2.1 → C2.2 → D2 → Pillar E reconciliation → graduated playtest) + final deck renders in the Summary panel.
- If chrome-devtools-mcp is unavailable, fall back to the Python tool that mega-task v5 Phase 5 used as a substitute (`tools/mega_task_v5_phase5_live_smoke.py` or similar — find and reuse).

**Tests:**

- Add `tests/test_agent_build_deck_v1_stream_e2e.py` that runs the server, sends a real POST to the streaming endpoint, parses the SSE wire format with a real parser equivalent to the UI's `_parseSseBuffer`, and asserts ≥6 progress events received + final `{"phase": "complete"}` event with a non-empty response payload. This is the regression test that prevents future SSE breakage.

- If the v5 unit tests (`tests/test_agent_build_deck_v1_stream.py`) are still passing despite the production bug, fix them to actually catch this class of issue.

**Estimated effort:** 4-8 hours CC time. Diagnosis is most of it; the fix once identified is likely small.

**Commit:** "Phase 1 (mega-task v6): SSE UI end-to-end fix + browser verification + e2e regression test".

---

### Phase 2 — Semantic-injection guarantee (BLOCKING)

Closes iter 6 criterion 6 (voyage_semantic_avg = 2.0 vs target ≥3). Replaces the failed score-boost + explicit-prompt approaches.

**Architectural insight (from `feedback_pool_score_does_not_drive_llm_picking` memory):** The LLM picks from prompt content based on its reasoning, not pool ranking. Score boosts reorder the prompt but don't shift LLM behavior. Explicit "MUST SELECT 3" instructions also failed when the upstream pool sometimes had 0 semantic neighbors. The only mechanism that GUARANTEES outcomes is a deterministic post-hoc layer that operates AFTER the LLM has picked.

**Implementation:**

1. New module `repo/api/engine/layers/agent_semantic_injection_v1.py`:
   - Function `inject_semantic_picks(deck, anchor_cards, color_identity, n_target=3) -> (modified_deck, swap_log)`
   - Queries Voyage for top-30 semantic neighbors of each anchor card (commander + must-includes + creative outliers from C2.1).
   - Filters by color-identity-subset, removes anchors themselves + cards already in deck + cards in the forbidden_set.
   - Selects up to `n_target` neighbors that aren't yet in deck.
   - For each neighbor to add: identifies the lowest-priority C2.2 wild-discovery pick currently in deck (a card NOT in must_includes and NOT in C2.1's primary selection) and swaps it out.
   - Returns modified deck + log of injections.
2. Integration: runs at the end of `_run_wild_combo_discovery` in `agent_build_deck_v1.py`, BEFORE D2 rationale rewrite. So D2 rewrites rationales for the post-injection deck composition.
3. Configuration: `n_target` is bracket-aware. B5 cEDH: n_target=4 (more semantic-novelty for tempo/combo). B2 casual: n_target=2 (less aggressive injection to honor user intent). B3/B4: n_target=3.
4. Source tagging: injected cards get `source: semantic_injection` so they're countable in the iter 7 sweep metric.

**Tests:**

- Unit tests for `inject_semantic_picks` with mocked Voyage backend (5+ scenarios: full pool, partial pool, all-anchors-overlap, color-identity edge cases, no-neighbors case).
- Integration test: agent build on Edgar with mocked Voyage returns deck containing ≥3 cards with `source: semantic_injection`.
- Regression: deck still has exactly 100 cards (99 + commander) post-injection.
- Backwards compatibility: when Voyage is unavailable, injection layer no-ops gracefully (returns unmodified deck).

**Smoke test:** 5-case Edgar/Krenko/Atraxa/Yuriko/Ur-Dragon. Each produces ≥3 semantic-injection cards (mean ≥3.0 across cases).

**Commit**: "Phase 1 (mega-task v6): semantic-injection guarantee — post-hoc N-card injection".

---

### Phase 3 — Ontology v2 with real counter/proliferate primitives (BLOCKING)

Closes iter 6 criterion 7 (intent_drift mean 0.614, per-case 2/5 vs target ≥4/5). Eliminates the anthem-effect proxy tribal-dilution that was added in mega-task v5 Phase 7.

**Implementation:**

1. Expand `repo/api/engine/data/primitives/ontology_v1.md` → `ontology_v2.md`:
   - Keep all 80+ v1 tags (64 v0 + 17 rules_modifiers).
   - Add new `counters_and_proliferate` dimension with 12-15 tags:
     - `proliferate-trigger` (cards that proliferate or trigger proliferation)
     - `proliferate-cost-reducer`
     - `plus1plus1-counter-distributor` (Hardened Scales pattern)
     - `plus1plus1-counter-doubler` (Doubling Season, Branching Evolution)
     - `plus1plus1-counter-payoff` (cards that benefit when +1/+1 counters are present)
     - `charge-counter-payoff`
     - `loyalty-counter-payoff`
     - `energy-counter-producer`
     - `energy-counter-payoff`
     - `keyword-counter-producer` (flying, haste, etc. via Heroic Reinforcements / pact patterns)
     - `counter-removal-or-relocation` (Vorel pattern)
     - `counter-trigger-scaling` (Inexorable Tide pattern)
   - Each tag has id, definition, extraction_rule (regex), 3+ examples, combos_with cross-references.
2. Update `primitive_extractor_v2.py` to load v2 ontology (95+ total tags).
3. **Revert the anthem-effect signal expansion from mega-task v5 Phase 7** in `agent_c22_prompt_templates_v1.py`. With real counter primitives now available, anthem-effect proxy is no longer needed.
4. Update theme classifier: `counters_matter` archetype now signals on the 12-15 new primitives, NOT anthem-effect.
5. **Full backfill of all 110k cards with v2 ontology**. Use batch processing for efficiency. Persist intermediate state every 10k cards so a crash mid-backfill doesn't lose progress. Estimated time: ~5-15 min for regex extraction, plus LLM-supplement pass on ambiguous cards (~$10-15 spend).
6. Golden tests: extend `test_primitive_extractor_golden.py` with 30+ new counter/proliferate cards.

**Tests:**

- 50/50 v1 golden tests still pass.
- New 30+ counter/proliferate golden tests.
- Coverage check: ≥90% of cards-with-abilities have ≥1 primitive (no regression from v1's 93%).
- Atraxa-specific test: classifying an Atraxa deck composition should detect `counters_matter` as primary archetype with stronger signal than v1 (specifically the 12-15 new tags should fire on Atraxa staples).

**Smoke test:** Re-run iter 6 sweep on Atraxa case specifically. intent_drift should drop below 0.7 (archetype-aware threshold for counters_matter). Other 4 cases should not regress.

**Commit**: "Phase 2 (mega-task v6): Pillar C ontology v2 + counter/proliferate primitives + 110k-card re-backfill".

---

### Phase 4 — Iter-validation eval-script multi-primitive counting fix (BLOCKING)

Closes iter 6 criterion 10 (pillar_e_v0_4_interaction_within target = 0/5). Fixes the first-match primitive classification undercount in the iter 6 eval script.

**Implementation:**

1. Audit `tools/test_pillar_d_iteration_6.py::_count_actual_interaction` (or equivalent function).
2. Identify the first-match classification bug: cards with multiple primitive tags currently classified by first-match-in-some-list, which undercounts when interaction-relevant tags appear later in the list.
3. Rewrite to consider ALL primitive tags per card, weighted by category. A card with tags `["sac-outlet", "removal-targeted-creature"]` counts toward BOTH interaction (via removal) and combo-engine (via sac-outlet) — not just whichever matches first.
4. Rename `test_pillar_d_iteration_6.py` → `test_pillar_d_iteration_7.py` and update its criteria definitions to match Phase 10 below.

**Tests:**

- Unit tests for `_count_actual_interaction` with 10+ deck-shape inputs (verify a deck with 8 removal pieces produces actual_interaction_count=8, not <8).
- Regression: existing v6 sweep on the 5 test cases produces interaction counts that fall within ±50% of Pillar E v0.4's target (currently 0/5; should be ≥4/5 after fix).

**Smoke test:** Single Edgar B3 case run produces interaction_count within ±50% of target (Phase 10 module's recommendation).

**Commit**: "Phase 3 (mega-task v6): iter-validation eval multi-primitive counting fix".

---

### Phase 5 — voyage_downgrade_pass wiring decision (from Coherence Sweep #3)

Module `agent_voyage_downgrade_pass_v1.py` thinks it shipped in mega-task v4 Phase 10, has a working test suite, but no production code imports it.

**Implementation:**

1. Read the module + its tests. Understand what it's supposed to do (cheaper-to-cast alternatives via Voyage neighbors).
2. Check the iter 5 prep notes Insight 2 about mana-cost-aware Voyage downgrade pass — verify whether the spec matches the implementation.
3. Make the wire-or-abandon decision:
   - **Wire option**: integrate into `agent_build_deck_v1.py` after Pillar E v0.3 curve smoother (right after curve analysis identifies bricks). When bracket is B4/B5 OR theme_profile includes storm/combo/tempo, run downgrade pass. Surface as `cheaper_alternatives_suggested` in build response.
   - **Abandon option**: mark module as deprecated with header comment + retire its tests with a comment explaining why. Move to a `repo/deprecated/` directory if one exists.
4. Document the decision in the progress log with rationale.

**Tests:**

- If wired: new integration test verifying the downgrade pass fires on a B5 case and surfaces suggestions.
- If abandoned: no test changes needed; existing tests remain green (since module still works in isolation).

**Recommendation:** wire it. The iter 5 prep memory explicitly identified this as a value-add for cEDH/tempo builds.

**Commit**: "Phase 4 (mega-task v6): voyage_downgrade_pass wiring [decision: wire|abandon]".

---

### Phase 6 — voyage_rules_embedding at-scale activation (from Coherence Sweep #3)

The rules-embedding pipeline shipped in mega-task v5 Phase 4 but at-scale was deferred. Activate it now.

**Implementation:**

1. Run the at-scale embedding pass on full MTG Comprehensive Rules + Scryfall card rulings.
2. Estimated cost: ~$1-2 at voyage-3 rates per the iter 5 prep memory.
3. Verify the index loads correctly + queries return relevant rules sections (e.g., "may put a token" returns rules about optional triggers).
4. Wire into C2.2 combo validation: when LLM proposes a combo, agent queries embedded rules for trigger-condition compatibility (limit 1-2 queries per build).

**Tests:**

- Index integrity tests (row count matches expected; sample queries work).
- Integration smoke: a build that proposes a combo should fire at least 1 rules query during validation.

**Smoke test:** Edgar build's progress log shows ≥1 rules query during C2.2.

**Commit**: "Phase 5 (mega-task v6): voyage_rules_embedding at-scale + C2.2 combo validation integration".

---

### Phase 7 — 8 pre-existing test-failure triage (from Coherence Sweep #3)

The sweep flagged 8 pre-existing failing tests (5 share `TestHttpEndpointWiring`).

**Implementation:**

1. List the 8 failing tests + their failure modes.
2. For each:
   - If the test references removed/superseded functionality, retire the test (delete or mark `@pytest.skip(reason=...)`).
   - If the test reveals a real bug, fix the bug.
   - If the test is flaky, characterize the flake source and either fix or mark with `@pytest.flaky` annotation.
3. Document each decision in the progress log.

**Tests:**

- After triage, pytest baseline should be 1489 + new tests - 8 retired (if all 8 were retired) OR 1489 + new + (fewer than 8 still failing if some had real bugs to fix).

**Smoke test:** `pytest -q` runs to completion with the expected count.

**Commit**: "Phase 6 (mega-task v6): pre-existing test-failure triage (8 tests)".

---

### Phase 8 — ENGINE_API_GUIDE.md overhaul (from Coherence Sweep #3)

The vault doc at `Mtg deck building brain/13_AI_AGENT_SURFACE/ENGINE_API_GUIDE.md` covers 9 endpoints but the actual surface is now ~18 v1-tier across mega-tasks v3-v5 (42 total routes).

**Implementation:**

1. Audit `repo/api/main.py` for all registered endpoints.
2. Categorize: Pillar A core (9), new agent endpoints from v1-v5, new SSE endpoint from v5, graduated_playtest_report endpoint from v5, snapshot endpoint from v5, etc.
3. Rewrite ENGINE_API_GUIDE.md with up-to-date coverage:
   - Each endpoint: path, method, request schema, response schema, error codes, sample call.
   - Group by Pillar (A / Agent / Diagnostics / etc.) for navigation.
   - Cross-link from `DESIGN_DECISIONS.md` and `Home.md` per Obsidian conventions.

**Tests:**

- Sanity check: every endpoint in `main.py` appears in the guide; every endpoint in the guide exists in `main.py`.

**Commit**: "Phase 7 (mega-task v6): ENGINE_API_GUIDE.md overhaul for current endpoint surface".

---

### Phase 9 — Pillar E v0.5 win-condition coherence checker

The penultimate Pillar E optimizer.

**Implementation:**

1. New module `repo/api/engine/layers/win_con_coherence_v1.py`:
   - Function `check_win_con_coherence(deck, theme_profile, bracket) -> WinConCoherenceReport`
   - Identifies the deck's primary win condition by primitive-pattern matching (combo win-paths from Pillar F v0.1 catalog + combat-based / mill / alt-win heuristics).
   - Counts enabling cards for the primary plan (e.g., for Thoracle plan: counts Thoracle + DC + Tainted Pact + tutors).
   - Validates a backup plan exists (≥4 enabling cards for a secondary path).
   - Flags decks where neither primary nor backup is clear ("75% pile of good cards").
2. Integration: runs after Pillar E v0.4 interaction designer's reconciliation; LLM critique pass on flagged decks (suggests cards to shore up the primary or add a backup).
3. Surface in build response as `win_con_coherence_report`.

**Tests:**

- Unit tests for coherence checker with 5+ deck shapes (clear primary / clear primary + backup / unclear pile / dual-archetype).
- Integration: 5-case sweep produces coherence reports for each; all should have ≥1 identified primary plan.

**Smoke test:** Edgar build returns `win_con_coherence_report` with primary plan identified + ≥6 enabling cards.

**Commit**: "Phase 8 (mega-task v6): Pillar E v0.5 win-condition coherence checker".

---

### Phase 10 — Pillar E v0.6 anti-meta hate optimizer

The final Pillar E optimizer in the 5-pillar plan.

**Implementation:**

1. New module `repo/api/engine/layers/anti_meta_hate_v1.py`:
   - Function `recommend_anti_meta_hate(deck, bracket, expected_meta) -> AntiMetaRecommendations`
   - Reads opposition_decks_v1.json (tiered registry from mega-task v5) to characterize "expected meta" for the deck's bracket.
   - Recommends hate piece counts and specific candidates:
     - Graveyard hate (if reanimator/dredge present in expected meta)
     - Artifact hate (if combo-rocks meta)
     - Stax / tax pieces
     - Format-specific tech (cEDH counters, B5-specific)
2. Integration: runs after Pillar E v0.5 coherence checker; LLM critique on whether the recommended hate fits the deck's theme.
3. Surface as `anti_meta_recommendations` in build response.

**Tests:**

- Unit tests with 5+ bracket+meta combinations.
- Integration: 5-case sweep produces anti-meta recommendations; B5 cEDH deck recommends ~2 counterspells + 1 grave hate; B2 casual recommends ~1 generic hate.

**Smoke test:** Yuriko B5 build recommends graveyard hate (because cEDH meta includes reanimator) + counterspell density.

**Commit**: "Phase 9 (mega-task v6): Pillar E v0.6 anti-meta hate optimizer".

---

### Phase 11 — Iter 7 final validation sweep + report (BLOCKING)

**Capture per case (5 iter-2 sweep cases + 5 UI cases via chrome-devtools-mcp):**

- All iter 6 metrics
- Phase 1 metric: semantic_injection_count per build (target ≥3 mean)
- Phase 2 metric: intent_drift per-case (target ≥4/5 below archetype-aware threshold)
- Phase 3 metric: interaction_within_target (target ≥4/5 within ±50%)
- Phase 4 metric: voyage_downgrade_pass present in response (Tier-3-skippable if abandoned in Phase 4)
- Phase 5 metric: voyage_rules_embedding query count (target ≥1 per build)
- Phase 8 metric: win_con_coherence_report present + primary plan identified
- Phase 9 metric: anti_meta_recommendations present

**Iter 7 success criteria (14 total, must hit at least 12):**

1. `iter1_structural_pass_5_of_5`
2. `mean_creativity_delta >= 35`
3. `mean_novel_combo >= 5`
4. `mean_cost <= $0.50`
5. `mean_wallclock <= 130s` (slight bump from iter 6 to accommodate new Pillar E v0.5/v0.6 critique passes + semantic injection)
6. `voyage_semantic_avg >= 3` (Phase 2 fix should close)
7. `intent_drift_archetype_aware_pass >= 4/5` (Phase 3 fix should close)
8. `pillar_e_v0_4_interaction_within target >= 4/5` (Phase 4 fix should close)
9. `pillar_c_coverage_v2 >= 90%` (Phase 3 backfill should hold)
10. `pillar_f_ordering_sane`
11. `theme_profile_structured 5/5`
12. `win_con_coherence_report 5/5` (Phase 9)
13. `anti_meta_recommendations 5/5` (Phase 10)
14. `ui_e2e_build_renders_5_of_5` (Phase 1 fix must hold — verified via chrome-devtools-mcp or equivalent that each of 5 sweep cases produces a rendered deck in the UI, not just a server-side completion)

Write report to `repo/api/engine/data/agent/pillar_d_iteration_7_validation_report.md`. Include iter 7 → iter 8 hand-off section.

**Halt condition:** if >= 3 of 13 success criteria fail, halt for user direction.

**Commit**: "Phase 10 (mega-task v6): iter 7 final validation sweep + report".

---

### Phase 12 — Final regression + report + memory update (BLOCKING)

**Run:**

1. Full pytest + vitest (must pass).
2. 5-case Python sweep (re-validate iter 7 metrics).
3. Live UI sanity via chrome-devtools-mcp (one full build start-to-deck-render with all new Pillar E surfaces visible).
4. Per-pillar standalone smokes.
5. Graduated playtest report card sanity on each case.

**Write final report** to `repo/api/engine/data/agent/mega_task_v6_final_report.md`.

**Update memory:**

- New memory file `spaces/.../memory/project_mega_task_v6_shipped_<date>.md`
- Update MEMORY.md to add index entry
- Update `project_5_pillar_forward_plan.md` — Pillar E now COMPLETE (v0.1-v0.6 all shipped)
- Update `project_iter_7_prep_notes_2026-05-22.md` to mark items shipped + queue iter 8 work

**Commit**: "Phase 11 (mega-task v6): final regression + report + memory update".

---

## Mega-task v6 success criteria

Mega-task is "done" when ALL hold:

1. All 13 phases (0-12) committed and Phase 12 final regression passes.
2. Phase 11 sweep meets ≥12 of 14 success criteria.
3. pytest + vitest baselines preserved + new tests pass.
4. SSE UI end-to-end fix shipped + verified via chrome-devtools-mcp — build renders deck in browser (Phase 1).
5. Semantic-injection guarantee landing ≥3 semantic neighbors per build (Phase 2).
6. Ontology v2 with real counter primitives + 110k backfill (Phase 3).
7. Eval-script multi-primitive counting fixed — interaction within target (Phase 4).
8. voyage_downgrade_pass wiring decision made + documented (Phase 5).
9. voyage_rules_embedding at-scale active + C2.2 querying it (Phase 6).
10. 8 pre-existing test failures triaged (Phase 7).
11. ENGINE_API_GUIDE.md current (Phase 8).
12. Pillar E v0.5 win-con coherence checker shipped (Phase 9).
13. Pillar E v0.6 anti-meta hate optimizer shipped (Phase 10).
14. Total API spend under $100.

---

## What NOT to do

- Don't upgrade to Opus.
- Don't break iter 1-6 baseline test cases.
- Don't modify `combo_brackets_v1.json` directly.
- Don't touch Phase 5b MPA substrate.
- Don't modify Pillar A endpoints.
- Don't churn memory.
- Don't pad.
- Don't disable the v3 per-set automation scheduled task.
- Don't try to ship Pillar F v0.2 game engine substrate (multi-month — iter 8+ work).
- Don't try Stage 2 graduated playtest (requires Pillar F v0.2).
- Don't re-extract primitives more than once (Phase 2 backfill costs $10-15; if it goes wrong, halt and diagnose rather than retry).

---

## Iter 7 → iter 8 hand-off questions (your Phase 11 final report must answer)

1. Did Phase 1 (semantic-injection guarantee) close the voyage_semantic gap to ≥3 reliably across all 5 cases? Sample 3 cases' injected cards and assess fit quality.
2. Did Phase 2 (ontology v2 + real counter primitives) close the intent_drift gap on Atraxa specifically? Compare per-case drift pre/post.
3. Did Phase 3 (eval-script fix) produce sensible interaction counts? Sample 3 cases' interaction-category breakdowns.
4. Phase 4 voyage_downgrade_pass: wired or abandoned? Rationale.
5. Phase 8 win-con coherence: did the LLM critique pass produce useful suggestions on flagged decks? Sample 3.
6. Phase 9 anti-meta hate: are the recommendations bracket-appropriate? Sample B2 vs B5 to verify scaling.
7. What's the most plausible iter 8 priority? Options:
   - Pillar F v0.2 game engine substrate (the multi-month rules-correct multiplayer engine — replaces Stage 1 statistical with actual game simulation)
   - Multi-deck cross-pollination + reverse-engineering target decks (iter 5 prep deferred candidates)
   - Bracket-partitioned corpus
   - Tournament/meta data tracking
   - Stage 2 graduated playtest (depends on Pillar F v0.2)

---

## You are go for launch

Run from Phase 0 to Phase 12 autonomously. Halt only on the narrow hard-halt conditions. Self-correct. Atomic commits. Log progress throughout.

When you hit Phase 12's final report, paste the executive summary inline.

Expected total wall-clock: 48-80 hours (Phase 1 SSE investigation adds 4-8 hours on top of the v5-scoped 36-72). Expected total API spend: $25-60.

Begin with Phase 0 pre-flight. Phase 1 (SSE UI fix) is the first blocking phase — no architectural work runs until the UI delivers events to the browser end-to-end.
