# Mega-task v2: Pillar D iter 4 + Pillar C primitive extractor + Pillar E v0.2 + Pillar F upgrade

Single self-contained kickoff. You are authorized to run autonomously from start to finish without further user interaction unless a hard halt condition (listed below) triggers. Self-correct using the tiered escalation. Atomic commit per phase. Maintain a running progress log throughout. Cross-phase regression check at every phase boundary.

---

## Substrate state

Mega-task v1 shipped 2026-05-21 with 17 atomic commits on top of iter 2 (`2ee152c9f`). Final commit `2f177ee7a` (Phase 14). Iter 3 of Pillar D + Pillar E v0.1 mana base + Pillar C ontology v0 + Pillar F v0.1 statistical approximator + Track 5 new-set pipeline scaffolding all shipped + tested + integrated. 6/6 iter 3 success criteria pass under user-revised targets.

Read these files at Phase 0:

1. `repo/api/engine/data/agent/mega_task_v1_final_report.md` (final report)
2. `repo/api/engine/data/agent/pillar_d_iteration_3_validation_report.md` (iter 3 metrics)
3. `repo/api/engine/data/agent/mega_task_v1_progress_log.md` (per-phase findings)
4. `repo/api/engine/data/primitives/ontology_v0.md` (Pillar C ontology spec from Phase 11 — the source of truth for the primitive extractor you'll build in Phase 5)
5. `spaces/.../memory/project_mega_task_v1_shipped_2026-05-21.md` (ship-state memory)
6. `spaces/.../memory/project_5_pillar_forward_plan.md` (the forward plan)

**Iter 3 baseline you must not regress:**

- iter1 structural pass: 5/5
- mean creativity_delta: 37.6
- mean novel_combo: 5.8
- mean cost: $0.295
- mean wallclock: 139.8s (the iter 4 target is to close this further to ~80-90s via outer-chain parallelization)
- Hellkite Charger absent on Ur-Dragon (Phase 2 combo-anchor guard)

**Per-LLM-call decomposition (iter 3 final sweep):**

- B2 intent interpreter: ~25s / ~$0.020
- C2.1 candidate critic: ~50s / ~$0.060
- C2.2 wild combo discovery: ~22s / ~$0.115
- D2 final critic + rewrite (3 parallel batches): max-batch ~40s / ~$0.080
- Pillar E mana_base_critique (when fires): ~15s / ~$0.020
- Serial sum: ~150s (139.8 measured average — close to the floor)

**Iter 4 architectural targets:**

- Outer-chain parallelization (Phase 3 below) drops serial sum from ~150s to ~max(B2, C2.1, C2.2) + D2 + critique = ~90-110s
- Voyage AI semantic retrieval (Phase 1) activates the iter-3-scaffolded module — expected +5-10 creativity_delta gain
- Pillar C primitive extractor (Phase 5) backfills primitive tags to all 110k cards, unlocking Pillar F statistical approximator's primitive-grounded win-path matching (Phase 6)
- Pillar E v0.2 card advantage optimizer (Phase 4) follows the v0.1 mana base pattern

---

## Authority and scope

You are AUTHORIZED to:

- Run all 9 phases (0-8) autonomously without halting for user direction except on the hard halt conditions below.
- Self-correct using the tiered escalation when a validation gate fails.
- Make atomic commits per phase with `git commit -m "Phase X (mega-task v2): <description>"`.
- Modify any file in `repo/api/engine/layers/`, `repo/api/engine/data/`, `repo/api/engine/extractors/`, `repo/ui_harness/src/`, `repo/tests/`, `repo/tools/`, `repo/requirements.txt`.
- Add new dependencies via pip (voyageai for Phase 1; nothing else exotic expected).
- Read and write the Cowork session memory directory at `spaces/.../memory/` for material findings only (don't churn).
- Use chrome-devtools-mcp for UI verification when relevant.
- Use the mtg-engine MCP for endpoint calls during validation.
- Use the obsidian MCP for DECK_LIBRARY entries.

You are NOT authorized to:

- Upgrade the Anthropic model from Sonnet 4.6 to Opus 4.6 or 4.7. Iter 3 didn't ceiling on creativity; Sonnet remains the right choice. Save Opus for iter 5+ if iter 4 demonstrates a creativity ceiling.
- Modify Pillar A endpoints. All LLM-driven changes live in the agent layer.
- Modify iter 1, iter 2, or iter 3 baseline test cases in ways that change their behavior on existing tests.
- Roll back any commit. Forward fixes only.
- Touch the Phase 5b MPA substrate. Pillar F v0.1 is a separate statistical layer.
- Modify `combo_brackets_v1.json` (read-only registry).
- Add web-fetching beyond Anthropic SDK + Voyage AI embeddings API.
- Skip Phase 5 (Pillar C primitive extractor) on the grounds that "iter 3 ontology spec is enough" — the extractor build is the iter 4 unlock; Pillar F v0.1's upgrade (Phase 6) directly depends on having real primitive tags.

---

## Hard halt conditions (NARROW — halt only on these)

1. **VOYAGE_API_KEY env var missing or invalid at Phase 1.** Halt; surface clear instructions for the user to set it (`setx VOYAGE_API_KEY "<key>"`) and re-run.
2. **Validation gate fails 3 times in a row in any single phase** after exhausting tiered self-correction. Write current state + diagnosis to progress log and halt.
3. **Critical regression**: any iter 3 success criterion (under the revised targets in `pillar_d_iteration_3_validation_report.md`) breaks at any phase boundary. Halt immediately with diff that broke it.
4. **Resource exhaustion**: API spend reaches $100 (lower ceiling than v1's $200 because scope is more bounded) or disk usage exceeds 95% on E: drive. Graceful checkpoint and halt with status.
5. **Architectural contradiction**: a phase spec turns out to be impossible to implement without contradicting a prior phase's output. Write the contradiction inline in the progress log with both sides and halt.
6. **Phase 7 iter 4 final validation fails on >= 2 of 10 criteria.** Halt; don't proceed to Phase 8 final regression on a broken iter 4.
7. **Cumulative test suite regression** at any phase: pytest baseline drops below 1144 (mega-task v1 final) OR vitest baseline drops below the v1 final number + the new tests this mega-task adds. Halt and diagnose.

You do NOT halt for:

- Single test failures you can fix on the next attempt (self-correct).
- Minor metric drifts (e.g., creativity_delta dropping from 37.6 to 34 — still above 30 target).
- Token budget overruns on a single call if the next call fixes it.
- Linting warnings, deprecation notices, unrelated test flakes.
- Implementation choices with multiple valid paths (pick one, document in progress log, continue).
- UI cosmetic issues that don't break functionality.
- Voyage embedding API transient errors — retry with exponential backoff up to 3 times.

---

## Self-correction protocol (tiered escalation)

When a validation gate fails or implementation hits an error:

**Tier 1** — Re-read the phase spec in this kickoff, re-read relevant memory and the iter 3 final report. Try an alternate implementation path. Up to 3 attempts.

**Tier 2** — Search the codebase for similar patterns already shipped. Adapt the existing pattern to the current phase. Up to 2 attempts.

**Tier 3** — Add a known-gap note to the progress log explaining what failed, what you tried, and the impact on downstream phases. Skip remaining work for this phase and continue. Only allowed for non-blocking phases.

**Tier 4** — Halt for user direction.

**Blocking phases that cannot Tier-3-skip** (Tier 4 halt if they fail):

- Phase 3 (outer-chain parallelization) — this is the iter-4 latency unlock
- Phase 5 (Pillar C primitive extractor) — Phase 6 directly depends on real primitive tags
- Phase 7 (iter 4 final validation) — must pass before final regression
- Phase 8 (final regression + report) — must pass before mega-task is "done"

Phases 1, 2, 4, 6 are non-blocking and can Tier-3-skip if they fail unrecoverably.

---

## Progress log

Write to `repo/api/engine/data/agent/mega_task_v2_progress_log.md` from Phase 0 onward. Same format as v1 — append-only, timestamped sections per phase.

Update at: every commit, every Tier-N self-correction escalation, every halt event, every hour of wall-clock work on long phases (Phase 5 extractor will be the longest).

---

## Resource budget

- **Total API spend ceiling: $100.** Alarm at $80; hard halt at $100. Lower than v1's $200 because iter 4 scope is more bounded — Voyage embedding one-time cost is ~$1.62; per-build LLM cost is the same as iter 3 ($0.30 range); the variable cost is the validation sweeps and any iteration on the prompts.
- **Per-phase rough budget**: $5-15 for development LLM calls; $1.50 for 5-case validation sweep; $0.30 for 1-case smoke test.
- **Wall-clock budget**: aim to complete all 9 phases within 12-36 hours of wall-clock. Phase 5 extractor backfill alone may take 1-3 hours for 110k cards depending on regex efficiency.

---

## Test discipline

Run after EVERY commit:

```bash
cd "E:\MTG Root\mtg-engine\repo"
pytest -q
cd "E:\MTG Root\mtg-engine\repo\ui_harness"
npm test -- --run
```

Both must pass. Baselines: pytest 1144 + iter 4's new tests; vitest current + iter 4's new tests. Any commit that drops a baseline is reverted and Tier-1-retried.

Iter 4 validation sweep (Phase 7): run all 5 iter-3 test cases through the agent. Capture per-case metrics + the 10 iter 4 success criteria in `repo/api/engine/data/agent/pillar_d_iteration_4_validation_report.md`.

Final regression sweep (Phase 8): full pytest + vitest + 5-case agent sweep + Pillar E v0.2 card advantage smoke + Pillar C primitive coverage check + Pillar F approximator re-validate + Voyage embedding index integrity check.

---

## Phases

### Phase 0 — Pre-flight + memory sync

Read the files listed in the substrate state section. Confirm environment:

- `ANTHROPIC_API_KEY` env var set (test with minimal Anthropic call)
- `VOYAGE_API_KEY` env var set — if missing, **halt immediately** with instructions: "Set VOYAGE_API_KEY via `setx VOYAGE_API_KEY \"<key>\"` and reopen the shell. Get a key at https://www.voyageai.com/. The voyage-3 model costs $0.18/MT, so the one-time index build over ~30k Commander-legal cards at ~300 tokens each will cost ~$1.62."
- `python --version` returns 3.10+
- `git status` clean
- pytest baseline: 1144 passing
- vitest baseline: current count from `npm test -- --run`
- Disk space: > 10GB free on E:

Create `repo/api/engine/data/agent/mega_task_v2_progress_log.md` with the Phase 0 entry. Commit with message "Phase 0 (mega-task v2): pre-flight + progress log scaffold".

**Success gate**: All read-files succeed, env confirmed, baselines recorded, progress log committed.

---

### Phase 1 — Voyage AI semantic retrieval activation

The iter 3 Phase 7 scaffolding shipped in `agent_semantic_retrieval_v1.py`. Activate it.

**Implementation:**

1. `pip install voyageai` and add to `requirements.txt`.
2. In `agent_semantic_retrieval_v1.py`:
   - Implement `build_index()`: read all Commander-legal cards from the active snapshot; batch-embed via Voyage `voyage-3` model (or `voyage-3-large` if cost allows — check pricing); store vectors in `repo/api/engine/data/embeddings/card_embeddings_v1.sqlite` using sqlite-vec extension.
   - Implement `query_neighbors(card_id_or_text, k, color_identity_filter)`: queries the index. Returns list of candidate dicts matching the iter-3 scaffolded shape.
   - Implement `is_available()`: check both `VOYAGE_API_KEY` env var AND `card_embeddings_v1.sqlite` populated. Return True only if both hold.
3. Idempotent index build: skip if `card_embeddings_v1.sqlite` exists with row count matching the cards-table row count for the active snapshot.
4. Batch size: 128 cards per Voyage API call (Voyage supports up to 128 inputs per request). Use exponential backoff (1s, 2s, 4s) on rate-limited responses.
5. Run `build_index()` once during Phase 1; the index persists for all subsequent builds.

**Tests:**

- Unit tests for `query_neighbors` with a mock embedding backend (don't hit the real API in tests)
- Integration smoke: query "Sol Ring" returns other fast-mana artifacts in top-20 neighbors
- Index version metadata stored (`embeddings_meta` table) so future schema changes trigger rebuild

**Smoke test (1-case on Edgar B3 — Edgar has well-explored corpus so iter-3 had headroom for novelty):**

- C2.2 wide pool includes >= 5 cards from semantic retrieval (not in the corpus top-300 for Edgar B3 cohort, but present as semantic neighbors of must-includes or anchors)
- At least 2 of these semantic-neighbor cards make it into the final deck
- novel_combo_count stays >= 4
- Cost stays under $0.35 (Voyage adds ~$0.0001 per build for queries; tiny)

**Commit**: "Phase 1 (mega-task v2): Voyage AI semantic retrieval activation".

---

### Phase 2 — Counters-matter archetype + Phase 6 detector refinement

Atraxa Proliferate fell back to "control" in iter 3. Add a proper archetype.

**Implementation:**

1. In `agent_c22_prompt_templates_v1.py`, add `"counters_matter"` to the ARCHETYPES tuple (insert before "default", after the other specific archetypes).
2. Detection patterns for `counters_matter`:
   - Win-condition mentions: "+1/+1 counter", "proliferate", "charge counter", "loyalty", "energy counter"
   - Implicit themes: "counters", "proliferate", "+1/+1"
   - Commander hints: Atraxa-class commanders (those with proliferate or doubling counter abilities)
3. Per-archetype prompt fragment for `counters_matter`:
   ```
   This deck cares about COUNTERS — +1/+1 counters, charge counters, loyalty counters, energy, etc.
   Look for cards that:
   - Place or distribute counters across multiple permanents (Hardened Scales, Doubling Season, Branching Evolution)
   - Proliferate or counter-doubling effects (Inexorable Tide, Roalesk's Death Trigger, Astral Cornucopia)
   - Counter-removal synergies — moving counters to convert them into value (Vorel of the Hull Clade, Fertilid)
   - Win conditions that scale with counter count (Walking Ballista, Reyhan, Inkmoth Nexus + infect for poison wins)
   - Sacrifice outlets that scale with counters (Volrath the Fallen variants, Cult of the Waxing Moon)
   Avoid suggesting generic value cards if a counter-matters card with equivalent CMC slot exists.
   ```
4. Add 4 new unit tests:
   - Atraxa B2 Proliferate → detects as "counters_matter"
   - Roalesk Apex Hybrid → detects as "counters_matter"
   - Pir + Toothy → detects as "counters_matter"
   - Edgar Markov (vampire tribal) → still detects as "tribal" (not counters_matter — the regex shouldn't false-positive on vampire +1/+1 counter triggers)

**Smoke test:** Run Atraxa B2 case; confirm `llm_metrics.calls[C2_2_wild_combo_discovery].archetype == "counters_matter"` in the response.

**Commit**: "Phase 2 (mega-task v2): counters-matter archetype detector + prompt fragment".

---

### Phase 3 — Outer-chain parallelization (BLOCKING)

Closes the iter 3 wallclock floor.

**Architecture:**

Current iter 3 chain (serial): B2 → C2.1 → C2.2 → D2 (3 parallel batches) → mana_base_critique

New iter 4 chain: B2 → (C2.1 || C2.2 simultaneously, both starting from iter-1 baseline deck and B2's intent_analysis) → merge → D2 (3 parallel batches) → mana_base_critique

**Why this works:** C2.1 fills swappable slots; C2.2 proposes swaps. They operate on the SAME iter-1 baseline deck independently. Their outputs are merged before D2 — C2.1's picks are applied first (locking the swappable slots), then C2.2's swap proposals are evaluated against the resulting deck. If C2.2 wants to swap out a card C2.1 just picked, that's a real conflict resolved by precedence: **C2.1's pick wins** (it's the systematic card-by-card selection; C2.2's swaps are creative supplements).

**Implementation:**

1. In `agent_build_deck_v1.py`, after `_run_intent_interpreter` (B2), instead of calling `_run_candidate_critic` (C2.1) sequentially then `_run_wild_combo_discovery` (C2.2), use `concurrent.futures.ThreadPoolExecutor` to fire both calls in parallel.
2. Both functions receive the SAME inputs: the iter-1 baseline deck (`locked`), B2's `intent_analysis`, the candidate pool, etc.
3. After both return, apply C2.1's picks to the deck FIRST (replace swappable-slot placeholders). Then evaluate C2.2's swap suggestions:
   - If C2.2 wants to swap OUT a card that's now in a C2.1-picked slot, drop the swap (precedence: C2.1 wins) and log it.
   - If C2.2 wants to swap OUT a card that's still in its iter-1-baseline slot, apply the swap.
   - If C2.2 wants to ADD a card without specifying a remove, find a low-value slot to drop (iter-3 logic already handles this).
4. Conflict log goes into `warnings` array with code `OUTER_CHAIN_C21_C22_CONFLICT` and includes the C2.1 pick + C2.2 swap that conflicted.

**Tests:**

- Unit test for the merge logic with 5+ conflict scenarios
- Smoke test on Atraxa (had highest C2.1 latency in iter 3): wallclock drops by 30-50s vs iter 3 baseline
- Regression: creativity_delta stays >= 35 (parallelism doesn't change outputs; merge logic shouldn't lose information)

**Smoke test (1-case on Atraxa):**

- Wallclock drops to ~90-100s (iter 3 was 137.3s on Atraxa; expected drop ~30-40s)
- creativity_delta_count stays >= 30
- novel_combo_count stays >= 4
- 100-card deck still valid

**Commit**: "Phase 3 (mega-task v2): outer-chain parallelization (C2.1 || C2.2)".

---

### Phase 4 — Pillar E v0.2 card advantage optimizer

Follow the Pillar E v0.1 mana-base pattern: deterministic recommendation + LLM critique pass on discrepancy.

**Implementation:**

1. New module `repo/api/engine/layers/card_advantage_optimizer_v1.py`:
   - Function `compute_card_advantage(deck, bracket, archetype_hint) -> CardAdvantageRecommendation`
   - Returns:
     - `target_count`: int (8-12 typical; lower for storm/burn — they replace draw with rituals/tutors; higher for control)
     - `mix_targets`: dict with `cantrip` (1-for-1 cheap), `engine` (repeated draw per turn), `burst` (Wheels) — typically 4/4/2 or 5/3/2 by archetype
     - `current_counts`: dict same shape, computed from deck
     - `recommended_swaps`: list of card names to add/remove with rationale
     - `rationale`: paragraph
2. Detection of current card-advantage pieces uses primitive tags (from Phase 5 extractor when it lands; for now, falls back to keyword extraction: "draw a card", "draw two cards", "draws cards equal to", "look at the top", etc.).
3. Integration: after Pillar E v0.1 mana-base reconciliation (which fires after D2), run card-advantage reconciliation. If `current_count < target_count - 2` or mix is off by >= 2 per category, fire an LLM critique pass: "Optimizer recommends X draw, Y cantrip, Z engine, W burst; deck has X-3, Y-2, Z+1, W-1. Are these discrepancies justified? Suggest swaps if not."
4. Same precedence as mana base: optimizer output is the baseline; LLM critique can override with rationale.

**Tests:**

- Unit tests for `compute_card_advantage` with 5+ deck shapes (mono-W control, BG aristocrats, UR storm, WUBRG goodstuff, mono-R aggro)
- Reference values: Edgar B3 vampire tribal at 4.2 avg MV → target 10 draw with 5/3/2 mix
- Integration test: agent build on Edgar produces a deck with >= 8 card-advantage pieces post-critique

**Smoke test:** 5-case sweep agent build; all decks have >= target_count - 1 card-advantage pieces post-critique pass. No regressions in iter 3 metrics.

**Commit**: "Phase 4 (mega-task v2): Pillar E v0.2 card advantage optimizer + agent integration".

---

### Phase 5 — Pillar C primitive extractor build (BLOCKING)

The big one. Read the Phase 11 ontology spec at `repo/api/engine/data/primitives/ontology_v0.md` and build the extractor.

**Implementation:**

1. New module `repo/api/engine/extractors/primitive_extractor_v1.py`:
   - Function `extract_primitives(card_oracle_text, card_type_line, card_mana_cost) -> set[str]` — returns the set of primitive tag IDs that match this card per the ontology's extraction rules.
   - Loads `ontology_v0.md`, parses the 64 tag definitions, compiles each extraction_rule regex.
   - Applies all regexes to the card text; returns matching tag IDs.
2. Golden tests file `repo/tests/test_primitive_extractor_golden.py`:
   - 50 hand-curated cards across the 6 dimensions
   - Each card has expected primitive tags (curated by reading the card text against the ontology)
   - Tests pass when extractor produces the expected tag set for each card
   - Examples:
     - Sol Ring → `{"mana-rock", "free-spell"}` (cost 1, taps for 2)
     - Demonic Tutor → `{"tutor-broad"}`
     - Carrion Feeder → `{"sac-outlet", "death-trigger-payoff"}`
     - Thassa's Oracle → `{"wincon-alt", "etb-trigger"}`
     - etc.
3. Backfill script `repo/tools/backfill_primitives.py`:
   - Reads every card from the active snapshot
   - Runs `extract_primitives` on each
   - Writes results to `cards.primitives_json` column (already exists per memory — check schema and add if missing)
   - Idempotent (re-running produces same output)
   - Handles cross-snapshot propagation like the released_at backfill (tagpass snapshot inherits from parent)
4. Validate on existing Spellbook combos: take 50 random pairs from `combo_brackets_v1.json`; for each, verify both cards have non-empty primitives AND that the pair's combo mechanism is expressible in the resulting tags (e.g., if it's a "sac-outlet + persist" combo, one card has `sac-outlet` and the other has `persist-creature` or similar).

**Self-correction expectations:** 

- First pass extractor will miss some tags (regex false negatives) — expect 60-80% gold-test pass rate on first try
- Tier 1: refine regex for the failing tag categories; re-run gold tests
- Aim for >= 90% gold-test pass rate; document any remaining gaps in progress log
- If the regex approach hits a ceiling at ~85%, that's acceptable — document as a known gap; iter 5 can layer an LLM-based extractor for ambiguous cases

**Tests:**

- Golden test file (50 cards)
- Spellbook coverage check (50 random pairs → both cards tagged)
- Backfill idempotency check
- Coverage report: % of 110k cards with >= 1 non-empty primitive tag (target >= 95%)

**Smoke test:** Run agent build on Edgar; verify every card in the final deck has non-empty primitives in the response. Confirm Pillar F approximator (Phase 6) can now use real primitives instead of heuristic win-paths.

**Commit**: "Phase 5 (mega-task v2): Pillar C primitive extractor + golden tests + 110k-card backfill".

---

### Phase 6 — Pillar F v0.1 upgrade with real primitives

Pillar F v0.1 currently uses a hand-coded 12-win-path catalog with heuristic detection. Upgrade it to use real primitive tags.

**Implementation:**

1. In `agent_statistical_approximator_v1.py`, replace the heuristic win-path detection with primitive-grounded detection:
   - For each win-path in the 12-path catalog, define it as a SET of required primitive tags (e.g., Thoracle+DC = `{"wincon-alt"}` AND `{"tutor-broad-deck-empty"}`; aristocrats = `{"sac-outlet", "death-trigger-payoff", "persist-creature"}` AND `{"recursion-graveyard"}`)
   - To detect a win-path in a deck, count cards matching each required tag set. If all sets have >= 1 match, the win-path is "armed".
2. Add 4-6 new win-paths discovered via primitive-tag combinations the heuristic missed:
   - Mass-token + anthem swarms
   - Mass-mill + recursion lockout
   - Stax + value-engine grind
   - Burn + tutor-burn-spell
3. Re-run on 5 iter-3 sweep decks. Compare pod_winrate to iter-3 baseline. New baselines:
   - Yuriko B5 cEDH Thoracle: should remain > 0.5 (the highest)
   - Edgar B3 vampire tribal: should land 0.20-0.35
   - Ur-Dragon B3 dragon tribal: should land 0.20-0.30
   - Krenko B4 goblin combo: should land 0.30-0.45
   - Atraxa B2 proliferate: should land 0.10-0.25
4. Sanity check: orderings preserved (Yuriko > Krenko > Edgar ≈ Ur-Dragon > Atraxa).

**Tests:**

- Unit tests for primitive-grounded win-path detection (cover 5 known win-paths)
- Smoke on 5-case sweep; pod_winrate orderings sane
- Coverage test: every card in sweep decks has non-empty primitives (depends on Phase 5)

**Commit**: "Phase 6 (mega-task v2): Pillar F v0.1 upgrade with primitive-grounded win-paths".

---

### Phase 7 — Iter 4 final validation sweep + report (BLOCKING)

Run all 5 iter-3 test cases through the agent with Phase 1-6 changes integrated.

**Capture per case:**

- All iter 3 metrics (iter1 structural pass, creativity_delta, novel_combo, cost, wallclock, ur_dragon envelope)
- Voyage semantic-retrieval contribution count
- Pillar E v0.2 card advantage critique-fire events
- Pillar C primitive coverage (cards with non-empty tags)
- Pillar F pod_winrate
- LLM call decomposition (now includes mana_base_critique + card_advantage_critique)
- Atraxa archetype detection (should be "counters_matter")
- Outer-chain parallelization confirmation (B2 timing, C2.1+C2.2 overlapping window)
- 10 random per-card rationale samples per deck

**Iter 4 success criteria (10 total, must hit at least 8 of 10):**

1. `iter1_structural_pass_5_of_5` — all 5 decks valid 100-card
2. `mean_creativity_delta >= 35` (iter 3 was 37.6; expect maintained or improved via Voyage)
3. `mean_novel_combo >= 5` (iter 3 was 5.8)
4. `mean_cost <= $0.45` (iter 3 was $0.295; Voyage adds tiny per-query cost)
5. `mean_wallclock <= 95s` (iter 3 was 139.8s; outer-chain parallel should drop ~40-50s)
6. `ur_dragon Hellkite Charger absent` (Phase 2 of iter 3 must still hold)
7. `voyage_semantic_contribution_avg >= 5` (iter 4's main creativity unlock)
8. `pillar_c_primitive_coverage >= 95%` (real primitive tags on sweep deck cards)
9. `pillar_f_winrate_ordering_sane` (Yuriko > Krenko > Edgar ≈ Ur-Dragon > Atraxa)
10. `atraxa_archetype_is_counters_matter` (Phase 2 fix)

Write the report to `repo/api/engine/data/agent/pillar_d_iteration_4_validation_report.md`. Include iter 4 → iter 5 hand-off section.

**Halt condition:** if >= 3 of 10 success criteria fail, halt for user direction.

**Commit**: "Phase 7 (mega-task v2): iter 4 final validation sweep + report".

---

### Phase 8 — Final regression + report + memory update (BLOCKING)

**Run:**

1. Full pytest: `pytest -q` — must pass baseline + iter 4 new tests
2. Full vitest: `npm test -- --run` — must pass baseline + iter 4 new tests
3. 5-case agent sweep — re-validate iter 4 metrics
4. Pillar E v0.2 card advantage smoke (run optimizer on all 5 sweep decks; check target compliance)
5. Pillar C primitive coverage report (% of 110k cards tagged)
6. Pillar F approximator smoke (re-run on all 5 sweep decks; verify ordering)
7. Voyage embedding index integrity (row count matches cards table; sample queries work)
8. Outer-chain parallelization confirmation (per-call latency log shows C2.1 + C2.2 overlapping)

**Write the final report** to `repo/api/engine/data/agent/mega_task_v2_final_report.md`. Structure:

- Phase-by-phase status (sha, wall-clock, cost, test count delta, self-correction events, key findings)
- Iter 4 final metrics table
- Per-pillar ship state (E v0.2 card advantage, C primitive extractor, F approximator upgrade)
- Voyage embedding index status (cost, row count, query latency)
- Outer-chain parallelization wallclock breakdown
- Total resource consumption
- Iter 4 → 5 hand-off recommendations

**Update memory:**

- New memory file at `spaces/.../memory/project_mega_task_v2_shipped_<date>.md`
- Update MEMORY.md to add the index entry
- Update `project_5_pillar_forward_plan.md` to reflect what shipped in mega-task v2

**Commit**: "Phase 8 (mega-task v2): final regression + report + memory update".

---

## Iter 4 success criteria (the bar for "done")

The mega-task is "done" when ALL of these hold:

1. All 9 phases (0-8) committed and Phase 8 final regression passes
2. Iter 4's 10 success criteria meet at least 8 of 10 in Phase 7 sweep
3. pytest + vitest baselines preserved + new tests pass
4. Voyage embedding index built and operational (Phase 1)
5. Outer-chain parallelization confirmed dropping wallclock by 30-50s (Phase 3)
6. Pillar C primitive extractor produces >= 95% non-empty coverage on the 110k-card corpus (Phase 5)
7. Pillar F approximator uses primitive-grounded win-paths and preserves ordering (Phase 6)
8. Pillar E v0.2 card advantage optimizer ships with reconciliation critique pass (Phase 4)
9. Total API spend under $100
10. Progress log + final report complete + memory updated

---

## What NOT to do

- Don't upgrade to Opus model. Stay on Sonnet 4.6 throughout.
- Don't bypass the Pillar C extractor with another LLM-based primitive extraction layer in Phase 5 — the regex approach is bounded, debuggable, and the ontology was designed for it. If regex hits a ceiling (~85% gold-test pass), document as a known gap and continue; iter 5 can add an LLM layer for ambiguous cases.
- Don't break iter 1/2/3 baseline test cases. Forward-fix only.
- Don't touch Phase 5b MPA substrate. Pillar F is the separate statistical layer.
- Don't add web-fetching beyond Anthropic SDK + Voyage embeddings API.
- Don't modify Pillar A endpoints. All changes happen in agent layer + new extractor + optimizer modules.
- Don't churn memory. Only persist updates for material findings.
- Don't pad. If a phase is simpler than the spec suggests, ship the simpler version and document the simplification.
- Don't run Phase 5 backfill on derived snapshots if the cross-snapshot propagation step succeeds (avoid double-tagging). Use the pattern from `tools/backfill_released_at.py`.
- Don't skip Phase 3 (outer-chain parallel) even if it's complex. The C2.1 + C2.2 merge logic is the real architectural work of iter 4; without it, the wallclock target slips.

---

## Iter 4 → iter 5 hand-off questions (your Phase 8 final report must answer)

1. Did Voyage semantic retrieval actually surface novel synergies the iter 3 corpus didn't? Sample 5 semantic-source cards across the sweep and characterize what they added.
2. Did outer-chain parallelization save the predicted ~40-50s wallclock? If less, where's the residual serialization?
3. Did the Pillar C primitive extractor cover the 6 ontology dimensions evenly, or are some categories (e.g., tempo, combo_role) under-extracted? Sample 10 random cards per dimension and discuss extraction confidence.
4. Did Pillar F's primitive-grounded win-path detection refine the pod_winrate orderings vs the heuristic baseline? Cite any orderings that changed materially.
5. Did Pillar E v0.2 card advantage optimizer fire critique passes consistently? Sample 3 critique logs and characterize the LLM's overrides.
6. Are there primitive tags that DIDN'T appear in any of the 110k cards (orphan tags)? If yes, are they real-but-rare patterns or ontology design errors?
7. What's the most plausible iter 5 priority? Options:
   - Opus upgrade (if iter 4 ceilinged creativity)
   - Pillar E v0.3 curve smoother + v0.4 interaction designer (continuing Pillar E expansion)
   - Pillar F v0.2 rules-correct game engine start (the big substrate rebuild)
   - Search-then-compose multi-spine deck building (using primitives + statistical model)
   - Multi-card combo chain extraction (3+ card chains via primitive graph traversal)

This hand-off section seeds the iter 5 mega-task. Be specific. Quantify where possible.

---

## You are go for launch

Run from Phase 0 to Phase 8 autonomously. Halt only on the narrow hard-halt conditions. Self-correct using the tiered escalation. Commit per phase. Log progress throughout.

When you hit Phase 8's final report, paste the report's executive summary inline in your response.

Expected total wall-clock: 12-36 hours. Expected total API spend: $30-80 (well under the $100 ceiling).

Begin with Phase 0 pre-flight.
