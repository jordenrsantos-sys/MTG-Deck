# Mega-task v7: Iter 8 — Candidate pool fix + Commander typeahead + LLM critique aggression + CC's 4 sweep gap closes

Single self-contained kickoff. You are authorized to run autonomously from start to finish without further user interaction unless a hard halt condition triggers. Self-correct using the tiered escalation. Atomic commit per phase. Maintain a running progress log throughout.

---

## What this mega-task delivers

Iter 7 mega-task v6 shipped massive architectural progress (UI works end-to-end via SSE fix, Pillar E v0.1-v0.6 COMPLETE, ontology v2 with real counter primitives, voyage_rules_embedding at scale, voyage_downgrade_pass wired). Live UI walkthrough on 2026-05-22 night against a real Edgar Markov B3 build confirmed the agent NOW produces a recognizable deck with real combos + theme detection + graduated playtest report card — BUT also exposed two new architectural gaps that prevent deck QUALITY from reaching production-ready:

1. **Candidate pool under-fills.** `POOL_UNDER_FILL_PADDED_WITH_BASICS` warning fires consistently — pool returns ~38 spells when 60-65 are needed; engine pads remainder with basic lands. This is the root cause of BOTH the 99-Wastes failure mode (empty pool from typo cascade) AND the 68-land failure mode (partial pool padded with 32 extra basics). Visible warning on every real-world build.

2. **User-typo cascade.** "Edgar Makrov" (missing 'r') typo earlier today cascaded into total deck failure (`BRIEF_NO_CORPUS_ENTRIES_FOR_COMMANDER` → empty color identity → no candidates → 99 Wastes). The deck-building page has Commander typeahead; the AI Build view doesn't.

3. **LLM critique pass under-acts on Pillar E flags.** Mana base optimizer flags `actual=68, target=36, delta+32`. Card advantage optimizer flags `1 vs target 10`. Curve smoother flags `7 holes`. Interaction designer flags `0/4 mass_removal`. ALL flagged correctly; ALL unaddressed by the LLM critique pass. The optimizers work; the critique LLM doesn't actually propose swaps to close the gaps.

Plus CC's 4 documented sweep gaps from iter 7 Phase 11 (each attempted in iter 7 but still pending):

4. *voyage_semantic_avg 2.2 vs ≥3* — semantic-injection guarantee shipped, but swappable set too narrow.
5. *intent_drift 3/5 vs ≥4/5* — archetype-aware thresholds shipped for counters_matter + tribal-with-value-engine; need extension to combo/storm/control.
6. *interaction_within 0/5* — multi-primitive counting now overshoots 1.5×target; needs per-category bounds.
7. *win_con 0/5* — coherence checker works but pool covers only ~30/100 deck cards' primitives; needs to hydrate from DB cards table.

After v7 ships, the agent produces a production-ready deck via the UI for any commander the user names (with or without typos), with the candidate pool returning 60+ spells, Pillar E recommendations honored by LLM critique, and all 4 iter 7 sweep criteria closed.

Read these files at Phase 0:

1. `spaces/.../memory/project_iter_8_prep_notes_2026-05-22.md` (locked iter 8 priorities — source of truth)
2. `spaces/.../memory/project_mega_task_v6_shipped_<date>.md` (whatever CC named the v6 ship memory entry)
3. `repo/api/engine/data/agent/mega_task_v6_final_report.md` (v6 final report + iter 8 hand-off section)
4. `repo/api/engine/data/agent/pillar_d_iteration_7_validation_report.md` (iter 7 sweep metrics)
5. `repo/api/engine/data/agent/coherence_sweep_3_health_report.md` (sweep findings, some carried forward)
6. `repo/api/engine/layers/agent_build_deck_v1.py` (main agent endpoint — Phase 3 LLM critique work lives here)
7. `repo/api/engine/layers/agent_wide_candidate_pool_v1.py` (Phase 1 candidate pool under-fill diagnosis target)
8. `repo/api/engine/layers/agent_semantic_injection_v1.py` (Phase 4 swappable set widening target)
9. `repo/ui_harness/src/views/AIBuildView.tsx` (Phase 2 Commander typeahead target)

---

## Substrate state

Mega-task v6 shipped 2026-05-22 (~26 commits on top of Coherence Sweep #3 at `4cee4a287`). Pillar E v0.1-v0.6 COMPLETE. SSE UI fix shipped clean (React 18 StrictMode mountedRef regression). Test baselines: pytest 1566 passed / 25 skipped / 0 failed. vitest 759 / 2 pre-existing fails. Total CC spend through v6: ~$25-27.

**Iter 7 baseline you must not regress (10/14 sweep criteria pass under user-revised targets):**

- iter1 structural pass: 5/5
- mean creativity_delta, novel_combo, cost, wallclock all within targets
- Hellkite Charger absent on Ur-Dragon
- Pillar C coverage 90.5% (ontology v2 backfill)
- pillar_f_ordering sane
- theme_profile_structured 5/5
- ui_e2e_build_renders_5_of_5 (Phase 1 SSE fix)
- **4 known failures iter 8 must close** (priorities 4-7 above)

**Architectural rules locked in feedback memories (must be honored):**

- Corpus is descriptive not prescriptive
- User intent locks deck shape — corpus optimum is not the target
- Mana base serves spells, computed last not locked first
- Pool ranking score does not drive LLM picking — iter 7 Phase 2 semantic-injection guarantee is the architectural answer; iter 8 widens its swappable set

**File layout iter 8 will work in:**

- `repo/api/engine/layers/agent_wide_candidate_pool_v1.py` — Phase 1 instrumentation + filter audit
- `repo/api/engine/layers/agent_build_deck_v1.py` — Phase 3 LLM critique pass refactor
- `repo/api/engine/layers/agent_semantic_injection_v1.py` — Phase 4 swappable set
- `repo/api/engine/layers/agent_c22_prompt_templates_v1.py` — Phase 5 archetype thresholds
- `repo/api/engine/layers/interaction_designer_v1.py` — Phase 6 per-category bounds
- `repo/api/engine/layers/win_con_coherence_v1.py` — Phase 7 DB primitive hydration
- `repo/ui_harness/src/views/AIBuildView.tsx` — Phase 2 typeahead UI
- New file `repo/ui_harness/src/components/CommanderTypeahead.tsx` — Phase 2 component
- New endpoint maybe needed: `repo/api/main.py` if `/card/search_v1?filter=is_commander` doesn't already exist

---

## Authority and scope

You are AUTHORIZED to:

- Run all 10 phases (0-9) autonomously without halting except on hard halt conditions.
- Self-correct using tiered escalation.
- Make atomic commits per phase: `git commit -m "Phase X (mega-task v7): <description>"`.
- Modify any file in `repo/api/`, `repo/ui_harness/`, `repo/tests/`, `repo/tools/`, `repo/requirements.txt`.
- Add new dependencies via pip if needed (nothing exotic expected).
- Read and write Cowork memory at `spaces/.../memory/` for material findings.
- Use mtg-engine MCP + obsidian MCP for verifications.
- Use chrome-devtools-mcp for live UI validation in Phase 2 + Phase 8.

You are NOT authorized to:

- Upgrade the Anthropic model from Sonnet 4.6.
- Modify Pillar A endpoints (changes in agent layer + new components + new optimizer extensions).
- Modify iter 1-7 baseline test cases.
- Roll back any commit. Forward fixes only.
- Touch the Phase 5b MPA substrate.
- Modify `combo_brackets_v1.json` directly.
- Touch the v3 per-set automation scheduled task.
- Start Pillar F v0.2 game engine substrate work — that's iter 9+ multi-month scope.
- Re-extract primitives across all 110k cards (iter 7 Phase 3 backfill was the once-per-arc operation).
- Add web-fetching beyond Anthropic SDK + Voyage embeddings API + Scryfall API.

---

## Hard halt conditions (NARROW — halt only on these)

1. **Validation gate fails 3 times in a row** in any single phase after exhausting tiered self-correction.
2. **Critical regression**: any iter 1-7 success criterion breaks at any phase boundary. Halt with diff.
3. **Resource exhaustion**: API spend reaches $100 (v7 ceiling). Hard halt.
4. **Architectural contradiction**: a phase spec turns out to be impossible without contradicting a prior phase's output.
5. **Phase 8 final validation fails on >= 3 of 12 success criteria**.
6. **Cumulative test suite regression**: pytest drops below 1566 OR vitest drops below 759 + new tests. Halt.
7. **Phase 1 candidate pool diagnosis surfaces a fundamental corpus gap** (e.g., theme tag coverage <60% for the cohort) — halt and surface; that's iter 9 corpus expansion work, not iter 8 patching.

---

## Self-correction protocol (tiered escalation)

**Tier 1** — Re-read phase spec + relevant memory + iter 7 final report. Try alternate implementation. Up to 3 attempts.

**Tier 2** — Search codebase for similar patterns. Up to 2 attempts.

**Tier 3** — Add known-gap note to progress log; skip remaining work for phase and continue. Only allowed for non-blocking phases.

**Tier 4** — Halt for user direction.

**Blocking phases that cannot Tier-3-skip:**

- Phase 1 (candidate pool fix) — load-bearing for deck quality
- Phase 2 (commander typeahead) — UX gap that prevents users from even using the agent reliably
- Phase 3 (LLM critique aggression) — without this, Pillar E optimizer flags stay unaddressed regardless of other phases
- Phase 8 (iter 8 final validation) — must pass before Phase 9
- Phase 9 (final regression + memory) — must pass to declare done

Phases 4, 5, 6, 7 are non-blocking but EACH closing a specific iter 7 sweep gap; allow Tier-3-skip per phase if blocked.

---

## Progress log

Write to `repo/api/engine/data/agent/mega_task_v7_progress_log.md` from Phase 0. Append-only, timestamped sections per phase.

---

## Resource budget

- **Total API spend ceiling: $100.** Alarm at $80; hard halt at $100. Expected actual: $20-50.
- **Per-phase rough budget**: Phase 1 diagnosis ($2-5 in LLM iteration); Phase 3 LLM critique prompt-engineering ($5-15 across iterations); Phase 8 sweep $2-3. Other phases mostly code work.
- **Wall-clock budget**: 36-72 hours.

---

## Test discipline

After EVERY commit: pytest + vitest must pass at baseline + new tests.

Phase 2 + Phase 8 use chrome-devtools-mcp for live UI verification.

---

## Phases

### Phase 0 — Pre-flight + memory sync

Read the 9 files in "What this mega-task delivers." Confirm env (Python 3.10, ANTHROPIC_API_KEY + VOYAGE_API_KEY set, MCPs connected, pytest 1566 + vitest 759 baselines, git clean, disk >10GB). Create `repo/api/engine/data/agent/mega_task_v7_progress_log.md`. Commit: "Phase 0 (mega-task v7): pre-flight + progress log scaffold".

---

### Phase 1 — Candidate pool under-fill diagnosis + fix (BLOCKING, highest priority)

**Symptom (from live walkthrough):** Edgar Markov B3 build returns 68 lands + 1 removal + 31 flex/other + 1 commander = 101 cards. Warning fires: `POOL_UNDER_FILL_PADDED_WITH_BASICS: Pool yielded fewer than 99 non-commander cards; padded 62 basics.` Same root cause as the 99-Wastes typo case (empty pool → all basics) and the Edgar partial-pool case (~38 spells → 62 basics padded).

**Investigation:**

1. *Instrument the pool builder.* In `agent_wide_candidate_pool_v1.py` (and `compute_deck_candidate_pool_v1` in the Pillar A endpoint), add per-filter-step logging: count of candidates remaining after each filter (color identity → theme match → bracket policy → primitive tag → semantic neighbor injection → dedup against must-includes → final ranking). Identify which filter is too restrictive.

2. *Verify corpus coverage.* Query the database: how many vampire-themed cards exist in the active snapshot? How many of those are primitive-tagged after iter 7's ontology v2 backfill? If the answer is <100, the corpus is genuinely thin for this archetype and Phase 1 needs to surface that as an iter 9+ corpus expansion item rather than patching the filter.

3. *Verify Pillar C primitive coverage for the cohort.* Iter 7 reported 90.5% coverage across the 110k corpus, but THAT specific 9.5% gap may concentrate in older or less-common cards that ARE Edgar Markov staples. If primitives are missing on actual Edgar staples, theme-match filter rejects them.

4. *Adjust filter thresholds OR expand fallback paths.* If the diagnosis points to over-restrictive filtering, loosen the relevant filter's threshold. If it points to corpus gap, add a fallback path: when theme-filter yields <60 spells, drop the theme-match requirement and pull from broader bracket+color-identity-legal candidates (with a warning logged).

**Tests:**

- Add `tests/test_candidate_pool_fill_rate.py`: for each of the 5 iter-7 sweep cases, assert pool returns ≥60 non-land candidates. Fail clearly if not.

**Smoke test:** Run Edgar Markov B3 + Krenko B4 + Atraxa B2 cases via Python tool. Each should now show pool size ≥60 spell candidates + the resulting deck has ≤38 lands (not 68).

**Commit:** "Phase 1 (mega-task v7): candidate pool under-fill diagnosis + fix".

---

### Phase 2 — Commander typeahead + fuzzy match (BLOCKING)

**Implementation:**

1. *Backend check.* Verify `/card/search_v1` (Pillar A endpoint) supports a commander-filter query. If not, extend it with a `?filter=is_commander` parameter that returns only legendary creatures + planeswalkers with "can be your commander" text + partners/backgrounds.

2. *UI component.* New file `repo/ui_harness/src/components/CommanderTypeahead.tsx`:
   - Debounced (250ms) fetch on keystroke
   - Renders top 5-10 results in a dropdown
   - Each result shows: card name + color identity pips + mana cost
   - Arrow keys + Enter to select, click to select, Esc to dismiss
   - Replaces the plain `<input>` in `AIBuildView.tsx` Commander field

3. *Fuzzy match fallback.* If typed name has no exact match but edit-distance ≤2 to a known commander, show "Did you mean: <closest>?" below the input. Click suggestion auto-populates the field.

4. *Backend support for fuzzy.* `/card/search_v1?q=<typed>&fuzzy=true` returns matches even with up-to-2 character edit distance.

**Tests:**

- Component test: typeahead populates dropdown, keyboard navigation works, selection auto-fills the field.
- Backend test: fuzzy=true on "Edgar Makrov" returns "Edgar Markov" in top 3.

**Smoke test via chrome-devtools-mcp:** Navigate to `localhost:5173/#ai-build`. Type "edg" in Commander field. Verify dropdown shows Edgar Markov + Edgar, Charmed Groom + other Edgars. Click Edgar Markov. Verify field populates. Type "Edgar Makrov" (with typo). Verify "Did you mean: Edgar Markov?" appears.

**Commit:** "Phase 2 (mega-task v7): commander typeahead + fuzzy match".

---

### Phase 3 — LLM critique aggression on Pillar E optimizer flags (BLOCKING)

**Symptom:** Pillar E optimizers (v0.1 mana base + v0.2 card advantage + v0.3 curve smoother + v0.4 interaction designer + v0.5 win-con coherence + v0.6 anti-meta hate) all flag discrepancies correctly. Warnings show: `MANA_BASE_DISCREPANCY_UNJUSTIFIED`, `CARD_ADVANTAGE_DISCREPANCY_UNJUSTIFIED`, `CURVE_DISCREPANCY`, `INTERACTION_DISCREPANCY`, `WIN_CON_75PCT_PILE`. The "UNJUSTIFIED" suffix means the LLM critique pass didn't justify the discrepancy and didn't propose swaps. The optimizers WORK; the critique LLM doesn't ACT on their flags.

**Implementation:**

1. *Audit the critique pass code path.* In `agent_build_deck_v1.py`, find where each optimizer's recommendation feeds the LLM critique. Identify whether the critique LLM has:
   - Visibility into the optimizer's recommendation
   - Authority to propose specific swaps (not just justify)
   - Explicit instruction to ACT on flagged discrepancies

2. *Refactor critique prompts.* Each optimizer's critique should:
   - Show the optimizer's recommendation explicitly (target count, current count, delta)
   - Show available candidates in the pool that could close the gap
   - Require either (a) propose specific swaps to close the gap OR (b) explicit archetype-specific justification for not closing it
   - Output structured swap proposals (card-out + card-in pairs) that the engine APPLIES

3. *Apply LLM-proposed swaps deterministically.* Don't let the LLM just suggest — actually apply approved swaps to the deck composition before final response.

**Tests:**

- For each of 5 iter-7 sweep cases, after Phase 3 the resulting deck has:
  - Mana base within ±5 of Pillar E v0.1 target (was ±32 for Edgar)
  - Card advantage within ±2 of v0.2 target (was -9 for Edgar)
  - Curve has ≤2 holes (was 7)
  - Interaction count within target ±20%
  - Win-con coherence ≥1 primary pattern at ≥5 enablers

**Commit:** "Phase 3 (mega-task v7): LLM critique pass aggression on Pillar E optimizer flags".

---

### Phase 4 — voyage_semantic widen injection swappable set (CC sweep gap #1)

Iter 7's semantic-injection guarantee shipped but voyage_semantic_avg stayed at 2.2 vs ≥3 target. CC's recommendation: widen the injection swappable set.

**Implementation:**

1. In `agent_semantic_injection_v1.py`, identify which cards are eligible for displacement when injecting semantic neighbors. Currently restricted to "lowest-priority C2.2 wild-discovery picks." Expand to include:
   - Cards from C2.1 candidate critic flagged as "marginal fit" (low rationale-confidence score)
   - Bracket-floor cards (cards picked because the bracket has a minimum count for them but with weak fit to the deck's theme)
2. Increase `n_target` from 3 to 4 for B3+ brackets where creativity headroom exists.

**Tests:** 5-case sweep should produce voyage_semantic_avg ≥3.

**Commit:** "Phase 4 (mega-task v7): voyage_semantic widen injection swappable set".

---

### Phase 5 — intent_drift archetype-aware thresholds extension (CC sweep gap #2)

Iter 7 shipped archetype-aware drift thresholds for `counters_matter` and `tribal-with-value-engine`. Iter 8 extends to combo, storm, control, aristocrats, voltron.

**Implementation:**

1. In the drift evaluation logic, extend per-archetype threshold map:
   - combo: 0.65 (combos naturally use cards across many themes)
   - storm: 0.70 (storm needs cantrips + rituals + storm payoffs that span themes)
   - control: 0.65 (control decks have wide cardpools)
   - aristocrats: 0.55 (relatively focused)
   - voltron: 0.55 (relatively focused)
   - default: 0.50

**Tests:** 5-case sweep should show ≥4/5 cases below their effective archetype threshold.

**Commit:** "Phase 5 (mega-task v7): intent_drift archetype-aware thresholds extension".

---

### Phase 6 — interaction_within per-category bounds (CC sweep gap #3)

Iter 7's multi-primitive counting fix now OVERSHOOTS target by >1.5×. Need per-category bounds.

**Implementation:**

1. In `interaction_designer_v1.py`, instead of one total interaction-count target, define per-category targets:
   - mass_removal: 2-4 cards
   - targeted_creature_removal: 4-7 cards
   - targeted_artifact_removal: 1-3 cards
   - targeted_enchantment_removal: 0-2 cards
   - counterspell (U decks only): 4-8 cards
2. Validation passes if EACH category is within its range (not a single total).

**Tests:** 5-case sweep should show ≥4/5 cases within all per-category bounds.

**Commit:** "Phase 6 (mega-task v7): interaction_within per-category bounds".

---

### Phase 7 — win_con hydrate primitives from DB (CC sweep gap #4)

Iter 7's win-con coherence checker works but only sees primitives for ~30/100 deck cards (those in the candidate pool). Need to hydrate primitives for ALL deck cards by querying the cards DB.

**Implementation:**

1. In `win_con_coherence_v1.py`, before pattern matching, hydrate the deck's full primitive set:
   - For each card in the deck (commander + 99 mainboard), query `cards.primitives_json` for that card_id
   - Merge with whatever primitives the candidate pool already carried for that card
2. Now pattern matching sees the FULL deck's primitives, not just pool-covered ones.

**Tests:** 5-case sweep should show ≥1 primary win-con pattern with ≥5 enablers per case (was 3 enablers max in iter 7).

**Commit:** "Phase 7 (mega-task v7): win_con hydrate primitives from DB for full deck coverage".

---

### Phase 8 — Iter 8 final validation sweep + report (BLOCKING)

**Capture per case** (5 iter-2 sweep cases via Python tool + 5 UI cases via chrome-devtools-mcp):

All iter 7 metrics + Phase 1 pool fill rate + Phase 2 typeahead live verification + Phase 3 critique outcomes + Phases 4-7 sweep gap closes.

**Iter 8 success criteria (12 total, must hit ≥10):**

1. `iter1_structural_pass_5_of_5`
2. `mean_creativity_delta >= 35`
3. `mean_novel_combo >= 5`
4. `mean_cost <= $0.50`
5. `mean_wallclock <= 130s`
6. `voyage_semantic_avg >= 3` (Phase 4 fix)
7. `intent_drift_archetype_aware_pass >= 4/5` (Phase 5 fix)
8. `interaction_within_per_category_bounds >= 4/5` (Phase 6 fix)
9. `win_con_pattern_5_enablers >= 4/5` (Phase 7 fix)
10. `candidate_pool_fill_rate >= 60 spells per case` (Phase 1 fix)
11. `commander_typeahead_e2e_verified` (Phase 2 fix via chrome-devtools-mcp)
12. `pillar_e_critique_resolves_discrepancies >= 4/5 categories` (Phase 3 fix)

Write report to `repo/api/engine/data/agent/pillar_d_iteration_8_validation_report.md`. Include iter 8 → iter 9 hand-off section.

**Halt condition:** if >= 3 of 12 success criteria fail, halt for user direction.

**Commit:** "Phase 8 (mega-task v7): iter 8 final validation sweep + report".

---

### Phase 9 — Final regression + report + memory update (BLOCKING)

Full pytest + vitest. 5-case sweep re-validation. Live UI sanity via chrome-devtools-mcp.

Write `repo/api/engine/data/agent/mega_task_v7_final_report.md`.

**Update memory:**
- `spaces/.../memory/project_mega_task_v7_shipped_<date>.md`
- MEMORY.md index
- Update `project_5_pillar_forward_plan.md` — Pillar F v0.2 game engine becomes the next major architectural step for iter 9+

**Commit:** "Phase 9 (mega-task v7): final regression + report + memory update".

---

## Mega-task v7 success criteria

Mega-task is "done" when ALL hold:

1. All 10 phases committed; Phase 9 final regression passes.
2. Phase 8 sweep meets ≥10 of 12 success criteria.
3. pytest + vitest baselines preserved + new tests pass.
4. Candidate pool returns ≥60 spells per build (Phase 1).
5. Commander typeahead + fuzzy match live-verified (Phase 2).
6. LLM critique pass actively closes Pillar E optimizer flags (Phase 3).
7. CC's 4 iter 7 sweep gaps closed (Phases 4-7).
8. Total API spend under $100.

---

## What NOT to do

- Don't upgrade to Opus. Stay on Sonnet 4.6.
- Don't break iter 1-7 baseline test cases.
- Don't modify `combo_brackets_v1.json` directly.
- Don't touch Phase 5b MPA substrate.
- Don't modify Pillar A endpoints (extend via new endpoints if needed).
- Don't churn memory.
- Don't pad.
- Don't disable the v3 per-set automation scheduled task.
- Don't try to ship Pillar F v0.2 game engine — iter 9+ work.
- Don't re-extract primitives across all 110k cards (iter 7 Phase 3 backfill is the once-per-arc operation; only re-extract specific cards if necessary in Phase 7).

---

## Iter 8 → iter 9 hand-off questions (your Phase 9 final report must answer)

1. Did Phase 1 close the pool under-fill gap reliably across all 5 sweep cases? What's the per-case spell count now?
2. Did Phase 2 typeahead live-verify cleanly via chrome-devtools-mcp? Sample 3 commander typeahead queries + 1 fuzzy match correction.
3. Did Phase 3 LLM critique resolve Pillar E flags reliably? Compare per-case discrepancy counts pre/post Phase 3.
4. Did Phases 4-7 close CC's iter 7 sweep gaps? Per-gap status.
5. What's the most plausible iter 9 priority? Options:
   - Pillar F v0.2 rules-correct multiplayer game engine (multi-month substrate rebuild)
   - Multi-deck cross-pollination + reverse-engineering target decks
   - Bracket-partitioned corpus
   - Corpus expansion for thin archetypes (if Phase 1 surfaced this)
   - Tournament/meta data tracking

---

## You are go for launch

Run from Phase 0 to Phase 9 autonomously. Halt only on the narrow hard-halt conditions. Self-correct. Atomic commits. Log progress.

When you hit Phase 9's final report, paste the executive summary inline.

Expected total wall-clock: 36-72 hours. Expected total API spend: $20-50.

Begin with Phase 0 pre-flight.
