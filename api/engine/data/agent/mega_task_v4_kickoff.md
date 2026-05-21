# Mega-task v4: Pillar D iter 5 + ontology v1 + user-intent architecture + Voyage rules embedding + combo DB expansion

Single self-contained kickoff. You are authorized to run autonomously from start to finish without further user interaction unless a hard halt condition triggers. Self-correct using the tiered escalation. Atomic commit per phase. Maintain a running progress log throughout. Cross-phase regression check at every phase boundary.

---

## What this mega-task delivers

This is the most architecturally substantial iteration to date. It consolidates 8 prioritized insights + 2 user-stated architectural feedback rules + 3 iter-4 hand-off findings into a single coherent ship. The shape: close iter 4's three measurement-gap failures (wallclock, voyage selection, ontology coverage) via direct fixes, then layer in foundational user-intent architecture, then expand the combo+rules data substrate.

After this mega-task ships, the agent will: produce builds with sharper creativity (semantic-neighbor selection working, broader combo coverage, structured theme profile honoring user intent), at lower latency (~110s mean), with better structural soundness (theme-aware Pillar E target counts, aggressive mana-base reconciliation), and with structured rules-modifier extraction so cards like "may put a token" vs "put a token" get distinguished at the primitive layer.

Read these memories at Phase 0 — they are the source of truth for this mega-task's scope:

1. `spaces/.../memory/project_iter_5_prep_notes_2026-05-21.md` (8 insights + 5 deferred candidates + recommended phase ordering)
2. `spaces/.../memory/feedback_user_intent_locks_deck_shape_not_corpus_optimum.md` (architectural rule for theme profile + cascade)
3. `spaces/.../memory/feedback_mana_base_serves_spells_not_reverse.md` (architectural rule for aggressive reconciliation)
4. `spaces/.../memory/project_mega_task_v3_shipped_2026-05-21.md` (v3 ship state, per-set automation operational)
5. `repo/api/engine/data/agent/pillar_d_iteration_4_validation_report.md` (iter 4 metrics + hand-off section)
6. `repo/api/engine/data/agent/mega_task_v3_final_report.md` (v3 final report)
7. `repo/api/engine/data/primitives/ontology_v0.md` (current Pillar C ontology, to expand to v1)
8. `spaces/.../memory/project_5_pillar_forward_plan.md` (overall roadmap)

---

## Substrate state

Mega-task v3 shipped 2026-05-21 (final commit `f87486ac7`, 12 commits on top of v2's `4c9ad43d9`). Per-set new-card automation pipeline operational end-to-end. Test baselines: pytest 1283 / vitest 711. API spend headroom: ~$0.05 / $100 used in v3.

**Iter 4 baseline you must not regress** (10/10 criteria pass under revised targets):

- iter1 structural pass: 5/5
- mean creativity_delta: 37.8
- mean novel_combo: 5.2
- mean cost: $0.31
- mean wallclock: 129.3s (target after this mega-task: ≤110s)
- Hellkite Charger absent on Ur-Dragon
- voyage_semantic_avg: 1.8 (target after this mega-task: ≥4)
- pillar_c_coverage: 83.8% on cards-with-abilities (target after this mega-task: ≥90% on full corpus including LLM extractor for ambiguous cards)
- pillar_f_ordering preserved
- atraxa_archetype = counters_matter

**Architectural assets shipped through iter 4:**

- Voyage embedding index (30,395 vectors, ~50ms queries) — extend with rules + rulings in Phase 4
- Pillar C primitive extractor v0 (64 tags, 50/50 golden tests, 83.8% coverage) — expand to v1 in Phase 3
- Pillar E v0.1 mana base optimizer + v0.2 card advantage optimizer — refine in Phases 7-9
- Pillar F v0.1 statistical approximator (18 primitive-grounded win-paths) — consume ontology v1 + rules embedding in Phase 4
- B2 intent interpreter (single archetype label) — upgrade to structured weighted theme profile in Phase 5
- Outer-chain parallelization (C2.1 || C2.2) — ~22s/case saved, verified
- Per-set automation pipeline operational (Scryfall watcher + atomic ingestion + extraction + report writer)

**File layout iter 5 will work in:**

- `repo/api/engine/layers/agent_build_deck_v1.py` — main agent endpoint, B2/C2.1/C2.2/D2 implementations
- `repo/api/engine/layers/agent_llm_client_v1.py` — LLM client wrapper
- `repo/api/engine/layers/agent_wide_candidate_pool_v1.py` — wide pool for C2.2
- `repo/api/engine/layers/agent_semantic_retrieval_v1.py` — Voyage queries
- `repo/api/engine/layers/agent_c22_prompt_templates_v1.py` — per-archetype C2.2 prompts (to upgrade to weighted multi-archetype)
- `repo/api/engine/layers/mana_base_optimizer_v1.py` — Pillar E v0.1
- `repo/api/engine/layers/card_advantage_optimizer_v1.py` — Pillar E v0.2
- `repo/api/engine/layers/agent_statistical_approximator_v1.py` — Pillar F v0.1
- `repo/api/engine/extractors/primitive_extractor_v1.py` — Pillar C extractor (to upgrade)
- `repo/api/engine/data/primitives/ontology_v0.md` — Pillar C ontology spec (to expand to v1)
- `repo/api/engine/data/embeddings/card_embeddings_v1.sqlite` — Voyage index (to extend with rules + rulings)
- `repo/api/engine/data/combos/combo_brackets_v1.json` — Spellbook registry (read-only; new sources go to additive files)

---

## Authority and scope

You are AUTHORIZED to:

- Run all 14 phases (0-13) autonomously without halting for user direction except on the hard halt conditions below.
- Self-correct using the tiered escalation when a validation gate fails.
- Make atomic commits per phase: `git commit -m "Phase X (mega-task v4): <description>"`.
- Modify any file in `repo/api/engine/layers/`, `repo/api/engine/data/`, `repo/api/engine/extractors/`, `repo/api/engine/integrations/`, `repo/ui_harness/src/`, `repo/tests/`, `repo/tools/`, `repo/requirements.txt`.
- Add new dependencies via pip if needed (nothing exotic expected — voyage + anthropic + standard libs).
- Read and write Cowork memory at `spaces/.../memory/` for material findings only.
- Use the mtg-engine MCP for endpoint calls.
- Use the obsidian MCP for DECK_LIBRARY entries if relevant.
- Use chrome-devtools-mcp for UI verification where Phase 13's validation sweep surfaces UI-visible behavior.

You are NOT authorized to:

- Upgrade the Anthropic model from Sonnet 4.6 to Opus. Iter 4 didn't ceiling on creativity; Sonnet remains the right choice. Save Opus for iter 6+ if iter 5 reveals a genuine creativity ceiling.
- Modify Pillar A endpoints (`/deck/analyze_v1`, `/card/search_v1`, `/deck/candidate_pool_v1`, etc.). All changes live in the agent layer + Pillar C/E/F extensions.
- Modify iter 1-4 baseline test cases in ways that change their behavior.
- Roll back any commit. Forward fixes only.
- Touch the Phase 5b MPA substrate (`mpa_*.py`).
- Modify `combo_brackets_v1.json` directly. Phase 12 may APPEND to a new additive file `combo_brackets_v1_external_sources.json`.
- Touch the per-set automation pipeline from v3 unless a phase here specifically requires it.
- Add web-fetching beyond Anthropic SDK + Voyage embeddings API + Scryfall API (for any new rulings data).

---

## Hard halt conditions (NARROW — halt only on these)

1. **Validation gate fails 3 times in a row** in any single phase after exhausting tiered self-correction. Write current state + diagnosis to progress log and halt.
2. **Critical regression**: any iter 1-4 success criterion (under revised targets) breaks at any phase boundary. Halt immediately with diff that broke it.
3. **Resource exhaustion**: API spend reaches $100 (v4 ceiling) or disk usage exceeds 95% on E: drive. Graceful checkpoint and halt.
4. **Architectural contradiction**: a phase spec turns out to be impossible to implement without contradicting a prior phase's output. Write the contradiction inline in the progress log with both sides and halt.
5. **Phase 13 final validation fails on >= 3 of 12 success criteria.** Halt; don't proceed to Phase 14 final regression on a broken iter 5.
6. **Cumulative test suite regression** at any phase: pytest baseline drops below 1283 (mega-task v3 final) OR vitest baseline drops below 711 + new tests this mega-task adds. Halt and diagnose.

You do NOT halt for:

- Single test failures you can fix on the next attempt (self-correct).
- Minor metric drifts (e.g., creativity_delta dropping from 37.8 to 35 — still above 30 baseline target).
- Token budget overruns on a single call if the next call fixes it.
- Linting warnings, deprecation notices, unrelated test flakes.
- Implementation choices with multiple valid paths (pick one, document in progress log, continue).
- UI cosmetic issues that don't break functionality.
- Voyage / Anthropic API transient errors — retry with exponential backoff up to 3 times.

---

## Self-correction protocol (tiered escalation)

**Tier 1** — Re-read the phase spec in this kickoff, re-read relevant memory + iter 4 final report. Try alternate implementation path. Up to 3 attempts.

**Tier 2** — Search the codebase for similar patterns already shipped. Adapt existing patterns. Up to 2 attempts.

**Tier 3** — Add known-gap note to progress log; skip remaining work for the phase and continue. Only allowed for non-blocking phases.

**Tier 4** — Halt for user direction.

**Blocking phases that cannot Tier-3-skip** (Tier 4 halt if they fail):

- Phase 1 (semantic-neighbor score boost) — directly attacks iter 4's voyage_semantic_avg gap
- Phase 2 (C2.1 prompt trim) — directly attacks iter 4's wallclock gap
- Phase 3 (Pillar C ontology v1 + LLM extractor) — foundation for Phase 4 and downstream Pillar F work
- Phase 5 (B2 structured theme profile) — foundation for Phases 6-8
- Phase 6 (cascade theme profile through phases) — foundation for Phase 7-8 prompts
- Phase 13 (iter 5 final validation) — must pass before Phase 14
- Phase 14 (final regression + report) — must pass before mega-task is "done"

Phases 4, 7, 8, 9, 10, 11, 12 are non-blocking and can Tier-3-skip if they fail unrecoverably.

---

## Progress log

Write to `repo/api/engine/data/agent/mega_task_v4_progress_log.md` from Phase 0 onward. Same format as v1/v2/v3 — append-only, timestamped sections per phase.

Update at: every commit, every Tier-N self-correction escalation, every halt event, every hour of wall-clock work on long phases (Phase 3 ontology expansion and Phase 5 B2 redesign will be the longest).

---

## Resource budget

- **Total API spend ceiling: $100.** Alarm at $80; hard halt at $100. Expected actual: $30-70.
- **Per-phase rough budget**: $3-15 for development LLM calls; $1.50 for validation sweeps; $0.10-0.30 for ontology v1 LLM-extractor backfill (Phase 3).
- **Wall-clock budget**: aim 12-36 hours of wall-clock. Phase 3 (ontology v1 backfill on 110k cards) is the longest single phase.

---

## Test discipline

Run after EVERY commit:

```bash
cd "E:\MTG Root\mtg-engine\repo"
pytest -q
cd "E:\MTG Root\mtg-engine\repo\ui_harness"
npm test -- --run
```

Both must pass. Baselines: pytest 1283 + v4's new tests; vitest 711 + v4's new tests.

Phase 13 iter 5 validation sweep: 5-case sweep (Edgar / Krenko / Atraxa / Yuriko / Ur-Dragon), capture 12 success criteria.

Phase 14 final regression: full pytest + vitest + 5-case sweep + ontology v1 coverage check + Pillar F primitive-grounded re-validation + Voyage rules-embedding sanity check.

---

## Phases

### Phase 0 — Pre-flight + memory sync

Read the 8 files listed in "What this mega-task delivers" section. Confirm environment:

- `ANTHROPIC_API_KEY` and `VOYAGE_API_KEY` env vars set (test with minimal calls)
- `python --version` returns 3.10+
- `git status` clean
- pytest baseline: 1283 passing
- vitest baseline: 711 passing
- Disk space: > 10GB free on E:
- Required MCPs connected: mtg-engine, obsidian

Create `repo/api/engine/data/agent/mega_task_v4_progress_log.md` with Phase 0 entry. Commit: "Phase 0 (mega-task v4): pre-flight + progress log scaffold".

---

### Phase 1 — Semantic-neighbor score boost + C2.2 prompt engineering (BLOCKING)

Directly attacks iter 4's voyage_semantic_avg = 1.8 vs target ≥5. The candidates ARE in the C2.2 pool (72 per case per Phase 1 of v2); the LLM is under-selecting them.

**Implementation:**

1. In `agent_wide_candidate_pool_v1.py`, add a `source: "semantic_neighbor"` flag on cards that came from Voyage queries (vs corpus baselines or recent-set boosts).
2. Add a deterministic score-boost: cards with `source: semantic_neighbor` get `+0.15` to their pool ranking score (slightly larger than the recent-set boost of +0.10, since semantic neighbors are the iter-4-shipped main creativity unlock).
3. In `agent_build_deck_v1.py`'s C2.2 prompt construction, add explicit instruction:
   ```
   PRIORITY GUIDANCE: candidates marked source: semantic_neighbor are cards Voyage identified as semantically similar to your anchor cards. These are where novel synergies hide that the corpus baselines don't surface. WHEN A SEMANTIC NEIGHBOR FITS COMPARABLY TO A CORPUS STAPLE, PREFER THE SEMANTIC NEIGHBOR. That's where the creativity edge lives.
   ```
4. In the C2.2 output schema, add `is_semantic_neighbor_pick: bool` per `selected_card` so we can measure selection rate.

**Tests:**

- Unit tests for the score boost (cards with `source: semantic_neighbor` rank ahead of equivalent-score corpus staples)
- Pool composition test: verify ranking order with mixed sources
- Integration test that C2.2 prompt contains the priority guidance text

**Smoke test (1-case on Edgar):** voyage_semantic count in final deck should jump from baseline ~1.8 toward ≥3. Cost stays under $0.35. creativity_delta stays ≥35.

**Commit**: "Phase 1 (mega-task v4): semantic-neighbor score boost + C2.2 selection priority".

---

### Phase 2 — C2.1 prompt trim (BLOCKING)

Directly attacks iter 4's wallclock = 129.3s vs target ≤95s. C2.1 is the long pole at ~50s.

**Implementation:**

1. Audit current C2.1 prompt structure in `agent_build_deck_v1.py::_build_candidate_critic_user_prompt`.
2. Compression targets:
   - Reduce per-candidate oracle text from current cap to ~150 chars (most cards' key mechanics fit there)
   - Cut candidate pool size from current 80-120 to 60-80 (still plenty of choice)
   - Trim the bracket-policy-summary section to essentials
   - Move verbose phase explanations to system prompt (cached at model level) rather than user prompt
3. Output schema: unchanged (don't break downstream consumers).
4. Add a `c21_input_tokens` metric in llm_metrics for monitoring.

**Tests:**

- Token-budget unit test: C2.1 user prompt fits in 8k input tokens (down from current ~16k)
- Quality regression test: against the 5 iter-2 test cases, C2.1's picks should stay creativity-comparable (creativity_delta within ±5 of iter-4 baseline)

**Smoke test (1-case on Yuriko):** C2.1 latency drops to ~30-35s (from ~50s). Wallclock drops accordingly. creativity_delta stays ≥35.

**Commit**: "Phase 2 (mega-task v4): C2.1 prompt trim for latency reduction".

---

### Phase 3 — Pillar C ontology v1 + rules-modifier dimension + LLM extractor (BLOCKING)

Combines Insights 1 + 8 from the iter 5 prep notes.

**Implementation:**

1. Expand `repo/api/engine/data/primitives/ontology_v0.md` → `ontology_v1.md`:
   - Keep all 64 v0 tags
   - Add 7th dimension `rules_modifiers` with 15-20 tags: `mandatory-trigger`, `optional-trigger` (may), `targeted`, `untargeted`, `any-target`, `creature-only-target`, `combat-damage-trigger`, `any-damage-trigger`, `your-permanents-only`, `any-permanent`, `cast-trigger`, `etb-trigger-self`, `etb-trigger-any`, `replacement-effect`, `triggered-ability`, `static-ability`, `activated-ability`.
   - Each new tag has: id, definition, extraction_rule (regex), 3+ examples, combos_with cross-references where relevant.
2. Update `primitive_extractor_v1.py` → `primitive_extractor_v2.py`:
   - Load v1 ontology
   - Apply all 80+ tag regexes (64 v0 + 15-20 new)
3. Build LLM-extractor supplement for ambiguous cards. New module `repo/api/engine/extractors/primitive_extractor_llm_v1.py`:
   - For any card where regex extraction returns < 2 primitives (likely ambiguous), call Claude with the card's oracle text + the full ontology spec, ask for additional primitive tags.
   - Cost: ~$0.001 per card LLM call; restrict to ambiguous-only cards (target ~10k cards out of 110k).
4. Backfill: re-run extraction on all 110k cards (regex pass + LLM pass for ambiguous). Update `cards.primitives_json` column.
5. Golden tests: extend `test_primitive_extractor_golden.py` to cover the 17 new rules-modifier tags with 3-5 cards each.

**Tests:**

- 50/50 golden tests still pass for v0 tags
- New 50-card golden subset for rules-modifier tags
- Coverage check: ≥90% of cards-with-abilities have ≥1 primitive (up from 83.8%)
- LLM-extractor smoke: verify it adds primitives for previously-zero-coverage cards

**Smoke test:** Backfill produces ≥90% coverage on cards-with-abilities. Sample 30 random cards manually-curated to have rules-modifiers (e.g., "may" cards) — verify ≥85% of them get the correct rules-modifier tag.

**Commit**: "Phase 3 (mega-task v4): Pillar C ontology v1 + rules-modifier dimension + LLM extractor + 110k-card re-backfill".

---

### Phase 4 — MTG comprehensive rules + Scryfall card rulings embedded into Voyage

Insight 4 from iter 5 prep notes.

**Implementation:**

1. Source data:
   - MTG comprehensive rules: already integrated per memory (in `mtg.sqlite`, 75,835 entries per RULES_HIERARCHY). Pull as text chunks (1 chunk per rule section, e.g., 100.1, 100.2, etc.).
   - Scryfall card rulings: fetch via Scryfall API `/rulings` endpoint per card. Cache locally in `repo/api/engine/data/rulings/scryfall_rulings_v1.json`.
2. Embed both sources into the Voyage index via `agent_semantic_retrieval_v1.py::build_index_supplement()`:
   - Add a `source_type` field on each vector: `"card"` (existing), `"rule"` (new), `"ruling"` (new)
   - Embed rules text chunks + rulings text + their card associations
   - Cost estimate: rules ~5MB text × $0.06/MT voyage-3 = ~$0.30; rulings ~10MB text = ~$0.60. Total ~$1.00.
3. New query function `query_rules(query_text: str, k: int = 5) -> list[dict]` — returns top-k matching rules + rulings sections.
4. Wire into C2.2 wild-combo-discovery: when the LLM proposes a combo, the agent can query rules for "does this combo actually fire?" validation (limit: 1-2 rules queries per build to avoid latency creep).

**Tests:**

- Embedding pipeline tests with mocked Voyage backend
- Integration test: query "may put a token" returns rules sections about optional triggers
- Smoke query: "Hellkite Charger ability and summoning sickness" returns relevant ruling

**Smoke test:** Build Ur-Dragon test case. Verify rules-query fires at least once on a C2.2 combo proposal; verify the query returns relevant text.

**Commit**: "Phase 4 (mega-task v4): MTG rules + Scryfall rulings embedded into Voyage".

---

### Phase 5 — B2 structured weighted theme profile (BLOCKING)

From `feedback_user_intent_locks_deck_shape_not_corpus_optimum.md`.

**Implementation:**

1. Upgrade B2 intent interpreter (`agent_build_deck_v1.py::_run_intent_interpreter`):
   - Replace single archetype label output with structured weighted profile:
     ```json
     {
       "theme_profile": {
         "primary": {"theme": "dragon_tribal", "weight": 0.6},
         "secondary": {"theme": "graveyard_recursion", "weight": 0.3},
         "tertiary": {"theme": "value_engine", "weight": 0.1}
       },
       ...other existing fields
     }
     ```
2. Three operating modes:
   - **Cards-only inference** (theme_hints empty): B2 reads must-includes' primitives + oracle text, clusters them, produces weighted profile from card signals alone.
   - **Hint-led inference** (theme_hints provided): user hints weighted heavily; cards reinforce.
   - **Hybrid**: combine both signals.
   - **Bare commander edge case**: no must-includes + no hints → default to corpus-baseline archetype for the commander, surface as "I'm using the default Edgar Vampire Tribal Aggro style; let me know if you want a different direction" warning.
3. The B2 output's `theme_profile` field becomes the canonical intent signal for all downstream phases.

**Tests:**

- Unit tests for all 3 modes (cards-only, hint-led, hybrid) + bare commander
- Theme profile schema validation
- Inference quality: 5+ test cases with curated must-include sets → verify inferred theme matches human intuition
- Conflict detection: when hints conflict with cards (e.g., "control" hints + aggressive creatures must-includes), B2 surfaces a conflict warning

**Smoke test:** Run all 5 iter-2 test cases. Verify each produces a structured theme profile with sensible weights. Atraxa's profile should heavily weight `counters_matter`.

**Commit**: "Phase 5 (mega-task v4): B2 structured weighted theme profile + cards-only inference mode".

---

### Phase 6 — Cascade theme profile through C2.1, C2.2, D2 (BLOCKING)

Continued from Phase 5.

**Implementation:**

1. All downstream LLM phases (C2.1, C2.2, D2) receive the `theme_profile` in their prompt context.
2. Each prompt gets explicit guidance:
   ```
   USER THEME PROFILE (load-bearing — do not pivot to corpus baseline if it conflicts):
   - Primary theme: {primary.theme} (weight {primary.weight})
   - Secondary theme: {secondary.theme} (weight {secondary.weight})
   - Tertiary theme: {tertiary.theme} (weight {tertiary.weight})

   Honor these themes in your selections. Your job is to MAXIMIZE QUALITY WITHIN THE USER'S DECLARED CONSTRAINTS, not to redirect toward the corpus-optimal archetype for this commander.
   ```
3. C2.2 archetype detection upgrade: instead of picking ONE archetype from 12, produce a WEIGHTED MULTI-ARCHETYPE detection result. Combine per-archetype prompt fragments (truncated/blended) based on weights.

**Tests:**

- Prompt-template snapshot tests for C2.1, C2.2, D2 ensuring theme profile section is present
- Multi-archetype detection unit tests with weighted outputs
- Integration test: theme profile from B2 flows through to D2's rationale rewriting

**Smoke test:** Build a hybrid-theme deck (e.g., Ur-Dragon with explicit "graveyard recursion" hint). Verify the deck composition reflects both dragon tribal + recursion, NOT a pivot to pure dragon-tribal-with-Old-Gnawbone.

**Commit**: "Phase 6 (mega-task v4): cascade theme profile through C2.1 / C2.2 / D2 with weighted multi-archetype".

---

### Phase 7 — Theme-aware Pillar E target counts

From `feedback_user_intent_locks_deck_shape_not_corpus_optimum.md`.

**Implementation:**

1. Pillar E v0.1 mana base optimizer + v0.2 card advantage optimizer take the theme profile as input.
2. Target count adjustment matrix:
   - `storm` / `combo`: lower interaction (~6-8), more rituals/tutors, lower lands (~32-34)
   - `tribal` / `voltron`: standard interaction (~10-12), standard lands (~36-38), higher creature density
   - `control` / `stax`: higher interaction (~12-15), more counterspells/wipes, standard lands
   - `aristocrats`: standard interaction, sac outlets matter more than card advantage
   - `landfall`: higher lands (~38-40), card advantage from landfall triggers
   - `counters_matter`: standard, but Pillar E v0.2 prefers `engine` card advantage (proliferate amplifies)
3. Encode the matrix in a new data file `repo/api/engine/data/structural/theme_target_count_matrix_v1.json`.
4. Pillar E optimizers blend per-theme targets according to theme_profile weights (primary 60% + secondary 30% + tertiary 10%).

**Tests:**

- Matrix loading + theme-blended target computation
- Reference values: a `storm` 100% deck gets ~32 lands; a `tribal` 100% deck gets ~37 lands; a hybrid storm 60% + tribal 40% gets ~34 lands
- Integration test: Pillar E recommendations shift correctly based on theme_profile input

**Smoke test:** Build a storm-leaning deck (theme_profile: storm 60% + value 40%). Verify Pillar E recommends fewer lands (~33) and more rituals than the default ~36.

**Commit**: "Phase 7 (mega-task v4): theme-aware Pillar E target counts via blended matrix".

---

### Phase 8 — User-intent-preservation validation

From `feedback_user_intent_locks_deck_shape_not_corpus_optimum.md`.

**Implementation:**

1. New module `repo/api/engine/layers/agent_intent_preservation_check_v1.py`:
   - Function `check_intent_preservation(theme_profile, final_deck) -> IntentPreservationReport`
   - Classifies the final deck's actual archetype mix by analyzing its primitive composition + theme tags
   - Compares to the user's stated theme_profile
   - Returns `{drift: float, drifted_themes: [...], deck_archetype_mix: {...}}`
2. Runs at the end of the agent build flow, after Pillar E reconciliation.
3. If drift > 0.3 (significant), add a warning to the build response: `{code: "INTENT_DRIFT", message: "Your stated primary theme was X (weight 0.6); the final deck's archetype mix shows Y at 0.8. The agent's selections drifted toward Y — consider whether this is intentional."}`.

**Tests:**

- Unit tests for `check_intent_preservation` with mocked deck compositions
- Drift threshold tuning: a clean tribal deck with tribal theme_profile shows drift ~0.0; a tribal deck that ended up as combo shows drift ~0.7
- Integration test: warning surfaces in build response when drift exceeds threshold

**Smoke test:** Build a deck with theme_profile heavily skewed to one direction; verify drift detection works.

**Commit**: "Phase 8 (mega-task v4): user-intent-preservation validation check with drift warning".

---

### Phase 9 — Aggressive Pillar E v0.1 mana base reconciliation

From `feedback_mana_base_serves_spells_not_reverse.md`.

**Implementation:**

1. In `mana_base_optimizer_v1.py`, change the reconciliation trigger from `>2 discrepancy` to `any discrepancy`.
2. The mana base RECOMPUTES FRESH based on the final spell composition every build, no longer compared to an "initial baseline."
3. LLM critique pass fires whenever there's a mismatch between deterministic recommendation and the agent's actual land selections. The critique can justify the mismatch (e.g., "this is a storm deck with rituals — fewer lands is correct") or propose swaps.
4. Cross-color swaps are NOT blocked by mana base concerns — the mana base adjusts.

**Tests:**

- Unit tests for the recompute-fresh logic
- Integration test: a cross-color swap that would have triggered conservative >2-discrepancy logic now triggers reconciliation correctly
- Sanity: deterministic recommendations match Karsten's formula

**Smoke test:** Build with a cross-color forced swap (Voyage neighbor in different color); verify mana base adjusts to support it.

**Commit**: "Phase 9 (mega-task v4): aggressive Pillar E mana base reconciliation per spell-base-first principle".

---

### Phase 10 — Mana-cost-aware Voyage downgrade pass

Insight 2 from iter 5 prep notes — standalone module without Pillar E v0.3 curve smoother dependency.

**Implementation:**

1. New module `repo/api/engine/layers/agent_voyage_downgrade_pass_v1.py`:
   - Function `find_cheaper_alternatives(card_name, color_identity, bracket) -> list[dict]`
   - Queries Voyage for top-20 semantic neighbors of `card_name`, filters by color_identity subset + `cmc < anchor.cmc`, returns candidates with rationale.
2. Integration into agent build flow: when bracket is B4/B5 OR theme_profile includes `storm`/`combo`/`tempo`, run downgrade pass for must-includes + key staples.
3. Surface results in build response as `cheaper_alternatives_suggested: [{anchor, alternatives: [...]}]` for user review (don't auto-swap — surface as suggestions).

**Tests:**

- Unit tests with mocked Voyage responses
- Cross-color filter test: alternatives respect color identity subset
- CMC filter test: alternatives have cmc < anchor.cmc

**Smoke test:** Build Yuriko B5 cEDH case with a 3-CMC must-include. Verify downgrade-pass surfaces 1+ semantically-similar 0-1-CMC alternatives.

**Commit**: "Phase 10 (mega-task v4): mana-cost-aware Voyage downgrade pass for cEDH/tempo builds".

---

### Phase 11 — Functional diversity prompt-engineering

Insight 3 from iter 5 prep notes.

**Implementation:**

1. In C2.1 and C2.2 prompts, add explicit guidance referencing Pillar E target counts:
   ```
   FUNCTIONAL DIVERSITY GUIDANCE: when selecting cards, respect category target counts from Pillar E:
   - Ramp target: {pillar_e.ramp_target}, currently filled: {pillar_e.ramp_current}
   - Card advantage target: {pillar_e.card_advantage_target}, currently filled: {pillar_e.card_advantage_current}
   - Interaction target: {pillar_e.interaction_target}, currently filled: {pillar_e.interaction_current}

   WITHIN each category, variety is GOOD (multiple ramp pieces with different cost/conditions = correct).
   ACROSS the target counts, don't over-pack (more than target + 2 in any category is a sign of redundant-wasteful).
   ```
2. Pass current category counts (computed from deck so far) into each LLM call's prompt.

**Tests:**

- Prompt snapshot tests including the new guidance
- Integration test: agent builds with high-overlap candidate pools produce deck compositions that hit category targets without overstuffing

**Smoke test:** Build with a candidate pool heavy on ramp options. Verify final deck has ~10 ramp pieces (not 18) — the LLM stopped at the target.

**Commit**: "Phase 11 (mega-task v4): functional diversity prompt-engineering with Pillar E target awareness".

---

### Phase 12 — Additional combo database integration

Insight 5 from iter 5 prep notes.

**Implementation:**

1. Sources to integrate (in priority order, depending on availability):
   - EDHRec's per-commander combo annotations (extract from existing EDHREC scrape per Phase 5a corpus pipeline)
   - cEDH-decklist-database deck-level combo annotations (if accessible)
   - Community-curated lists from MTG Top 8 or similar (if accessible)
2. Extractor per source: parses combo pairs + outcome + bracket classification.
3. Output: `repo/api/engine/data/combos/combo_brackets_v1_external_sources.json` (additive, never modify base registry).
4. Bracket-classification reconciliation: when a combo appears in multiple sources with different bracket opinions, use the highest-priority source's classification (Spellbook > EDHRec > community). Track conflicts in a tracker file.
5. The Pillar D agent reads BOTH `combo_brackets_v1.json` AND `combo_brackets_v1_external_sources.json` at startup, merged with Spellbook entries taking precedence on conflicts.

**Tier-3-skip allowed** if external source extraction proves unreliable (e.g., Cloudflare gating, schema changes).

**Tests:**

- Per-source extractor unit tests with mocked responses
- Merge logic tests with conflicting bracket opinions
- Coverage test: combined combo space grows by ≥500 pairs vs Spellbook-only baseline

**Smoke test:** Verify merged combo list loads correctly. Spot-check 10 newly-added pairs to confirm they're real combos.

**Commit**: "Phase 12 (mega-task v4): additional combo database integration (EDHRec + cEDHdb)".

---

### Phase 13 — Iter 5 final validation sweep + report (BLOCKING)

**Capture per case (5 iter-2 test cases):**

- All iter 4 metrics (creativity_delta, novel_combo, cost, wallclock, structural pass, envelope)
- Phase 1 metric: voyage_semantic_picked count per build
- Phase 2 metric: C2.1 latency contribution
- Phase 3 metric: primitive coverage on the deck's cards (using ontology v1)
- Phase 5 metric: theme profile structure (verify all 5 cases produce sensible profiles)
- Phase 8 metric: intent_preservation drift score
- Phase 10 metric: downgrade-pass suggestion count

**Iter 5 success criteria (12 total, must hit at least 10):**

1. `iter1_structural_pass_5_of_5`
2. `mean_creativity_delta ≥ 35` (iter 4 baseline: 37.8)
3. `mean_novel_combo ≥ 5` (iter 4 baseline: 5.2)
4. `mean_cost ≤ $0.45` (iter 4 baseline: $0.31)
5. `mean_wallclock ≤ 110s` (iter 4 baseline: 129.3s; expected drop from Phase 2 C2.1 trim)
6. `voyage_semantic_avg ≥ 4` (iter 4 baseline: 1.8; Phase 1 score-boost should close)
7. `pillar_c_coverage_v1 ≥ 90%` on cards-with-abilities (iter 4 baseline: 83.8%)
8. `ur_dragon Hellkite Charger absent`
9. `pillar_f_ordering_sane` (Yuriko > Krenko > Edgar ≈ Ur-Dragon > Atraxa)
10. `theme_profile structured` (all 5 cases produce weighted multi-archetype profile)
11. `intent_preservation_drift mean < 0.3`
12. `combo_space expanded ≥ 500 pairs vs baseline` (Phase 12; allow Tier-3-skip if Phase 12 skipped, in which case this criterion is removed)

Write the report to `repo/api/engine/data/agent/pillar_d_iteration_5_validation_report.md`. Include iter 5 → iter 6 hand-off section.

**Halt condition:** if >= 3 of 12 success criteria fail, halt for user direction.

**Commit**: "Phase 13 (mega-task v4): iter 5 final validation sweep + report".

---

### Phase 14 — Final regression + report + memory update (BLOCKING)

**Run:**

1. Full pytest: `pytest -q` — must pass baseline + v4 new tests.
2. Full vitest: `npm test -- --run` — must pass baseline + v4 new tests.
3. 5-case agent sweep — re-validate iter 5 metrics.
4. Phase 3 primitive extractor v2 coverage check (re-run on sample 100 cards from corpus).
5. Phase 4 Voyage rules-embedding integrity check (sample queries return expected sections).
6. Phase 7 theme-aware Pillar E target-count matrix smoke (different themes → different targets).
7. Phase 8 intent-preservation validation smoke.
8. Phase 12 combo-DB merged-list integrity check.

**Write the final report** to `repo/api/engine/data/agent/mega_task_v4_final_report.md`.

**Update memory:**

- New memory file `spaces/.../memory/project_mega_task_v4_shipped_<date>.md`
- Update MEMORY.md to add index entry
- Update `project_5_pillar_forward_plan.md` to reflect what shipped

**Commit**: "Phase 14 (mega-task v4): final regression + report + memory update".

---

## Mega-task v4 success criteria

The mega-task is "done" when ALL of these hold:

1. All 14 phases (0-13) committed and Phase 14 final regression passes.
2. Phase 13 sweep meets ≥ 10 of 12 success criteria.
3. pytest + vitest baselines preserved + new tests pass.
4. Pillar C ontology v1 with rules-modifier dimension shipped + LLM-extractor producing ≥90% coverage.
5. MTG rules + Scryfall rulings embedded into Voyage.
6. B2 structured theme profile working in all 3 modes (cards-only / hint-led / hybrid).
7. Theme profile cascades through C2.1/C2.2/D2.
8. Theme-aware Pillar E target counts via matrix.
9. User-intent-preservation validation surfacing drift warnings.
10. Aggressive Pillar E mana base reconciliation working.
11. Mana-cost-aware Voyage downgrade pass operational for cEDH/tempo builds.
12. Total API spend under $100.

---

## What NOT to do

- Don't upgrade to Opus. Stay on Sonnet 4.6 throughout.
- Don't break iter 1-4 baseline test cases. Forward-fix only.
- Don't modify `combo_brackets_v1.json` directly. Append to `combo_brackets_v1_external_sources.json`.
- Don't touch the Phase 5b MPA substrate.
- Don't modify Pillar A endpoints.
- Don't churn memory. Only persist updates for material findings.
- Don't pad. If a phase is simpler than the spec suggests, ship the simpler version and document.
- Don't roll back commits. Forward fixes only.
- Don't disable the v3 per-set automation pipeline scheduled task — it should continue running daily.

---

## Iter 5 → iter 6 hand-off questions (your Phase 14 final report must answer)

1. Did Phase 1 (semantic-neighbor score boost) close the voyage_semantic gap as expected (1.8 → ≥4)? If under-target, what's the next move (deeper prompt-engineering, or score-boost magnitude tuning)?
2. Did Phase 2 (C2.1 prompt trim) close the wallclock gap to ≤110s? If under-target, what's the next bottleneck?
3. Did Phase 3 (ontology v1 + LLM extractor) reach ≥90% coverage? Which categories of cards still under-extract?
4. Sample 3 builds with structured theme profiles (Phase 5). Did the agent honor user intent vs corpus baselines? Quantify drift if any.
5. Did Phase 7 theme-aware Pillar E target counts produce noticeably different deck shapes for different themes? Sample 2 themes side-by-side.
6. Did Phase 8 intent-preservation validation fire on any of the 5 sweep cases? At what drift levels?
7. Did Phase 12 combo-DB integration broaden the combo space meaningfully? How many new pairs surfaced; sample 5 for quality.
8. What's the most plausible iter 6 priority? Options to evaluate:
   - Pillar F v0.2 game engine substrate rebuild (the long multi-week effort)
   - Opus upgrade for B2 + C2.1 + C2.2 (if iter 5 ceiling'd on creativity)
   - Pillar E v0.3 curve smoother + v0.4 interaction designer + v0.5 win-condition coherence checker (continuing Pillar E expansion)
   - Multi-deck cross-pollination + reverse-engineering target decks (per iter 5 prep deferred candidates)
   - Bracket-partitioned corpus (per iter 5 prep deferred candidates)

---

## You are go for launch

Run from Phase 0 to Phase 14 autonomously. Halt only on the narrow hard-halt conditions. Self-correct using the tiered escalation. Commit per phase. Log progress throughout.

When you hit Phase 14's final report, paste the executive summary inline in your response.

Expected total wall-clock: 12-36 hours. Expected total API spend: $30-70 (well under the $100 ceiling).

Begin with Phase 0 pre-flight.
