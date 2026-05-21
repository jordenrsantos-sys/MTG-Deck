# Mega-task: Pillar D Iteration 3 + Pillar E v0.1 + Pillar C ontology + Pillar F v0.1 scaffolding + Track 5 new-set pipeline

This is a single self-contained kickoff that authorizes you to run autonomously from start to finish without further user interaction unless a hard halt condition (listed below) triggers. Self-correct on findings using the tiered escalation protocol. Atomic commit per phase. Maintain a running progress log throughout. Cross-phase regression check at every phase boundary.

---

## Substrate state

Pillar D iteration 2 shipped 2026-05-20 (commits 29a60e5c1 -> 2ee152c9f on top of iter 1's 6058c619c). 146 iter-2 + Pillar-D tests pass; iter-1's 67-test suite preserved. Fallback path (`MTG_ENGINE_DISABLE_LLM=1` or missing API key) confirmed to keep iter-1 behavior intact.

Iter 2 validation report at `repo/api/engine/data/agent/pillar_d_iteration_2_validation_report.md`. Read this at start.

**Iter 2 metrics (baseline you must not regress):**

- creativity_delta_count: 36.8 mean (target was >= 8 — 4.5x over)
- novel_combo_count: 6.0 mean / 30 total / >= 1 per deck
- mean_cost_usd: $0.278 (target was <= $0.50)
- mean_wallclock_s: 192.4s (TARGET WAS <= 45s — this is the only failing criterion)
- per-card rationales: substantively deck-aware, reads as player notes
- Ur-Dragon envelope held by 1 card (Hellkite Charger absent from deterministic pool; Old Gnawbone applied as swap by C2.2) — needs design-level guard

**Per-LLM-call decomposition (averaged):**

- B2 intent interpreter: ~26s / ~$0.020
- C2.1 candidate critic: ~51s / ~$0.060
- C2.2 wild combo discovery: ~21s / ~$0.115 (at 35k input token ceiling)
- D2 final critic + rationale rewrite: ~91s / ~$0.080 (the latency bottleneck — single-call rewrite of ~95 cards, 4200+ output tokens)

**File layout iter 3 will work in:**

- `repo/api/engine/layers/agent_build_deck_v1.py` — the agent endpoint
- `repo/api/engine/layers/agent_llm_client_v1.py` — LLM client wrapper
- `repo/api/engine/layers/agent_wide_candidate_pool_v1.py` — wide pool for C2.2
- `repo/api/engine/data/agent/` — reports, progress logs
- `repo/api/engine/data/combos/combo_brackets_v1.json` — combo registry
- `repo/api/engine/data/playtest/opposition_decks_v1.json` — for Pillar F integration
- `repo/ui_harness/src/views/AIBuildView.tsx` — UI surface
- New files iter 3 will create: see per-phase specs below

---

## Authority and scope

You are AUTHORIZED to:

- Run all 15 phases autonomously without halting for user direction except on the hard halt conditions below.
- Self-correct using the tiered escalation when a validation gate fails.
- Make atomic commits per phase with `git commit -m "Phase X (mega-task v1): <description>"`.
- Modify any file in `repo/api/engine/layers/`, `repo/api/engine/data/`, `repo/ui_harness/src/`, `repo/tests/`, `repo/tools/`, `repo/requirements.txt`.
- Add new dependencies via pip if needed (anthropic SDK already wired from iter 2; sqlite-vec or FAISS for Phase 7; nothing else exotic expected).
- Read and write memory at the spaces directory `C:\Users\jorde\AppData\Roaming\Claude\local-agent-mode-sessions\9f2d68e4-6579-41dd-a8ca-3462c3f52398\a461a706-2a03-44fd-8292-3267addb5d29\spaces\d463abef-278c-4a7e-b5e3-34c83dad7ccc\memory\` if and only if you need to log a new finding worth persisting across sessions (don't churn — only persist material findings).
- Use the chrome-devtools-mcp for UI verification in Phase 9 and Phase 14.
- Use the mtg-engine MCP for endpoint calls during validation.
- Use the obsidian MCP for DECK_LIBRARY entries if Phase 14 validation suggests it.

You are NOT authorized to:

- Upgrade the Anthropic model from Sonnet 4.6 to Opus 4.6 or 4.7. Iter 2's rationale quality is already excellent; cost+latency are the bottlenecks. Save Opus for iter 4 if iter 3 ceilings.
- Modify Pillar A endpoints (`/deck/analyze_v1`, `/card/search_v1`, `/deck/candidate_pool_v1`, `/deck/strength_check_v1`, `/agent/context_bundle_v1`, etc.). All LLM-driven changes live in the agent layer.
- Modify iter 1's deterministic skeleton in ways that change behavior on iter-1 test cases.
- Roll back any commit. Forward fixes only.
- Touch the Phase 5b MPA substrate (mpa_actions.py / mpa_policy.py / mpa_runner.py). Phase 5b architecture is substrate-blocked; Pillar F v0.1 scaffolding in this mega-task is a SEPARATE statistical layer that does NOT depend on or interact with the MPA.
- Modify or extend `combo_brackets_v1.json`. It's the read-only registry; use it for lookups only.
- Add web-fetching or external API calls beyond the Anthropic SDK and (optionally for Phase 7) the Anthropic or Voyage embeddings API.

---

## Hard halt conditions (NARROW — halt only on these)

1. **Validation gate fails 3 times in a row in any single phase** after exhausting tiered self-correction. Write current state + diagnosis to progress log and halt.
2. **Critical regression**: any iter-1 or iter-2 auto-pass success criterion breaks at any phase boundary. Halt immediately with diff that broke it.
3. **API key invalid or rate-limited for >10 min sustained**. Write status + last-completed phase + cost-to-date to progress log and halt.
4. **Resource exhaustion**: API spend reaches $200 or disk usage exceeds 95% on E: drive. Graceful checkpoint and halt with status.
5. **Architectural contradiction**: a phase spec turns out to be impossible to implement without contradicting a prior phase's output. Write the contradiction inline in the progress log with both sides and halt.
6. **Phase 9 iter 3 final validation fails on >= 2 of 6 criteria** (creativity_delta_count, novel_combo_count, wallclock, cost, envelope guard, structural pass). Halt; don't proceed to Pillars E/C/F on broken creativity layer.
7. **Cumulative test suite regression** at any phase: pytest baseline drops below 922 OR vitest baseline drops below 633 + the new tests this mega-task adds. Halt and diagnose.

You do NOT halt for:

- Single test failures that you can fix on the next attempt (self-correct).
- Minor metric regressions (e.g., creativity_delta drops from 36.8 to 32 — still well above iter 3's target of >= 30).
- Token budget overruns on a single call if the next call fixes it.
- Linting warnings, deprecation notices, unrelated test flakes.
- Implementation choices that have multiple valid paths (pick one, document in progress log, continue).
- UI cosmetic issues that don't break functionality.

---

## Self-correction protocol (tiered escalation)

When a validation gate fails or implementation hits an error:

**Tier 1** — Re-read the phase spec in this kickoff, re-read relevant memory at `spaces/.../memory/MEMORY.md` and any indexed files it points to. Try an alternate implementation path that still satisfies the success criterion. Up to 3 attempts.

**Tier 2** — Search the codebase for similar patterns already shipped (`grep -r` or Glob). Adapt the existing pattern to the current phase. Up to 2 attempts.

**Tier 3** — Add a known-gap note to the progress log explaining what failed, what you tried, and the impact on downstream phases. Skip the remaining work for this phase and continue to the next phase. Only allowed for non-blocking phases (see "blocking phases" list below).

**Tier 4** — Halt for user direction (only if Tier 3 isn't allowed for this phase).

**Blocking phases that cannot Tier-3-skip** (Tier 4 halt if they fail):

- Phase 1 (D2 prompt cap) — downstream phases depend on D2 not being the bottleneck
- Phase 2 (B2 combo-anchor hard guard) — this is the architectural fix for the envelope leak; cannot ship iter 3 without it
- Phase 3 (D2 batched rewrites) — required to hit wallclock target
- Phase 9 (iter 3 final validation) — must pass before starting Pillars E/C/F
- Phase 14 (final regression sweep) — must pass before mega-task is "done"

Phases 4, 5, 6, 7, 8, 10, 11, 12, 13 are non-blocking and can Tier-3-skip if they fail unrecoverably.

---

## Progress log

Write to `repo/api/engine/data/agent/mega_task_v1_progress_log.md` from Phase 0 onward. Format: append-only, timestamped sections.

```
## Phase X — [phase name] — [STARTED|IN_PROGRESS|COMPLETED|HALTED]
- timestamp: YYYY-MM-DD HH:MM
- commit: <sha>
- cost_to_date: $X.XX
- tests: pytest <N>/<922+delta> pass, vitest <M>/<633+delta> pass
- self-correction events: [list of Tier-N escalations and outcomes]
- key findings: [anything material for iter 4 planning or for memory]
- next phase: Phase Y
```

Update at: every commit, every Tier-N self-correction escalation, every halt event, every hour of wall-clock work on long phases (e.g., Phase 7 embedding setup).

---

## Resource budget

- **Total API spend ceiling: $200.** Track cumulatively across all anthropic SDK calls (development + validation sweeps). Alarm at $150 (log + continue with extra caution); hard halt at $200.
- **Per-phase rough budget**: $5-15 for code-iteration LLM calls (when CC asks Claude for help mid-phase via Claude in Chrome / Anthropic SDK, NOT counted — only the build endpoint LLM calls count); $0.30 for 1-case smoke test (one build); $1.40 for 5-case validation sweep.
- **Wall-clock budget**: aim to complete all 15 phases within 24-72 hours of wall-clock. If running over 72h with significant work remaining, log status and continue (no halt for time alone unless cost ceiling hits).

---

## Test discipline

Run after EVERY commit:

```bash
cd "E:\MTG Root\mtg-engine\repo"
pytest -q
cd "E:\MTG Root\mtg-engine\repo\ui_harness"
npm test -- --run
```

Both must pass. Baselines: pytest 922 + new tests this mega-task adds; vitest 633 + new tests this mega-task adds. Any commit that drops a baseline is reverted and Tier-1-retried.

Iter 3 validation sweep (Phase 9): run all 5 iter-2 test cases through the agent. Capture per-case metrics + the 6 success criteria in `repo/api/engine/data/agent/pillar_d_iteration_3_validation_report.md`.

Final regression sweep (Phase 14): full pytest + vitest + 5-case agent sweep + Pillar E mana base smoke + Pillar F approximator smoke + Pillar C ontology spec consistency check + Track 5 stub run.

---

## Phases

### Phase 0 — Pre-flight + memory sync

Read these files first:

1. `repo/api/engine/data/agent/pillar_d_iteration_2_validation_report.md` (iter 2 baseline)
2. `spaces/.../memory/project_5_pillar_forward_plan.md` (the forward plan — your master spec)
3. `spaces/.../memory/project_pillar_d_iteration_2_shipped_2026-05-20.md` (iter 2 ship state)
4. `repo/api/engine/layers/agent_build_deck_v1.py` (current agent)
5. `repo/api/engine/layers/agent_llm_client_v1.py` (LLM client)
6. `repo/api/engine/data/combos/combo_brackets_v1.json` (combo registry — needed for Phase 2)

Confirm environment:

- `ANTHROPIC_API_KEY` env var set (test with a minimal Anthropic call; cost ~$0.001)
- `python --version` returns 3.11+
- `git status` clean (no uncommitted changes blocking)
- pytest baseline: `pytest -q` shows 922 passing (or whatever the actual baseline is — record it)
- vitest baseline: `npm test -- --run` shows 633 passing (record actual)
- Disk space: > 10GB free on E:

Create the progress log file at `repo/api/engine/data/agent/mega_task_v1_progress_log.md` with the Phase 0 entry. Commit with message "Phase 0 (mega-task v1): pre-flight + progress log scaffold".

**Success gate**: All read-files succeed, env confirmed, baselines recorded, progress log committed.

---

### Phase 1 — D2 prompt cap to 30 rewrites (BLOCKING)

D2 currently rewrites ~95 cards in a single call. Cap to 30 priority cards.

**Implementation:**

In `agent_build_deck_v1.py` (or wherever D2 is implemented), in the D2 final-critic prompt:

1. Select 30 priority cards from the final deck for rationale rewriting. Priority order:
   - All must-include cards (typically 2-5)
   - All commander cards (1-2)
   - All cards flagged as `creative_outlier=True` from C2.1 (typically 0-3)
   - All cards involved in `novel_combo_flags` (typically 5-10)
   - Fill remaining slots up to 30 with highest-corpus-delta cards (cards furthest from corpus top-30 staples)
2. Pass only these 30 cards to D2 for rewriting. The other ~65 cards keep their iter-2 rationales (which were already substantive — D2 was just over-doing it).
3. D2 output JSON shape unchanged; just fewer entries.

**Smoke test (1-case on Edgar B3 vampire tribal):**

- Wallclock drops to <= 150s (rough target; Phase 3 will close the rest)
- Cost stays under $0.30 per build
- creativity_delta_count stays >= 30 (was 35 for Edgar; allow some drop)
- novel_combo_count stays >= 4 (was 6 for Edgar; allow some drop)
- 100-card deck still valid

**Commit**: "Phase 1 (mega-task v1): D2 prompt cap to 30 priority cards".

---

### Phase 2 — B2 combo-anchor hard guard (BLOCKING)

Implement the architectural envelope guard.

**Rule:** For each must-include card the user provided, check `combo_brackets_v1.json` for any combo pair where that card is one of the two anchors. If found, add the OTHER anchor to a `combo_completion_forbidden` set. No LLM phase (B2 / C2.1 / C2.2 / D2) may suggest any card in that set, UNLESS the user also listed it as a must-include.

**Implementation:**

1. New module `repo/api/engine/layers/agent_combo_anchor_guard_v1.py`:
   - Function `build_forbidden_set(must_includes: list[str], combo_registry: dict) -> set[str]`
   - Returns the set of cards forbidden by the rule above
2. In `agent_build_deck_v1.py`, call this at the top of `build_deck()` after must-includes are validated. Pass `forbidden_set` into each LLM phase's context.
3. In each LLM phase's prompt, add an instruction:
   ```
   FORBIDDEN cards (DO NOT suggest under any circumstances): {forbidden_set}
   These cards would complete a combo with the user's must-includes. The user did not list them, which signals they want the must-include card for non-combo reasons. Respect this.
   ```
4. After each LLM phase, validate the output: any card in `forbidden_set` that the LLM suggested gets dropped + logged as a guard-fire event.

**Tests:**

- Unit tests for `build_forbidden_set`: cover 5+ commander/must-include combinations including Ur-Dragon + Tiamat (should forbid Old Gnawbone, Hellkite Charger, and any other multi-card-anchor partners).
- Smoke test on Ur-Dragon B3 case: Old Gnawbone AND Hellkite Charger BOTH absent from final deck. Guard-fire events logged.
- Edge case: user lists BOTH anchors of a combo. Forbidden set is empty (user explicitly opted in).

**Smoke test (1-case on Ur-Dragon):**

- Final deck contains 0 of {Old Gnawbone, Hellkite Charger}
- Guard-fire log shows the suggestions were caught and dropped
- creativity_delta_count stays >= 30
- 100-card deck still valid

**Commit**: "Phase 2 (mega-task v1): B2 combo-anchor hard guard for creativity envelope".

---

### Phase 3 — D2 batched rewrites (BLOCKING)

Parallelize D2's 30-card rewrite into 3 concurrent calls of 10 cards each.

**Implementation:**

1. In the D2 phase code, split the 30 priority cards into 3 batches of 10 (or however they distribute — last batch can be smaller).
2. Use `asyncio.gather` (or `concurrent.futures.ThreadPoolExecutor` if the Anthropic SDK is sync) to fire 3 LLM calls in parallel.
3. Merge results back into a single per-card rationale map.
4. Same per-call output schema as iter 2; just 3 smaller calls instead of 1 big one.
5. Each call's prompt is identical except for the specific 10 cards it rewrites.

**Smoke test (1-case on Atraxa B2 — chose Atraxa because it had the longest iter-2 wallclock):**

- Wallclock drops to <= 90s (rough target; Phase 9 sweep will validate <= 60s mean)
- Cost stays under $0.30 per build (parallelization shouldn't increase cost much; output tokens are similar in aggregate)
- All 30 priority cards have rewritten rationales (no batches dropped)
- creativity_delta_count stays >= 30
- 100-card deck still valid

**Self-correction note:** If the 3 parallel calls fight for rate limits, fall back to 2-call parallelism. If still rate-limited, document as a Tier-2 finding in progress log and continue at 2-call.

**Commit**: "Phase 3 (mega-task v1): D2 batched rewrites (3 parallel calls)".

---

### Phase 4 — C2.2 oracle-text trim + pool-size tuning

C2.2 was at 35k input token ceiling. Trim to fit under 30k with margin.

**Implementation:**

1. In the C2.2 wide candidate pool builder (`agent_wide_candidate_pool_v1.py`), trim each card's oracle text to:
   - Type line (full)
   - Mana cost (full)
   - Power/toughness if creature
   - Oracle text trimmed to 300 chars max (cut on sentence boundary; ellipsis if cut)
2. Reduce default pool size from current (likely 400-500) to 250-300 cards.
3. Keep the recent-set boost in mind for Phase 5 (which adds released_at).

**Smoke test (1-case on Atraxa):**

- C2.2 input tokens drop to <= 28k (margin of 7k from 35k ceiling)
- novel_combo_count stays >= 4 (was 8 for Atraxa; some drop allowed but not catastrophic)
- Cost stays under $0.30

**Commit**: "Phase 4 (mega-task v1): C2.2 oracle-text trim + pool-size tuning".

---

### Phase 5 — released_at column + recent-set boost

Add released_at to cards table; recent-set boost in C2.2 scoring.

**Implementation:**

1. New migration `repo/api/engine/db/migrations/00XX_add_released_at_to_cards.sql`:
   ```sql
   ALTER TABLE cards ADD COLUMN released_at TEXT;
   -- backfill from scryfall data: requires re-ingesting or running a one-time update script
   ```
2. Backfill script `repo/tools/backfill_released_at.py`:
   - Reads from the Scryfall bulk data JSON (already in the corpus per Phase 5a)
   - Updates `cards.released_at` for each card_id
   - Atomic (transaction or staged temp table)
3. In `agent_wide_candidate_pool_v1.py`, add a recent-set boost to candidate scoring:
   - If `card.released_at` is within last 24 months (relative to today's date 2026-05-20), add +0.10 to candidate score
   - Cap the boost so a brand-new card doesn't outrank a corpus staple by absurd margins (e.g., final score is `base_score + min(0.10, recent_boost)`)

**Tests:**

- Migration is idempotent (running twice doesn't break)
- Backfill is deterministic on the same input
- A known recent card (printed within 24 months) gets the boost; a known old card doesn't
- Pool composition shifts to include more recent cards in the smoke test

**Smoke test (1-case on Yuriko B5 tempo — tempo decks benefit most from recent printings):**

- At least 3 cards from the last 24 months appear in C2.2's candidate pool that weren't there in the iter 2 baseline run
- novel_combo_count stays >= 4
- Cost stays under $0.30

**Commit**: "Phase 5 (mega-task v1): released_at column + recent-set boost".

---

### Phase 6 — Per-theme C2.2 prompts

Customize C2.2 wild-combo-discovery prompt per archetype.

**Implementation:**

1. New module `repo/api/engine/layers/agent_c22_prompt_templates_v1.py`:
   - Dictionary of archetype -> prompt template (or fragment)
   - Archetypes: tribal, voltron, storm, aristocrats, control, combo, blink, reanimator, landfall, group_hug, tokens, default (fallback)
   - Each template emphasizes archetype-specific combo patterns:
     - tribal: "look for off-tribe enablers that synergize with the tribe (e.g., creature-type-doubling, anthem effects, lord effects)"
     - storm: "look for ritual chains, free spells, cost reduction stacks"
     - aristocrats: "look for sacrifice outlets, death triggers, persist/undying enablers"
     - etc.
2. The archetype is detected by B2's intent interpreter output (already returns `likely_win_condition` and `implicit_themes`). Map these to one of the archetype keys.
3. C2.2 uses the matched template (or default if no match).

**Tests:**

- Unit tests for archetype detection logic
- Smoke test on Krenko (tribal) + Yuriko (tempo, expect default or "ninja_tempo" if added): both get archetype-appropriate prompts visible in logs.

**Smoke test (2 cases — Krenko + Yuriko):**

- novel_combo_count stays >= 4 on each
- Cost stays under $0.30 each
- Different archetypes show visibly different C2.2 suggestion patterns in their output (e.g., Krenko gets tribal-amplification suggestions; Yuriko gets tempo-extension suggestions)

**Commit**: "Phase 6 (mega-task v1): per-theme C2.2 prompts".

---

### Phase 7 — Card-text semantic retrieval

Embed all Commander-legal cards; query semantic neighbors at build-time.

**Implementation:**

1. New module `repo/api/engine/layers/agent_semantic_retrieval_v1.py`:
   - `build_index()`: reads all Commander-legal cards from the cards table, calls Anthropic embeddings API (or Voyage AI if cheaper — pick one and document choice). Stores vectors in `repo/api/engine/data/embeddings/card_embeddings_v1.sqlite` (use sqlite-vec extension).
   - `query_neighbors(card_id_or_text: str, k: int = 50) -> list[card_id]`: returns top-k semantically similar cards.
2. Index is built ONCE during Phase 7 setup (~30k cards * cheap embedding cost = ~$15-30 total one-time spend). Future builds query the index.
3. Wire into:
   - **B2 context**: for each must-include, add top-20 semantic neighbors as "cards the user might also enjoy" context.
   - **C2.2 candidate pool**: for each anchor card in the deck (commander + must-includes + creative_outliers from C2.1), add top-20 semantic neighbors to the candidate pool (filtered by color identity legality).
4. Idempotent index build: if `card_embeddings_v1.sqlite` exists and matches the cards-table row count, skip rebuild.

**Tests:**

- Unit tests for index build + query (use a mock embedding for tests)
- Integration test: query "Sol Ring" should return other fast-mana artifacts in top-20 neighbors
- Index version metadata stored so we can rebuild on schema changes

**Smoke test (1-case on Edgar B3):**

- C2.2 candidate pool includes at least 5 cards that came from semantic retrieval (cards not in the corpus top-300 for the commander+bracket cohort but present as semantic neighbors)
- At least 2 of these semantic-neighbor cards make it into the final deck
- novel_combo_count stays >= 4
- Cost stays under $0.35 (semantic retrieval costs a tiny per-query amount; one-time index build is amortized)

**Commit**: "Phase 7 (mega-task v1): card-text semantic retrieval via embeddings".

---

### Phase 8 — Positional context engineering for C2.1

Smarter C2.1 candidate-evaluation context.

**Implementation:**

1. Before sending the 80-120 candidates to C2.1, for each candidate compute:
   - `interacts_with_in_deck`: list of cards already in the partial deck that this candidate has clear text-level interaction with (use simple heuristics: shared types, shared keywords like "sacrifice"/"ETB"/"die", combo registry lookups)
   - `pairs_with_not_yet_picked`: list of other candidates in the pool that this candidate has interaction with
   - `primitive_tag_hint`: a quick classification using a small prompt (single Haiku call or a hardcoded keyword extractor — pick the cheaper option). Tags: "free-spell", "etb-trigger", "death-trigger", "sac-outlet", "ramp-mana", "ramp-land", "draw-cantrip", "draw-engine", "tutor-narrow", "tutor-broad", "removal-targeted", "removal-mass", "counterspell", "protection", "evasion-granting", "anthem", "token-producer", etc. (Use a tag set that fits Pillar C ontology when Phase 11 ships — for now use a working set of 30-40 tags.)
2. Include these fields per-candidate in the C2.1 prompt.
3. C2.1 prompt updated to: "Use the interacts_with_in_deck and pairs_with_not_yet_picked fields when reasoning about positional value. A card that has no interactions in the deck and pairs with nothing in the pool is a weaker pick than one that interacts with 3 existing cards or pairs with 5 pool cards. Use primitive_tag_hint as a quick filter."

**Tests:**

- Unit tests for `interacts_with_in_deck` (cover sac-outlet + death-trigger detection, ETB + flicker detection)
- Unit tests for `pairs_with_not_yet_picked`
- Unit tests for primitive_tag_hint extractor (cover 10 known cards across tag types)

**Smoke test (1-case on Atraxa):**

- C2.1 output rationales reference at least 1 other card by name in >= 80% of selections (verify by sampling 10 random rationales)
- creativity_delta_count stays >= 30
- Cost stays under $0.35

**Commit**: "Phase 8 (mega-task v1): positional context engineering for C2.1".

---

### Phase 9 — Iter 3 final validation sweep + report (BLOCKING)

Run all 5 iter-2 test cases through the agent with all Phase 1-8 changes integrated.

**Capture per case:**

- Iter1 structural pass (must be 5/5)
- Wall-clock seconds
- Cost USD
- LLM call count (should be 6: B2 + C2.1 + C2.2 + D2 batch 1 + D2 batch 2 + D2 batch 3)
- creativity_delta_count
- novel_combo_count
- Theme coherence
- Must-include count
- 10 random per-card rationale samples (verbatim, for human review)
- Guard-fire events (from Phase 2 combo-anchor guard)
- Semantic-retrieval source counts (how many cards came from semantic neighbors per Phase 7)

**Iter 3 success criteria (must hit all 6):**

1. `iter1_structural_pass_5_of_5` — all 5 decks valid 100-card with must-includes honored
2. `mean_creativity_delta_count >= 30` (regressed from 36.8 baseline; allows 6.8-point drop to accommodate combo-anchor guard reducing some "novelty")
3. `mean_novel_combo_count >= 4` (regressed from 6.0; same rationale)
4. `mean_cost_usd <= $0.40` (raised from $0.278 to accommodate semantic retrieval; still well under iter 2's $0.50 envelope)
5. `mean_wallclock_s <= 60` (the critical fix — was 192.4s, target was always 45s but 60s is the realistic step-down)
6. `ur_dragon_envelope_held_by_design` — guard-fire log shows Hellkite Charger AND/OR Old Gnawbone were suggested by some phase and BLOCKED by the guard. Deck contains 0 of {Hellkite Charger, Old Gnawbone}.

Write the report to `repo/api/engine/data/agent/pillar_d_iteration_3_validation_report.md`. Format matches iter 2's report.

Include an "Iteration 3 -> 4 hand-off" section at the bottom with:

- Which prompts still under-perform (especially flag if rationale quality dropped vs iter 2)
- Whether semantic retrieval moved the needle (if yes, do more of it; if no, why not)
- Whether positional context engineering moved the needle
- Likely iter 4 priorities (Opus upgrade? Pillar C primitive integration? Multi-spine search?)

**Halt condition:** if >= 2 of 6 success criteria fail, halt for user direction. Do NOT proceed to Phase 10. Pillars E/C/F should not build on a broken creativity layer.

**Commit**: "Phase 9 (mega-task v1): iter 3 final validation sweep + report".

---

### Phase 10 — Pillar E v0.1 mana base optimizer + integration

Deterministic optimizer for the mana base. Hybrid architecture: optimizer outputs recommendations + rationale; LLM critiques/overrides.

**Implementation:**

1. New module `repo/api/engine/layers/mana_base_optimizer_v1.py`:
   - Function `compute_mana_base(commander_color_identity: set, nonland_cards: list[Card], bracket: int, archetype_hint: str | None = None) -> ManaBaseRecommendation`
   - Returns a structured recommendation with:
     - `target_land_count`: int (typically 32-38; storm/reanimator lower; landfall higher)
     - `color_source_targets`: dict color_letter -> int (per Karsten's formula: per-CMC color requirements)
     - `tap_land_tolerance`: int (0 for cEDH = bracket 5; 5-10 for B2-B3 casual)
     - `utility_land_budget`: int (fetches + duals + utility lands like Bojuka Bog)
     - `basic_nonbasic_ratio`: float (typically 0.20-0.40 in casual; 0.10-0.25 in cEDH)
     - `rationale`: string (one paragraph explaining the math: "For a 3-color WUB deck with 4.2 average MV and 6 cards requiring BB at CMC 3+, you need ~14 sources of B...")
2. Karsten's color-requirement formula:
   - For each color requirement (e.g., "BB at CMC 4"), look up Karsten's table: `BB at CMC 4 requires ~20 sources of B`
   - Take the MAX across all requirements for a given color
   - Adjust for deck size (Commander is 99 + commander)
   - Reference: https://www.channelfireball.com/article/how-many-colored-mana-sources-do-you-need-to-consistently-cast-your-spells-a-2022-update/ (you don't need to fetch this — encode the table from memory of Karsten's published values; documented values for CMC 1-7 by color requirement count)
3. Encode the Karsten table as a Python constant in the module. Document the source in a comment.
4. Integration with Pillar D:
   - After Phase D2 in the agent build flow, before final validation, run mana base reconciliation:
     - Compute current land/source counts in the proposed deck
     - Compare to optimizer's recommendation
     - If counts are off by > 2 (lands) or > 2 (any color source), call an LLM critique pass: "Optimizer recommends X lands and Y sources of B; deck has X-3 lands and Y-4 B sources. Are these discrepancies justified by archetype (e.g., storm runs more rituals so fewer lands)? If yes, explain. If no, suggest swaps."
     - LLM either justifies or proposes swaps; deterministic enforcer applies swaps.
5. The optimizer is deterministic; the LLM critique only fires on discrepancies.

**Tests:**

- Unit tests for `compute_mana_base`: cover 5+ deck shapes (mono-W, 2-color RG, 3-color WUB, 4-color WUBR, 5-color WUBRG; each at 2-3 bracket levels)
- Reference values: check that a 3-color B3 vampires deck with 4.2 avg MV and BB requirements at CMC 3+ gets recommended ~37 lands with ~14 B sources, ~10 W sources, ~10 R sources (Edgar shape)
- Integration test: agent build on Edgar produces a deck whose actual mana base is within 2 of the optimizer's recommendation (or has LLM critique justifying the discrepancy)

**Smoke test (5-case sweep through agent):**

- All 5 decks have mana bases within 2 of optimizer recommendation OR have logged LLM critique rationale
- Karsten color requirements satisfied on >= 4 of 5 decks
- Zero tap lands on the cEDH case (Yuriko B5)
- creativity_delta + novel_combo + cost + wallclock metrics from Phase 9 do NOT regress (Pillar E is additive — the structural fundamentals add but don't subtract from creativity)

**Commit**: "Phase 10 (mega-task v1): Pillar E v0.1 mana base optimizer + agent integration".

---

### Phase 11 — Pillar C primitive ontology design (no implementation, design only)

Write the ontology spec. No code beyond the spec file.

**Implementation:**

1. New file `repo/api/engine/data/primitives/ontology_v0.md`
2. Cover 6 dimensions:
   - Mana valuation (e.g., free-spell, cost-discount, mana-positive, color-conversion, alternative-cost)
   - Card velocity (cantrip, engine, burst-draw, tutor-narrow, tutor-broad, recursion-graveyard, recursion-exile, impulse-draw)
   - Interaction type (counterspell-hard, counterspell-soft, removal-creature, removal-artifact, removal-enchantment, removal-planeswalker, removal-mass-creatures, removal-mass-board, exile-vs-destroy, bounce, tap-down)
   - Tempo (turn-skip, untap, extra-turn, haste-grant, evasion-grant, vigilance-grant, flash-grant)
   - Combo role (sac-outlet, etb-trigger, death-trigger, attack-trigger, persist-creature, flicker-effect, infinite-mana-source, infinite-mana-sink, infinite-damage-source, infinite-mill-source, infinite-untap-source, doubler-effect, fizzle-prevention)
   - Win-condition role (deck-out, drain-all-life, mass-damage, mill-all, infinite-tokens-with-evasion, voltron-finish, combat-extra-step)
3. Total tag count: aim for 50-80 tags across the 6 dimensions. Don't pad — only include tags that are extractable from card text AND meaningful for combo discovery.
4. For each tag:
   - `id`: kebab-case slug (e.g., `sac-outlet`)
   - `dimension`: one of the 6 above
   - `definition`: one-sentence definition in plain English
   - `extraction_rule`: regex pattern or text pattern that identifies the tag from oracle text. Multiple patterns OK.
   - `examples`: 3+ card names (e.g., "Viscera Seer, Carrion Feeder, Phyrexian Altar")
   - `combos_with`: list of tag IDs that this tag forms combos with (e.g., `sac-outlet` combos with `etb-trigger` -> recursion, `death-trigger` -> aristocrats, `persist-creature` -> infinite-sac-engine)
5. At the end of the spec, an "interaction graph" section listing 20+ canonical primitive pairs that produce known combos (with brief description of the combo line).
6. Validate the ontology against the known Spellbook combos: pick 10 random pairs from `combo_brackets_v1.json` and demonstrate that the ontology can describe each combo via its primitive tags. If any pair doesn't fit, expand the ontology to cover it.

**Tests:**

- Spec consistency check: every tag has all required fields; every tag's `combos_with` references actual tags; no orphan tags (every tag is referenced by at least one example or combos_with link).
- Coverage check: 10 random Spellbook combos are each describable via the ontology (write the descriptions inline in the spec).

**No smoke test** — design-only phase. Spec is the deliverable.

**Commit**: "Phase 11 (mega-task v1): Pillar C primitive ontology v0 design".

---

### Phase 12 — Pillar F v0.1 statistical approximator scaffolding

Stub the statistical deck-strength approximator. Uses primitive tags where possible (from Phase 11 spec); falls back to heuristics elsewhere.

**Implementation:**

1. New module `repo/api/engine/layers/agent_statistical_approximator_v1.py`:
   - Function `approximate_pod_winrate(deck: Deck, opponents: list[Deck]) -> PodWinrateReport`
   - Returns:
     - `pod_winrate`: float 0-1 (probability this deck wins the 4-player pod)
     - `per_opponent_winrate`: dict opponent_name -> float
     - `decomposition`: dict with sub-scores:
       - `speed_score`: avg-turn-to-win for the deck
       - `interaction_density`: count of interaction primitives in deck
       - `resilience_score`: count of protection/recursion primitives
       - `vulnerability_to`: list of opponent strategies this deck is weak against
2. Win-path decomposition: identify the deck's main win-path(s) by looking for primitive patterns:
   - "infinite-mana-source + uncapped-X-cost-spell" -> X-spell win-path, ~turn 5-6
   - "etb-trigger + flicker-effect" -> infinite-ETB win-path, ~turn 4-6
   - "sac-outlet + persist-creature + death-trigger" -> aristocrats win-path, ~turn 5-8
   - "Thoracle in deck + DC or Tainted Pact" -> Thoracle win-path, ~turn 3-4
   - etc. — encode 10+ common win-paths
3. Speed-vs-interaction matchup logic: deck A's speed_score (lower = faster) vs deck B's interaction_density. If A is faster than B's average answer turn, A wins more. If B has more interaction than A has resilience, B disrupts more.
4. Opposition deck set: read `opposition_decks_v1.json` for benchmark opponents. Pod composition for testing: 3 opponents (varied brackets) + the deck being evaluated.
5. STUB AREAS (allowed to be heuristic placeholders for v0.1):
   - Multi-card win-path matching (cover only 2-3-card combos for now; 4+-card chains are too complex for v0.1)
   - Mana stochasticity (assume average opening hand)
   - Mid-game adaptation (assume linear play)
6. Output schema: JSON, suitable for inclusion in build response and DECK_LIBRARY entries.

**Tests:**

- Unit tests for win-path detection (cover 5 known win-paths)
- Unit tests for matchup logic (faster deck vs slower deck; high-interaction deck vs low-resilience deck)
- Smoke test: run on 5 iter-2 sweep decks against opposition_decks_v1 opponents. Sanity checks:
  - cEDH decks (Yuriko B5) should have pod_winrate > 0.30 (1/3 of seats; cEDH against weaker is favored)
  - B2 decks (Atraxa B2) should have pod_winrate < 0.30 against same opponents
  - B3 decks should fall in between
- Output validity: JSON parses; all fields present

**Smoke test:**

- 5-case agent sweep + approximator produces sane decomposition for each deck
- No regressions in iter 3 metrics from Phase 9 (Pillar F is additive — runs after build, doesn't modify deck)

**Commit**: "Phase 12 (mega-task v1): Pillar F v0.1 statistical approximator scaffolding".

---

### Phase 13 — Track 5 new-set pipeline scaffolding

Stub the per-set new-card pipeline.

**Implementation:**

1. New module `repo/tools/new_set_pipeline_v0.py`:
   - Function `ingest_new_cards(new_cards_json: dict)` — takes Scryfall-format card data for a new set
   - Pipeline steps (each as a sub-function, stubs for now):
     - `tag_with_primitives(new_cards)` — calls primitive extractor (stub: returns empty tag set; will be wired when Pillar C extractor lands in iter 4)
     - `score_for_themes(new_cards)` — calls theme classifier (already exists from Phase 2.1a)
     - `update_corpus_metadata(new_cards)` — writes to cards table
     - `update_embedding_index(new_cards)` — calls Phase 7's semantic retrieval module to add new vectors
     - `flag_potential_combo_pairs(new_cards)` — heuristic: any new card with sac-outlet, etb-trigger, infinite-mana primitives is flagged for combo-pair scan against existing cards
2. New file `repo/api/engine/data/scripts/new_set_pipeline_v0.md` with the runbook.
3. Stub tests with a small fake new-set payload (3-5 cards).

**Tests:**

- Pipeline runs without error on stub payload
- Each step is independently testable (mockable)
- New cards make it into the embedding index after running

**No smoke test on agent** — this is infrastructure, not a build-time path.

**Commit**: "Phase 13 (mega-task v1): Track 5 new-set pipeline scaffolding".

---

### Phase 14 — Cross-phase regression + final report (BLOCKING)

Final integration validation.

**Run:**

1. Full pytest: `pytest -q` — must pass at baseline + new tests
2. Full vitest: `npm test -- --run` — must pass at baseline + new tests
3. 5-case agent sweep (same as Phase 9) — all 6 iter 3 success criteria still hold
4. Pillar E mana base smoke (run optimizer on all 5 sweep decks; check Karsten compliance)
5. Pillar F approximator smoke (run on all 5 sweep decks; check output validity + sanity)
6. Pillar C ontology consistency check (re-run the 10-Spellbook-pairs coverage test from Phase 11)
7. Track 5 pipeline smoke (run on stub payload)

**Write the final report** to `repo/api/engine/data/agent/mega_task_v1_final_report.md`. Structure:

- Phase-by-phase status (committed sha, wall-clock, cost, test count delta, self-correction events, key findings)
- Iter 3 final metrics table (matches Phase 9's report)
- Pillar E v0.1 ship state + sample mana bases
- Pillar C ontology spec summary (tag count, dimension coverage, Spellbook coverage rate)
- Pillar F v0.1 sample outputs (per-deck approximator results)
- Track 5 pipeline status
- Total resource consumption (API cost, wall-clock hours, tests added)
- Iter 3 -> 4 hand-off recommendations

**Update memory:**

- Add a new memory file at `spaces/.../memory/project_mega_task_v1_shipped_<date>.md` summarizing what shipped + key findings.
- Update MEMORY.md to add the index entry.

**Commit**: "Phase 14 (mega-task v1): final regression + report + memory update".

---

## Mega-task success criteria

The mega-task is "done" when ALL of these hold:

1. All 14 phases committed and Phase 14 final regression passes
2. Iter 3's 6 success criteria all met in Phase 9 sweep AND re-validated in Phase 14 sweep
3. pytest + vitest baselines preserved + new tests pass
4. Pillar E v0.1 mana base optimizer produces Karsten-compliant recommendations on all 5 sweep decks
5. Pillar C ontology spec written with 50-80 tags + 10-Spellbook-pair coverage demonstrated
6. Pillar F v0.1 approximator produces valid output on all 5 sweep decks with sane decomposition
7. Track 5 pipeline runs on stub payload without error
8. Total API spend under $200
9. Progress log + final report complete + memory updated

---

## What NOT to do

- Don't upgrade to Opus model. Stay on Sonnet 4.6 throughout.
- Don't expand combo chains from must-include anchors (the whole point of Phase 2's combo-anchor guard).
- Don't break iter 1 or iter 2 test cases. Forward-fix only; never modify iter-1/iter-2 baselines.
- Don't touch the Phase 5b MPA substrate (mpa_actions/policy/runner). Pillar F v0.1 is a SEPARATE statistical layer.
- Don't add web-fetching beyond Anthropic SDK + optional Voyage embeddings API.
- Don't modify Pillar A endpoints. All changes happen in the agent layer.
- Don't churn memory. Only persist memory updates for material findings — the final ship state, any surprising architectural discoveries, any new feedback rules learned.
- Don't pad. If Phase X is simpler than the spec suggests, ship the simpler version and document the simplification in the progress log.
- Don't over-engineer Pillar C, F, Track 5. Each is a v0.1 SCAFFOLD, not a production-ready implementation. The follow-up iteration (iter 4 / Pillar C extractor build / Pillar F engine build) will productionize.

---

## Iter 3 -> iter 4 hand-off (your final report must answer)

When you write the Phase 14 final report, answer these questions to seed iter 4 planning:

1. Did Phase 7 semantic retrieval move the needle on creativity_delta or novel_combo_count? If yes, by how much? If no, why not — was it the embedding model, the query strategy, or the integration point?
2. Did Phase 8 positional context engineering improve rationale quality measurably? Sample 10 rationales pre/post Phase 8 and characterize the difference.
3. Did the Phase 2 combo-anchor guard fire on cases beyond Ur-Dragon? Which? Is the rule over-strict (blocking cards the user would have wanted)?
4. Where did wallclock land in Phase 9 sweep? If still > 60s, what's the remaining bottleneck? Is it C2.2 input tokens still hitting ceiling, or D2 still slow despite batching?
5. Did Pillar E's mana base optimizer cause any LLM critique-pass disagreements? Sample 3 critique-pass logs and characterize what the LLM overrode and why.
6. Did Pillar F v0.1's approximator predictions look plausible? Are cEDH decks ranked above casual decks reliably? Are any predictions clearly off?
7. Are the Pillar C ontology tags going to extract reliably from card text? Pick 5 random tags and discuss extraction confidence.
8. What's the most plausible iter 4 priority based on what shipped? Options to evaluate:
   - Opus upgrade for B2 + C2.1 + C2.2 (creativity reasoning)
   - Pillar C primitive extractor build (so Pillar F + future Pillar D iterations can use structured combo space)
   - Pillar E v0.2 (card advantage optimizer) + v0.3 (curve smoother) + v0.4 (interaction designer)
   - Pillar F v0.2 (rules-correct game engine) start

This hand-off section is the user's input for the iter 4 mega-task. Be specific. Quantify where possible.

---

## You are go for launch

Run from Phase 0 to Phase 14 autonomously. Halt only on the narrow hard-halt conditions. Self-correct using the tiered escalation. Commit per phase. Log progress throughout.

When you hit Phase 14's final report, paste the report's executive summary inline in your response to the user.

Expected total wall-clock: 24-72 hours. Expected total API spend: $80-180 (well under the $200 ceiling).

Begin with Phase 0 pre-flight.
