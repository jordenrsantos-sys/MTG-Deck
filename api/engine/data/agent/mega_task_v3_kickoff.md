# Mega-task v3: Per-set new-card automation pipeline

Single self-contained kickoff. You are authorized to run autonomously from start to finish without further user interaction unless a hard halt condition triggers. Self-correct using the tiered escalation. Atomic commit per phase. Maintain a running progress log throughout. Cross-phase regression check at every phase boundary.

---

## What this mega-task builds

A hands-off automation layer that compounds the substrate built in iter 1-4 (Pillar A corpus + Pillar C primitive extractor + Voyage embedding index + Pillar F statistical approximator) into a system that processes new MTG set releases automatically. When a set drops, the pipeline detects it, ingests the new cards, runs primitive extraction + embedding updates + theme classification + statistical scoring, and an LLM agent writes a "what's new in [set]" markdown report to Obsidian — all without user intervention.

This closes the user's vision stated 2026-05-21: *"when new cards come out it should be able to think and find slots to fit those new cards into."* Manual triggering of the existing scaffolded pipeline (Track 5 of iter 3) becomes automatic.

**Architectural shape — hybrid program + LLM agent.** Deterministic program handles the heavy lifting (data pull, extraction, indexing, scoring). LLM agent runs ON TOP of the program's structured output to write the natural-language discovery report. Split labor cleanly: program does fast/cheap/deterministic work; agent does interpretive work where judgment matters.

---

## Substrate state

Mega-task v2 shipped 2026-05-21 (final commit `4c9ad43d9`, 10 commits on top of v1's `2f177ee7a`). 10/10 iter 4 success criteria pass under user-revised targets. Test baselines: pytest 1200 / vitest 711.

Critical dependencies for v3 (all met):

- **Pillar C primitive extractor** (`repo/api/engine/extractors/primitive_extractor_v1.py`) — 64 ontology tags, 50/50 golden tests pass, 110k-card backfill in 36s, 83.8% coverage. Used to tag new cards in Phase 3.
- **Voyage embedding index** (`repo/api/engine/data/embeddings/card_embeddings_v1.sqlite`) — 30,395 vectors at ~50ms query latency. New cards get embedded and appended via Phase 3.
- **Pillar F v0.1 statistical approximator** (`repo/api/engine/layers/agent_statistical_approximator_v1.py`) — 18 primitive-grounded win-paths, sane orderings on 5-case sweep. New cards scored for archetype impact in Phase 5.
- **Track 5 scaffolding** (`repo/tools/new_set_pipeline_v0.py`, `repo/api/engine/data/scripts/new_set_pipeline_v0.md`) — 5-step orchestrator, 3 functional + 2 stubbed steps. Phase 3 fills in the stubs.

Read these files at Phase 0:

1. `repo/api/engine/data/agent/mega_task_v2_final_report.md` (v2 final report)
2. `repo/api/engine/data/agent/pillar_d_iteration_4_validation_report.md` (iter 4 metrics)
3. `repo/api/engine/data/agent/mega_task_v2_progress_log.md`
4. `repo/api/engine/data/primitives/ontology_v0.md` (Pillar C ontology spec)
5. `repo/tools/new_set_pipeline_v0.py` (existing scaffolding)
6. `repo/api/engine/data/scripts/new_set_pipeline_v0.md` (existing runbook)
7. `spaces/.../memory/project_mega_task_v3_per_set_automation_queued.md` (v3 queue spec)
8. `spaces/.../memory/project_mega_task_v2_shipped_2026-05-21.md` (v2 ship state)

---

## Authority and scope

You are AUTHORIZED to:

- Run all 12 phases (0-11) autonomously without halting for user direction except on the hard halt conditions below.
- Self-correct using the tiered escalation when a validation gate fails.
- Make atomic commits per phase: `git commit -m "Phase X (mega-task v3): <description>"`.
- Modify any file in `repo/api/engine/layers/`, `repo/api/engine/data/`, `repo/api/engine/extractors/`, `repo/ui_harness/src/`, `repo/tests/`, `repo/tools/`, `repo/requirements.txt`.
- Add new dependencies via pip if needed (`requests` for Scryfall API if not already present; nothing else exotic expected).
- Read and write Cowork memory at `spaces/.../memory/` for material findings only (don't churn).
- Use the **scheduled-tasks MCP** for Phase 1's watcher.
- Use the **obsidian MCP** for Phase 7's report writing to the `NEW_SETS/` folder.
- Use the **mtg-engine MCP** for endpoint calls during validation.
- Use chrome-devtools-mcp for UI verification if Phase 7 surfaces UI changes.

You are NOT authorized to:

- Upgrade the Anthropic model from Sonnet 4.6 to Opus. Stay on Sonnet for all LLM calls in this mega-task. The discovery report writer's reasoning is well within Sonnet's competence.
- Modify Pillar A endpoints. All v3 work happens in `tools/`, new extractor extensions, and new pipeline modules.
- Modify iter 1-4 baseline test cases in ways that change their behavior.
- Roll back any commit. Forward fixes only.
- Touch the Phase 5b MPA substrate (`mpa_*.py`). Pillar F v0.1 statistical approximator is the relevant layer; MPA is unrelated.
- Modify `combo_brackets_v1.json`. v3 may APPEND to it via the new-combo-pair discovery in Phase 4, but use the additive override-tracker pattern (write to `combo_brackets_v1_set_appended.json` or a similar incremental file, never edit the base registry).
- Add web-fetching beyond Scryfall API + Anthropic SDK + Voyage embeddings API.

---

## Hard halt conditions (NARROW — halt only on these)

1. **Scryfall API unavailable or rate-limited for >30 min sustained.** Scryfall's bulk-data endpoint is the load-bearing data source; without it, the pipeline can't fetch new card data. Write status + last-completed phase + cost-to-date to progress log and halt.
2. **Validation gate fails 3 times in a row** in any single phase after exhausting tiered self-correction. Write current state + diagnosis to progress log and halt.
3. **Critical regression**: any iter 1-4 success criterion (under revised targets) breaks at any phase boundary. Halt immediately with diff that broke it.
4. **Resource exhaustion**: API spend reaches $100 (v3 ceiling) or disk usage exceeds 95% on E: drive. Graceful checkpoint and halt with status.
5. **Architectural contradiction**: a phase spec turns out to be impossible to implement without contradicting a prior phase's output. Write the contradiction inline in the progress log with both sides and halt.
6. **Phase 9 validation harness fails on >= 2 of its sanity checks** on the known-historical-set golden test. Halt; don't proceed to Phase 10 end-to-end smoke on a broken pipeline.
7. **Cumulative test suite regression** at any phase: pytest baseline drops below 1200 (mega-task v2 final) OR vitest baseline drops below 711 + new tests this mega-task adds. Halt and diagnose.

You do NOT halt for:

- Single test failures you can fix on the next attempt (self-correct).
- Scryfall rate limits under 30 min (back off with exponential delay, retry).
- Minor metric drifts that don't break any baseline criterion.
- Implementation choices with multiple valid paths (pick one, document in progress log, continue).
- Scheduled-tasks MCP transient errors — retry with backoff.

---

## Self-correction protocol (tiered escalation)

When a validation gate fails or implementation hits an error:

**Tier 1** — Re-read the phase spec, re-read relevant memory and the iter 4 final report. Try alternate implementation path. Up to 3 attempts.

**Tier 2** — Search the codebase for similar patterns already shipped (`grep -r` or Glob). Adapt existing patterns to the current phase. Up to 2 attempts.

**Tier 3** — Add a known-gap note to progress log explaining what failed, what you tried, impact on downstream phases. Skip remaining work for this phase and continue. Only allowed for non-blocking phases.

**Tier 4** — Halt for user direction.

**Blocking phases that cannot Tier-3-skip** (Tier 4 halt if they fail):

- Phase 1 (set-release watcher) — without it, the pipeline isn't autonomous
- Phase 2 (ingestion) — without it, new card data can't be processed
- Phase 3 (pipeline orchestration upgrade) — fills the stubs the existing scaffolding left; downstream phases depend on this
- Phase 9 (validation harness) — must pass to validate the pipeline produces correct results
- Phase 10 (end-to-end smoke) — must pass to confirm the whole chain works
- Phase 11 (final regression + report) — must pass to declare done

Phases 4, 5, 6, 7, 8 are non-blocking and can Tier-3-skip if they fail unrecoverably.

---

## Progress log

Write to `repo/api/engine/data/agent/mega_task_v3_progress_log.md` from Phase 0 onward. Same format as v1/v2 — append-only, timestamped sections per phase.

Update at: every commit, every Tier-N self-correction escalation, every halt event, every hour of wall-clock work on long phases.

---

## Resource budget

- **Total API spend ceiling: $100.** Alarm at $80; hard halt at $100. Scryfall API is free for non-commercial use (rate-limited; no spend); Voyage embedding cost for new cards is tiny (~$0.027 per ~500-card set release at voyage-3 rates); the main cost is the LLM discovery report writer (Phase 6) running on real or simulated set data during validation/smoke phases.
- **Per-phase rough budget**: $3-10 for development LLM calls; $0.30-1.00 for validation smoke tests; ~$0.10 for each discovery report generation (Phase 6).
- **Wall-clock budget**: aim to complete all 12 phases within 12-36 hours of wall-clock. Phase 9 (validation harness on a known set) is the longest single phase — possibly 2-4 hours depending on the set's card count.

---

## Test discipline

Run after EVERY commit:

```bash
cd "E:\MTG Root\mtg-engine\repo"
pytest -q
cd "E:\MTG Root\mtg-engine\repo\ui_harness"
npm test -- --run
```

Both must pass. Baselines: pytest 1200 + v3's new tests; vitest 711 + v3's new tests. Any commit that drops a baseline is reverted and Tier-1-retried.

Phase 9 golden test: run the pipeline against a known historical set (recommend Modern Horizons 3 or the most recent set the engine doesn't already have processed). Compare extracted primitives + flagged combos + Pillar F scores against expected values written into the test file. Tolerance: 90% match on primitives (regex extraction has inherent fuzziness), 100% match on architectural sanity checks (no errors thrown, all schema fields populated).

Phase 10 end-to-end smoke: simulate a "new set drop" by passing a small synthetic 5-10 card payload to the watcher → ingestion → extraction → embedding → scoring → report writer chain. Verify the report file appears in Obsidian's `NEW_SETS/` folder with all expected sections populated.

Phase 11 final regression: full pytest + vitest + 5-case agent sweep (re-validate iter 4 metrics) + Phase 9 historical-set golden replay + Phase 10 smoke + summary report.

---

## Phases

### Phase 0 — Pre-flight + memory sync

Read the files listed in the substrate state section. Confirm environment:

- `ANTHROPIC_API_KEY` env var set (test with minimal call)
- `VOYAGE_API_KEY` env var set (test with minimal embedding query)
- `python --version` returns 3.10+
- `git status` clean
- pytest baseline: 1200 passing
- vitest baseline: 711 passing
- Disk space: > 10GB free on E:
- scheduled-tasks MCP connected (check `/mcp` if needed)
- obsidian MCP connected
- mtg-engine MCP connected

Create `repo/api/engine/data/agent/mega_task_v3_progress_log.md` with the Phase 0 entry. Commit: "Phase 0 (mega-task v3): pre-flight + progress log scaffold".

**Success gate**: All read-files succeed, env confirmed, baselines recorded, progress log committed.

---

### Phase 1 — Set-release watcher (BLOCKING)

Build the Scryfall API client + scheduled-task that detects new MTG set releases.

**Implementation:**

1. New module `repo/api/engine/integrations/scryfall_sets_watcher_v1.py`:
   - Function `fetch_set_index() -> list[dict]` — calls `https://api.scryfall.com/sets`, returns parsed JSON list of all sets.
   - Function `find_new_sets(known_codes: set[str], today_iso: str) -> list[dict]` — returns sets whose `released_at` is past today AND whose `code` is not in known_codes.
   - Function `load_known_set_codes() -> set[str]` — reads `repo/api/engine/data/scripts/known_set_codes_v1.json` (initialize with all currently-ingested set codes from the cards table).
   - Function `save_known_set_codes(codes: set[str])` — writes the file atomically.
   - Rate limiting: max 1 request/100ms per Scryfall guidelines; use exponential backoff on 429.
2. Initialize `known_set_codes_v1.json` by querying the cards table for `DISTINCT set_code` (or whatever the schema field is called).
3. New CLI tool `repo/tools/check_new_sets.py` — runs `find_new_sets()` and prints any results. Exits 0 if no new sets, 1 if new sets detected.
4. Schedule the check via the scheduled-tasks MCP: create a daily task at 06:00 UTC that runs `python repo/tools/check_new_sets.py` and, on exit 1, triggers Phase 2's ingestion. The task is created with metadata so the system knows it's the mega-task v3 watcher.

**Tests:**

- Unit tests for `find_new_sets` with mocked Scryfall responses (5+ scenarios)
- Idempotency check on `load_known_set_codes` / `save_known_set_codes`
- Integration smoke: call `fetch_set_index()` once against the live Scryfall API; confirm response shape (set_code, released_at, card_count present)
- Test the CLI tool with a mocked known_codes set; verify exit codes

**Smoke test:** Run `python repo/tools/check_new_sets.py` once manually. Should exit 0 (no new sets unless a Scryfall set was released after last ingest).

**Commit**: "Phase 1 (mega-task v3): Scryfall set-release watcher + scheduled daily check".

---

### Phase 2 — Set data ingestion + diff detection (BLOCKING)

Build the ingestion path that, given a new set code, pulls its cards from Scryfall and appends them to the corpus.

**Implementation:**

1. New module `repo/api/engine/integrations/scryfall_set_ingest_v1.py`:
   - Function `fetch_set_cards(set_code: str) -> list[dict]` — calls `https://api.scryfall.com/cards/search?q=set:<code>&unique=cards`, paginates if needed, returns all card data for that set.
   - Function `diff_against_corpus(set_code: str, set_cards: list[dict]) -> dict` — returns `{new_cards: [...], reprints: [...], errata: [...]}` by checking each card's `oracle_id` against the cards table (new = not present; reprint = present with same oracle_text; errata = present with different oracle_text).
   - Function `ingest_new_set(set_code: str)` — orchestrates fetch + diff + insert into cards table + update cards_raw + propagate released_at across snapshots (use the pattern from `tools/backfill_released_at.py`).
2. Atomic snapshot append: ingestion either fully succeeds or fully rolls back. Use a transaction.
3. New CLI tool `repo/tools/ingest_new_set.py <set_code>` — runs `ingest_new_set` for a specific code. Used by Phase 1's watcher trigger and for manual testing.

**Tests:**

- Unit tests for `diff_against_corpus` with mocked cards table state
- Idempotency: ingesting the same set twice produces no changes on the second run
- Integration test with a small fake set payload (5-10 cards); verify all rows appear correctly

**Smoke test:** Run `python repo/tools/ingest_new_set.py blb` (Bloomburrow — likely already ingested; should report 0 new). Verify no errors, no duplicate rows.

**Commit**: "Phase 2 (mega-task v3): Scryfall set ingestion + corpus diff detection".

---

### Phase 3 — Pipeline orchestration upgrade (BLOCKING)

Fill the stubs in `tools/new_set_pipeline_v0.py` and chain everything with error handling.

**Implementation:**

1. Upgrade `tools/new_set_pipeline_v0.py` to `tools/new_set_pipeline_v1.py` (preserve v0 for rollback):
   - `tag_with_primitives(new_cards)` — calls the Pillar C primitive extractor (`api/engine/extractors/primitive_extractor_v1.py::extract_primitives`) on each new card; writes `primitives_json` column. (Stub-replaced — extractor exists from iter 4 Phase 5.)
   - `score_for_themes(new_cards)` — calls the existing theme classifier from Phase 2.1a on each new card; produces theme scores.
   - `update_corpus_metadata(new_cards)` — writes to cards table (already functional from v0).
   - `update_embedding_index(new_cards)` — calls `agent_semantic_retrieval_v1::build_index` in incremental mode to embed and append new cards' vectors. Cost: ~$0.027 per ~500-card set release.
   - `flag_potential_combo_pairs(new_cards)` — already functional from v0; runs primitive-graph traversal against existing cards.
2. Orchestration: chain all 5 steps with error handling, per-step logging to progress log, rollback on any failure (transaction or compensating actions).
3. Idempotency: running the pipeline twice on the same set produces the same final state.

**Tests:**

- Unit tests for each filled-in step (primitive tagging, theme scoring, embedding update)
- Pipeline integration test with a 5-card synthetic payload; verify all 5 steps complete + all expected DB rows written
- Rollback test: introduce a failure in step 4; verify steps 1-3's work is reverted

**Smoke test:** Run the full pipeline on a small synthetic 5-card payload. Verify every card ends up with primitives + theme scores + embedding vector + corpus metadata + potential combo flags.

**Commit**: "Phase 3 (mega-task v3): pipeline orchestration upgrade — stubs filled, full chain working".

---

### Phase 4 — New-combo-pair discovery via primitive interaction graph

Extend the combo-pair flagging to use the primitive interaction graph systematically.

**Implementation:**

1. New module `repo/api/engine/extractors/new_combo_discovery_v1.py`:
   - Function `discover_new_combo_pairs(new_cards: list[dict], existing_cards: list[dict] | None = None) -> list[dict]`
   - For each new card with non-empty primitives, query the existing-cards index for cards whose primitives form known combo patterns (per the ontology's `combos_with` cross-references).
   - Returns list of `{new_card, paired_with, combo_pattern, confidence}` entries.
2. Confidence scoring: 1.0 for primitive pairs that match an exact `combos_with` edge in ontology; 0.7 for pairs that share a primitive cluster but no explicit edge; 0.5 for pairs with single-primitive overlap.
3. Filter output to confidence >= 0.5 to avoid noise.
4. Append discovered pairs to `repo/api/engine/data/combos/combo_brackets_v1_set_appended.json` (new file, additive — never modify `combo_brackets_v1.json`).

**Tests:**

- Unit tests for `discover_new_combo_pairs` with mocked primitive data
- Coverage: test 5+ known combo patterns (sac-outlet + persist, etb-trigger + flicker, infinite-mana-source + uncapped-x-spell, etc.)
- Confidence-scoring sanity checks

**Smoke test:** Run on a synthetic set with 3 cards: one sac-outlet, one persist-creature, one death-trigger-payoff. Should discover at least 2 combo pairs.

**Commit**: "Phase 4 (mega-task v3): new combo-pair discovery via primitive interaction graph".

---

### Phase 5 — Statistical-approximator extension for "new card archetype impact"

Use Pillar F to score each new card's potential impact on existing archetypes.

**Implementation:**

1. New function in `agent_statistical_approximator_v1.py`: `score_card_archetype_impact(new_card: dict, archetypes: list[str] | None = None) -> dict`
   - For each archetype (default: the 12 from `agent_c22_prompt_templates_v1.py`), compute how much the new card would shift the archetype's typical deck composition's pod_winrate if substituted in.
   - Returns `{archetype: {delta: float, fits_role: str, displaces: str | None}}` per archetype.
2. Delta computation: take a reference "typical" deck for each archetype (derived from corpus top-30 staples for that archetype's signature commanders), simulate replacement, compute pod_winrate delta.
3. Sort output by absolute delta; surface top 3 archetypes per card.

**Tests:**

- Unit tests for `score_card_archetype_impact` with mocked archetypes
- Sanity: a counters-matter card scored against archetypes should have highest delta for `counters_matter`
- Edge case: vanilla creatures with no primitives should produce ~zero delta across all archetypes

**Smoke test:** Run on 3 synthetic cards (one sac-outlet, one ramp piece, one wincon-combo enabler). Verify each scores highest for their natural archetype.

**Commit**: "Phase 5 (mega-task v3): Pillar F new-card archetype-impact scoring".

---

### Phase 6 — LLM discovery report writer

The interpretive layer that turns the structured pipeline output into a natural-language report.

**Implementation:**

1. New module `repo/api/engine/layers/new_set_report_writer_v1.py`:
   - Function `write_set_report(set_code: str, set_name: str, ingest_data: dict) -> str` — returns markdown content.
   - Inputs: pipeline output from Phases 2-5 (new cards + primitives + theme scores + combo pairs + archetype impacts).
   - Calls Claude (Sonnet 4.6, via `agent_llm_client_v1`) with a system prompt that establishes the report writer role.
   - User prompt includes all structured data + instruction to write a 5-section report:
     - **Set overview** — N new cards, primitive coverage breakdown by ontology dimension.
     - **Most impactful new cards** — top 5-10 ranked by archetype impact (Phase 5 output) + new combo participation (Phase 4 output).
     - **New combo pairs** — list the top 5-10 newly discovered combo pairs with their cards + outcome.
     - **Archetype winners and losers** — which existing archetypes gain the most from this set; any noteworthy displaced staples.
     - **Suggested deck updates** — for each deck in user's `DECK_LIBRARY` (queried via obsidian MCP), suggest 0-3 candidate swaps based on the new cards' archetype fit. (If no DECK_LIBRARY entries exist, skip this section.)
2. Output schema: structured JSON envelope wrapping the markdown content. Schema validated before write.
3. Cost budget per report: ~$0.10-0.30 (depending on set size and DECK_LIBRARY count). Within v3's budget.

**Tests:**

- Unit tests with mocked LLM responses
- Schema validation tests
- Verify report structure: all 5 sections present, no empty sections (or graceful "no entries" notes)
- Test with small (5-card) and large (300-card) set payloads; verify report quality scales

**Smoke test:** Generate a report for a synthetic 10-card "new set" payload. Read the output; verify it's coherent, identifies the most-impactful cards correctly, doesn't hallucinate cards not in the input.

**Commit**: "Phase 6 (mega-task v3): LLM discovery report writer".

---

### Phase 7 — Obsidian integration (NEW_SETS folder + report writing)

Write the discovery report to Obsidian via the obsidian MCP.

**Implementation:**

1. New module `repo/api/engine/integrations/obsidian_new_set_writer_v1.py`:
   - Function `publish_set_report(set_code: str, report_markdown: str)` — writes to `Mtg deck building brain/NEW_SETS/<YYYY-MM-DD>_<set_code>_<set_name>.md` via the obsidian MCP.
   - Function `update_new_sets_index()` — appends the new report to a `NEW_SETS/_INDEX.md` hub file (creates if missing).
2. Frontmatter on each report: `tags: [new-set, automation]`, `set_code`, `released_at`, `processed_at`.
3. Wikilink the report from `99_META/Home.md` under a "Recent set releases" section (create section if missing).

**Tests:**

- Unit tests with mocked obsidian MCP responses
- Idempotency: writing the same report twice doesn't duplicate
- Integration smoke: write a sample report to a test path; verify it appears in Obsidian

**Smoke test:** Generate a synthetic report and publish it via the obsidian MCP. Verify the file appears at the expected path, frontmatter is correct, _INDEX.md is updated, Home.md has the section.

**Commit**: "Phase 7 (mega-task v3): Obsidian integration — NEW_SETS folder + index + Home.md section".

---

### Phase 8 — Notification integration (optional, Tier-3 skippable)

Optional desktop notification when a new set is processed.

**Implementation:**

1. If running on Windows (check `os.name == 'nt'`), use Windows toast notifications via PowerShell or the `win10toast` library (pip install if needed).
2. Notification content: "MTG set [name] processed — N new cards, top archetype impacts: [list]. Report at NEW_SETS/<file>."
3. Notification fires at the end of Phase 6 (after report is written).
4. Configuration: add `MTG_ENGINE_NOTIFICATIONS_ENABLED` env var; default false. User opts in.

**Tier-3-skip allowed:** if Windows notification setup is finicky, skip and document. Notifications are quality-of-life, not load-bearing.

**Tests:**

- Unit test the notification module with a mocked toast backend
- Manual smoke: trigger a notification with a sample payload; verify it appears on the desktop

**Smoke test:** Run the notification function manually; verify desktop toast appears (or document the skip).

**Commit**: "Phase 8 (mega-task v3): desktop notification integration".

---

### Phase 9 — Validation harness on a known historical set (BLOCKING)

Run the full pipeline against a known set and verify outputs match expected values.

**Implementation:**

1. Pick a historical set the engine has NOT yet processed (or simulate "un-process" by removing one set's data, then re-ingest). Recommended: pick the most recent set in the corpus that has the smallest card count for fast testing.
2. New test file `repo/tests/test_new_set_pipeline_golden.py`:
   - Loads a fixed expected-output JSON: per-card primitive tags, expected combo pairs, expected archetype impact rankings.
   - Runs the full pipeline on the chosen set.
   - Compares actual to expected: 90% match on primitives (regex extraction has inherent fuzziness); 100% match on structural sanity checks; 70% match on combo pair discovery (some pairs are subjective).
3. Build the golden expected-output by hand-curating 30-50 cards from the chosen set: assign expected primitives by reading oracle text against the ontology, identify expected combo pairs against `combo_brackets_v1.json`, score expected archetype impacts.
4. The golden file is checked into git so future re-runs are verifiable.

**Self-correction expectations:** First-pass extractor will miss some tags. Tier 1: refine regex per the failing tags; re-run. Aim for >= 85% gold-test pass.

**Tests:**

- The golden test IS the test
- Plus subsidiary tests: each pipeline step in isolation against the golden input

**Smoke test:** Run the golden test. Pass = ready to proceed; fail = Tier-1 self-correct.

**Commit**: "Phase 9 (mega-task v3): validation harness golden test on historical set".

---

### Phase 10 — End-to-end smoke test (BLOCKING)

Simulate a "new set drop" and watch the entire automation chain fire.

**Implementation:**

1. Synthetic payload: 5-10 fictional cards with known primitive signatures (e.g., a sac-outlet, a persist creature, a counters-matter card, a vanilla creature).
2. Inject into the watcher's "new set detected" path manually (bypass the scheduled trigger; call the ingestion CLI directly).
3. Watch the full chain execute:
   - Phase 2 ingestion
   - Phase 3 pipeline (extract + score + embed + flag combos)
   - Phase 4 combo-pair discovery
   - Phase 5 archetype impact scoring
   - Phase 6 LLM report writing
   - Phase 7 Obsidian publication
   - Phase 8 notification (if enabled)
4. Verify the Obsidian report appears, has expected sections, references the synthetic cards correctly.
5. Cleanup: remove the synthetic set's data so it doesn't pollute the real corpus.

**Tests:**

- The smoke test IS the test; pass = full chain working

**Commit**: "Phase 10 (mega-task v3): end-to-end smoke test on synthetic set".

---

### Phase 11 — Final regression + report + memory update (BLOCKING)

**Run:**

1. Full pytest: `pytest -q` — must pass baseline + v3 new tests.
2. Full vitest: `npm test -- --run` — must pass baseline + new tests.
3. 5-case agent sweep — confirm iter 4 metrics still hold (no regression).
4. Phase 9 golden test replay.
5. Phase 10 end-to-end smoke replay.

**Write the final report** to `repo/api/engine/data/agent/mega_task_v3_final_report.md`. Structure:

- Phase-by-phase status (sha, wall-clock, cost, test count delta, self-correction events, key findings)
- Pipeline status: all 5 orchestration steps functional, automated trigger configured
- Golden test results
- End-to-end smoke results
- Per-set processing cost estimate (extrapolated from smoke test)
- Total resource consumption (API spend, wall-clock, tests added)
- Mega-task v3 → next-iteration hand-off recommendations

**Update memory:**

- New memory file at `spaces/.../memory/project_mega_task_v3_shipped_<date>.md` summarizing what shipped.
- Update MEMORY.md to add the index entry.

**Commit**: "Phase 11 (mega-task v3): final regression + report + memory update".

---

## Mega-task v3 success criteria

The mega-task is "done" when ALL of these hold:

1. All 12 phases (0-11) committed and Phase 11 final regression passes.
2. Scryfall set-release watcher running as scheduled daily task (Phase 1).
3. Pipeline orchestration upgrade fully functional — all 5 steps non-stubbed (Phase 3).
4. Phase 9 golden test passes >= 85% on primitives + 100% on structural sanity.
5. Phase 10 end-to-end smoke produces a complete Obsidian report from a synthetic set payload.
6. pytest + vitest baselines preserved + new tests pass.
7. Total API spend under $100.
8. Iter 4 5-case agent sweep still passes (no regression).
9. Progress log + final report complete + memory updated.

---

## What NOT to do

- Don't upgrade to Opus. Stay on Sonnet for all LLM calls.
- Don't break iter 1-4 baseline test cases. Forward-fix only.
- Don't modify `combo_brackets_v1.json` directly. Append to the new `combo_brackets_v1_set_appended.json` file.
- Don't auto-publish the report to public-facing channels — Obsidian is local-only.
- Don't add web-fetching beyond Scryfall API + Anthropic SDK + Voyage API.
- Don't churn memory. Only persist updates for material findings.
- Don't pad. If a phase is simpler than spec suggests, ship the simpler version and document.
- Don't over-engineer the LLM report writer. v0.1 is a working baseline; iterate later if quality is insufficient.

---

## Mega-task v3 → next-iteration hand-off (your Phase 11 final report must answer)

1. What's the per-set processing cost (extrapolated from Phase 10 smoke)? Is it sustainable for the typical MTG set release cadence (~4-6 sets/year)?
2. Did the golden test reveal any extraction quality issues that should inform iter 5's ontology v1 work?
3. Did the LLM discovery report writer produce coherent reports? Sample 3 reports' executive summaries and characterize quality.
4. Is the combo-pair discovery (Phase 4) producing high-precision results (low false-positive rate) or noisy results?
5. Are the archetype impact scores (Phase 5) plausible? Sample 5 high-impact cards and verify the scored archetypes match human intuition.
6. What's the most plausible next iteration priority? Options:
   - Iter 5 (creativity refinement: C2.1 trim + semantic-neighbor score boost + ontology v1 + rules-modifier dimension + MTG rules embedding + 5 more accumulated architectural refinements per `project_iter_5_prep_notes_2026-05-21.md`)
   - Pillar F v0.2 game engine (the rules-correct multi-week substrate rebuild)
   - User-intent-preservation architecture work (B2 structured theme profile + cascading + Pillar E theme-aware target counts per `feedback_user_intent_locks_deck_shape_not_corpus_optimum.md`)
   - Multi-deck cross-pollination + reverse-engineering target decks (per iter 5 prep deferred candidates)

This hand-off section seeds whatever comes after v3.

---

## You are go for launch

Run from Phase 0 to Phase 11 autonomously. Halt only on the narrow hard-halt conditions. Self-correct using the tiered escalation. Commit per phase. Log progress throughout.

When you hit Phase 11's final report, paste the executive summary inline in your response.

Expected total wall-clock: 12-36 hours. Expected total API spend: $20-50 (well under the $100 ceiling).

Begin with Phase 0 pre-flight.
