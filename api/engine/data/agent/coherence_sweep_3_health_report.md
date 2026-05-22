# Coherence Sweep #3 health report

Project-wide health audit after mega-tasks v1-v5. Sweep started 2026-05-22 on top of `4cee4a287` (mega-task v5 ship). Tested baselines: pytest 1489 / vitest 758.

This report is populated phase-by-phase. The final executive summary + categorized punch list at the top are written during Phase 10 synthesis.

---

## Executive summary (Phase 10)

`<pending — populated during Phase 10>`

---

## Categorized punch list (Phase 10)

### Fixed inline
`<pending>`

### Queued for iter 7 mega-task v6
`<pending>`

### Out-of-scope / wontfix
`<pending>`

---

## Section 1 — Substrate cache audit (Phase 1)

**Verdict: clean.** No caches exceed the 30s cold-start trigger for inline-fix. The only cache that previously did (`deck_strength_check_v1._CORPUS_VECTORS` at ~111 min cold-start) was already fixed in mega-task v5 Phase 5 via the persistent JSON disk cache. All other module-level caches load in under 1.6 seconds.

### Module-level lazy caches (populated on first call)

| Cache | Module | Cold-start | Size | Persistence | Verdict |
|---|---|---|---|---|---|
| `_ONTOLOGY_CACHE` | `extractors/new_combo_discovery_v1.py:88` | 26.7ms | 64 ontology tags + their combos_with edges | none (re-derived from loaded ontology object) | clean |
| `_cache` | `playtest/opposition_decks_v1.py:36` | 1.2ms | 54-entry tiered opposition registry | none (16 KB JSON file is on disk) | clean |
| `_COMBO_BRACKETS_CACHE` | `layers/corpus_batch_ingest_v1.py:32` | 477.6ms | 3679 combo-pair index | none (source `combo_brackets_v1.json` is on disk) | clean |
| `_CURVES_CACHE` | `layers/curve_smoother_v1.py:50` | 1.6ms | 14-archetype curve targets | none (source JSON is on disk) | clean |
| `_CACHE` (Voyage matrix) | `layers/agent_semantic_retrieval_v1.py:54` | 1557.8ms | 30,395-row × 1024-dim float32 matrix (~120 MB in-memory) | none (source `card_embeddings_v1.sqlite` is on disk) | clean — under threshold |
| `_CORPUS_RAW` + `_CORPUS_VECTORS` | `layers/deck_strength_check_v1.py:33-34` | 418.7ms | 13,408 corpus vectors | **disk-persisted** via `corpus_vectors_cache_v1.json` (v5 Phase 5) | clean — already fixed |

### Module-level eager-loaded data (populated at import time)

| Loader | Module | Import cost | Data |
|---|---|---|---|
| `_THEMES + _TYPAL_THEMES + _SIGNAL_VOCAB_BASE + _SIGNAL_VOCAB + _CONFIDENCE_BANDS` | `layers/deck_theme_classifier_v1.py:85-107` | 26.7ms | 41 themes, 137 signals |
| `_PAIR_INDEX + _OUTCOMES` | `layers/combo_enabler_reasons_v1.py:121-122` | 24.5ms | 4,423 pairs, 4,527 outcomes |
| `_OUTCOMES + _BRACKET_PAIR_INDEX` | `layers/deck_combo_insights_v1.py:209-210` | 449.8ms | 4,527 outcomes, 3,679 bracket-pairs |

Full `api.main` import (which transitively imports all of the above plus the entire engine): **897ms** on this box. uvicorn worker boot is dominated by this module-init cost; there is no significant "first-request" cold-start beyond the Voyage matrix (1.5s) and the strength_check vector cache load (~420ms from disk).

### Function-local caches (per-call, intentional)

`proof_scaffold_v1.ruleset_sha_cache` and `proof_attempt_v1.evidence_lookup_cache` are intentional per-call lookup caches scoped to a single endpoint invocation. Not module-level; not in scope for this audit.

### Inline fixes landed

None. No cache exceeded the 30s trigger. The audit verifies the substrate is already in good shape post-mega-task-v5.

### Queued for iter 7 / wontfix

None from Phase 1.


## Section 2 — Cross-pillar integration verification (Phase 2)

**Verdict: clean, with one piece of memory drift queued for inline fix in Phase 3.** All pillars wire end-to-end. The only finding is that `project_pillar_a_c_shipped_2026-05-17` memory claims "9 AI-facing endpoints" — current count is **~42 routes / ~18 v1-tier AI-facing endpoints** after subsequent mega-tasks added agent/v1 endpoints. That's pure memory staleness, not a wiring issue (Phase 3 fixes inline).

### Pillar A — endpoint smoke

`api.main` imports + 42 routes register in 1.0s. Sampled endpoints respond cleanly:

| Endpoint | Status | Latency | Notes |
|---|---|---|---|
| `GET /health` | 200 | 18ms | engine_version 0.2.3, image_cache OK |
| `GET /snapshots/active` | 200 | 3ms | returns `20260217_190902_tagpass_20260222` |
| `GET /snapshots` | 200 | 2ms | snapshot list |
| `GET /playtest/opposition_decks_v1` | 200 | <10ms | **54 entries** (matches Phase 11 expansion) |
| `POST /deck/analyze_v1` | 200 | 29ms | full Edgar B3 minimal-deck analysis |
| `GET /commander/archetype_brief_v1` | 200 | 432ms | uses corpus data |
| `POST /card/search_v1` | 422 | <1ms | validates input schema (test sent wrong fields — proves validation works) |
| `POST /deck/candidate_pool_v1` | 422 | 2ms | input schema validation working |

All routes load + respond. 422 responses are FastAPI schema validation working correctly when the test sent malformed bodies (not endpoint bugs).

### Pillar C — primitive extractor pipeline

Direct DB inspection on the active snapshot:
- Total cards: 36,709
- Cards with `primitives_v1_json` populated (non-empty): **28,666 (78.1%)**
- Sample tags on real cards: `["haste-grant", "tutor-broad"]`, `["etb-trigger"]`, `["death-trigger", "mana-fixing-utility", "optional-trigger", "persist-creature"]`

Pillar C is producing tags. The 78.1% coverage rate is consistent with the iter 5 baseline of 93% on cards-with-abilities (the 78.1% includes lands + basic-aoe cards that intentionally have no primitives).

### Pillar D — agent build_deck_v1

Verified via iter 6 sweep data (Phase 13 — 2 stochastic runs against 5 baseline commanders, 10 builds total). All 4 LLM injection points fired on every case:

- **B2 intent_interpreter**: theme_profile produced for all 5 cases (per Phase 13 report `theme_profile` column)
- **C2.1 candidate_critic**: latency_ms ranged 35,943–43,885 across the 10 builds — non-zero on every case (Phase 8 fix confirmed live; iter-5's Atraxa 0.0s pathology is dead)
- **C2.2 wild_combo_discovery**: archetype detection populated (consumed by Pillar E + curve smoother as archetype_hint)
- **D2 final_critic**: card rationales rewritten on every case (verified via per-case wall_clock pattern, ~30-50s D2 component)

Theme profile cascade through phases verified: B2 produces profile → flows to C2.1/C2.2/D2 system prompts (`forbidden_prompt_block` injection threading) → intent_preservation_drift downstream computes against the same B2 profile. End-to-end confirmed by iter 6 sweep yielding non-zero `intent_drift` for all 5 cases (drift is only computable if the cascade works).

### Pillar E — 4 optimizers

iter 6 sweep confirms presence on 5/5 cases for v0.3 + v0.4 (Phase 13 report `E v0.3 = y` and graduated playtest `GP = y` columns). v0.1 + v0.2 were verified earlier in mega-task v4 / iter 5; their summary fields still appear in the v5 build response.

Live integration smoke (Sol Ring × 99 + Edgar Markov synthetic deck):
- v0.3 `curve_smoother_v1.analyze_curve()` runs and returns `CurveAnalysis` with archetype_target, deck_curve, bricks, holes.
- v0.4 `interaction_designer_v1.compute_interaction_targets()` runs and returns `InteractionTargets` with bracket policy + color-gated allocation.

### Pillar F + Graduated playtest

Direct module smokes:
- `agent_statistical_approximator_v1.approximate_pod_winrate(deck=..., db_snapshot_id=...)` → `PodWinrateReport(pod_winrate=0.087, ...)`. Synthetic deck of 99 Sol Rings yields low winrate as expected (no win-paths armed).
- `agent_graduated_playtest_v1.run_graduated_sweep(deck=..., bracket="B3")` → tier_results=[Tier 0 stalled, advanced=False]. Behavior matches the design (won't advance past Tier 0 without real win-paths).

Both produce valid output on degenerate input; both produced sensible output on real iter 6 builds (verified via Phase 13 sweep).

### Track 5 — per-set automation chain

Verified file presence (not triggered — per kickoff "Don't touch the v3 per-set automation scheduled task"):

- `tools/new_set_pipeline_v0.py` + `v1.py` — entry points
- `api/engine/integrations/new_set_notifier_v1.py` — chain link
- `api/engine/layers/new_set_report_writer_v1.py` — chain link
- `api/engine/data/scripts/known_set_codes_v1.json` — set-tracking state file
- `api/engine/data/scripts/new_set_pipeline_v0.md` + `new_set_watcher_v1.md` — runbooks

Chain intact. The Phase 2 audit cannot verify the *scheduled* invocation (Windows Task Scheduler config is out-of-scope) but the script chain is loadable.

### Inline fixes landed

None inline this phase. The Pillar A endpoint-count drift is recorded; the inline fix lands in Phase 3 (memory↔code alignment) where memory updates are in scope.

### Queued for iter 7 / wontfix

- *Memory drift inline-fix (lands Phase 3)*: `project_pillar_a_c_shipped_2026-05-17.md` says "9 AI-facing endpoints"; actual is ~18 v1-tier endpoints + 24 utility/non-v1. Phase 3 updates the memory entry inline.
- *Out-of-scope*: Scheduled task wiring verification (Windows Task Scheduler) — kickoff explicitly excludes.


## Section 3 — Memory ↔ code alignment (Phase 3)

**Verdict: 1 inline fix landed (Pillar A endpoint count drift); 4 architectural feedback rules all honored by current code; sampled "shipped" memories self-consistent.**

The cowork memory directory has **52 entries** (the kickoff estimated "~17" — that estimate predated the v3-v5 ship phase). 15 are `feedback_*` rules; the rest are project-state snapshots, prep notes, or design specs.

### 4 architectural feedback rules verified honored

| Rule | Memory file | Honored at | Evidence |
|---|---|---|---|
| Pool ranking score does not drive LLM picking | `feedback_pool_score_does_not_drive_llm_picking.md` | `agent_build_deck_v1.py:3657` | Prompt contains explicit "**YOU MUST SELECT AT LEAST 3 SEMANTIC-NEIGHBOR CARDS**" — the prompt-level mechanism replacing the failed score boost. iter 6 sweep showed this still doesn't fully close the gap, which is consistent with the rule's "stronger guidance lifts selection rate; soft guidance + score boost doesn't" — iter 7 priority #1 (semantic-injection GUARANTEE) is the next escalation. |
| User intent locks deck shape | `feedback_user_intent_locks_deck_shape_not_corpus_optimum.md` | `agent_build_deck_v1.py:287, 2235-2422` | B2 intent_interpreter produces `theme_profile` with `primary`/`secondary`/`tertiary`; downstream phases consume it and cascade weights through C2.1/C2.2/D2. Weights MUST sum to 1.0 per the B2 prompt template (line 2295). Pillar E v0.3/v0.4 target counts shift with archetype_hint derived from this profile. |
| Mana base serves spells, not reverse | `feedback_mana_base_serves_spells_not_reverse.md` | `mana_base_optimizer_v1.py:366, 396` | Comment "reconciliation should be AGGRESSIVE: any non-zero discrepancy is reconciled" + `"policy": "aggressive_recompute_fresh"` in the recommendation payload. Mega-task v4 Phase 9 shipped this. |
| Corpus is descriptive not prescriptive | `feedback_corpus_descriptive_not_prescriptive.md` | `agent_build_deck_v1.py creativity_envelope_metrics + staples_avoided` | Build response reports `creativity_delta_count` (cards added beyond corpus baseline) + `staples_avoided_count` (high-frequency cards intentionally excluded). The agent is rewarded for novelty against corpus, not for matching corpus optimum. |

### Inline fix landed: Pillar A endpoint count drift

`project_pillar_a_c_shipped_2026-05-17.md` description claimed "9 endpoints" — accurate at 2026-05-17 ship date but stale by mega-task v5 (now 42 total routes / ~18 v1-tier endpoints). Fix landed inline: updated the description + added an "Update 2026-05-22" paragraph documenting the post-v5 endpoint count + confirmed all original 9 endpoints still ship (no regression).

### Sampled "shipped" memories (self-consistent)

- `project_mega_task_v5_shipped_2026-05-22.md` — written during Phase 14 of v5; metrics match `pillar_d_iteration_6_validation_report.md`.
- `project_mega_task_v4_shipped_2026-05-21.md` — claims iter 5 wallclock 118s / coverage 93%; matches `pillar_d_iteration_5_validation_report.md`.
- `project_iter_6_prep_notes_2026-05-21.md` — Coherence Sweep #3 spec section is the source-of-truth for THIS sweep's 10 audit areas.
- `project_iter_7_prep_notes_2026-05-22.md` — written during Phase 14 of v5; iter 7 priorities #1-#3 match the iter 6 sweep failures documented in the final report.

### "Queued" memories still genuinely pending

`project_iter_5_prep_notes_2026-05-21.md` Priority #5 deferred items (bracket-partitioned corpus, at-scale Voyage rules, live combo extractors, reverse-engineering target decks) — none of these shipped in mega-task v5; all carry to iter 7. Verified via repo grep: no module references `bracket_partitioned`, no live `edhrec_extractor.py`, etc. Still pending as claimed.

### Inline fixes landed

1. `project_pillar_a_c_shipped_2026-05-17.md` — endpoint count drift fixed (9 → "9 at ship; 42 total today").

### Queued for iter 7 / wontfix

None additional from Phase 3. The remaining 48 memory entries weren't exhaustively spot-checked but the sampled subset is consistent and the 4 load-bearing architectural rules are honored. Full memory audit at this depth would be its own sub-task; Phase 3 met the kickoff bar of "audit each [feedback rule] against current code" + spot-check shipped memories.


## Section 4 — Test coverage gaps (Phase 4)

**Verdict: solid coverage — 212 test files / 1489 passing / 17 skipped / 8 pre-existing failures.** Major modules all have direct unit tests; agent_build_deck_v1 alone has 12 dedicated test files. No dead test files found pointing at removed code.

### Test file inventory

- **Total test files**: 212
- **Total passing pytests**: 1489 (post-mega-task-v5)
- **Skipped**: 17 (intentional — gated on env vars / network)
- **Pre-existing failures**: 8 (carried unchanged from iter 4; documented in mega-task v4 final report)
- **Subtests passing**: 58
- **Total test runtime**: ~110s

### Coverage for v3-v5 shipped modules

| Module | Test files | Notable test patterns |
|---|---|---|
| `agent_build_deck_v1` | 12 | Phase-organized: phase_a2, phase_b, phase_b2, phase_c, phase_c2_1, phase_c2_2, phase_d, phase_d2, iter3_phase_1, iter3_phase_3, stream (v5 Phase 3), forbidden_budget_overhead (v5 Phase 8) |
| `agent_semantic_retrieval_v1` | 3 | iter3_phase_7 + iter5_phase_1 + iter6_phase_6 color-filter edge cases (v5 Phase 6) |
| `mana_base_optimizer_v1` (Pillar E v0.1) | 1 | full optimizer + Karsten + aggressive reconciliation |
| `card_advantage_optimizer_v1` (Pillar E v0.2) | 1 | full optimizer + per-bracket mix |
| `curve_smoother_v1` (Pillar E v0.3 / Phase 9) | 1 | 17 tests across 6 classes |
| `interaction_designer_v1` (Pillar E v0.4 / Phase 10) | 1 | 16 tests across 6 classes |
| `agent_intent_preservation_check_v1` | 1 | 17 tests (8 original + 9 Phase 7 archetype-aware) |
| `agent_graduated_playtest_v1` (Phase 12) | 1 | 18 tests with mocked approximator |
| `agent_statistical_approximator_v1` (Pillar F v0.1) | 1 | multi-tier matchup matrix |
| `deck_strength_check_v1` | 1 | incremental + persistent disk cache (4 + 4 = 8 tests after v5 Phase 5 add) |
| `agent_combo_anchor_guard_v1` (iter 3 Phase 2) | 1 | forbidden_set generation + filter |
| `opposition_decks_v1` | 2 | 14 original schema tests + 13 Phase 11 tiered tests |

### Phase-named test pattern

Many tests use `test_agent_buildN_phase_X.py` naming (15+ files). This is intentional — tests are organized by "which mega-task phase introduced or modified this code path" rather than per-module. The `test_agent_iterN_phase_M_*.py` naming is similar. This makes regression traceability easy: failures point at the originating phase.

### 8 pre-existing failures (documented carry-over)

```
tests/test_bracket_gc_limits_v1.py::BracketGcLimitsV1Tests::test_b4_and_b5_are_unlimited
tests/test_complete_bracket_violations_v1.py::TestHttpEndpointWiring (5 tests)
tests/test_no_random_imports.py::NoRandomImportsTests::test_runtime_modules_avoid_nondeterministic_time_and_random_usage
tests/test_pipeline_profile_bracket_enforcement_v1.py::test_pipeline_reports_profile_bracket_enforcement_payload_and_panel
```

Same 8 failures appeared at end of mega-task v4 ship (`e97589870`). They're documented in `mega_task_v4_final_report.md` and were not addressed in v5. They block strict "all tests pass" but iter 4 + iter 5 + iter 6 baselines all preserve them as-is. Iter 7 should consider whether to fix any of these vs. continue carry-over.

### "Dead tests" — checked, none found

The "orphan test files" scan flagged tests whose filenames don't directly match a module name — but on inspection these are all phase-organized tests (test_agent_iter3_phase_5_released_at.py tests the released_at field in agent_build_deck_v1's iter 3 Phase 5 addition, etc.) or test harness files (conftest, decklist_fixture_harness, guardrails_fixture_harness). No tests for genuinely-removed code found.

### Missing integration tests (queue for iter 7)

The kickoff calls out that Phase 10 of THIS sweep will add per-pillar smoke tests as the integration layer. So the only "missing coverage" finding is: there is no single "Pillar A across all 18 v1-tier endpoints in one run" integration test today. Phase 10's deliverable closes that gap.

### Inline fixes landed

None — Phase 4 is read-only audit per kickoff policy.

### Queued for iter 7 / wontfix

- *Queued for iter 7*: review the 8 pre-existing test failures, decide which to fix vs. retire (documenting why) — they've been carried across 4 mega-tasks without resolution.
- *Wontfix*: phase-named test file naming convention. It's intentional + works.


## Section 5 — Database + schema integrity (Phase 5)

**Verdict: clean.** All 3 snapshots have identical row counts (36,709). Iter-3 `released_at` migration is universally populated. Iter-4 / mega-task-v4 `primitives_v1_json` correctly grows from 22,169 (60.4%) on source snapshots → 28,666 (78.1%) on the tagpass snapshot. Tagpass inheritance metadata (`cloned_for_tag_import_v1: true`, `source_snapshot_id`) confirms the clone-and-tag pipeline works. Corpus + opposition deck registries match v5 expected sizes.

### Cards table — per-snapshot column populations

| Snapshot | Total rows | released_at populated | primitives_v1_json populated |
|---|---|---|---|
| `20260217_185403` (raw download) | 36,709 | 36,709 (100%) | 22,169 (60.4%) |
| `20260217_190902` (tags compiled) | 36,709 | 36,709 (100%) | 22,169 (60.4%) |
| `20260217_190902_tagpass_20260222` (active) | 36,709 | 36,709 (100%) | **28,666 (78.1%)** |

The tagpass snapshot adds 6,497 more cards with `primitives_v1_json` than the source. That's the iter-4 ontology v1 rules_modifier extraction (mega-task v4 Phase 4) doing its job — extracting tags from rules-text-described abilities that the regex extractor at the original snapshot couldn't see.

### Cards schema (current)

```
snapshot_id (TEXT), oracle_id (TEXT), name (TEXT), mana_cost (TEXT),
cmc (REAL), type_line (TEXT), oracle_text (TEXT), colors (TEXT),
color_identity (TEXT), produced_mana (TEXT), keywords (TEXT),
legalities_json (TEXT), primitives_json (TEXT), image_uris_json (TEXT),
card_faces_json (TEXT), image_status (TEXT), released_at (TEXT),
primitives_v1_json (TEXT)
```

`primitives_json` (the v0 column) and `primitives_v1_json` (the v1 column) are both present. v0 is legacy; v1 is what current Pillar D consumes. Both populated on the active snapshot.

### Snapshot inheritance metadata

The active snapshot's row in the `snapshots` table includes the manifest:
- `cloned_for_tag_import_v1: true`
- `source_snapshot_id: "20260217_190902"`
- `tags_compiled: true`
- `tool: "update_scryfall_bulk.py"`

Inheritance works correctly — the tagpass clone-and-tag pipeline carries forward all source columns (released_at, oracle_text, etc.) and then runs the v1 extractor on top to populate `primitives_v1_json`. New columns added in iter 5/6 would inherit by the same mechanism if any had been added (none were in v5).

### Data file sizes

| File | Size | Entries |
|---|---|---|
| `engine/data/mtg.sqlite` | 732 MB | 3 snapshots × 36,709 cards + ancillary tables |
| `api/engine/data/corpus/corpus_v1.json` | 48.7 MB | 13,408 decks (matches iter 6 finding) |
| `api/engine/data/playtest/opposition_decks_v1.json` | ~16 KB | 54 entries (matches Phase 11) |
| `api/engine/data/corpus/corpus_vectors_cache_v1.json` | 16.4 MB | 13,408 vectors (mega-task v5 Phase 5 disk cache) |

### Ancillary tables

15 tables total in `mtg.sqlite`. Most are documented in module docstrings:
- `cards`, `cards_raw`, `card_tags`, `card_images` — primary card data
- `snapshots` — snapshot metadata
- `card_primitive_tags_v0`, `primitive_defs_v0`, `primitive_rules_v0`, `primitive_tag_runs_v0`, `primitive_tag_unknowns_v0`, `primitive_to_cards` — Pillar C v0 ontology (legacy)
- `equiv_to_cards`, `patches_applied`, `run_history_v0`, `unknowns_queue` — agent run tracking + manual overrides

No orphan tables. No tables with anomalous row counts.

### Inline fixes landed

None — Phase 5 is read-only audit and the data is clean.

### Queued for iter 7 / wontfix

- *Out-of-scope*: `primitives_json` (v0 column) duplicates capability of `primitives_v1_json` — could be deprecated in iter 7 for storage savings, but no functional bug. Lower priority than the 3 iter-7 locked priorities.


## Section 6 — UI ↔ endpoint contract drift (Phase 6)

**Verdict: clean.** Every endpoint the UI calls exists in `api/main.py`. Both v5 contract additions (`GET /snapshots/active` and `POST /agent/build_deck_v1/stream`) are wired correctly into AIBuildView + useBuildStreaming hook.

### UI fetch sites surveyed

| UI file | Endpoint called | Exists at | Notes |
|---|---|---|---|
| `views/AIBuildView.tsx:275` | `GET /snapshots/active` | `api/main.py:893` | v5 Phase 2 auto-default snapshot |
| `hooks/useBuildStreaming.ts:160` | `POST /agent/build_deck_v1/stream` | `api/main.py:2498` | v5 Phase 3 SSE stream |
| `App.tsx:685` | `POST /build` | `api/main.py:1254` | legacy build endpoint (pre-v1) |
| `components/workspaceUtils.ts:451` | `GET /snapshots?limit=1` | `api/main.py:888` | snapshot list |
| `views/WorkspaceView.tsx:2137, 2884, 2966, 3087, 3391, 3495, 3896` | `/snapshots`, custom endpoint, `/cards/resolve_names`, `/build`, `/deck/validate`, `/deck/complete_v1`, `/deck/tune_v1` | `api/main.py:888, 1128, 1254, 1553, 1973, 1676` | Workspace's deck-builder uses Pillar A v1 endpoints |
| `components/deck/DeckEditorPanel.tsx:374` | `POST /cards/resolve_names` | `api/main.py:1128` | name resolution |
| `components/CardSuggestInput.tsx:79` | typeahead URL | `api/main.py:913` (`/cards/suggest`) | autocomplete |
| `EngineViewerV0.tsx:184` | generic URL | varies | debug surface |
| `Phase1Harness.tsx:353, 515, 701, 788` | `/snapshots`, `/build`, various | varies | Phase 1 harness (debug-only view) |

### v5 contract verification

**Auto-snapshot (Phase 2)**: `GET /snapshots/active` returns `{"snapshot_id": <str>}`. AIBuildView.tsx:275 reads `data.snapshot_id` and seeds the snapshotId state. Live response from /snapshots/active in Phase 2 verified: `{"snapshot_id":"20260217_190902_tagpass_20260222"}`. ✓ Contract honored.

**SSE streaming (Phase 3)**: `POST /agent/build_deck_v1/stream` returns SSE events. useBuildStreaming.ts:160 sets `Accept: text/event-stream`, manually parses the response stream via `fetch + ReadableStream` (NOT EventSource — that's GET-only). Server emits events with shape `{phase, status, elapsed_s, cost_usd, ...}` per phase boundary. Verified live via Phase 5 smoke (mega-task v5 Phase 5) — 19 events on Edgar B3, all phase boundaries fire, complete event carries 100-card deck. ✓ Contract honored.

**graduated_playtest_report UI render (Phase 12)**: AIBuildView.tsx renders the new `response.summary.graduated_playtest_report` block when present. Field is added to both the success-path `summary` and the failure-path `_failure_response()` for schema consistency. Verified via grep of the summary object construction in `agent_build_deck_v1.py`. ✓ Contract honored.

### Endpoints UI doesn't call (informational)

The 18 v1-tier endpoints registered in api/main.py include several that no UI surface calls today:
- `/agent/context_bundle_v1` (MCP tool surface; called by Claude Code via MCP, not by browser UI)
- `/playtest/benchmark_v1` (MCP tool surface)
- `/corpus/batch_ingest_v1` (CLI tool surface)
- `/playtest/opposition_decks_v1` (used by Pillar F internally; UI's graduated_playtest_report block already consumes via Pillar F output)
- `/deck/save_to_library_v1` (Obsidian write integration)
- `/corpus/similar_decks_v1` (MCP tool surface)
- `/strategy_hypothesis_v0`, `/deck_complete_v0`, `/runs_v0`, `/run_v0/{id}`, `/run_diff_v0`, `/run_bundle_v0` (Phase 1 harness debug surface)
- `/primitive_tag_index_v0/*` (Phase 1 harness debug surface)

These are intentional non-UI endpoints. No contract drift.

### Inline fixes landed

None — Phase 6 is read-only audit and the contracts are clean.

### Queued for iter 7 / wontfix

- *Wontfix*: legacy `/build` endpoint coexists with `/agent/build_deck_v1` + `/agent/build_deck_v1/stream`. WorkspaceView still uses `/build`; AIBuildView uses the streaming endpoint. Both work; the legacy endpoint will eventually deprecate but no functional drift today.


## Section 7 — Documentation drift (Phase 7)

**Verdict: module docstrings + DESIGN_DECISIONS.md are current; Obsidian vault ENGINE_API_GUIDE.md drifted across 3 mega-tasks (queued for iter 7 doc-overhaul).**

### Module docstrings — current

Spot-checked the v5 modules' docstrings; all comprehensive:
- `curve_smoother_v1.py` (Phase 9) — full docstring with API, JSON-config reference, "future iter may add LLM critique" note.
- `interaction_designer_v1.py` (Phase 10) — categories, per-bracket policy, color-gating, primitive-classification table, all documented.
- `agent_graduated_playtest_v1.py` (Phase 12) — module docstring + dataclass field comments + per-function explanations.
- `deck_strength_check_v1.py` — `STRENGTH_CHECK_VERSION = "strength_check_v1.4_persistent_vector_cache"` reflects the Phase 5 disk-cache addition.
- `agent_intent_preservation_check_v1.py` — `INTENT_PRESERVATION_VERSION = "agent_intent_preservation_check_v1.1_archetype_aware"` reflects Phase 7.
- `agent_build_deck_v1.py` `_budget_with_forbidden_overhead` — full docstring explaining Phase 8 fix context.

### DESIGN_DECISIONS.md — current (last modified 2026-05-21, mega-task v4 era)

Structure verified intact: the 4 architectural rules (1.1 Creativity envelope / 1.2 Speed budget / 1.3 Strength oracle / 1.4 Pilot anti-bias), calibration boundary, deterministic vs AI split, self-learning boundaries. The 4 feedback rules in cowork memory map to these — Phase 3 confirmed all 4 are honored in code. DESIGN_DECISIONS.md doesn't need v5 updates because the rules are stable across iterations; only the implementations change.

### Obsidian vault ENGINE_API_GUIDE.md — drifted (queued for iter 7)

Last modified **2026-05-17** (Pillar A+C ship date). 20 endpoint mentions. **Predates mega-tasks v3 + v4 + v5.** Endpoints added since v3-v5 (`/agent/build_deck_v1/stream`, `/snapshots/active`, `/playtest/opposition_decks_v1` extensions, `/corpus/batch_ingest_v1`, etc.) are not documented in the guide.

This is a substantive doc overhaul (~half day's work to cover the ~10 new endpoints + the SSE streaming surface + the graduated_playtest_report response shape). **Queue for iter 7.**

### Obsidian vault MPA_SPEC.md

Last modified 2026-05-17. The MPA (Pilot) substrate is in maintenance mode (Phase 5b — game engine awaits Pillar F v0.2 multi-month work). Spec is accurate for the current state.

### Pillar C ontology docs

- `ontology_v0.md` — legacy regex-based extractor. Still referenced by v0 primitive tables.
- `ontology_v1.md` — current Pillar C ontology with 17 rules_modifier tags (mega-task v4 Phase 4 + 5 extraction work). Matches current code.

### README files

- `mtg-engine/repo/README.md` — main README, exists. Did not exhaustively diff against v5 changes.
- `mtg-engine/repo/README_TESTING.md` — testing notes, exists.

### Inline fixes landed

None — all the docstring drift candidates are within the queue-for-iter-7 threshold rather than 1-2 paragraph fixes. ENGINE_API_GUIDE.md needs a real overhaul, not an inline patch.

### Queued for iter 7 / wontfix

- *Queued for iter 7*: `13_AI_AGENT_SURFACE/ENGINE_API_GUIDE.md` overhaul to cover the ~10 endpoints added across mega-tasks v3-v5 + the SSE streaming contract + the response.summary fields added for Pillar E v0.3/v0.4 + graduated playtest. ~half day of focused doc work.
- *Wontfix*: `README.md` minor refresh — not blocking anything.


## Section 8 — Orphan code detection (Phase 8)

**Verdict: 2 true production orphans flagged for iter 7 wiring decisions; 1 stale `.pyc` cleanup; 30 false positives (CLI scripts that are RUN rather than imported).** No inline deletions per kickoff policy.

### Method

AST-walked all 454 `.py` files (excluding `.venv` and `__pycache__`) and recorded every `Import` / `ImportFrom` target. For each file in `api/engine/layers/`, `extractors/`, `integrations/`, and `tools/`, checked whether the file's module name (or its leaf component) appeared anywhere as an import target. Files with no importers flagged. Followed up with explicit `grep -rn <module_name>` on each candidate to filter false positives (e.g., scripts only referenced via `__main__` invocation).

Scanner lives at `tools/_coherence_sweep_3_orphan_scan.py` (added during this phase; can be deleted post-sweep or kept as ongoing audit harness).

### True production orphans

| Module | Status | Disposition |
|---|---|---|
| `api/engine/layers/agent_voyage_downgrade_pass_v1.py` | **TRUE ORPHAN — possibly a wiring bug.** Module claims "Mega-task v4 Phase 10" in its docstring; mega-task v4 final report claims this shipped as "mana-cost-aware Voyage downgrade pass." But the only importers are its own test file. No production code path imports `agent_voyage_downgrade_pass_v1` or `compute_voyage_downgrade`. | **Queued for iter 7.** Decision needed: wire it into `agent_build_deck_v1.py` (between Voyage retrieval and pool composition?) OR document why it was abandoned + remove. The test file passes today, which is why this wasn't caught — tests exercise the module standalone. |
| `api/engine/layers/voyage_rules_embedding_v1.py` | **TRUE ORPHAN — expected.** Module is the "at-scale Voyage rules embedding pipeline" from mega-task v3 Phase 4 / v4 Phase 4. Documented in memory as "Phase 4 hook in place; at-scale embedding deferred." Test imports it; production doesn't. | **Queued for iter 7** as part of "at-scale Voyage rules embedding" priority (per `project_iter_6_prep_notes_2026-05-21` Priority #5 + `project_iter_7_prep_notes_2026-05-22` deferred items). |

### Stale `.pyc` (no source file)

| Path | Disposition |
|---|---|
| `scripts/__pycache__/build_primitive_tag_index_v0.cpython-310.pyc` | Source `.py` already removed (per mega-task v5 progress log). The `.pyc` is harmless — Python only loads `.pyc` if its source `.py` exists; this file will be ignored on next bytecode invalidation pass and naturally cleared on a fresh `__pycache__` rebuild. **Wontfix** (cleanup-on-rebuild). |

### False positives (30 items — CLI scripts in `tools/`)

These are scripts intended to be invoked directly (`python tools/<name>.py`), not imported. The orphan scanner flagged them because nothing imports them — by design. All 30 are legitimate CLI entry points:

- `tools/test_pillar_d_iteration_{2,3,4,5,6}.py` — per-iter validation sweep harnesses (each iter has one).
- `tools/mega_task_v5_phase5_live_smoke.py`, `phase6_query_smoke.py` — mega-task v5 smoke harnesses.
- `tools/warm_corpus_vector_cache.py` — Coherence Sweep #3 / mega-task v5 disk-cache warmup.
- `tools/_coherence_sweep_3_orphan_scan.py` — this audit's own scanner (added in Phase 8).
- `tools/update_scryfall_bulk.py`, `ingest_new_set.py`, `check_new_sets.py`, `bulk_corpus_ingest.py`, `run_update_pipeline.py` — per-set automation (Track 5) entry points.
- `tools/perf/run_perf_timing_audit.py` — perf timing harness.
- `tools/playtest/{opposition_pool,pod_sim,turn_loop}.py` — Pillar F + MPA playtest scripts.
- `tools/run_mpa_calibration.py`, `smoke_prod.py`, `smoke_v3_end_to_end.py`, `test_pillar_d_agent.py` — various harnesses.
- `tools/backfill_primitives.py`, `backfill_primitives_v2.py`, `generate_combo_outcomes_v1.py` — one-shot data tooling.

### Other false positives in layers/

The scanner flagged the following but explicit grep confirmed they ARE imported:
- `proof_scaffold_v1` — 2 production usages
- `invariants_v1` — 43 production usages
- `duplicate_enforcement` — 1 production usage
- `patch_loop_v0` — 8 production usages
- `proof_attempt_v1` — also imported

These appeared in my initial list because the AST scanner's leaf-name match was insufficient for nested package references; the explicit grep pass cleared them.

### Files explicitly in `deprecated/`

- `api/engine/layers/deprecated/_deprecated_motifs_v1.py` — folder name + filename prefix both say "deprecated". Intentional. **Wontfix.**

### Inline fixes landed

None per kickoff policy ("do NOT delete inline; that's the user's call").

### Queued for iter 7 / wontfix

- *Queued for iter 7*: `agent_voyage_downgrade_pass_v1` — wiring decision needed (wire it in OR abandon). This is the more interesting finding because the module thinks it shipped.
- *Queued for iter 7*: `voyage_rules_embedding_v1` — wire as part of "at-scale Voyage rules embedding" deferred item.
- *Wontfix*: `_deprecated_motifs_v1.py` (already in deprecated/ folder).
- *Wontfix*: stale `.pyc` (cleanup-on-rebuild).
- *Decision needed*: keep or remove `tools/_coherence_sweep_3_orphan_scan.py`. Suggest keeping as a periodic audit harness (run it before each future sweep).


## Section 9 — External-dep audit (Phase 9)

`<pending — populated during Phase 9>`

## Section 10 — Per-pillar smoke tests (Phase 10)

`<pending — populated during Phase 10>`
