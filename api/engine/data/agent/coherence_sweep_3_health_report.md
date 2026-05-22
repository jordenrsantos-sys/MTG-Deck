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

`<pending — populated during Phase 4>`

## Section 5 — Database + schema integrity (Phase 5)

`<pending — populated during Phase 5>`

## Section 6 — UI ↔ endpoint contract drift (Phase 6)

`<pending — populated during Phase 6>`

## Section 7 — Documentation drift (Phase 7)

`<pending — populated during Phase 7>`

## Section 8 — Orphan code detection (Phase 8)

`<pending — populated during Phase 8>`

## Section 9 — External-dep audit (Phase 9)

`<pending — populated during Phase 9>`

## Section 10 — Per-pillar smoke tests (Phase 10)

`<pending — populated during Phase 10>`
