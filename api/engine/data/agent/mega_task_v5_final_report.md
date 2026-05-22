# Mega-task v5 final report

Shipped 2026-05-22. 14 phases committed. pytest 1489 / vitest 758.

## Executive summary

Mega-task v5 closes Pillar D iter 6 with the UX bundle live-validated end-to-end and the BLOCKING iter-5 Atraxa C2.1 silent-failure dead. Pillar E adds v0.3 curve smoother + v0.4 interaction designer; the graduated playtest framework lands its Stage 1 (tiered opposition + tier-by-tier pod prediction + UI report card). Phase 13's iter 6 sweep landed 9/12 success criteria across two stochastic LLM runs; per user direction the 3 failures ship to iter 7 as explicit priorities #1-#3 (semantic-injection guarantee, ontology v2 with counter primitives, iter-6 eval-script multi-primitive counting).

The substrate now supports real production use: the UI streams phase-by-phase progress with an elapsed timer + cancel button + 240s timeout, uvicorn runs 2 workers so /health stays responsive during builds, snapshot_id auto-defaults, and the deck_strength_check_v1 corpus vector cache persists to disk (one-time ~20 min warm vs the prior ~110 min cold-start). Two stealth substrate bugs surfaced and were fixed during Phase 5: a Python 3.14 venv upgrade had broken voyageai (no precompiled wheel for 3.14), and the corpus vector cache was in-memory-only.

## Headline metrics

| Metric                              | iter 5 baseline | iter 6 measurement | target | status |
|------------------------------------|----------------|--------------------|--------|--------|
| pytest passing                     | 1377           | **1489** (+112)    |        |        |
| iter1 structural pass              | 5/5            | 5/5                | 5/5    | PASS   |
| mean creativity_delta              | 35.2           | 37.6               | ≥35    | PASS   |
| mean novel_combo                   | 5.6            | 5.6                | ≥5     | PASS   |
| mean cost USD                      | $0.30          | $0.31              | ≤$0.45 | PASS   |
| mean wallclock                     | 118.0s         | 112.1s             | ≤120s  | PASS   |
| voyage_semantic_avg                | 1.8            | **1.4-2.0**        | ≥3     | **FAIL → iter 7 #1** |
| intent_drift mean                  | 0.592          | 0.614              | <0.5   | **FAIL → iter 7 #2** |
| **Atraxa C2.1 latency**            | **0.0s**       | **36.5s**          | >0     | **PASS — BLOCKING bug DEAD** |
| Pillar E v0.3 present              | n/a            | 5/5                | 5/5    | PASS   |
| Pillar E v0.4 interaction within   | n/a            | 0/5                | ≥4/5   | **FAIL → iter 7 #3 (eval-script bug)** |
| graduated_playtest present         | n/a            | 5/5                | 5/5    | PASS   |
| UI-equivalent build path           | n/a            | 5/5                | 5/5    | PASS   |

## What shipped (14 phases)

**UX bundle (Phases 1-5)** — uvicorn workers ≥2 (Phase 1); auto-default snapshot_id + placeholder text + Advanced collapsed (Phase 2); SSE streaming via `POST /agent/build_deck_v1/stream` + `useBuildStreaming` hook + progress UI (Phase 3); elapsed timer + Cancel button + 240s timeout + Apply-hidden-during-build (Phase 4); live SSE smoke as chrome-devtools-mcp substitute (Phase 5, passing on Edgar B3).

**Substrate work during Phase 5** — venv rebuilt 3.14 → 3.10.11; sse-starlette pinned; deck_strength_check_v1 gains disk-persistent vector cache (16.4 MB at `corpus_vectors_cache_v1.json`, gitignored, 16-worker parallel warmup tool included).

**Phase 6 — Voyage color-filter regression check** — zero code changes; the kickoff's "color-filter bug" was actually the venv-dep gap. 9 new unit tests pin the contract for every commander-shape (mono / 2-color / 3-color / 4-color / 5-color / colorless / hybrid / case-insensitive). Live smoke confirms all 5 baseline commanders produce 18-20 neighbors.

**Phase 7 — Theme signal density + archetype-aware drift threshold** — `anthem-effect` added to counters_matter signals (v1 ontology proxy); `_ARCHETYPE_AWARE_DRIFT_THRESHOLD = 0.7` for counters_matter primary OR tribal+value_engine; `IntentPreservationReport.effective_drift_threshold` field surfaces the threshold actually used.

**Phase 8 — Atraxa C2.1 silent-failure fix (THE BLOCKING BUG)** — `_budget_with_forbidden_overhead(base, block)` dynamically grows `max_input_tokens` by the forbidden_prompt_block's estimated tokens. Wired into B2/C2.1/C2.2/D2. Atraxa C2.1 confirmed 0.0s → 36.5s live.

**Phase 9 — Pillar E v0.3 curve smoother** — `curve_smoother_v1.py` + `curve_targets_by_archetype_v1.json` with 14 archetype curves. `analyze_curve(deck, archetype_hint) → CurveAnalysis` with bricks (CMC > ceiling) + holes (slots under target × hole_pct). 17 new tests including the kickoff invariant (tribal flags 7-CMC as brick; control treats 7-CMC as fine).

**Phase 10 — Pillar E v0.4 interaction designer** — `interaction_designer_v1.py` with per-bracket policy (B1/B2: 9/70%/2; B3: 11/50%/2; B4: 11/50%/3; B5: 13/20%/1). Color-gating (counterspells require U). Card classification via primitives. 16 new tests including the kickoff reference cases (Atraxa gets counterspells > 0; Krenko gets 0).

**Phase 11 — Tiered opposition registry (BLOCKING)** — `opposition_decks_v1.json` 19 → 54 entries with `opposition_tier` field (0/1/2). Every (B2-B5, tier 0/1/2) has ≥3 entries. New loader helpers `filter_by_tier()` + `filter_by_bracket_and_tier()`. 13 new tests.

**Phase 12 — Graduated playtest Stage 1** — `agent_graduated_playtest_v1.py`. `run_graduated_sweep(deck, bracket) → GraduationReport`. Tier 0 → 1 → 2 with 0.55 advancement gate. UI report card shows `✓/✗ Tier X (XX%)` badges + collapsible tweak suggestions. 18 new tests with mocked approximator.

**Phase 13 — Iter 6 validation sweep** — two stochastic LLM runs, both 9/12. Run 1 used original criteria + flat-mean drift; user direction revised criterion 7 to per-case archetype-aware threshold + loosened criterion 10 to ±50%; run 2 same outcome. Reported at `pillar_d_iteration_6_validation_report.md`. Phase 13 commits at 9/12 with halt #5 explicitly triggered, then user accepts 9/12 and ships Phase 14.

**Phase 14 — Final regression + report + memory update** — pytest 1489, vitest 758, no regressions. Final report (this file) + cowork memory update (`project_mega_task_v5_shipped_2026-05-22.md` + `project_iter_7_prep_notes_2026-05-22.md` + MEMORY.md index + project_5_pillar_forward_plan.md status update).

## Iter 6 → iter 7 hand-off

The 3 iter-6 failures map to iter 7 priorities #1-#3 (see `project_iter_7_prep_notes_2026-05-22.md` in cowork memory):

**Priority #1: semantic-injection GUARANTEE.** Both the +0.15 score-boost (iter 5) and the explicit "MUST SELECT AT LEAST 3 [VOYAGE_SEMANTIC_NEIGHBOR]" prompt-level requirement (iter 5 / iter 6) failed to lift voyage_semantic_avg above 2.0. Iter 7 needs a *post-hoc* semantic-injection pass that runs after C2.2, counts how many semantic_neighbor cards are in the deck, and displaces the lowest-priority C2.2 picks if fewer than N are present. Hard guarantee, not prompt-level lever. The `feedback_pool_score_does_not_drive_llm_picking` learning is now load-bearing — both pool-side and prompt-side mechanisms have been tested and found insufficient.

**Priority #2: Ontology v2 with real counter/proliferate primitives.** Phase 7's `anthem-effect` addition was a v1-ontology proxy that partially helped Ur-Dragon (now passes archetype-aware threshold 0.7) but Atraxa stays at 0.84-0.88 across both sweeps. The v1 ontology has zero counter-themed primitives, so the classifier physically cannot see proliferate engines as counters_matter contribution. Iter 7 ships ontology v2 with: proliferate-trigger, counter-payoff, counter-distributor, charge-counter, energy-counter, cost-reduction-tribal, big-creature-aggro, etb-payoff-creature. LLM-driven extraction at scale (per iter 5 prep notes Insight 1 expansion).

**Priority #3: Iter-6 eval-script multi-primitive counting.** Phase 10's interaction_designer module is correct (16 passing unit tests). The iter-6 eval script's `_count_actual_interaction` does first-match primitive classification — for cards with multi-tag primitives, only the FIRST matched category counts. Multi-tag removal cards (etb-trigger + removal-creature) miss the removal classification. Rewrite to consider ALL primitive tags per card, weighted by primary-category specificity. Quick fix (<1 day).

**Coherence Sweep #3 dispatches BEFORE iter 7 mega-task v6** — per the `project_iter_6_prep_notes_2026-05-21` commitment. 10 audit areas covering substrate cache audit + cross-pillar integration + memory↔code alignment + test coverage + schema + UI contract + docs + orphan code + dep audit + per-pillar smokes. Output at `repo/api/engine/data/agent/coherence_sweep_3_health_report.md`. ~1-2 days CC time.

## Commit chain

17 atomic phase commits + 7 progress-log SHA fixups + 1 Phase 13 re-run amendment, on top of mega-task v4 (e97589870):

```
418c48197 Phase 13 amendment: revised criteria 7 + 10, re-run, still 9/12
f182007cb Phase 13 progress log: record actual commit SHA (2017cf2db)
2017cf2db Phase 13 (mega-task v5): iter 6 final validation sweep — 9/12 pass, HALT per #5
983db3c8d Phase 12 progress log: record actual commit SHA (1257567cd)
1257567cd Phase 12 (mega-task v5): graduated playtest Stage 1 logic + UI report card
09a84c616 Phase 11 progress log: record actual commit SHA (28f0d3866)
28f0d3866 Phase 11 (mega-task v5): tiered opposition deck registry (BLOCKING)
2d5076902 Phase 10 progress log: record actual commit SHA (0e004c6f2)
0e004c6f2 Phase 10 (mega-task v5): Pillar E v0.4 interaction designer
c1a26454f Phase 9 progress log: record actual commit SHA (5bd97577e)
5bd97577e Phase 9 (mega-task v5): Pillar E v0.3 curve smoother analysis
735c314de Phase 7 progress log: record actual commit SHA (589672661)
589672661 Phase 7 (mega-task v5): theme signal density expansion + archetype-aware drift thresholds
0bb8441df Phase 8 progress log: record actual commit SHA (be5570809)
be5570809 Phase 8 (mega-task v5): Atraxa C2.1 silent-failure fix via dynamic forbidden_block budget overhead
a1d0f46a7 Phase 6 progress log: record actual commit SHA (f3c81aa18)
f3c81aa18 Phase 6 (mega-task v5): Voyage color-filter regression check (no code fix needed)
140d05af2 Phase 5 progress log: record actual commit SHA (709a16bab)
709a16bab Phase 5 (mega-task v5): UX bundle live validation + venv recovery + corpus disk cache
7f5fc86e4 Phase 4 (mega-task v5): elapsed timer + cancel button + 240s timeout
d61bd8465 Phase 3 (mega-task v5): build progress streaming via SSE + UI progress display
83270d3c6 Phase 2 (mega-task v5): auto-default snapshot_id + UI placeholder/help text
3e5ff5f07 Phase 1 (mega-task v5): uvicorn multi-worker for concurrent request handling
74abcc8f8 Phase 0 (mega-task v5): pre-flight + progress log scaffold
e97589870 Phase 14 (mega-task v4): final regression + report + memory update
```

Phase 14 itself is committed after this report writes.

## Spend

~$5 of $100 ceiling. Most went to iter 6 sweep runs (~$1.50 × 2 = $3); Phase 5 SSE smoke (~$0.30); Phase 6 query smoke + development LLM calls (~$1); Phase 7-12 unit-test runs (negligible). Well under budget.

## Hand-off

Next dispatch: **Coherence Sweep #3** (separate mega-task, ~1-2 days). After sweep ships, **iter 7 mega-task v6** with the 3 architectural fixes above + Pillar E v0.5 win-condition coherence + Pillar E v0.6 anti-meta hate. Estimated 3-5 weeks CC time + $50-100 budget.
