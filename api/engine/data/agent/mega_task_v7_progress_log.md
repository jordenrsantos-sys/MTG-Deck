# Mega-task v7 — progress log

Iter 8 dispatch on top of mega-task v6 ship (`6a84de825`). Kickoff: `mega_task_v7_kickoff.md`.

Append-only, timestamped sections per phase.

---

## Phase 0 — Pre-flight + memory sync + progress log (2026-05-23)

**Started:** 2026-05-23 (autonomous dispatch).

**Substrate snapshot:**
- Working tree: `E:/MTG Root/mtg-engine/repo` at `6a84de825` (v6 Phase 12 ship — SHIPPED with halt at Phase 11 10/14).
- Untracked at dispatch: `api/engine/data/agent/mega_task_v7_kickoff.md`, `api/engine/data/primitives/llm_supplement_audit_v1.json`, `engine_path_test.md`. None block.
- Python 3.10.11 confirmed (matches v6 baseline).
- ANTHROPIC_API_KEY + VOYAGE_API_KEY both SET.
- Disk free: 1.1 TB on E:, well above 10 GB floor.
- pytest baseline: 1566 passing / 25 skipped / 0 failed (per v6 final report). vitest baseline: 759 passing / 2 pre-existing failed (metricPillHeader source-grep drift). Full-suite reverification deferred to Phase 9 final regression; per-commit verification via targeted tests + smoke runs.

**Context files read (9 of 9):**

1. `mega_task_v7_kickoff.md` — driver spec.
2. `mega_task_v6_final_report.md` — v6 ship summary + iter-7 sweep 10/14 + iter 8 hand-off answers.
3. `pillar_d_iteration_7_validation_report.md` — per-case iter-7 metrics (5 cases, 14 criteria).
4. `coherence_sweep_3_health_report.md` — substrate health audit (caches, deps, contracts, orphans).
5. `agent_wide_candidate_pool_v1.py` — Phase 1 wide pool (300-500 cards for C2.2). Filter chain: snapshot → CI → type → exclude → primitive overlap (70/30 split).
6. `agent_build_deck_v1.py` — main agent endpoint. `_select_deck` is where `POOL_UNDER_FILL_PADDED_WITH_BASICS` warning fires (line 2090). Pool deficit fed by Phase B `compute_deck_candidate_pool_v1` (in `deck_candidate_pool_v1.py`).
7. `agent_semantic_injection_v1.py` — Phase 4 widening target.
8. `agent_c22_prompt_templates_v1.py` — Phase 5 archetype thresholds target.
9. `interaction_designer_v1.py` + `win_con_coherence_v1.py` — Phase 6/7 targets.

**Memory entries refreshed (3 of 3 active):**
- `project_mega_task_v6_shipped.md` — v6 commit chain + load-bearing constraints v7 must honor (SSE mountedRef in useEffect body; ontology v2 default; anthem-effect removal; multi-category interaction; rules embedding index populated).
- `feedback_pool_score_does_not_drive_llm_picking.md` — drove v6 Phase 2 semantic-injection guarantee; Phase 4 widens its swappable set.
- `project_5_pillar_forward_plan.md` — Pillar E COMPLETE; iter 8 = close v6 gaps + (deferred iter 9+) Pillar F v0.2 game engine.

**Phase 0 deliverables:**
- Progress log scaffolded (this file).
- Task list created (10 phases, this session).

**Risks identified at Phase 0:**
- v6 Phase 11 hard-halt root cause (10/14, not 12/14): 4 sweep gaps. Phases 4-7 address each. If those tuning fixes don't deliver, expect Phase 8 to re-trip the same halt — but with 8 of the 12 Phase 8 criteria coming from Phase 1-3 work (orthogonal to iter-7 gaps), the path to ≥10/12 is well-defined.
- Phase 2 chrome-devtools-mcp not currently surfaced in this tool roster (only mtg-engine + obsidian + figma MCP available). Will substitute with vitest component test + Python httpx smoke + dev-server check; if user wants live browser verification, they can drive that manually after Phase 9.

**Commit message:** "Phase 0 (mega-task v7): pre-flight + progress log scaffold".

---

## Phase 1 — Candidate pool under-fill diagnosis + fix (BLOCKING) (2026-05-23)

**Diagnosis (took 10 min via Python tool against live snapshot):**

Edgar Markov B3 vampire tribal, hint=`['TYPAL_VAMPIRES:Vampire']`:
- Pool size: 97 candidates (was expected 60+ spells)
- Slot classification: `creature: 73, flex: 24`. ZERO ramp, ZERO draw, ZERO
  removal, ZERO wincon, ZERO land. → all 30 archetype staples (Sol Ring,
  Arcane Signet, Command Tower, basics, etc.) classified as `flex`.

**Root cause #1:** `_upsert(..., source="archetype_staple", ...)` did NOT
pass `type_line` or `primitives`. The brief endpoint returns only
`(name, usage_pct)` for staples, so they entered the pool with
`type_line=None` + `primitives=[]` → `_classify_card` defaulted to `flex`
for every staple. Result: Pass 3 land fill found 0 lands → 36 basics
padded; ramp/draw/removal/wincon slots stayed empty → Pass 4 padded
another 20-30 basics with `POOL_UNDER_FILL_PADDED_WITH_BASICS`.

**Root cause #2:** Even after hydration, `_classify_card`'s primitive
constants were stale relative to the v6 Phase 3 ontology v2 backfill.
Pre-v7: `_RAMP_PRIMITIVES = {MANA_ROCK, MANA_RAMP_LAND_SEARCH, ...}`.
But `find_card_by_name` returns `cards.primitives_v1_json` which uses
`RAMP_MANA`, `RAMP_LAND`, `MANA_FIXING`, `REMOVAL_SINGLE`, `BOARD_WIPE`,
`COUNTERSPELL`, `DIRECT_DAMAGE`, `CARD_DRAW`, `CARD_SELECTION`. Sol Ring's
primitives are `[MANA_FIXING, MANA_ROCK, RAMP_MANA]` — only `MANA_ROCK`
matched, but Cultivate has only `RAMP_LAND` → routed to `flex`.

**Root cause #3:** `search_cards_v1` (Pillar A endpoint) silently disables
the `primitives_any` SQL filter when the inverted-index oid match set
exceeds 950 (SQLite param limit). Out of scope to fix per v7 kickoff
("Don't modify Pillar A endpoints"). Worked around in the agent layer by
direct DB query in the slot-fallback helper.

**Implementation (`agent_build_deck_v1.py`):**

1. New `_hydrate_card_metadata(db_snapshot_id, names)` — batches
   `find_card_by_name` lookups so archetype staples carry `type_line +
   primitives + cmc + color_identity` into the pool.

2. Updated `_RAMP_/_DRAW_/_REMOVAL_/_WIN_CONDITION_PRIMITIVES` to be the
   UNION of legacy `primitive_to_cards` vocab and v6 Phase 3
   `cards.primitives_v1_json` vocab. Substrate-agnostic — works whether
   primitives come via `search_cards_v1` (legacy index) or
   `find_card_by_name` (v2 ontology).

3. New `_inject_slot_fallback_candidates(...)` — per-slot DB-direct query
   (bypasses `search_cards_v1`'s 950-oid quirk) that fills slots below
   their fallback floor (ramp 12, card_draw 12, removal 10, win_condition 4)
   with color-legal cards. Injected cards get score=1.0 (below
   archetype_staple baseline, above zero), so they only fill empty slots
   and never crowd theme/staple picks.

4. New `_classify_pool_slots(candidates)` helper used by both fallback
   injection and the `pool_filter_trace` instrumentation.

5. New `pool_filter_trace` dict in the pool response, surfacing:
   `staples_in_brief`, `staples_hydrated`, `theme_hints_used`,
   `forbidden_filtered_count`, `slot_fallback (triggered + added_per_slot
   + pre_counts + post_counts)`, `final_pool_size`, `slot_distribution`.

6. New Pass 3.5 backfill in `_select_deck` — when slot caps strand cards
   in the pool that would otherwise be padded with basics, take overflow
   from any-slot non-land non-user-pick candidates. Combo policy still
   applies. Emits `POOL_BACKFILL_USED_OVERFLOW_CANDIDATES` warning when
   active.

**Verification (live DB, 5 iter-7 sweep cases):**

| Case | Pre-v7 pool | Post-v7 pool | Spells | Basics | Under-fill |
|---|---|---|---|---|---|
| edgar_b3_vampire_tribal | ~38 spells (pre-v7) | 127 | 113 | 25 | NO |
| krenko_b4_goblin_combo | ~? | 130 | 124 | 31 | NO |
| atraxa_b2_proliferate | ~? | 129 | 113 | 24 | NO |
| ur_dragon_b3_dragon_tribal | ~? | 127 | 107 | 21 | NO |
| yuriko_b5_ninja_tempo | ~? | 117 | 106 | 27 | NO |

All 5 cases now hit the kickoff target (pool spells ≥60). Basic land
count dropped from 32-62 (iter 7) to 21-31 (post-v7). No
`POOL_UNDER_FILL_PADDED_WITH_BASICS` warning on any case.

**Test added:** `tests/test_candidate_pool_fill_rate.py` — 5 unittest
methods × 5 sweep cases (subTests). Asserts ≥60 spells, no under-fill
warning, ≤38 basics, healthy ramp/draw/removal counts, `pool_filter_trace`
populated. Includes an `MTG_ENGINE_DB_PATH` swap in setUp to bypass
conftest's autouse hermetic-DB fixture and exercise the real 36k-card
live snapshot.

**Regression checks:**
- `tests/test_agent_build_deck_v1*.py` + `test_candidate_pool_v1.py`:
  142 passed.
- Broader sweep (`-k "build_deck or candidate_pool or interaction_designer
  or curve_smoother or semantic_injection"`): 213 passed, 20 subtests
  passed.
- Full pytest baseline reverification pending (running in background).

**Commit message:** "Phase 1 (mega-task v7): candidate pool under-fill diagnosis + fix".
