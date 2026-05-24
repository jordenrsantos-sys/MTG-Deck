# Mega-task v12 / iter 12 / Pillar F v0.2 sub-mega-task C — progress log

Iter 12 dispatch on top of v10 ship (`a53cfcb68`) + v11 ship (`c105285c5`,
parallel arc, already merged). Kickoff:
`mega_task_v12_kickoff.md`. Sub-mega-task C only — Stage 2 graduated
playtest harness + the two deferred sub-B Phase 9 gates (combat
multi-block, counter-war depth >= 2).

10 phases (0-9). Budget: $400 API spend, ~4-5 weeks CC.

---

## Phase 0 — Pre-flight + scoping read + module scaffold (2026-05-23)

**Baseline verified:**
- `pytest tests/ -q -x` -> 2234 pass + 25 skip + 88 subtests passed
  in 258s. Matches iter-11 baseline.
- vitest baseline 774 + 2 pre-existing fails (unchanged) -- verified
  in v10 Phase 10; not re-run for Phase 0.

**Scoping doc + reports read:**
- `MTG-Deck-Builder-Claude/pillar_f_v0_2_sub_c_stage_2_playtest_scoping.md`
  (sub-C scoping; this iter implements it).
- `MTG-Deck-Builder-Claude/mega_task_v10_final_report.md` (sub-B ship
  state + deferred gates 5/6 owned by sub-C here).
- `MTG-Deck-Builder-Claude/pillar_f_v0_2_sub_b_llm_policy_scoping.md`
  (sub-B prompt-template + cost-tracker contracts sub-C reuses).
- `api/engine/pillar_f/v0_2/` walk-through (substrate + policy + cards).
- `api/engine/layers/agent_graduated_playtest_v1.py` (Stage 1 orchestrator
  sub-C extends in Phase 5).

**Substrate boundary confirmed:** sub-C will ONLY add via
`register_resolver` (Phase 2) and the existing
`combat.declare_attackers` / `combat.declare_blockers` / callback
APIs (Phase 1). No state/stack/turn/replacement/layers/combat edits.

**Module scaffold created** at `api/engine/pillar_f/v0_2/playtest/`:

```
playtest/
  __init__.py            (PLAYTEST_VERSION)
  orchestrator/__init__.py   (Phase 3)
  cycle/__init__.py          (Phase 4 + Phase 6)
  combat_glue/__init__.py    (Phase 1)
  counter_war/__init__.py    (Phase 2)
  aggregation/__init__.py    (Phase 4)
  reports/__init__.py        (Phase 4)
```

**Coordination note.** v11 (per-card oracle compilation) shipped
already as `c105285c5`. Its module tree (`pillar_f/v0_2/cards/`) is
disjoint from sub-C's (`pillar_f/v0_2/playtest/`). No conflict
expected.

**Commit message:** "Phase 0 (mega-task v12): pre-flight + scoping read + playtest module scaffold".
