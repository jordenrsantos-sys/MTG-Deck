# Mega-task v14 / Pillar F v0.2 substrate extension — progress log

Iter 14 dispatch on top of v13 ship (`db130a343`). Kickoff:
`mega_task_v14_kickoff.md`. v14 is THE substrate-modification mega-
task — explicitly authorized to modify `pillar_f/v0_2/{state,stack,
turn,replacement,layers,combat}/`. Closes 4 v11/v12 deferrals.

7 phases (0-6). Budget: $20 spend, ~1-2 days CC.

---

## Phase 0 — Pre-flight + scope audit (2026-05-24)

**Baseline verified:**
- `pytest tests/ -q` -> 2321 pass + 25 skip + 88 subtests in 180s.
  Matches iter-13 baseline.
- vitest unchanged from iter-13 (774 pass + 2 pre-existing UI fails).
- `python tools/oracle_seed_coverage.py`:
  - 500 cards. 432 (86.4%) full handler; 45 (9.0%) data-only;
    23 (4.6%) fall-through; 0 exceptions.
  - **Addressed: 95.4%.** v14 target: 100%.

**Reading list completed:**
- v11 final report + v9/v11/v12/v13 memory entries.
- Substrate inventory (10 files across 6 subdirs).
- iter-11 cards module + static_modifier registry introspection.

**Audit deliverable:** `mega_task_v14_audit.md`.

**Key audit findings:**

1. **Phase 1 events**: 2 net-new types (`TokenCreateEvent`,
   `LibrarySearchEvent`); 1 v11-shim promotion (`CombatDamageDealtEvent`
   already exists in `cards/triggered/framework.py` as a thin
   dataclass that listeners subscribe to — Phase 1 moves it to
   substrate + adds emission from combat code; keeps v11 re-export
   for backward compat).

2. **Phase 2 UEOT cleanup**: substrate-side hook ALREADY EXISTS in
   `turn/turn_machine.py::cleanup_step` (line 240). Walks
   `state.continuous_effects` and removes entries with
   `target_pattern["until_end_of_turn"]=True`. Phase 2 adds a
   convenience helper `state.register_until_end_of_turn_effect(...)`
   + audits iter-11 cards to ensure consistent use. Smaller than
   kickoff suggested.

3. **Phase 3 cast pipeline**: kickoff says "45 static_modifier
   cards"; that's the v11 coverage-tool "data-only" bucket. Of those
   45, only **17 cards** benefit from a LIVE cast-pipeline consumer
   (cost_reduction 8 + spell_restriction 2 + attack_tax 2 +
   additional_land_drops 2 + additional_mana_when_land_taps 2 +
   uncounterable 1). The other 28 are iter-12+ work (complex
   behavior, ETB framework consumption, substitution effects).

4. **Phase 4 long-tail**: the 23 fall-through cards are mostly lands
   with special tap-for-mana patterns (Urza's lands, dual lands,
   castles, artifact lands) + 3-4 artifacts/creatures with multi-
   modal or graveyard-source activations.
   - **0 hard substrate extensions required.** 0-4 candidates that
     MIGHT need substrate help (Treasure Vault X-cost; Bender's
     Waterskin mana-storage; Staff of Domination multi-modal;
     Reassembling Skeleton graveyard-source activation).
   - Kickoff halt-trigger is `>8 substrate extensions`. We are at
     **0-4** — well under threshold.

**Halt-triggers checked (all NOT triggered):**
- Substrate extension count for long-tail: 0-4 vs >8 threshold.
- Disruption to iter-10 fixtures: mitigated by promoting existing
  shim rather than introducing parallel event.
- sub-B eligible_actions disruption: mitigated by ADDITIVE consumer
  API.

**Phase 0 deliverable summary:**
- `mega_task_v14_audit.md` — 280-line audit doc with per-deferral
  scope + LOC estimates.
- This progress log.
- No production code changes.

**Commit message:** "Phase 0 (mega-task v14): pre-flight + substrate-extension audit (4 deferrals scoped; estimated 0-4 substrate extensions for long-tail vs kickoff halt threshold of >8)".
