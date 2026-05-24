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

Committed as `d0c946149`.

---

## Phase 1 — Substrate event emission (2026-05-24)

**Three new event types added to substrate**
(`api/engine/pillar_f/v0_2/replacement/events.py`):

1. **TokenCreateEvent** (NEW). Fields: creator_card_id,
   controller_id, token_name/power/toughness/types/subtypes/colors/
   keywords, count. Iter-12+ wires emission as token-creating cards
   need it.
2. **LibrarySearchEvent** (NEW). Fields: searcher_id,
   target_player_id, search_predicate (free-form string), reveal,
   shuffle_after. Forward-compat for tutor cards in iter-12+.
3. **CombatDamageDealtEvent** (PROMOTED from v11 shim). Fields
   match v11's exact shim contract (source_card_id,
   source_controller, target_kind, target_id, amount,
   is_first_strike). v11's `cards/triggered/framework.py` now
   re-exports the substrate class so existing listeners
   (Sanctum Seeker, Ragavan, etc.) work unchanged.

**EVENT_TYPES** registry extended with the 3 new names.

**Combat code wiring** in `combat/combat.py`:

- New `_emit_combat_damage_dealt(...)` helper performs lazy import
  of v11's `fire_event_triggers` (substrate stays runnable in
  isolation when cards/ isn't loaded).
- 4 emission sites added in `deal_combat_damage`:
  - Unblocked damage (attacker -> player/planeswalker)
  - Blocked damage (attacker -> blocker creature)
  - Trample excess (attacker -> player/planeswalker)
  - Blocker -> attacker damage
- All sites skip emission when amount <= 0.
- COMBAT_VERSION bumped from v1 -> v2.

**Tests** in `tests/pillar_f_v0_2/test_v14_phase1_events.py`:
13 tests across 4 classes:
- **TokenCreateEventTests** (3): default construction, treasure-
  token-spec, EVENT_TYPES registration.
- **LibrarySearchEventTests** (4): default, tutor predicate, fetch-
  land predicate, EVENT_TYPES registration.
- **CombatDamageDealtEventTests** (3): default, typical creature
  attack, v11-shim re-exports substrate class via `is` identity.
- **CombatEmissionEndToEndTests** (3): unblocked attack fires event
  for the unblocked damage; blocked attack fires events both
  directions (attacker -> blocker, blocker -> attacker); 0-power
  attacker emits no events (amount guard).

**All 13 pass. Full regression: 2334 pass + 25 skip + 88 subtests**
(+13 vs Phase 0 baseline 2321; iter-10 substrate 224/224, policy
167/167, playtest 78/78 -- no regressions).

LOC: ~80 production (events + emission helper + 4 wiring sites) +
~190 test = ~270 LOC.

**Commit message:** "Phase 1 (mega-task v14): substrate event types (TokenCreateEvent + LibrarySearchEvent + CombatDamageDealtEvent promoted from v11 shim) + combat damage emission at 4 sites".

Committed as `423204002`.

---

## Phase 2 — Until-end-of-turn cleanup substrate hook (2026-05-24)

**Scope shrunk from kickoff estimate.** Kickoff said:
> Add `state.until_end_of_turn_effects` list. Each entry: ...

The substrate ALREADY has `state.continuous_effects` + `cleanup_step`
already filters by `target_pattern["until_end_of_turn"]=True`. Phase
2 doesn't need a parallel list -- it needs an ERGONOMIC HELPER that
makes registering UEOT effects safe + consistent + with the right
flag.

**Implementation** in `api/engine/pillar_f/v0_2/state/state.py`:

- New `GameState._ueot_effect_counter: int` field (private counter
  for auto-generated effect ids).
- New `GameState.register_until_end_of_turn_effect(source_card_id,
  controller, layer, sublayer, effect_fn_name, target_pattern,
  description) -> ContinuousEffect` method:
  - Auto-injects `target_pattern["until_end_of_turn"] = True` so
    the existing cleanup_step filter catches the effect.
  - Auto-stamps `target_pattern["applies_during_turn_number"] =
    state.turn_number` for audit / debugging.
  - Generates a unique `effect_id` of the form
    `ueot_<turn>_<counter>`.
  - Appends to `state.continuous_effects` and returns the registered
    ContinuousEffect.
  - Preserves caller-supplied target_pattern keys alongside the
    auto-injected ones.

**No iter-11 cards need migration.** Audited via
`grep -rn "ContinuousEffect(" cards/`: v11 cards only register
permanent-while-on-battlefield effects (anthems, type-adders,
keywords). No UEOT cards in v11's top-500 handler set -- the
helper is infrastructure for iter-12+ adoption (Giant Growth,
Berserk, Threaten, etc., which weren't in v11's coverage seed).

**Substrate cleanup_step UNCHANGED.** The existing filter
(`if not ce.target_pattern.get("until_end_of_turn")`) already
removes the effects the new helper registers. No substrate behavior
change; v14 just made the registration path ergonomic + auditable.

**Tests** in `tests/pillar_f_v0_2/test_v14_phase2_ueot.py`:
8 tests across 2 classes:
- **UEOTHelperTests** (5): returns ContinuousEffect, sets UEOT flag
  + turn_number, appends to continuous_effects, generates unique
  effect_ids, preserves caller-supplied target_pattern keys.
- **UEOTCleanupExpiryTests** (3): cleanup removes UEOT effects,
  cleanup keeps non-UEOT effects (anthem), mixed-only-UEOT-expires
  scenario.

**All 8 pass. Full regression: 2342 pass + 25 skip + 88 subtests**
(+8 Phase 2 tests; no regressions).

LOC: ~55 production (helper + counter field) + ~150 test = ~205 LOC.

**Commit message:** "Phase 2 (mega-task v14): UEOT cleanup substrate hook -- ergonomic register_until_end_of_turn_effect helper on GameState (existing cleanup_step filter unchanged)".
