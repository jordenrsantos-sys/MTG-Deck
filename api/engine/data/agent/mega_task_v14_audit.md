# Mega-task v14 — substrate extension audit

**Generated:** 2026-05-24 (Phase 0)
**Parent commit:** v13 ship `db130a343`

Goal: enumerate every substrate-side change required to close the
four v11/v12 deferrals. Per-deferral analysis follows.

---

## Baseline + coverage state (pre-v14)

- `pytest tests/ -q` -> **2321 pass + 25 skip + 88 subtests** in 180s.
  Matches iter-13 baseline.
- vitest baseline unchanged (verified iter-13).
- `python tools/oracle_seed_coverage.py` reports:
  - **Total cards:** 500
  - **Full handler coverage:** 432 (86.4%)
  - **Data-only (static_modifier):** 45 (9.0%)
  - **Fall-through (no handler):** 23 (4.6%)
  - **Exception:** 0 (0.0%)
  - **Addressed (full + data):** 477 (95.4%)
- v14 target: addressed -> 500/500 = **100%**.

## Substrate inventory

```
api/engine/pillar_f/v0_2/
  state/    state.py (GameState + StackEntry + Effects) + card.py + player.py + zones.py
  stack/    stack.py
  turn/     turn_machine.py (cleanup_step at line 208) + mulligan.py
  replacement/  events.py (10 existing event types) + replacement.py + sba.py
  layers/   layer_engine.py + characteristics.py
  combat/   combat.py
```

Existing event types in `replacement/events.py`: `Event` (base),
`DrawEvent`, `DamageEvent`, `EnterBattlefieldEvent`, `DieEvent`,
`LifeChangeEvent`, `CounterAddEvent`, `CounterRemoveEvent`,
`DiscardEvent`, `MillEvent`.

---

## Deferral 1 — Substrate event emission (Phase 1)

Three new event types. Two are net-new; one already exists as a v11
"shim" in `cards/triggered/framework.py` and needs to be promoted to
the substrate.

### TokenCreateEvent (NEW)

**Substrate work:**
- Add dataclass to `replacement/events.py`:
  ```python
  @dataclass(frozen=True)
  class TokenCreateEvent(Event):
      event_type: str = "TokenCreateEvent"
      creator_card_id: Optional[str] = None
      controller_id: int = 0
      token_name: str = ""
      token_power: Optional[str] = None
      token_toughness: Optional[str] = None
      token_types: List[str] = field(default_factory=list)
      token_colors: List[str] = field(default_factory=list)
      token_keywords: List[str] = field(default_factory=list)
      count: int = 1   # for Doubling Season multiplication
  ```
- Add a substrate helper `state.emit_token_create(...)` that callers
  use instead of directly inserting a Card on the battlefield.

**Card-side call sites to update:**
- v11 already creates tokens directly in resolvers (e.g.,
  `cards/spell/counterspells.py::_swan_song_resolve` creates a Bird
  Token; `_an_offer_resolve` creates Treasure tokens). Update these
  to go through the substrate emitter.
- Other token creators in iter-11 (search by `name="Treasure Token"`,
  `name="Bird Token"`, etc.).

### LibrarySearchEvent (NEW)

**Substrate work:**
- Add dataclass to `replacement/events.py`:
  ```python
  @dataclass(frozen=True)
  class LibrarySearchEvent(Event):
      event_type: str = "LibrarySearchEvent"
      searcher_id: int = 0
      search_predicate: str = ""   # "any" | "basic_land" | "creature" | ...
      reveal: bool = False
      shuffle_after: bool = True
  ```
- Add `state.emit_library_search(...)` helper.

**Card-side call sites:** v11 has no tutor cards in the top-500
fully-handled set (the 23 long-tail does include search-adjacent
abilities); Phase 1's LibrarySearchEvent is forward-compat
infrastructure for iter-12+ tutor cards. Emission wiring will land
when first tutor card needs it.

### CombatDamageDealtEvent (already a v11 shim; promote)

**Current state:** Defined in `cards/triggered/framework.py` as a
v11 shim dataclass. v11 fires it from card-side resolvers (e.g.,
Sanctum Seeker lifegain trigger reads this).

**Substrate work:**
- Move dataclass to `replacement/events.py` so substrate combat
  code can emit it directly.
- Keep `cards/triggered/framework.py` shim as a thin re-export so
  existing v11 listeners don't break.
- Update `combat/combat.py::deal_combat_damage` to emit the event
  after each damage assignment.

**Risk:** the existing 100-fixture suite covers combat extensively.
Adding emission must not change damage MATH or SBA semantics, only
emit a NEW signal. Approach: emit AFTER damage marks are written
(verified by SBA loop reading damage_marked).

---

## Deferral 2 — Until-end-of-turn cleanup hook (Phase 2)

**Current state:** `turn/turn_machine.py::cleanup_step` (line 208)
ALREADY expires "until end of turn" continuous effects via:
```python
state.continuous_effects = [
    ce for ce in state.continuous_effects
    if not ce.target_pattern.get("until_end_of_turn")
]
```
Layer engine consumes these on every re-application. So the
substrate-side hook EXISTS.

**What's missing per the kickoff:**
- Cards in iter-11 that emit UEOT effects need to consistently use
  `target_pattern["until_end_of_turn"] = True` when registering
  continuous effects.
- Convenience helper `state.register_until_end_of_turn_effect(...)`
  to make the registration ergonomic and reduce bug surface.

**Scope at Phase 2:** thin substrate helper + audit iter-11 cards
that emit pump/buff/control-change for the UEOT flag. ~10-15 cards
to verify (Giant Growth, Berserk, Searing Blaze, Threaten, etc.).

**Verdict:** smaller than the kickoff suggested. The substrate hook
exists; v14 just adds a helper + makes iter-11 cards use it
consistently.

---

## Deferral 3 — Cast pipeline consumer for static_modifier cards (Phase 3)

**The "45 cards" count** is from `tools/oracle_seed_coverage.py`'s
"data-only (static_modifier)" bucket — cards where v11 registered a
StaticModifier annotation but the substrate doesn't yet read it.

**Note:** the static_modifier registry has **78 total entries**
across all effect_keys; the **45** are specifically those that fall
in the "data-only" bucket (the rest are read by triggered/replacement
handlers). Per-effect-key breakdown:

| effect_key | count | needs cast-pipeline consumer? |
|------------|-------|-------------------------------|
| complex_pending | 40 | iter-12+ (complex behavior; per-card) |
| etb_trigger_pending | 9 | iter-12+ (ETB framework can consume) |
| **cost_reduction** | 8 | **YES — Phase 3 consumer reads at cast** |
| additional_land_drops | 2 | YES — read at play_land |
| spell_restriction | 2 | YES — read at cast |
| attack_tax | 2 | YES — read at declare_attackers |
| combat_damage_to_player | 2 | YES — triggered (already wired) |
| attack_trigger_equipped | 2 | YES — triggered (already wired) |
| additional_mana_when_land_taps | 2 | YES — read at land tap |
| token_creation_substitution | 1 | iter-12+ (substitution effect) |
| token_creation_multiplier | 1 | Phase 1 TokenCreateEvent enables |
| library_search_restriction | 1 | Phase 1 LibrarySearchEvent enables |
| uncounterable | 1 | YES — read at counterspell resolution |
| cast_trigger_modal_bounce | 1 | YES — triggered |
| etb_trigger_damage_each_opp | 1 | YES — triggered |
| etb_trigger_multiplier | 1 | YES — triggered |
| escape_grant_to_graveyard | 1 | iter-12+ |
| cast_trigger_silence_opp | 1 | YES — triggered |

**Phase 3 ACTUAL scope** (the cast-pipeline consumer):
- **cost_reduction (8)** — primary target
- **spell_restriction (2)** — primary target (cost increase)
- **attack_tax (2)** — primary target (combat-side consumer)
- **additional_land_drops (2)** — adjacent target (play_land consumer)
- **additional_mana_when_land_taps (2)** — adjacent (tap-for-mana consumer)
- **uncounterable (1)** — adjacent (counter-resolution consumer)

**Total: 17 cards** that get LIVE cast/combat consumer behavior in
v14 Phase 3. The remaining 28 "data-only" cards continue to be
read-as-data for iter-12+ surfaces (sub-B LLM prompts already
consume the registry for context).

**Substrate work:**
- New module `cast/cost_modifier.py` exporting:
  - `effective_cast_cost(state, card, caster_id) -> ManaCost`
  - `effective_attack_cost(state, attacker_card_id, attacker_id) -> ManaCost`
  - `is_spell_cast_legal(state, card, caster_id) -> (bool, reason)`
  - `is_spell_uncounterable(state, stack_entry) -> bool`
  - `additional_land_drops_available(state, player_id) -> int`
  - `extra_mana_for_land_tap(state, land_card_id) -> List[Mana]`
- These functions read `query_active_static_modifiers(state,
  effect_key=...)` from v11's existing registry.

**Integration with sub-B's compute_eligible_actions:** sub-B's
eligible-actions reader emits `play_land` and `cast_spell` actions.
Phase 3 enriches these with the modified cost so the LLM sees the
right number. Backwards compatible: callers can ignore the modifier
fields and get iter-11 behavior.

---

## Deferral 4 — 23 long-tail activated-bucket fall-throughs (Phase 4)

The 23 cards (from `oracle_seed_coverage.py` output):

| # | Card | Type | Activation pattern | Substrate work needed? |
|---|------|------|--------------------|------------------------|
| 1 | Urza's Tower | Land | Tap: {C} (or {C}{C}{C}{C} with full set) | No -- existing tap-for-mana framework |
| 2 | Urza's Mine | Land | Tap: {C} (or {C}{C}{C}{C} with full set) | No |
| 3 | Urza's Power Plant | Land | Tap: {C} (or {C}{C}{C}{C} with full set) | No |
| 4 | Crystal Vein | Land | Tap, sacrifice: {C}{C} | No -- needs sac-as-cost framework |
| 5 | Sungrass Prairie | Land | Tap: {G} or {W}, conditional | No |
| 6 | Reflecting Pool | Land | Tap: any color a land you control could produce | No -- can iterate state |
| 7 | Graven Cairns | Land | {R} or {B}: filter to BR | No |
| 8 | Underground Sea | Land | Tap: {U} or {B} | No |
| 9 | Mossfire Valley | Land | Tap: {R} or {G}, conditional | No |
| 10 | Blazemire Verge | Land | Tap: {R} or {B}, conditional | No |
| 11 | Castle Garenbrig | Land | Tap: {G}; {2}, tap: 6 mana for creature | No (multi-mode in iter-11) |
| 12 | Castle Locthwain | Land | Tap: {B}; {1}{B}, tap: scry+discard | No |
| 13 | Throne of Eldraine | Land | Tap: {C}; conditional activations | No |
| 14 | Ancient Den | Artifact Land | Tap: {W} | No |
| 15 | Great Furnace | Artifact Land | Tap: {R} | No |
| 16 | Treasure Vault | Artifact Land | Tap: {C}; X cost: X treasures | Maybe -- X-cost activation pattern |
| 17 | Bender's Waterskin | Artifact | Tap: store mana / release | Maybe -- mana-storage pattern |
| 18 | Hall of Heliod's Generosity | Land | Tap: ench from gy to top of lib | No |
| 19 | The Mycosynth Gardens | Land | Tap: {C}; copy artifact | No (Phase 1 token helper covers copy) |
| 20 | Shizo, Death's Storehouse | Land | Tap: {B}; grants fear to legend | No |
| 21 | Palladium Myr | Creature | Tap: {C}{C} | No -- creature tap-for-mana |
| 22 | Staff of Domination | Artifact | 5 modal activations | Maybe -- multi-modal framework |
| 23 | Reassembling Skeleton | Creature | {1}{B}: return from gy | Maybe -- graveyard-source activation |

**Substrate extensions needed: 0 hard, up to 4 candidates** (Treasure
Vault X-cost; Bender's Waterskin mana-storage; Staff of Domination
multi-modal; Reassembling Skeleton graveyard-source activation).

The kickoff halt-trigger is `>8 cards need substrate extensions`. We
are at **0-4** — well under the threshold.

**Phase 4 actual scope:**
- Extend `cards/activated/` with handlers for the 23 cards.
- Most are simple variants of existing tap-for-mana patterns; iter-11
  framework gaps are about specific conditional logic, multi-mode
  activations, or alternate-cost activations (sac, X, graveyard-source).
- Substrate extensions only if a handler genuinely cannot be expressed
  in the existing framework (current estimate: 0-2).

---

## Total estimated v14 LOC delta

| Component | LOC est. |
|-----------|----------|
| Phase 1 substrate events + emission | ~150 prod + ~80 test |
| Phase 2 UEOT helper + audit + cards | ~100 prod + ~60 test |
| Phase 3 cast cost-modifier consumer | ~400 prod + ~200 test |
| Phase 4 23 activated-card handlers | ~500 prod + ~250 test |
| Phase 5 regression tests + fixtures | ~150 test |
| **Total** | **~1150 prod + ~740 test = ~1890 LOC** |

---

## Phase 0 deliverable summary

- This audit doc.
- Verified the kickoff's halt-triggers:
  - Phase 0 long-tail substrate-extension count >8: **NOT triggered** (estimated 0-4)
  - Phase 1 disruption to existing fixtures: **mitigated** by promoting
    CombatDamageDealtEvent rather than introducing a new one
  - Phase 3 cast pipeline disruption to sub-B eligible_actions:
    **mitigated** by making the consumer ADDITIVE (sub-B reads new
    fields opt-in)
- No production code changes in Phase 0.

**Commit message:** "Phase 0 (mega-task v14): pre-flight + substrate-extension audit (4 deferrals scoped; estimated 0-4 substrate extensions for long-tail vs kickoff halt threshold of >8)".
