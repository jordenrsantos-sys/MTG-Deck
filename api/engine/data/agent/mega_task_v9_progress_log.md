# Mega-task v9 / Pillar D iter 10 — Pillar F v0.2 sub-mega-task A progress log

Iter 10 dispatch on top of v8 ship (`a6845fd7e`). Kickoff:
`mega_task_v9_kickoff.md`. Sub-mega-task A only — core rules engine
substrate. B + C dispatch separately in iter 11+.

Append-only, timestamped sections per phase.

---

## Phase 0 — Pre-flight + scoping deep-read + module skeleton (2026-05-23)

**Substrate snapshot:**
- HEAD: `a6845fd7e` (v8 Phase 8 — SHIPPED at 5/5 sweep / 7/7 gates).
- pytest baseline (from v8 ship): 1628 / 25 skipped / 0 failed.
- vitest baseline (from v8 ship): 774 / 2 pre-existing failed.
- Pillar F v0.1 location: `api/engine/layers/agent_statistical_approximator_v1.py`
  (UNCHANGED — sub-mega-task C will eventually wire Stage 2 alongside).
- MTG Comprehensive Rules text:
  `E:/MTG Root/Mtg deck building brain/01_RULES_SOURCE/source_documents/MagicCompRules_20260417.txt`
  (1MB; already embedded into Voyage index per v6 Phase 6 at 667 sections).

**Context read (3 of 6 confirmed accessible):**
- `MTG-Deck-Builder-Claude/pillar_f_v0_2_game_engine_scoping.md` — source of truth (authored at end of v8).
- `MTG-Deck-Builder-Claude/mega_task_v8_final_report.md` — iter-9 ship state + hand-offs.
- `project_mega_task_v8_shipped.md` — iter-9 outcomes + 10 load-bearing invariants.
- 3 referenced memories NOT in current memory dir (`project_phase_5b_substrate_blocker`,
  `project_graduated_playtest_spec_2026-05-21`, `project_pillar_a_c_shipped_2026-05-17`).
  Substance covered in the scoping doc + kickoff itself; proceeding.

**Module scaffold created** at `repo/api/engine/pillar_f/v0_2/`:
```
v0_2/
  __init__.py
  state/__init__.py        — Phase 1
  stack/__init__.py        — Phase 2
  turn/__init__.py         — Phase 3
  replacement/__init__.py  — Phase 4
  layers/__init__.py       — Phase 5
  combat/__init__.py       — Phase 6
```
Plus `repo/tests/pillar_f_v0_2/__init__.py` for the test tree.

**Phase 0 deliverables:**
- Progress log scaffolded (this file).
- Task list created (10 phases, this session).
- Module scaffold committed.

**Phase 0 cost:** $0 (no LLM calls).

**Commit message:** "Phase 0 (mega-task v9): pre-flight + Pillar F v0.2 module scaffold".

Committed as `6a76fcb4d`.

---

## Phase 1 — Game-state object model + serialization (2026-05-23)

**Implementation** in `api/engine/pillar_f/v0_2/state/`:

1. **card.py** — `Card` dataclass per scoping doc section (a). Identity
   fields (name, oracle_id, mana_cost, cmc, type_line, subtypes,
   oracle_text, power, toughness, loyalty, colors, color_identity,
   keywords, owner) + mutable state (controller, face_down, tapped,
   summoning_sick, damage_marked, counters, attached_to, attached_by,
   card_id). `card_id` is UUID4 hex prefix (12 chars). Helpers:
   is_creature, is_land, is_planeswalker, is_legendary, has_keyword,
   power_int (handles `*` CDA), toughness_int, to_dict/from_dict,
   to_opaque.

2. **zones.py** — `PlayerZones` dataclass with 6 zones (hand, library,
   battlefield, graveyard, exile, command). Zones hold card_ids
   (Card objects live in GameState.cards_by_id by id). Library index 0
   = top. API: all_card_ids, find_zone, remove_card, add_card (with
   `to_top` for library), to_dict/from_dict.

3. **player.py** — `ManaPool` (W/U/B/R/G/C buckets + empty/total/dict),
   `PlayerState` (player_id, name, life_total=40, zones, mana_pool,
   commander_damage_taken_from {oracle_id → int}, lands_played_this_turn,
   cards_drawn_this_turn, spells_cast_this_turn,
   priority_passed_this_round, has_lost, has_drawn_from_empty_library,
   politics_state {sub-mega-task B slot}).

4. **state.py** — `GameState` aggregating players, cards_by_id, global
   turn state (turn_number, Phase enum, Step enum, active_player,
   priority_holder, priority_passes_this_round Set, stack), optional
   designations (the_monarch, the_initiative, day_or_night DayNight enum),
   effect registries (replacement_effects, continuous_effects,
   delayed_triggers_pending), commander_card_ids, game-result fields.
   `StackEntry`, `ReplacementEffect`, `ContinuousEffect` dataclasses
   defined here (used by Phases 2/4/5).

   API:
   - `get_card(card_id)`, `add_card(card)`, `move_card(card_id,
     from_player, from_zone, to_player, to_zone, to_top=False)`
   - `to_dict()`, `to_json()`, `from_dict(d)`, `from_json(s)`
   - `perspective_view(viewer_player_id)` redacts hidden info:
     * Opponents' hand cards → opaque markers.
     * ALL libraries (including viewer's) → opaque markers (CR: nobody
       sees library order).
     * Face-down battlefield/exile cards → opaque (except to controller).
     * Stack + battlefield (face-up) + graveyard + exile (face-up) +
       command zone fully visible.
   - STATE_VERSION = "pillar_f_v0_2_state_v1" for JSON forward-compat.

5. **state/\_\_init\_\_.py** — exposes public API for Phases 2-6.

**Tests** in `tests/pillar_f_v0_2/test_phase1_state.py`:

22 tests across 6 classes:
- **CardModelTests** (5): card_id uniqueness, creature/legendary/land
  detection, keyword case-insensitive, power_int handles `*`, controller
  defaults to owner.
- **ZonesMutationTests** (5): move_card between zones, move with
  controller change, raise on missing card, library to_top semantics,
  find_zone correctness.
- **CommanderDamageTests** (2): per-commander damage tracking,
  commander_card_ids per player.
- **ManaPoolTests** (2): empty clears pool, dict round-trip.
- **JsonRoundTripTests** (2): full 4-player game state round-trip
  (turn_number, phase, step, active_player, monarch, day_or_night,
  3-deep stack, zones); per-card mutable state round-trip
  (tapped/damage_marked/counters/summoning_sick).
- **PerspectiveViewTests** (6): opponent hand opaque, ALL libraries
  opaque (incl. viewer's), battlefield public, face-down
  battlefield opaque to non-controller, viewer_player_id carried,
  stack public.

**All 22 pass.** Total ~600 LOC across 4 production files + 320 LOC test
file. Pure data modeling — no game logic yet.

**Commit message:** "Phase 1 (mega-task v9): game-state object model + perspective_view + serialization".
