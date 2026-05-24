"""Phase 6 — Spell-cast triggers.

Rhystic Study, Esper Sentinel, Smothering Tithe (opp-draws-trigger
variant, technically), Beast Whisperer, Aetherflux Reservoir,
Hullbreaker Horror (partial).

Each watches the v11-shim `SpellCastEvent` and fires a trigger if the
caster + spell match the card's condition.
"""
from __future__ import annotations

from typing import Any, Dict, List

from api.engine.pillar_f.v0_2.cards.triggered.framework import (
    register_event_trigger, SpellCastEvent,
)
from api.engine.pillar_f.v0_2.replacement import DrawEvent, Event
from api.engine.pillar_f.v0_2.stack import register_resolver
from api.engine.pillar_f.v0_2.state import GameState, StackEntry


# ==========================================================
# Rhystic Study — "Whenever an opponent casts a spell, you may draw a
# card unless that player pays {1}."
# Iter-10 stub: opp doesn't pay → controller draws.
# ==========================================================


def _rhystic_study_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    lib = state.players[pid].zones.library
    if not lib:
        state.players[pid].has_drawn_from_empty_library = True
        return None
    state.players[pid].zones.hand.append(lib.pop(0))
    state.players[pid].cards_drawn_this_turn += 1
    return None


register_resolver("trig_rhystic_study", _rhystic_study_resolve)


def _rhystic_study_cast_trigger(
    state: GameState, event: Event,
) -> List[Dict[str, Any]]:
    if not isinstance(event, SpellCastEvent):
        return []
    for ps in state.players:
        for cid in ps.zones.battlefield:
            card = state.get_card(cid)
            if card is None or card.name != "Rhystic Study":
                continue
            # Trigger only if caster ≠ Rhystic's controller.
            if event.caster_player_id == ps.player_id:
                return []
            return [{
                "source_card_id": cid,
                "controller": ps.player_id,
                "resolver": "trig_rhystic_study",
                "targets": [],
                "payment": {},
                "description": "Rhystic Study: opp cast, you draw (unless they pay 1)",
            }]
    return []


register_event_trigger("SpellCastEvent", "Rhystic Study",
                       _rhystic_study_cast_trigger)


# ==========================================================
# Esper Sentinel — "Whenever an opponent casts their first NONCREATURE
# spell each turn, draw a card unless that player pays {X} (X =
# Sentinel's power)."
# ==========================================================


def _esper_sentinel_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    lib = state.players[pid].zones.library
    if not lib:
        state.players[pid].has_drawn_from_empty_library = True
        return None
    state.players[pid].zones.hand.append(lib.pop(0))
    state.players[pid].cards_drawn_this_turn += 1
    return None


register_resolver("trig_esper_sentinel", _esper_sentinel_resolve)


def _esper_sentinel_cast_trigger(
    state: GameState, event: Event,
) -> List[Dict[str, Any]]:
    if not isinstance(event, SpellCastEvent):
        return []
    if "Creature" in (event.spell_types or []):
        return []
    for ps in state.players:
        for cid in ps.zones.battlefield:
            card = state.get_card(cid)
            if card is None or card.name != "Esper Sentinel":
                continue
            if event.caster_player_id == ps.player_id:
                return []
            # "First noncreature spell each turn" tracking — stash a
            # per-turn flag.
            flag = f"esper_first_noncreature_{state.turn_number}_{event.caster_player_id}"
            if ps.politics_state.get(flag):
                return []
            ps.politics_state[flag] = True
            return [{
                "source_card_id": cid,
                "controller": ps.player_id,
                "resolver": "trig_esper_sentinel",
                "targets": [],
                "payment": {},
                "description": "Esper Sentinel: opp 1st noncreature, you draw",
            }]
    return []


register_event_trigger("SpellCastEvent", "Esper Sentinel",
                       _esper_sentinel_cast_trigger)


# ==========================================================
# Beast Whisperer — "Whenever you cast a creature spell, draw a card."
# ==========================================================


def _beast_whisperer_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    lib = state.players[pid].zones.library
    if not lib:
        state.players[pid].has_drawn_from_empty_library = True
        return None
    state.players[pid].zones.hand.append(lib.pop(0))
    state.players[pid].cards_drawn_this_turn += 1
    return None


register_resolver("trig_beast_whisperer", _beast_whisperer_resolve)


def _beast_whisperer_cast_trigger(
    state: GameState, event: Event,
) -> List[Dict[str, Any]]:
    if not isinstance(event, SpellCastEvent):
        return []
    if "Creature" not in (event.spell_types or []):
        return []
    for ps in state.players:
        for cid in ps.zones.battlefield:
            card = state.get_card(cid)
            if card is None or card.name != "Beast Whisperer":
                continue
            # Trigger only when CONTROLLER of Beast Whisperer is caster.
            if event.caster_player_id != ps.player_id:
                continue
            return [{
                "source_card_id": cid,
                "controller": ps.player_id,
                "resolver": "trig_beast_whisperer",
                "targets": [],
                "payment": {},
                "description": "Beast Whisperer: you cast creature, draw 1",
            }]
    return []


register_event_trigger("SpellCastEvent", "Beast Whisperer",
                       _beast_whisperer_cast_trigger)


# ==========================================================
# Aetherflux Reservoir — "Whenever you cast a spell, you gain 1 life
# for each spell you've cast this turn."
# ==========================================================


def _aetherflux_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    spells_this_turn = state.players[pid].spells_cast_this_turn
    # Aetherflux's text: gain life equal to spells cast this turn.
    # spells_cast_this_turn counter is incremented by the substrate's
    # cast-pipeline at iter-12+; for iter-10 tests, the test sets the
    # counter manually before resolving.
    state.players[pid].life_total += max(1, spells_this_turn)
    return None


register_resolver("trig_aetherflux", _aetherflux_resolve)


def _aetherflux_cast_trigger(
    state: GameState, event: Event,
) -> List[Dict[str, Any]]:
    if not isinstance(event, SpellCastEvent):
        return []
    for ps in state.players:
        for cid in ps.zones.battlefield:
            card = state.get_card(cid)
            if card is None or card.name != "Aetherflux Reservoir":
                continue
            if event.caster_player_id != ps.player_id:
                continue
            return [{
                "source_card_id": cid,
                "controller": ps.player_id,
                "resolver": "trig_aetherflux",
                "targets": [],
                "payment": {},
                "description": "Aetherflux: gain life = spells cast this turn",
            }]
    return []


register_event_trigger("SpellCastEvent", "Aetherflux Reservoir",
                       _aetherflux_cast_trigger)


# ==========================================================
# Smothering Tithe — "Whenever an opponent draws a card, that player
# may pay {2}. If the player doesn't, you create a Treasure token."
# This fires on DrawEvent, not SpellCastEvent.
# ==========================================================


def _smothering_tithe_resolve(state: GameState, entry: StackEntry) -> None:
    """Iter-10 stub: opp doesn't pay → controller creates Treasure
    token."""
    from api.engine.pillar_f.v0_2.state import Card
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    token = Card(
        name="Treasure Token", owner=pid, controller=pid,
        type_line="Token Artifact — Treasure",
        subtypes=["Treasure"],
    )
    state.add_card(token)
    state.players[pid].zones.battlefield.append(token.card_id)
    return None


register_resolver("trig_smothering_tithe", _smothering_tithe_resolve)


def _smothering_tithe_draw_trigger(
    state: GameState, event: Event,
) -> List[Dict[str, Any]]:
    if not isinstance(event, DrawEvent):
        return []
    drawing_pid = event.player_id
    for ps in state.players:
        for cid in ps.zones.battlefield:
            card = state.get_card(cid)
            if card is None or card.name != "Smothering Tithe":
                continue
            if drawing_pid == ps.player_id:
                # Don't fire when controller draws.
                continue
            return [{
                "source_card_id": cid,
                "controller": ps.player_id,
                "resolver": "trig_smothering_tithe",
                "targets": [],
                "payment": {},
                "description": "Smothering Tithe: opp drew, you create Treasure",
            }]
    return []


register_event_trigger("DrawEvent", "Smothering Tithe",
                       _smothering_tithe_draw_trigger)
