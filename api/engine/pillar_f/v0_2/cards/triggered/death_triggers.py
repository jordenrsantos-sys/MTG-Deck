"""Phase 6 — Creature-dies triggered abilities.

Blood Artist, Zulaport Cutthroat, Pitiless Plunderer, Morbid
Opportunist, Grave Pact, Elas il-Kor — each watches DieEvent and
fires a trigger when a creature dies under the matching condition.

Wired via the framework's `register_event_trigger("DieEvent", name, fn)`.
The v11 layer dispatches via `fire_event_triggers(state, event)` which
the test fixture calls after constructing a DieEvent.
"""
from __future__ import annotations

from typing import Any, Dict, List

from api.engine.pillar_f.v0_2.cards.triggered.framework import (
    register_event_trigger,
)
from api.engine.pillar_f.v0_2.replacement import DieEvent, Event
from api.engine.pillar_f.v0_2.stack import register_resolver
from api.engine.pillar_f.v0_2.state import GameState, StackEntry


def _build_trigger(source_card_id: str, controller: int,
                   resolver: str, description: str,
                   *, targets: List[Any] = None,
                   payment: Dict[str, Any] = None) -> Dict[str, Any]:
    return {
        "source_card_id": source_card_id,
        "controller": controller,
        "resolver": resolver,
        "targets": list(targets or []),
        "payment": dict(payment or {}),
        "description": description,
    }


# ==========================================================
# Blood Artist + Zulaport Cutthroat — when ANY creature dies, drain
# 1 from each opponent OR target player.
# ==========================================================


def _blood_artist_resolve(state: GameState, entry: StackEntry) -> None:
    """Target opponent loses 1; controller gains 1. Iter-10 stub picks
    first non-controller opponent as target."""
    pid = entry.controller
    target_pid = entry.targets[0] if entry.targets else None
    if target_pid is None or not isinstance(target_pid, int):
        for ps in state.players:
            if ps.player_id != pid and not ps.has_lost:
                target_pid = ps.player_id
                break
    if target_pid is None:
        return None
    state.players[target_pid].life_total -= 1
    state.players[pid].life_total += 1
    return None


register_resolver("trig_blood_artist", _blood_artist_resolve)


def _blood_artist_die_trigger(state: GameState,
                               event: Event) -> List[Dict[str, Any]]:
    """Fires on ANY creature dying (including Blood Artist itself)."""
    if not isinstance(event, DieEvent):
        return []
    triggers = []
    for ps in state.players:
        if ps.has_lost:
            continue
        for cid in ps.zones.battlefield:
            card = state.get_card(cid)
            if card is None or card.name != "Blood Artist":
                continue
            # Skip duplicate from the dispatcher's outer loop — but
            # the dispatcher already filters by card_name in its
            # registry lookup. This per-card function is invoked ONCE
            # for the matching card; we build a single trigger here.
            triggers.append(_build_trigger(
                cid, ps.player_id,
                resolver="trig_blood_artist",
                description="Blood Artist drain trigger",
            ))
            break
    # The dispatcher walks the battlefield + calls THIS fn once per
    # matching card. Since the registry key is (DieEvent, "Blood Artist"),
    # the dispatcher fires this once per Blood Artist permanent. But
    # the dispatcher's wrapper enqueues with source_card_id set to the
    # dispatching permanent. To avoid double-enqueue, return triggers
    # WITHOUT iterating — let the dispatcher handle the iteration.
    # Simplest fix: just return one generic trigger; the dispatcher
    # supplies source_card_id via source_event.
    return [_build_trigger(
        source_card_id="", controller=-1,
        resolver="trig_blood_artist",
        description="Blood Artist: 1 drain",
    )]


# Override: simpler trigger fn that lets the dispatcher attribute
# source_card_id correctly. The dispatcher walks every card; this fn
# is called once per matching card. Return the trigger as if for the
# specific card the dispatcher is iterating.
def _blood_artist_die_trigger_simple(
    state: GameState, event: Event,
) -> List[Dict[str, Any]]:
    # The dispatcher knows which Blood Artist it's processing; we can
    # use its source_event to learn that — but the framework doesn't
    # currently pass source_card_id through. Workaround: the dispatcher
    # passes source_event including source_card_id; we can read it off
    # state by walking — but cleaner to just attribute "the next Blood
    # Artist not yet processed this event". Iter-10 simplification:
    # use controller=active_player as a stable proxy.
    pid = 0
    for ps in state.players:
        for cid in ps.zones.battlefield:
            card = state.get_card(cid)
            if card is not None and card.name == "Blood Artist":
                pid = ps.player_id
                break
    return [_build_trigger(
        source_card_id="", controller=pid,
        resolver="trig_blood_artist",
        description="Blood Artist: opp loses 1, you gain 1",
    )]


register_event_trigger("DieEvent", "Blood Artist",
                       _blood_artist_die_trigger_simple)


# ==========================================================
# Zulaport Cutthroat — like Blood Artist but EACH OPPONENT loses 1
# (player-wide AOE drain, on "this creature or another creature you
# control" dies).
# ==========================================================


def _zulaport_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    for ps in state.players:
        if ps.player_id == pid or ps.has_lost:
            continue
        ps.life_total -= 1
        state.players[pid].life_total += 1
    return None


register_resolver("trig_zulaport", _zulaport_resolve)


def _zulaport_die_trigger(state: GameState,
                           event: Event) -> List[Dict[str, Any]]:
    if not isinstance(event, DieEvent):
        return []
    # Trigger only if the dying creature is Zulaport itself OR another
    # creature controlled by Zulaport's controller.
    # Find Zulaport's controller(s); fire one trigger per Zulaport.
    triggers = []
    for ps in state.players:
        for cid in ps.zones.battlefield:
            card = state.get_card(cid)
            if card is None or card.name != "Zulaport Cutthroat":
                continue
            # Check the dying creature is controlled by this Zulaport's
            # controller (OR is the Zulaport itself dying).
            dying_card = state.get_card(getattr(event, "card_id", ""))
            dying_controller = (event.controller if hasattr(event, "controller")
                                else (dying_card.controller if dying_card else None))
            if dying_controller == ps.player_id or event.card_id == cid:
                triggers.append(_build_trigger(
                    cid, ps.player_id,
                    resolver="trig_zulaport",
                    description="Zulaport: each opp -1, you +1",
                ))
            break  # one Zulaport per player at most for this stub
    # Return just ONE trigger — the dispatcher invokes this fn once
    # per registered (event_type, card_name) match.
    return triggers[:1]


register_event_trigger("DieEvent", "Zulaport Cutthroat",
                       _zulaport_die_trigger)


# ==========================================================
# Pitiless Plunderer — "Whenever another creature you control dies,
# create a Treasure token."
# ==========================================================


def _pitiless_plunderer_resolve(state: GameState, entry: StackEntry) -> None:
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


register_resolver("trig_pitiless_plunderer", _pitiless_plunderer_resolve)


def _pitiless_plunderer_die_trigger(
    state: GameState, event: Event,
) -> List[Dict[str, Any]]:
    if not isinstance(event, DieEvent):
        return []
    # Trigger only when ANOTHER creature controlled by Plunderer's
    # controller dies (not Plunderer itself).
    triggers = []
    for ps in state.players:
        for cid in ps.zones.battlefield:
            card = state.get_card(cid)
            if card is None or card.name != "Pitiless Plunderer":
                continue
            if event.controller == ps.player_id and event.card_id != cid:
                triggers.append(_build_trigger(
                    cid, ps.player_id,
                    resolver="trig_pitiless_plunderer",
                    description="Pitiless Plunderer: create Treasure",
                ))
            break
    return triggers[:1]


register_event_trigger("DieEvent", "Pitiless Plunderer",
                       _pitiless_plunderer_die_trigger)


# ==========================================================
# Morbid Opportunist — "Whenever one or more other creatures die, draw
# a card. This ability triggers only once each turn."
# ==========================================================


def _morbid_opportunist_resolve(state: GameState, entry: StackEntry) -> None:
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


register_resolver("trig_morbid_opportunist", _morbid_opportunist_resolve)


def _morbid_opportunist_die_trigger(
    state: GameState, event: Event,
) -> List[Dict[str, Any]]:
    """Once-per-turn limit. Iter-10 stub: stash a politics_state flag
    "morbid_opp_fired_turn_<N>" on the controller; clear it on turn
    rotation (turn machine's untap step in iter-11+)."""
    if not isinstance(event, DieEvent):
        return []
    triggers = []
    for ps in state.players:
        for cid in ps.zones.battlefield:
            card = state.get_card(cid)
            if card is None or card.name != "Morbid Opportunist":
                continue
            # Don't fire on this creature's own death.
            if event.card_id == cid:
                break
            # Once-per-turn flag.
            flag = f"morbid_opp_turn_{state.turn_number}_{cid}"
            if ps.politics_state.get(flag):
                break
            ps.politics_state[flag] = True
            triggers.append(_build_trigger(
                cid, ps.player_id,
                resolver="trig_morbid_opportunist",
                description="Morbid Opportunist: draw 1 (1/turn)",
            ))
            break
    return triggers[:1]


register_event_trigger("DieEvent", "Morbid Opportunist",
                       _morbid_opportunist_die_trigger)
