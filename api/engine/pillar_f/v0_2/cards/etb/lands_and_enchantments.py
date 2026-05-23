"""ETB triggers on non-creature permanents — utility lands + enchantments.

Bojuka Bog (graveyard exile on ETB) is the standout. Garruk's Uprising
fires conditionally on ETB ("if you control a creature with power 4 or
greater, draw a card"); we wire the optional-draw trigger plus the
ongoing "creatures you control have trample" static effect lands in
the continuous bucket (Phase 4) — this module only does the ETB side.

Knight of the White Orchid's conditional ETB ("if an opponent controls
more lands than you, you may search...") is the same conditional-on-
ETB pattern — included here.

Cards covered:
  - Bojuka Bog (exile target player's graveyard)
  - Garruk's Uprising (ETB conditional draw)
  - Knight of the White Orchid (ETB conditional land tutor — only
    the conditional draw side; the first-strike keyword + ongoing
    static are printed/Phase 4 work)
"""
from __future__ import annotations

from typing import Any, Dict, List

from api.engine.pillar_f.v0_2.cards.etb.framework import (
    register_etb_trigger, build_etb_trigger_dict,
)
from api.engine.pillar_f.v0_2.replacement import EnterBattlefieldEvent
from api.engine.pillar_f.v0_2.stack import register_resolver
from api.engine.pillar_f.v0_2.state import GameState, StackEntry


# ==========================================================
# Bojuka Bog — "When this land enters, exile target player's graveyard."
# ==========================================================


def _bojuka_bog_resolve(state: GameState, entry: StackEntry) -> None:
    """`targets[0]` is the player_id whose graveyard gets exiled.
    Implementation: move every card from that player's graveyard to
    their exile zone."""
    if not entry.targets:
        return None
    target_pid = entry.targets[0]
    if not isinstance(target_pid, int):
        return None
    if not (0 <= target_pid < len(state.players)):
        return None
    ps = state.players[target_pid]
    # Snapshot to avoid mutating-while-iterating.
    gy_snapshot = list(ps.zones.graveyard)
    for cid in gy_snapshot:
        state.move_card(
            cid,
            from_player=target_pid, from_zone="graveyard",
            to_player=target_pid, to_zone="exile",
        )
    return None


register_resolver("etb_bojuka_bog", _bojuka_bog_resolve)


def _bojuka_bog_etb_trigger(
    state: GameState, etb_event: EnterBattlefieldEvent,
) -> List[Dict[str, Any]]:
    """Stub target picker: first opponent with a non-empty graveyard,
    else first opponent. Iter-11+ plumbs the strategic choice (typically
    "biggest threat in graveyard")."""
    pid = etb_event.controller
    target_pid = None
    for ps in state.players:
        if ps.player_id == pid or ps.has_lost:
            continue
        if ps.zones.graveyard:
            target_pid = ps.player_id
            break
    if target_pid is None:
        # No opponent with a graveyard — pick first non-controller
        # opponent for the "trigger fires, picks legal target" case.
        for ps in state.players:
            if ps.player_id == pid or ps.has_lost:
                continue
            target_pid = ps.player_id
            break
    if target_pid is None:
        return []
    return [build_etb_trigger_dict(
        etb_event, resolver="etb_bojuka_bog",
        targets=[target_pid],
        description="Bojuka Bog ETB: exile target player's graveyard",
    )]


register_etb_trigger("Bojuka Bog", _bojuka_bog_etb_trigger)


# ==========================================================
# Garruk's Uprising — "When this enchantment enters, if you control a
# creature with power 4 or greater, draw a card."
# Plus an ongoing static (in Phase 4 — continuous bucket).
# ==========================================================


def _garruks_uprising_resolve(state: GameState, entry: StackEntry) -> None:
    """Draw 1 if the controller controls a creature with power ≥ 4 at
    resolution time (CR 603.4 — intervening-if check at resolution)."""
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    # Recheck the intervening-if condition.
    has_big = False
    for cid in state.players[pid].zones.battlefield:
        card = state.get_card(cid)
        if card is None:
            continue
        if card.is_creature() and card.power_int() >= 4:
            has_big = True
            break
    if not has_big:
        return None
    # Draw 1.
    lib = state.players[pid].zones.library
    if not lib:
        state.players[pid].has_drawn_from_empty_library = True
        return None
    cid = lib.pop(0)
    state.players[pid].zones.hand.append(cid)
    state.players[pid].cards_drawn_this_turn += 1
    return None


register_resolver("etb_garruks_uprising", _garruks_uprising_resolve)


def _garruks_uprising_etb_trigger(
    state: GameState, etb_event: EnterBattlefieldEvent,
) -> List[Dict[str, Any]]:
    """Check the intervening-if condition at TRIGGER time (CR 603.4 —
    the condition is checked twice: once when the trigger event happens
    and once on resolution). If the condition isn't met at trigger time,
    the trigger doesn't fire at all."""
    pid = etb_event.controller
    if not (0 <= pid < len(state.players)):
        return []
    has_big = False
    for cid in state.players[pid].zones.battlefield:
        card = state.get_card(cid)
        if card is None:
            continue
        if card.is_creature() and card.power_int() >= 4:
            has_big = True
            break
    if not has_big:
        return []
    return [build_etb_trigger_dict(
        etb_event, resolver="etb_garruks_uprising",
        description="Garruk's Uprising ETB: draw 1 if pow≥4 creature",
    )]


register_etb_trigger("Garruk's Uprising", _garruks_uprising_etb_trigger)


# ==========================================================
# Knight of the White Orchid — "First strike. When this enters, if an
# opponent controls more lands than you, you may search your library
# for a Plains card, put it onto the battlefield, then shuffle."
# (First strike is already in printed keywords; this module wires the
# ETB tutor.)
# ==========================================================


def _knight_white_orchid_resolve(state: GameState, entry: StackEntry) -> None:
    """If condition still holds (intervening-if at resolution): tutor
    a Plains from library to battlefield (tapped or untapped — Oracle
    says onto battlefield, default untapped)."""
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    my_lands = sum(
        1 for cid in state.players[pid].zones.battlefield
        if state.get_card(cid) and state.get_card(cid).is_land()
    )
    opp_lands_max = 0
    for ps in state.players:
        if ps.player_id == pid:
            continue
        ol = sum(
            1 for cid in ps.zones.battlefield
            if state.get_card(cid) and state.get_card(cid).is_land()
        )
        if ol > opp_lands_max:
            opp_lands_max = ol
    if opp_lands_max <= my_lands:
        return None
    # Find a Plains in library.
    for cid in state.players[pid].zones.library:
        card = state.get_card(cid)
        if card is None:
            continue
        if card.name == "Plains" or "Plains" in (card.subtypes or []):
            state.move_card(
                cid, from_player=pid, from_zone="library",
                to_player=pid, to_zone="battlefield",
            )
            return None
    return None


register_resolver("etb_knight_white_orchid", _knight_white_orchid_resolve)


def _knight_white_orchid_etb_trigger(
    state: GameState, etb_event: EnterBattlefieldEvent,
) -> List[Dict[str, Any]]:
    """Intervening-if at trigger time."""
    pid = etb_event.controller
    if not (0 <= pid < len(state.players)):
        return []
    my_lands = sum(
        1 for cid in state.players[pid].zones.battlefield
        if state.get_card(cid) and state.get_card(cid).is_land()
    )
    for ps in state.players:
        if ps.player_id == pid:
            continue
        ol = sum(
            1 for cid in ps.zones.battlefield
            if state.get_card(cid) and state.get_card(cid).is_land()
        )
        if ol > my_lands:
            return [build_etb_trigger_dict(
                etb_event, resolver="etb_knight_white_orchid",
                description="Knight of White Orchid ETB: tutor Plains",
            )]
    return []


register_etb_trigger("Knight of the White Orchid",
                    _knight_white_orchid_etb_trigger)
