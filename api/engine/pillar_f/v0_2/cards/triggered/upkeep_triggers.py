"""Phase 6 — Upkeep / start-of-turn triggers.

Phyrexian Arena, Sylvan Library, Land Tax, Black Market Connections,
Mystic Remora (cumulative upkeep — partial).

These use the substrate's `register_step_trigger(Step.UPKEEP, ...)`
API. The v11 framework's `register_step_trigger_for_card` indexes the
attached triggers so LTB can detach cleanly.

For iter-10 the substrate's step-trigger registry is global; this
module's per-card registry IS the source of truth for which triggers
are "live" (i.e., the source card is on the battlefield). The
v11-local `attach_upkeep_triggers_for(state, card)` is called on ETB;
`detach_upkeep_triggers_for(state, card)` removes them on LTB.
"""
from __future__ import annotations

from typing import Dict, List

from api.engine.pillar_f.v0_2.cards.triggered.framework import (
    register_step_trigger_for_card, get_step_triggers_for_card,
)
from api.engine.pillar_f.v0_2.stack import register_resolver
from api.engine.pillar_f.v0_2.state import GameState, Step, StackEntry


# ==========================================================
# Phyrexian Arena — "At the beginning of your upkeep, you draw a card
# and you lose 1 life."
# ==========================================================


def _phyrexian_arena_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    # Only fire on YOUR upkeep — check active_player.
    if state.active_player != pid:
        return None
    lib = state.players[pid].zones.library
    if lib:
        state.players[pid].zones.hand.append(lib.pop(0))
        state.players[pid].cards_drawn_this_turn += 1
    else:
        state.players[pid].has_drawn_from_empty_library = True
    state.players[pid].life_total -= 1
    return None


register_resolver("trig_phyrexian_arena", _phyrexian_arena_resolve)


# Per-card upkeep-trigger registration helpers. The actual install into
# the substrate's step trigger registry happens via the test fixture
# (or iter-12+'s cast-and-ETB pipeline) calling this on ETB.

PHYREXIAN_ARENA_TRIGGER = {
    "source_card_id": "",  # filled in on ETB
    "controller": 0,        # filled in on ETB
    "resolver": "trig_phyrexian_arena",
    "payment": {},
    "description": "Phyrexian Arena: upkeep draw + 1 life loss",
}


# ==========================================================
# Sylvan Library — "At the beginning of your draw step, you may draw
# two additional cards. If you do, choose two cards in your hand drawn
# this turn. For each of those cards, you put it on top of your library
# OR pay 4 life."
#
# Iter-10 stub: always draws 2 extra + always picks the 4-life option
# (LLM strategic choice is iter-11+). Net: +2 cards, -8 life.
# ==========================================================


def _sylvan_library_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    if state.active_player != pid:
        return None
    lib = state.players[pid].zones.library
    drawn = 0
    for _ in range(2):
        if not lib:
            state.players[pid].has_drawn_from_empty_library = True
            break
        state.players[pid].zones.hand.append(lib.pop(0))
        state.players[pid].cards_drawn_this_turn += 1
        drawn += 1
    # Pay 4 life per extra card (iter-10 stub: pay rather than top).
    state.players[pid].life_total -= 4 * drawn
    return None


register_resolver("trig_sylvan_library", _sylvan_library_resolve)


SYLVAN_LIBRARY_TRIGGER = {
    "source_card_id": "", "controller": 0,
    "resolver": "trig_sylvan_library", "payment": {},
    "description": "Sylvan Library: draw 2 extra, pay 4 life each",
}


# ==========================================================
# Land Tax — "At the beginning of your upkeep, if an opponent controls
# more lands than you, you may search your library for up to three
# basic land cards, reveal them, put them into your hand, then shuffle."
# ==========================================================


def _land_tax_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    if state.active_player != pid:
        return None
    # Intervening-if check at resolution: opp must control MORE lands.
    my_lands = sum(
        1 for cid in state.players[pid].zones.battlefield
        if state.get_card(cid) and state.get_card(cid).is_land()
    )
    opp_max = 0
    for ps in state.players:
        if ps.player_id == pid:
            continue
        n = sum(
            1 for cid in ps.zones.battlefield
            if state.get_card(cid) and state.get_card(cid).is_land()
        )
        if n > opp_max:
            opp_max = n
    if opp_max <= my_lands:
        return None
    # Find up to 3 basics in library.
    found = 0
    lib = list(state.players[pid].zones.library)  # snapshot
    for cid in lib:
        if found >= 3:
            break
        card = state.get_card(cid)
        if card is None:
            continue
        if card.is_land() and "Basic" in card.type_line:
            state.move_card(
                cid, from_player=pid, from_zone="library",
                to_player=pid, to_zone="hand",
            )
            found += 1
    return None


register_resolver("trig_land_tax", _land_tax_resolve)


LAND_TAX_TRIGGER = {
    "source_card_id": "", "controller": 0,
    "resolver": "trig_land_tax", "payment": {},
    "description": "Land Tax: 3 basics if behind",
}


# ==========================================================
# Black Market Connections — at first main phase, choose one or more
# modes. Multi-modal trigger; iter-10 stub picks "Create a Treasure
# token" mode (cheapest).
# ==========================================================


def _bmc_resolve(state: GameState, entry: StackEntry) -> None:
    from api.engine.pillar_f.v0_2.state import Card
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    if state.active_player != pid:
        return None
    # Stub: Sell Contraband mode = Treasure + lose 1 life.
    token = Card(
        name="Treasure Token", owner=pid, controller=pid,
        type_line="Token Artifact — Treasure",
        subtypes=["Treasure"],
    )
    state.add_card(token)
    state.players[pid].zones.battlefield.append(token.card_id)
    state.players[pid].life_total -= 1
    return None


register_resolver("trig_black_market_connections", _bmc_resolve)


BMC_TRIGGER = {
    "source_card_id": "", "controller": 0,
    "resolver": "trig_black_market_connections", "payment": {},
    "description": "BMC: Treasure + 1 life (Sell Contraband mode)",
}


# ============================================================
# Pre-register the metadata for each upkeep-trigger card in the v11
# step-trigger index. This is purely a discoverability record — the
# substrate's actual step_trigger registration happens at
# install_upkeep_trigger() time (ETB). The pre-registration is so
# the Phase 9 coverage sweep + the LLM policy can see "this card has
# an upkeep trigger" without needing an instance on the battlefield.
# ============================================================


_UPKEEP_METADATA_CARDS = (
    "Phyrexian Arena", "Sylvan Library", "Land Tax",
    "Black Market Connections",
)
for _meta_name in _UPKEEP_METADATA_CARDS:
    register_step_trigger_for_card(_meta_name, {
        "metadata_only": True,
        "card_name": _meta_name,
    })


# ==========================================================
# attach / detach helpers
# ==========================================================


def install_upkeep_trigger(state: GameState, card_name: str, card_id: str,
                            controller: int) -> bool:
    """Install the upkeep trigger for a named card into the substrate's
    step-trigger registry. Mirrors the trigger dict into the v11 index
    so detach can find it later. Returns True if installed."""
    from api.engine.pillar_f.v0_2.turn import register_step_trigger as substrate_register
    trigger_map = {
        "Phyrexian Arena": (Step.UPKEEP, PHYREXIAN_ARENA_TRIGGER),
        "Sylvan Library": (Step.DRAW, SYLVAN_LIBRARY_TRIGGER),
        "Land Tax": (Step.UPKEEP, LAND_TAX_TRIGGER),
        "Black Market Connections": (Step.MAIN_1, BMC_TRIGGER),
    }
    if card_name not in trigger_map:
        return False
    step, template = trigger_map[card_name]
    trigger = dict(template)
    trigger["source_card_id"] = card_id
    trigger["controller"] = controller
    substrate_register(step, trigger)
    register_step_trigger_for_card(card_name, trigger)
    return True
