"""Phase 7 — Removal spells.

Swords to Plowshares, Path to Exile, Beast Within, Generous Gift,
Chaos Warp, Feed the Swarm, Doom Blade.

Each takes one target permanent (creature, or broader) + applies the
exile / destroy / bounce / shuffle action. Targets are passed via
StackEntry.targets[0].
"""
from __future__ import annotations

from typing import Optional

from api.engine.pillar_f.v0_2.cards.spell.framework import register_spell
from api.engine.pillar_f.v0_2.state import Card, GameState, StackEntry


def _find_battlefield_owner(state: GameState, card_id: str) -> Optional[int]:
    for ps in state.players:
        if card_id in ps.zones.battlefield:
            return ps.player_id
    return None


def _move_to_zone(state: GameState, card_id: str, to_zone: str) -> bool:
    """Move card from wherever it lives on the battlefield to its
    owner's zone. Returns True if moved."""
    card = state.get_card(card_id)
    if card is None:
        return False
    holder = _find_battlefield_owner(state, card_id)
    if holder is None:
        return False
    state.move_card(card_id, from_player=holder, from_zone="battlefield",
                   to_player=card.owner, to_zone=to_zone)
    return True


# ==========================================================
# Swords to Plowshares — "Exile target creature. Its controller gains
# life equal to its power."
# ==========================================================


def _stp_resolve(state: GameState, entry: StackEntry) -> None:
    if not entry.targets:
        return None
    target_cid = entry.targets[0]
    target = state.get_card(target_cid)
    if target is None or not target.is_creature():
        return None
    controller = target.controller
    power = target.power_int()
    if _move_to_zone(state, target_cid, "exile"):
        if 0 <= controller < len(state.players):
            state.players[controller].life_total += max(0, power)
    return None


register_spell("Swords to Plowshares", _stp_resolve)


# ==========================================================
# Path to Exile — "Exile target creature. Its controller may search
# their library for a basic land, put it onto battlefield tapped."
# Iter-10 stub: controller always searches (no choice plumbing).
# ==========================================================


def _path_resolve(state: GameState, entry: StackEntry) -> None:
    if not entry.targets:
        return None
    target_cid = entry.targets[0]
    target = state.get_card(target_cid)
    if target is None or not target.is_creature():
        return None
    controller = target.controller
    if not _move_to_zone(state, target_cid, "exile"):
        return None
    # Search controller's library for a basic land.
    if 0 <= controller < len(state.players):
        for cid in state.players[controller].zones.library:
            card = state.get_card(cid)
            if card is None:
                continue
            if card.is_land() and "Basic" in card.type_line:
                state.move_card(cid, from_player=controller,
                               from_zone="library",
                               to_player=controller, to_zone="battlefield")
                card.tapped = True
                break
    return None


register_spell("Path to Exile", _path_resolve)


# ==========================================================
# Beast Within — "Destroy target permanent. Its controller creates a
# 3/3 green Beast creature token."
# ==========================================================


def _beast_within_resolve(state: GameState, entry: StackEntry) -> None:
    if not entry.targets:
        return None
    target_cid = entry.targets[0]
    target = state.get_card(target_cid)
    if target is None:
        return None
    controller = target.controller
    if not _move_to_zone(state, target_cid, "graveyard"):
        return None
    if 0 <= controller < len(state.players):
        token = Card(name="Beast Token", owner=controller, controller=controller,
                     type_line="Token Creature — Beast",
                     power="3", toughness="3", colors=["G"], color_identity=["G"])
        state.add_card(token)
        state.players[controller].zones.battlefield.append(token.card_id)
    return None


register_spell("Beast Within", _beast_within_resolve)


# ==========================================================
# Generous Gift — "Destroy target permanent. Its controller creates a
# 3/3 green Elephant creature token."
# Same as Beast Within but Elephant token (white spell, but the token
# is the same shape).
# ==========================================================


def _generous_gift_resolve(state: GameState, entry: StackEntry) -> None:
    if not entry.targets:
        return None
    target_cid = entry.targets[0]
    target = state.get_card(target_cid)
    if target is None:
        return None
    controller = target.controller
    if not _move_to_zone(state, target_cid, "graveyard"):
        return None
    if 0 <= controller < len(state.players):
        token = Card(name="Elephant Token", owner=controller, controller=controller,
                     type_line="Token Creature — Elephant",
                     power="3", toughness="3", colors=["G"], color_identity=["G"])
        state.add_card(token)
        state.players[controller].zones.battlefield.append(token.card_id)
    return None


register_spell("Generous Gift", _generous_gift_resolve)


# ==========================================================
# Chaos Warp — "The owner of target permanent shuffles it into their
# library, then reveals the top card. If that card is a permanent,
# put it onto the battlefield."
# Iter-10 stub: shuffle target into library (insert at index 0 — top),
# then check top card → if permanent type, put onto battlefield.
# ==========================================================


def _chaos_warp_resolve(state: GameState, entry: StackEntry) -> None:
    if not entry.targets:
        return None
    target_cid = entry.targets[0]
    target = state.get_card(target_cid)
    if target is None:
        return None
    holder = _find_battlefield_owner(state, target_cid)
    if holder is None:
        return None
    state.move_card(target_cid, from_player=holder, from_zone="battlefield",
                   to_player=target.owner, to_zone="library")
    # Iter-10 stub: top of library = index 0 (no real shuffle here).
    lib = state.players[target.owner].zones.library
    if not lib:
        return None
    top_cid = lib[0]
    top_card = state.get_card(top_cid)
    if top_card is None:
        return None
    # If top card is a permanent type, put onto battlefield.
    permanent_types = ("creature", "artifact", "enchantment",
                       "land", "planeswalker", "battle")
    tl = (top_card.type_line or "").lower()
    if any(t in tl for t in permanent_types):
        state.move_card(top_cid, from_player=target.owner,
                       from_zone="library",
                       to_player=target.owner, to_zone="battlefield")
    return None


register_spell("Chaos Warp", _chaos_warp_resolve)


# ==========================================================
# Feed the Swarm — "Destroy target creature or enchantment an opponent
# controls. You lose life equal to that permanent's mana value."
# ==========================================================


def _feed_swarm_resolve(state: GameState, entry: StackEntry) -> None:
    if not entry.targets:
        return None
    target_cid = entry.targets[0]
    target = state.get_card(target_cid)
    if target is None:
        return None
    holder = _find_battlefield_owner(state, target_cid)
    if holder is None or holder == entry.controller:
        return None
    cmc = int(target.cmc or 0)
    if not _move_to_zone(state, target_cid, "graveyard"):
        return None
    pid = entry.controller
    if 0 <= pid < len(state.players):
        state.players[pid].life_total -= cmc
    return None


register_spell("Feed the Swarm", _feed_swarm_resolve)


# ==========================================================
# Doom Blade (and Go for the Throat) — destroy target creature
# (non-black for Doom Blade; we ship the generic destroy + skip the
# color check for iter-10 simplicity).
# ==========================================================


def _doom_blade_resolve(state: GameState, entry: StackEntry) -> None:
    if not entry.targets:
        return None
    target_cid = entry.targets[0]
    target = state.get_card(target_cid)
    if target is None or not target.is_creature():
        return None
    _move_to_zone(state, target_cid, "graveyard")
    return None


register_spell("Doom Blade", _doom_blade_resolve)
register_spell("Go for the Throat", _doom_blade_resolve)
