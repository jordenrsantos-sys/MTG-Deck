"""Phase 7 — Mass removal + protection spells.

  Mass removal:
    - Toxic Deluge (all creatures get -X/-X)
    - Blasphemous Act (13 damage to each creature; cost reduction is
      a static-modifier deferral)
    - Cyclonic Rift (single bounce; overload bounces all opp nonland —
      iter-10 ships the single-bounce side)
    - Vandalblast (destroy target artifact; overload destroys all opp
      artifacts — iter-10 ships the single side)

  Protection:
    - Heroic Intervention (permanents you control gain hexproof + indest
      until EOT)
    - Deflecting Swat (change targets — iter-10 stub: counter target
      spell for testing convenience)
"""
from __future__ import annotations

from api.engine.pillar_f.v0_2.cards.spell.framework import register_spell
from api.engine.pillar_f.v0_2.stack import counter_target
from api.engine.pillar_f.v0_2.state import GameState, StackEntry


# ==========================================================
# Toxic Deluge — "As an additional cost to cast, pay X life. All
# creatures get -X/-X until end of turn."
# Iter-10 stub: X is read from payment["x"]; caller pays the X life
# cost. Resolver applies -X/-X to all creatures (immediate Card.power/
# toughness mutation; until-EOT cleanup is iter-11+).
# ==========================================================


def _toxic_deluge_resolve(state: GameState, entry: StackEntry) -> None:
    x = int(entry.payment.get("x", 0))
    if x <= 0:
        return None
    for ps in state.players:
        for cid in ps.zones.battlefield:
            card = state.get_card(cid)
            if card is None or not card.is_creature():
                continue
            try:
                card.power = str(card.power_int() - x)
                card.toughness = str(card.toughness_int() - x)
            except (TypeError, ValueError):
                pass
    return None


register_spell("Toxic Deluge", _toxic_deluge_resolve)


# ==========================================================
# Blasphemous Act — "Deals 13 damage to each creature."
# ==========================================================


def _blasphemous_act_resolve(state: GameState, entry: StackEntry) -> None:
    """Each creature takes 13 damage → SBA cleanup moves them to GY
    if toughness ≤ 13 (which is every creature except Eldrazi titans).
    Iter-10 stub: directly move to GY if damage exceeds toughness."""
    for ps in state.players:
        for cid in list(ps.zones.battlefield):
            card = state.get_card(cid)
            if card is None or not card.is_creature():
                continue
            card.damage_marked += 13
            # SBA check — toughness ≤ damage → dies.
            if card.toughness_int() <= card.damage_marked:
                state.move_card(cid, from_player=ps.player_id,
                               from_zone="battlefield",
                               to_player=card.owner, to_zone="graveyard")
    return None


register_spell("Blasphemous Act", _blasphemous_act_resolve)


# ==========================================================
# Cyclonic Rift — "Return target nonland permanent you don't control
# to its owner's hand." (Overload deferred — iter-10 ships single target.)
# ==========================================================


def _cyclonic_rift_resolve(state: GameState, entry: StackEntry) -> None:
    if not entry.targets:
        return None
    target_cid = entry.targets[0]
    card = state.get_card(target_cid)
    if card is None or card.is_land():
        return None
    if card.controller == entry.controller:
        return None  # "you don't control"
    # Find the battlefield holder.
    for ps in state.players:
        if target_cid in ps.zones.battlefield:
            state.move_card(target_cid, from_player=ps.player_id,
                           from_zone="battlefield",
                           to_player=card.owner, to_zone="hand")
            return None
    return None


register_spell("Cyclonic Rift", _cyclonic_rift_resolve)


# ==========================================================
# Vandalblast — "Destroy target artifact you don't control."
# (Overload deferred.)
# ==========================================================


def _vandalblast_resolve(state: GameState, entry: StackEntry) -> None:
    if not entry.targets:
        return None
    target_cid = entry.targets[0]
    card = state.get_card(target_cid)
    if card is None or "artifact" not in card.type_line.lower():
        return None
    if card.controller == entry.controller:
        return None
    for ps in state.players:
        if target_cid in ps.zones.battlefield:
            state.move_card(target_cid, from_player=ps.player_id,
                           from_zone="battlefield",
                           to_player=card.owner, to_zone="graveyard")
            return None
    return None


register_spell("Vandalblast", _vandalblast_resolve)


# ==========================================================
# Heroic Intervention — "Permanents you control gain hexproof and
# indestructible until end of turn."
# Iter-10 stub: grant the keywords directly; "until end of turn"
# cleanup is iter-11+.
# ==========================================================


def _heroic_intervention_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    for cid in state.players[pid].zones.battlefield:
        card = state.get_card(cid)
        if card is None:
            continue
        for kw in ("hexproof", "indestructible"):
            if not card.has_keyword(kw):
                card.keywords.append(kw)
    return None


register_spell("Heroic Intervention", _heroic_intervention_resolve)


# ==========================================================
# Deflecting Swat — "If you control a commander, you may cast this
# without paying its mana cost. Change the target(s) of target spell
# or ability."
# Iter-10 stub: counter the targeted spell (a substantively different
# but testable behavior; iter-11+ adds proper target-rewriting). Mainly
# tests the "free spell if commander" pathway.
# ==========================================================


def _deflecting_swat_resolve(state: GameState, entry: StackEntry) -> None:
    if not entry.targets:
        return None
    # Stub: counter (real behavior would change targets).
    counter_target(state, entry.targets[0])
    return None


register_spell("Deflecting Swat", _deflecting_swat_resolve)
register_spell("Deadly Rollick", _deflecting_swat_resolve)  # commander-cast removal
