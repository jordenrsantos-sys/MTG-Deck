"""ETB creature triggers — per-card handlers for the in-top-500 ETB
creatures in the v9 corpus.

Each card registers two pieces:
  1. A `_<slug>_resolve(state, entry)` resolver fn — what happens when
     the trigger resolves off the stack. Wired into the substrate via
     `register_resolver`.
  2. A `_<slug>_etb_trigger(state, etb_event)` trigger-build fn — what
     trigger dict (or none) to enqueue when the card enters the
     battlefield. Wired into the v11 registry via `register_etb_trigger`.

Cards covered in this module:
  - Eternal Witness
  - Reclamation Sage
  - Gray Merchant of Asphodel
  - Craterhoof Behemoth
  - Plaguecrafter
  - Accursed Marauder
  - Ranger-Captain of Eos
  - Imperial Recruiter
  - Avenger of Zendikar

Skipped (moved to complex/ in Phase 8):
  - Phyrexian Metamorph + Sculpting Steel (copy-on-ETB; needs layer-1
    plumbing per card)
  - Mithril Coat (flash + ETB-attach; needs equip target plumbing)
  - Marionette Apprentice (fabricate choice — needs player choice)
  - Orcish Bowmasters (ETB + whenever-opponent-draws — multi-trigger)
  - Bastion of Remembrance (ETB + on-creature-death; multi-trigger)
  - Massacre Wurm + The Meathook Massacre (ETB + ongoing trigger)
  - Garruk's Uprising + Knight of the White Orchid (conditional ETB
    on game-state inspection — can do them here later)
  - Thassa's Oracle (combo finisher → spell bucket, Phase 7)

This module ships the "headline ETB only" handlers — the trigger fires
when the card enters; its specific effect resolves. Cards with multi-
ability interactions are deferred to Phase 8.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from api.engine.pillar_f.v0_2.cards.etb.framework import (
    register_etb_trigger, build_etb_trigger_dict,
)
from api.engine.pillar_f.v0_2.replacement import EnterBattlefieldEvent
from api.engine.pillar_f.v0_2.stack import register_resolver
from api.engine.pillar_f.v0_2.state import GameState, StackEntry


# ==========================================================
# Eternal Witness — "When this enters, you may return target card from
# your graveyard to your hand."
# ==========================================================


def _eternal_witness_resolve(state: GameState, entry: StackEntry) -> None:
    """`targets[0]` is the graveyard card_id to return. Move it from
    the controller's graveyard to the controller's hand. If the target
    is no longer in the graveyard (e.g., another effect moved it),
    do nothing — CR 608.2b: if a target becomes illegal, the ability's
    instructions that target the now-illegal target are skipped."""
    if not entry.targets:
        return None
    target_cid = entry.targets[0]
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    gy = state.players[pid].zones.graveyard
    if target_cid not in gy:
        return None
    state.move_card(
        target_cid,
        from_player=pid, from_zone="graveyard",
        to_player=pid, to_zone="hand",
    )
    return None


register_resolver("etb_eternal_witness", _eternal_witness_resolve)


def _eternal_witness_etb_trigger(
    state: GameState, etb_event: EnterBattlefieldEvent,
) -> List[Dict[str, Any]]:
    # Target = first card in controller's graveyard (deterministic stub —
    # iter-11+ LLM choice will pick the optimal target).
    pid = etb_event.controller
    gy = state.players[pid].zones.graveyard if 0 <= pid < len(state.players) else []
    targets = [gy[0]] if gy else []
    if not targets:
        # "may" — if no legal target, the optional trigger can be
        # declined; no stack entry needed.
        return []
    return [build_etb_trigger_dict(
        etb_event,
        resolver="etb_eternal_witness",
        targets=targets,
        description="Eternal Witness ETB: return target graveyard card",
    )]


register_etb_trigger("Eternal Witness", _eternal_witness_etb_trigger)


# ==========================================================
# Reclamation Sage — "When this enters, you may destroy target artifact
# or enchantment."
# ==========================================================


def _reclamation_sage_resolve(state: GameState, entry: StackEntry) -> None:
    """`targets[0]` is the artifact/enchantment card_id to destroy.
    Destroy = move from battlefield to owner's graveyard (CR 701.7)."""
    if not entry.targets:
        return None
    target_cid = entry.targets[0]
    card = state.get_card(target_cid)
    if card is None:
        return None
    # Find current battlefield zone (owner == controller for permanents
    # absent control-changing effects).
    for ps in state.players:
        if target_cid in ps.zones.battlefield:
            state.move_card(
                target_cid,
                from_player=ps.player_id, from_zone="battlefield",
                to_player=card.owner, to_zone="graveyard",
            )
            return None
    return None


register_resolver("etb_reclamation_sage", _reclamation_sage_resolve)


def _reclamation_sage_etb_trigger(
    state: GameState, etb_event: EnterBattlefieldEvent,
) -> List[Dict[str, Any]]:
    # Stub target picker: first opponent-controlled artifact or enchantment.
    pid = etb_event.controller
    target_cid: Optional[str] = None
    for ps in state.players:
        if ps.player_id == pid:
            continue
        for cid in ps.zones.battlefield:
            card = state.get_card(cid)
            if card is None:
                continue
            tl = card.type_line.lower()
            if "artifact" in tl or "enchantment" in tl:
                target_cid = cid
                break
        if target_cid is not None:
            break
    if target_cid is None:
        return []
    return [build_etb_trigger_dict(
        etb_event, resolver="etb_reclamation_sage",
        targets=[target_cid],
        description="Reclamation Sage ETB: destroy artifact/enchantment",
    )]


register_etb_trigger("Reclamation Sage", _reclamation_sage_etb_trigger)


# ==========================================================
# Gray Merchant of Asphodel — "When this enters, each opponent loses X
# life, where X is your devotion to black. You gain life equal to the
# life lost this way."
# ==========================================================


def _devotion_to_color(state: GameState, pid: int, color_letter: str) -> int:
    """Count mana symbols of `color_letter` (e.g., 'B') in the mana costs
    of permanents `pid` controls. Hybrid symbols count toward both halves
    (CR 700.5 — devotion counts pip-by-pip)."""
    if not (0 <= pid < len(state.players)):
        return 0
    count = 0
    for cid in state.players[pid].zones.battlefield:
        card = state.get_card(cid)
        if card is None:
            continue
        cost = card.mana_cost or ""
        # Count standalone color letters in {X} pip positions. Cover
        # simple {B} and {B/U} and {B/P} (Phyrexian) cases.
        # Iter-10 minimal: walk character-by-character to count.
        i = 0
        while i < len(cost):
            if cost[i] == "{":
                j = cost.find("}", i)
                if j < 0:
                    break
                pip = cost[i + 1:j]
                if color_letter in pip.split("/"):
                    count += 1
                i = j + 1
            else:
                i += 1
    return count


def _gray_merchant_resolve(state: GameState, entry: StackEntry) -> None:
    """X = devotion to black for the controller. Each opponent loses X
    life; controller gains total life lost."""
    pid = entry.controller
    x = _devotion_to_color(state, pid, "B")
    if x <= 0:
        return None
    total_lost = 0
    for ps in state.players:
        if ps.player_id == pid or ps.has_lost:
            continue
        ps.life_total -= x
        total_lost += x
    if 0 <= pid < len(state.players):
        state.players[pid].life_total += total_lost
    return None


register_resolver("etb_gray_merchant", _gray_merchant_resolve)


def _gray_merchant_etb_trigger(
    state: GameState, etb_event: EnterBattlefieldEvent,
) -> List[Dict[str, Any]]:
    return [build_etb_trigger_dict(
        etb_event, resolver="etb_gray_merchant",
        description="Gray Merchant ETB: drain devotion to black",
    )]


register_etb_trigger("Gray Merchant of Asphodel", _gray_merchant_etb_trigger)


# ==========================================================
# Craterhoof Behemoth — "When this enters, creatures you control gain
# trample and get +X/+X until end of turn, where X is the number of
# creatures you control."
# ==========================================================


def _craterhoof_resolve(state: GameState, entry: StackEntry) -> None:
    """Apply +X/+X + trample to each creature controller controls.
    Iter-10 simplification: directly mutate Card.power/toughness +
    append 'trample' to keywords. The "until end of turn" expiration
    isn't tracked card-by-card — Phase 4 of the substrate cleans up
    ContinuousEffects flagged `until_end_of_turn`. To match the
    until-EOT semantics we instead register a ContinuousEffect that
    expires in cleanup. For iter-11+ simplicity here, we mutate the
    keywords + a `_temp_pt_buff` counter that cleanup_step doesn't
    currently zero; flagged as a known limitation.

    Iter-11+ TODO: switch this to register a proper ContinuousEffect
    on layer 7c with until_end_of_turn=True so the substrate cleans up
    at end of turn. The current implementation persists past the
    intended duration (acceptable for the smoke-test gate; not for
    multi-turn fixtures)."""
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    # X = number of creatures controller controls.
    creature_ids = [
        cid for cid in state.players[pid].zones.battlefield
        if (state.get_card(cid) is not None and
            state.get_card(cid).is_creature())
    ]
    x = len(creature_ids)
    for cid in creature_ids:
        card = state.get_card(cid)
        if card is None:
            continue
        try:
            card.power = str(int(card.power_int()) + x)
            card.toughness = str(int(card.toughness_int()) + x)
        except (TypeError, ValueError):
            pass
        if not card.has_keyword("trample"):
            card.keywords.append("trample")
    return None


register_resolver("etb_craterhoof", _craterhoof_resolve)


def _craterhoof_etb_trigger(
    state: GameState, etb_event: EnterBattlefieldEvent,
) -> List[Dict[str, Any]]:
    return [build_etb_trigger_dict(
        etb_event, resolver="etb_craterhoof",
        description="Craterhoof Behemoth ETB: +X/+X + trample on creatures",
    )]


register_etb_trigger("Craterhoof Behemoth", _craterhoof_etb_trigger)


# ==========================================================
# Plaguecrafter — "When this enters, each player sacrifices a creature
# or planeswalker of their choice."
# ==========================================================


def _plaguecrafter_resolve(state: GameState, entry: StackEntry) -> None:
    """Iter-10 deterministic stub: each player sacrifices their WORST
    creature (lowest power+toughness sum) or a planeswalker. Real
    player choice plumbs in iter-11+. If a player controls no eligible
    targets, they skip (CR 701.16)."""
    for ps in state.players:
        if ps.has_lost:
            continue
        # Eligible permanents: creatures + planeswalkers ps controls.
        eligible_ids: List[str] = []
        for cid in ps.zones.battlefield:
            card = state.get_card(cid)
            if card is None:
                continue
            if card.is_creature() or card.is_planeswalker():
                eligible_ids.append(cid)
        if not eligible_ids:
            continue
        # Pick lowest P+T (a generic "weakest" heuristic — iter-11+
        # will plumb the LLM's strategic choice).
        def _pt_sum(cid: str) -> int:
            c = state.get_card(cid)
            if c is None:
                return 9999
            return c.power_int() + c.toughness_int()
        eligible_ids.sort(key=_pt_sum)
        sacrifice_cid = eligible_ids[0]
        sacrificed = state.get_card(sacrifice_cid)
        if sacrificed is None:
            continue
        state.move_card(
            sacrifice_cid,
            from_player=ps.player_id, from_zone="battlefield",
            to_player=sacrificed.owner, to_zone="graveyard",
        )
    return None


register_resolver("etb_plaguecrafter", _plaguecrafter_resolve)


def _plaguecrafter_etb_trigger(
    state: GameState, etb_event: EnterBattlefieldEvent,
) -> List[Dict[str, Any]]:
    return [build_etb_trigger_dict(
        etb_event, resolver="etb_plaguecrafter",
        description="Plaguecrafter ETB: edict each player",
    )]


register_etb_trigger("Plaguecrafter", _plaguecrafter_etb_trigger)


# Accursed Marauder fires the same edict effect — alias.
register_etb_trigger("Accursed Marauder", _plaguecrafter_etb_trigger)


# ==========================================================
# Ranger-Captain of Eos — "When this enters, you may search your
# library for a creature card with mana value 1 or less, reveal it,
# put it into your hand, then shuffle."
# ==========================================================


def _ranger_captain_resolve(state: GameState, entry: StackEntry) -> None:
    """Iter-10 deterministic: find the first creature in library with
    cmc ≤ 1, move to hand. (Real tutoring needs targeted-search +
    shuffle ordering plumbing — iter-11+.)"""
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    lib = state.players[pid].zones.library
    for cid in lib:
        card = state.get_card(cid)
        if card is None:
            continue
        if card.is_creature() and (card.cmc or 0) <= 1:
            state.move_card(
                cid, from_player=pid, from_zone="library",
                to_player=pid, to_zone="hand",
            )
            return None
    return None


register_resolver("etb_ranger_captain", _ranger_captain_resolve)


def _ranger_captain_etb_trigger(
    state: GameState, etb_event: EnterBattlefieldEvent,
) -> List[Dict[str, Any]]:
    return [build_etb_trigger_dict(
        etb_event, resolver="etb_ranger_captain",
        description="Ranger-Captain ETB: tutor creature cmc≤1",
    )]


register_etb_trigger("Ranger-Captain of Eos", _ranger_captain_etb_trigger)


# Imperial Recruiter — "creature card with power 2 or less". Similar
# shape; different filter.
def _imperial_recruiter_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    lib = state.players[pid].zones.library
    for cid in lib:
        card = state.get_card(cid)
        if card is None:
            continue
        if card.is_creature() and card.power_int() <= 2:
            state.move_card(
                cid, from_player=pid, from_zone="library",
                to_player=pid, to_zone="hand",
            )
            return None
    return None


register_resolver("etb_imperial_recruiter", _imperial_recruiter_resolve)


def _imperial_recruiter_etb_trigger(
    state: GameState, etb_event: EnterBattlefieldEvent,
) -> List[Dict[str, Any]]:
    return [build_etb_trigger_dict(
        etb_event, resolver="etb_imperial_recruiter",
        description="Imperial Recruiter ETB: tutor creature power≤2",
    )]


register_etb_trigger("Imperial Recruiter", _imperial_recruiter_etb_trigger)


# ==========================================================
# Avenger of Zendikar — "When this enters, create a 0/1 green Plant
# creature token for each land you control."  (Landfall trigger is
# Phase 8 / complex.)
# ==========================================================


def _avenger_resolve(state: GameState, entry: StackEntry) -> None:
    """Create N 0/1 green Plant tokens where N = controller's lands."""
    from api.engine.pillar_f.v0_2.state import Card
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    n = sum(
        1 for cid in state.players[pid].zones.battlefield
        if (state.get_card(cid) is not None and
            state.get_card(cid).is_land())
    )
    for _ in range(n):
        token = Card(
            name="Plant Token", owner=pid, controller=pid,
            type_line="Token Creature — Plant",
            power="0", toughness="1",
            colors=["G"], color_identity=["G"],
        )
        state.add_card(token)
        state.players[pid].zones.battlefield.append(token.card_id)
    return None


register_resolver("etb_avenger", _avenger_resolve)


def _avenger_etb_trigger(
    state: GameState, etb_event: EnterBattlefieldEvent,
) -> List[Dict[str, Any]]:
    return [build_etb_trigger_dict(
        etb_event, resolver="etb_avenger",
        description="Avenger of Zendikar ETB: N Plant tokens (N = lands)",
    )]


register_etb_trigger("Avenger of Zendikar", _avenger_etb_trigger)
