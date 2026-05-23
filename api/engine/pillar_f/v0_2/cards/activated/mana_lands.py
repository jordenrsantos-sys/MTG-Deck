"""Phase 3 — Mana lands (non-basic).

Covers the most-common non-basic mana lands in the top 500. The bucket
holds ~112 cards; this module registers explicit handlers for the
high-impact ones and a generic "tap-for-color" factory that covers
duals/shocks/painlands/check-lands by parameter.

Patterns covered:
  - Reliquary Tower:  {T}: Add {C}.  (+ "no max hand size" static — Phase 4)
  - Rogue's Passage:  {T}: Add {C}. // {4},{T}: target creature
                      can't be blocked this turn  (utility ability stub)
  - Exotic Orchard / Fellwar Stone-style "could-produce" lands
                      → any-color stub (same as Fellwar Stone)
  - City of Brass:    {T}: Add 1 of any color. (Lose 1 life trigger
                      lives in triggered/ Phase 6.)
  - Mana Confluence:  {T}, Pay 1 life: Add 1 of any color.
  - Ancient Tomb:     {T}: Add {C}{C}. (Self-damage trigger Phase 6.)
  - War Room:         {T}: Add {C}. // {3},{T},Pay X life: Draw a card.
  - Urborg, Tomb of Yawgmoth / Yavimaya, Cradle of Growth → continuous
    bucket (Phase 4) for the "each land is also X" effect; this module
    leaves them to that phase.
  - Nykthos:          devotion-tap is Phase 4 (continuous-driven X);
                      the {T}: Add {C} side wired here.

For the painlands + dual lands not explicitly listed here, the future
generic registration loop can pull from a config table. This module
ships the named entries; the loop is iter-12+ scope.
"""
from __future__ import annotations

from api.engine.pillar_f.v0_2.cards.activated.framework import (
    ActivatedAbilityMeta, register_activated_ability,
    add_mana_resolver, add_multiple_mana_resolver, add_any_color_resolver,
)
from api.engine.pillar_f.v0_2.stack import register_resolver
from api.engine.pillar_f.v0_2.state import GameState, StackEntry


# ==========================================================
# Reliquary Tower — {T}: Add {C}.
# ==========================================================

register_resolver("act_reliquary_tower_tap", add_mana_resolver("C"))
register_activated_ability(ActivatedAbilityMeta(
    card_name="Reliquary Tower", ability_key="tap_mana",
    resolver_name="act_reliquary_tower_tap",
    cost_tap=True, description="{T}: Add {C}",
))


# ==========================================================
# Rogue's Passage — {T}: Add {C}. + {4},{T}: target creature can't be
# blocked this turn.
# ==========================================================

register_resolver("act_rogues_passage_tap", add_mana_resolver("C"))
register_activated_ability(ActivatedAbilityMeta(
    card_name="Rogue's Passage", ability_key="tap_mana",
    resolver_name="act_rogues_passage_tap",
    cost_tap=True, description="{T}: Add {C}",
))


def _rogues_passage_unblockable_resolve(state: GameState, entry: StackEntry) -> None:
    """Target creature gets "can't be blocked this turn". Iter-10 stub:
    add a "unblockable_turn" keyword that the combat module's
    declare_blockers honors. The keyword is removed at cleanup
    (iter-11+ should switch to a until-EOT ContinuousEffect)."""
    if not entry.targets:
        return None
    target_cid = entry.targets[0]
    card = state.get_card(target_cid)
    if card is None:
        return None
    if "unblockable_turn" not in card.keywords:
        card.keywords.append("unblockable_turn")
    return None


register_resolver("act_rogues_passage_unblockable",
                 _rogues_passage_unblockable_resolve)
register_activated_ability(ActivatedAbilityMeta(
    card_name="Rogue's Passage", ability_key="make_unblockable",
    resolver_name="act_rogues_passage_unblockable",
    cost_mana="{4}", cost_tap=True,
    description="{4},{T}: Target creature unblockable this turn",
))


# ==========================================================
# Exotic Orchard — {T}: Add 1 of any color that a land an opponent
# controls could produce. Iter-10 stub: caller picks color.
# ==========================================================

register_resolver("act_exotic_orchard_tap", add_any_color_resolver())
register_activated_ability(ActivatedAbilityMeta(
    card_name="Exotic Orchard", ability_key="tap_mana",
    resolver_name="act_exotic_orchard_tap",
    cost_tap=True,
    description="{T}: Add 1 of color opp-land could produce (stub)",
))


# ==========================================================
# City of Brass — {T}: Add 1 of any color. (Self-damage trigger in
# Phase 6.)
# ==========================================================

register_resolver("act_city_of_brass_tap", add_any_color_resolver())
register_activated_ability(ActivatedAbilityMeta(
    card_name="City of Brass", ability_key="tap_mana",
    resolver_name="act_city_of_brass_tap",
    cost_tap=True,
    description="{T}: Add 1 of any color",
))


# ==========================================================
# Mana Confluence — {T}, Pay 1 life: Add 1 of any color.
# ==========================================================

register_resolver("act_mana_confluence_tap", add_any_color_resolver())
register_activated_ability(ActivatedAbilityMeta(
    card_name="Mana Confluence", ability_key="tap_mana",
    resolver_name="act_mana_confluence_tap",
    cost_tap=True, cost_pay_life=1,
    description="{T},1 life: Add 1 of any color",
))


# ==========================================================
# Ancient Tomb — {T}: Add {C}{C}. (Self-damage trigger Phase 6.)
# ==========================================================

register_resolver("act_ancient_tomb_tap",
                 add_multiple_mana_resolver({"C": 2}))
register_activated_ability(ActivatedAbilityMeta(
    card_name="Ancient Tomb", ability_key="tap_mana",
    resolver_name="act_ancient_tomb_tap",
    cost_tap=True,
    description="{T}: Add {C}{C}",
))


# ==========================================================
# War Room — {T}: Add {C}. + {3},{T},Pay life=colors-in-commander-id:
# Draw a card.
# ==========================================================

register_resolver("act_war_room_tap", add_mana_resolver("C"))
register_activated_ability(ActivatedAbilityMeta(
    card_name="War Room", ability_key="tap_mana",
    resolver_name="act_war_room_tap",
    cost_tap=True, description="{T}: Add {C}",
))


def _war_room_draw_resolve(state: GameState, entry: StackEntry) -> None:
    """Draw a card. Caller has paid 3 + tap + life-equal-to-color-count.
    Iter-10 stub: don't validate the life cost (caller's job)."""
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


register_resolver("act_war_room_draw", _war_room_draw_resolve)
register_activated_ability(ActivatedAbilityMeta(
    card_name="War Room", ability_key="draw_card",
    resolver_name="act_war_room_draw",
    cost_mana="{3}", cost_tap=True,
    description="{3},{T},Pay X life: Draw a card",
))


# ==========================================================
# Bonders' Enclave — {T}: Add {C}. + {3},{T}: Draw if controller controls
# a creature with power ≥ 4. (Common Commander land in top 500.)
# ==========================================================

register_resolver("act_bonders_enclave_tap", add_mana_resolver("C"))
register_activated_ability(ActivatedAbilityMeta(
    card_name="Bonders' Enclave", ability_key="tap_mana",
    resolver_name="act_bonders_enclave_tap",
    cost_tap=True, description="{T}: Add {C}",
))


# ==========================================================
# Nykthos, Shrine to Nyx — {T}: Add {C}. (Devotion-tap is Phase 4.)
# ==========================================================

register_resolver("act_nykthos_tap", add_mana_resolver("C"))
register_activated_ability(ActivatedAbilityMeta(
    card_name="Nykthos, Shrine to Nyx", ability_key="tap_mana",
    resolver_name="act_nykthos_tap",
    cost_tap=True,
    description="{T}: Add {C}",
))
