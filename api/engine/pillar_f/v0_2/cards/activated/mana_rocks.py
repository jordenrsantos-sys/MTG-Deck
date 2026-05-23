"""Phase 3 — Mana rocks.

The 16 in-top-500 mana-producing artifacts. Each registers a resolver
for its tap-for-mana ability via the substrate's register_resolver +
a metadata entry via the local register_activated_ability so test code
+ the eventual cast-and-activate plumbing can discover the ability.

Cards covered:
  - Sol Ring                 ({T}: Add {C}{C})
  - Arcane Signet            ({T}: Add 1 in commander color identity)
  - Mind Stone               ({T}: Add {C} | {1}{T}{sac}: Draw)
  - Thought Vessel           ({T}: Add {C} + unlimited hand size [Phase 4])
  - Fellwar Stone            ({T}: Add 1 of color in opp's lands [stub])
  - Lotus Petal              ({T}{sac}: Add 1 of any color)
  - Talisman cycle (5)       ({T}: Add {C} | {T}: Add color, 1 life — stub)
  - Mox Diamond              ({T}: Add 1 of any color, after discard)
  - Chrome Mox               ({T}: Add 1 of imprinted color)
  - Mana Vault               ({T}: Add {C}{C}{C} | doesn't untap | {4} untap)
  - Worn Powerstone          ({T}: Add {C}{C} | ETB tapped)
  - Ashnod's Altar           ({sac creature}: Add {C}{C})
  - Phyrexian Altar          ({sac creature}: Add 1 of any color)
"""
from __future__ import annotations

from api.engine.pillar_f.v0_2.cards.activated.framework import (
    ActivatedAbilityMeta, register_activated_ability,
    add_mana_resolver, add_multiple_mana_resolver,
    add_any_color_resolver, add_commander_color_identity_resolver,
)
from api.engine.pillar_f.v0_2.stack import register_resolver
from api.engine.pillar_f.v0_2.state import GameState, StackEntry


# ==========================================================
# Sol Ring — {T}: Add {C}{C}.
# ==========================================================

register_resolver("act_sol_ring_tap_mana",
                 add_multiple_mana_resolver({"C": 2}))
register_activated_ability(ActivatedAbilityMeta(
    card_name="Sol Ring", ability_key="tap_mana",
    resolver_name="act_sol_ring_tap_mana",
    cost_mana="", cost_tap=True,
    description="{T}: Add {C}{C}",
))


# ==========================================================
# Arcane Signet + Command Tower + Path of Ancestry — {T}: Add 1 in
# commander color identity. Caller's payment["color"] picks which color.
# ==========================================================

register_resolver("act_commander_identity_tap_mana",
                 add_commander_color_identity_resolver())
for name in ("Arcane Signet", "Command Tower", "Path of Ancestry"):
    register_activated_ability(ActivatedAbilityMeta(
        card_name=name, ability_key="tap_mana",
        resolver_name="act_commander_identity_tap_mana",
        cost_mana="", cost_tap=True,
        description="{T}: Add 1 of any color in commander identity",
    ))


# ==========================================================
# Mind Stone — {T}: Add {C}. // {1}, {T}, Sacrifice this artifact: Draw a card.
# ==========================================================

register_resolver("act_mind_stone_tap_mana", add_mana_resolver("C"))
register_activated_ability(ActivatedAbilityMeta(
    card_name="Mind Stone", ability_key="tap_mana",
    resolver_name="act_mind_stone_tap_mana",
    cost_mana="", cost_tap=True,
    description="{T}: Add {C}",
))


def _mind_stone_sac_draw_resolve(state: GameState, entry: StackEntry) -> None:
    """Caller has already paid 1, tapped, sacrificed Mind Stone. Just
    draw 1 card for the controller. (Sacrificing the source means
    Mind Stone is in graveyard before resolution — but the ability
    resolved from the stack which retains its instance.)"""
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


register_resolver("act_mind_stone_sac_draw", _mind_stone_sac_draw_resolve)
register_activated_ability(ActivatedAbilityMeta(
    card_name="Mind Stone", ability_key="sac_draw",
    resolver_name="act_mind_stone_sac_draw",
    cost_mana="{1}", cost_tap=True, cost_sacrifice_self=True,
    description="{1},{T},Sacrifice: Draw a card",
))


# ==========================================================
# Thought Vessel — {T}: Add {C}. (Static "no max hand size" lives in
# the continuous bucket / Phase 4.)
# ==========================================================

register_resolver("act_thought_vessel_tap_mana", add_mana_resolver("C"))
register_activated_ability(ActivatedAbilityMeta(
    card_name="Thought Vessel", ability_key="tap_mana",
    resolver_name="act_thought_vessel_tap_mana",
    cost_mana="", cost_tap=True,
    description="{T}: Add {C}",
))


# ==========================================================
# Fellwar Stone — {T}: Add 1 of any color a land an opponent controls
# could produce. Iter-10 stub: same as any-color (the "could produce"
# check is iter-11+; substrate doesn't yet track land mana-production
# capabilities in a way the resolver can query).
# ==========================================================

register_resolver("act_fellwar_stone_tap_mana", add_any_color_resolver())
register_activated_ability(ActivatedAbilityMeta(
    card_name="Fellwar Stone", ability_key="tap_mana",
    resolver_name="act_fellwar_stone_tap_mana",
    cost_mana="", cost_tap=True,
    description="{T}: Add 1 of any color (Fellwar — opp-land stub)",
))


# ==========================================================
# Lotus Petal — {T}, Sacrifice this artifact: Add 1 of any color.
# Caller pays the sacrifice cost (move Lotus Petal to graveyard) before
# pushing to stack.
# ==========================================================

register_resolver("act_lotus_petal_tap_mana", add_any_color_resolver())
register_activated_ability(ActivatedAbilityMeta(
    card_name="Lotus Petal", ability_key="tap_mana",
    resolver_name="act_lotus_petal_tap_mana",
    cost_mana="", cost_tap=True, cost_sacrifice_self=True,
    description="{T},Sacrifice: Add 1 of any color",
))


# ==========================================================
# Talisman cycle — 5 enemy-color talismans. Each: {T}: Add {C}.
# {T}: Add 1 of color1 OR color2 (the talisman's pair), 1 life.
# Iter-10 ships both abilities; the pay-life cost is caller-driven.
# ==========================================================

_TALISMAN_COLORS = {
    "Talisman of Progress": ("W", "U"),
    "Talisman of Dominance": ("U", "B"),
    "Talisman of Indulgence": ("B", "R"),
    "Talisman of Impulse": ("R", "G"),
    "Talisman of Unity": ("G", "W"),
    # Allied-color talismans (also cycle):
    "Talisman of Conviction": ("R", "W"),
    "Talisman of Curiosity": ("G", "U"),
    "Talisman of Hierarchy": ("W", "B"),
    "Talisman of Resilience": ("B", "G"),
    "Talisman of Creativity": ("U", "R"),
}

register_resolver("act_talisman_tap_C", add_mana_resolver("C"))
register_resolver("act_talisman_tap_color", add_any_color_resolver())
for talisman_name, colors in _TALISMAN_COLORS.items():
    register_activated_ability(ActivatedAbilityMeta(
        card_name=talisman_name, ability_key="tap_colorless",
        resolver_name="act_talisman_tap_C",
        cost_mana="", cost_tap=True,
        description="{T}: Add {C}",
    ))
    register_activated_ability(ActivatedAbilityMeta(
        card_name=talisman_name, ability_key="tap_color",
        resolver_name="act_talisman_tap_color",
        cost_mana="", cost_tap=True, cost_pay_life=1,
        description=f"{{T}}: Add {{{colors[0]}}} or {{{colors[1]}}}, lose 1",
    ))


# ==========================================================
# Mox Diamond — {T}: Add 1 of any color. (Discard-a-land-or-sac is the
# ETB cost paid at cast time — caller's responsibility.)
# ==========================================================

register_resolver("act_mox_diamond_tap_mana", add_any_color_resolver())
register_activated_ability(ActivatedAbilityMeta(
    card_name="Mox Diamond", ability_key="tap_mana",
    resolver_name="act_mox_diamond_tap_mana",
    cost_mana="", cost_tap=True,
    description="{T}: Add 1 of any color",
))


# ==========================================================
# Chrome Mox — {T}: Add 1 of any of the exiled (imprinted) card's colors.
# Iter-10 stub: caller picks color via payment["color"]; substrate
# doesn't yet track imprint (iter-11+ wires the imprint-payload check).
# ==========================================================

register_resolver("act_chrome_mox_tap_mana", add_any_color_resolver())
register_activated_ability(ActivatedAbilityMeta(
    card_name="Chrome Mox", ability_key="tap_mana",
    resolver_name="act_chrome_mox_tap_mana",
    cost_mana="", cost_tap=True,
    description="{T}: Add 1 of imprinted-card's colors",
))


# ==========================================================
# Mana Vault — {T}: Add {C}{C}{C}. // {4}: Untap. (Doesn't untap during
# untap step — substrate doesn't yet model per-card untap restrictions
# in iter-10; documented as iter-11+ scope. The tap-for-3 ability is
# wired here; the {4}: untap ability is wired as ability_key "untap_cost".)
# ==========================================================

register_resolver("act_mana_vault_tap_mana",
                 add_multiple_mana_resolver({"C": 3}))
register_activated_ability(ActivatedAbilityMeta(
    card_name="Mana Vault", ability_key="tap_mana",
    resolver_name="act_mana_vault_tap_mana",
    cost_mana="", cost_tap=True,
    description="{T}: Add {C}{C}{C}",
))


def _mana_vault_untap_resolve(state: GameState, entry: StackEntry) -> None:
    """{4} cost-paid by caller. Untap Mana Vault. The "may pay {4}"
    trigger lives in the triggered bucket / Phase 6; this resolver
    handles the activation when the player chooses to pay."""
    if not entry.card_id:
        return None
    card = state.get_card(entry.card_id)
    if card is None:
        return None
    card.tapped = False
    return None


register_resolver("act_mana_vault_untap", _mana_vault_untap_resolve)
register_activated_ability(ActivatedAbilityMeta(
    card_name="Mana Vault", ability_key="untap_cost",
    resolver_name="act_mana_vault_untap",
    cost_mana="{4}",
    description="{4}: Untap Mana Vault",
))


# ==========================================================
# Worn Powerstone — {T}: Add {C}{C}. (ETB tapped is a replacement
# effect; Phase 5 wires.)
# ==========================================================

register_resolver("act_worn_powerstone_tap_mana",
                 add_multiple_mana_resolver({"C": 2}))
register_activated_ability(ActivatedAbilityMeta(
    card_name="Worn Powerstone", ability_key="tap_mana",
    resolver_name="act_worn_powerstone_tap_mana",
    cost_mana="", cost_tap=True,
    description="{T}: Add {C}{C}",
))


# ==========================================================
# Ashnod's Altar — Sacrifice a creature: Add {C}{C}.
# Phyrexian Altar — Sacrifice a creature: Add 1 of any color.
# Caller pays the sacrifice cost (move target creature to graveyard)
# before pushing.
# ==========================================================

register_resolver("act_ashnods_altar_sac_mana",
                 add_multiple_mana_resolver({"C": 2}))
register_activated_ability(ActivatedAbilityMeta(
    card_name="Ashnod's Altar", ability_key="sac_mana",
    resolver_name="act_ashnods_altar_sac_mana",
    cost_sacrifice_other="creature you control",
    description="Sac creature: Add {C}{C}",
))

register_resolver("act_phyrexian_altar_sac_mana", add_any_color_resolver())
register_activated_ability(ActivatedAbilityMeta(
    card_name="Phyrexian Altar", ability_key="sac_mana",
    resolver_name="act_phyrexian_altar_sac_mana",
    cost_sacrifice_other="creature",
    description="Sac creature: Add 1 of any color",
))
