"""Cast pipeline static_modifier consumer.

Sub-mega-task v14 Phase 3. Reads v11's existing static_modifier
registry (`cards/continuous/framework.query_active_static_modifiers`)
and applies the modifier semantics for the 17 cards in the data-only
bucket that affect cast / play / combat / counter behavior.

The substrate's perspective_view + sub-B's compute_eligible_actions
call these functions to expose modifier-aware costs + legality to
the LLM. Without v14, the LLM saw raw mana costs even when
Goblin Electromancer was on the board reducing them; with v14, the
LLM sees the effective cost.

Functions are PURE on (state, candidate_action) -> result. They do
not mutate state. The actual cost-payment + legality enforcement
stays in sub-B's action-application layer.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from api.engine.pillar_f.v0_2.state import Card, GameState


COST_MODIFIER_VERSION = "pillar_f_v0_2_cast_cost_modifier_v1"


@dataclass
class ManaCostDelta:
    """A modification to a card's printed cost. Aggregated across all
    applicable static_modifier sources.

    `generic_reduction`: number of generic mana symbols removed (e.g.
    Etherium Sculptor: artifact spells cost {1} less -> generic_reduction=1).
    `generic_increase`: additional generic mana symbols added (e.g.
    Thalia: noncreature spells cost {1} more -> generic_increase=1).
    `minimum_total_after`: floor on the post-reduction CMC. The Medallion
    cycle and similar reducers cap the reduction so a card doesn't go
    below {0}.
    `sources`: list of (card_name, effect_key) tuples for diagnostics.
    """
    generic_reduction: int = 0
    generic_increase: int = 0
    minimum_total_after: int = 0
    sources: List[Tuple[str, str]] = field(default_factory=list)

    @property
    def net_generic_delta(self) -> int:
        """Net change to the spell's generic-mana CMC. Positive = costs
        more; negative = costs less."""
        return self.generic_increase - self.generic_reduction


def _query_modifiers(
    state: GameState, effect_key: str,
) -> List[Dict[str, Any]]:
    """Lazy-import wrapper around v11's query_active_static_modifiers.
    Returns an empty list if cards/ isn't loaded -- the substrate
    stays runnable in isolation."""
    try:
        from api.engine.pillar_f.v0_2.cards.continuous.framework import (
            query_active_static_modifiers,
        )
        return query_active_static_modifiers(state, effect_key=effect_key)
    except ImportError:
        return []


# ============================================================
# cost_reduction (Medallions + 4 type/color reducers)
# ============================================================


_TYPE_KEYWORD_MAP = {
    "instant_or_sorcery": ["instant", "sorcery"],
    "artifact": ["artifact"],
    "creature": ["creature"],
    "noncreature": ["__noncreature__"],   # special: NOT creature
}


def _normalize_applies_to(applies_to: str) -> str:
    """Map v11 registry vocab ('your_spells' / 'opponents' /
    'controller' / 'all') to internal codes ('own' / 'opp' / 'all').
    v11's vocab and the earlier audit-doc vocab diverged across
    effect_keys; this normalizer hides the diff from per-effect logic.
    """
    if applies_to in (
        "own", "your_spells", "your_creatures", "your_attacks",
        "you", "controller",
    ):
        return "own"
    if applies_to in ("opp", "opponents", "opponent"):
        return "opp"
    return "all"


def _spell_matches_filter(
    card: Card, *, color_filter: Any, color_filter_any: Any,
    type_filter: Any,
) -> bool:
    """Does the spell match the modifier's applicability filter?
    A modifier with no filter applies to ALL spells (broad).
    """
    # Color filter (single color, e.g. Jet Medallion -> "B").
    if color_filter:
        cf = color_filter if isinstance(color_filter, list) else [color_filter]
        if not any(c in (card.colors or []) for c in cf):
            return False
    # Color filter any (color is in the card's color identity, used for
    # multicolor reducers like Goblin Anarchomancer applying to RG).
    if color_filter_any:
        cfa = color_filter_any if isinstance(color_filter_any, list) else [color_filter_any]
        if not any(c in (card.colors or []) for c in cfa):
            return False
    # Type filter (case-insensitive substring match against type_line).
    if type_filter:
        tf = type_filter if isinstance(type_filter, list) else [type_filter]
        type_line = (card.type_line or "").lower()
        # Expand keywords like "instant_or_sorcery"; lowercase all.
        expanded: List[str] = []
        for t in tf:
            t_lower = (t or "").lower()
            expanded.extend(_TYPE_KEYWORD_MAP.get(t_lower, [t_lower]))
        if "__noncreature__" in expanded:
            if "creature" in type_line:
                return False
            expanded = [e for e in expanded if e != "__noncreature__"]
        if expanded and not any(t in type_line for t in expanded):
            return False
    return True


def effective_cast_cost_delta(
    state: GameState, card: Card, caster_id: int,
) -> ManaCostDelta:
    """Aggregate all applicable cost_reduction + spell_restriction
    cost-side modifiers and return a ManaCostDelta. Caller applies
    the delta to the printed cost (typically `card.cmc` or parsed
    mana_cost) to get the effective cost.

    `caster_id` is who's casting; controller-scoped modifiers
    (`applies_to: "own"`) only apply when controller==caster.
    """
    delta = ManaCostDelta()
    reductions = _query_modifiers(state, "cost_reduction")
    for mod in reductions:
        params = mod.get("params") or {}
        applies_to = _normalize_applies_to(params.get("applies_to", "all"))
        if applies_to == "own" and mod.get("controller") != caster_id:
            continue
        if applies_to == "opp" and mod.get("controller") == caster_id:
            continue
        if not _spell_matches_filter(
            card,
            color_filter=params.get("color_filter"),
            color_filter_any=params.get("color_filter_any"),
            type_filter=params.get("type_filter"),
        ):
            continue
        reduction = int(params.get("reduction", 0) or 0)
        if reduction > 0:
            delta.generic_reduction += reduction
            delta.sources.append((mod.get("card_name", "?"), "cost_reduction"))
        min_after = int(params.get("minimum_cost_after", 0) or 0)
        if min_after > delta.minimum_total_after:
            delta.minimum_total_after = min_after
    # Cost-increase modifiers (Thalia-style) would be a separate
    # effect_key. v11's registry uses "spell_restriction" with a
    # `restrict: cost_increase` shape for some cards; iter-12+ may
    # split into explicit cost_increase entries. Implemented here as
    # a pattern check on spell_restriction modifiers that bear a
    # `cost_increase` param.
    restrictions = _query_modifiers(state, "spell_restriction")
    for mod in restrictions:
        params = mod.get("params") or {}
        if not isinstance(params.get("cost_increase"), int):
            continue
        applies_to = _normalize_applies_to(params.get("applies_to", "all"))
        if applies_to == "own" and mod.get("controller") != caster_id:
            continue
        if applies_to == "opp" and mod.get("controller") == caster_id:
            continue
        if not _spell_matches_filter(
            card,
            color_filter=params.get("color_filter"),
            color_filter_any=params.get("color_filter_any"),
            type_filter=params.get("type_filter"),
        ):
            continue
        delta.generic_increase += int(params["cost_increase"])
        delta.sources.append((mod.get("card_name", "?"), "cost_increase"))
    return delta


# ============================================================
# spell_restriction (Drannith, Grand Abolisher)
# ============================================================


def is_spell_cast_legal(
    state: GameState, card: Card, caster_id: int,
) -> Tuple[bool, Optional[str]]:
    """Returns (True, None) if casting `card` is legal; otherwise
    (False, reason) where reason names the prohibiting card.

    Iter-11 simplification: ignores `during` semantics
    (your_turn/opp_turn) -- treats restrictions as always-on. Iter-12+
    can read state.active_player to honor those.
    """
    restrictions = _query_modifiers(state, "spell_restriction")
    for mod in restrictions:
        params = mod.get("params") or {}
        # Skip cost_increase-style; that's handled by
        # effective_cast_cost_delta above.
        if isinstance(params.get("cost_increase"), int):
            continue
        applies_to = _normalize_applies_to(params.get("applies_to", "all"))
        # "opp" = the restriction targets OPPONENTS of the modifier's
        # controller. "own" = applies only to the modifier's
        # controller. "all" = everyone.
        controller = mod.get("controller", -1)
        if applies_to == "opp" and controller == caster_id:
            continue
        if applies_to == "own" and controller != caster_id:
            continue
        # restrict types: v11 registry uses list-of-strings. Common
        # codes: "cast_creature_spells", "cast_noncreature_spells",
        # "cast_any_spells", "cast_from_non_hand_zones" (Drannith).
        restrict = params.get("restrict", "")
        if isinstance(restrict, list):
            restrict_codes = [str(r) for r in restrict]
        else:
            restrict_codes = [str(restrict)]
        type_line = (card.type_line or "").lower()
        for code in restrict_codes:
            if code == "cast_creature_spells" and "creature" in type_line:
                return False, mod.get("card_name", "?")
            if code == "cast_noncreature_spells" and "creature" not in type_line:
                return False, mod.get("card_name", "?")
            if code == "cast_any_spells":
                return False, mod.get("card_name", "?")
            # "cast_from_non_hand_zones" (Drannith) is a per-zone
            # restriction; iter-11 stub: doesn't block cast-from-
            # hand. Iter-12+ wires zone-of-origin tracking.
    return True, None


# ============================================================
# uncounterable (Hexing Squelcher)
# ============================================================


def is_spell_uncounterable(
    state: GameState, stack_entry_card_id: str,
) -> Tuple[bool, Optional[str]]:
    """Returns (True, source_card_name) if the spell on the stack
    cannot be countered; (False, None) otherwise. Iter-11 simplest:
    if any uncounterable modifier is active and applies to this spell,
    return True.

    Iter-12+ may refine for selective uncounterable (e.g. "your
    instant and sorcery spells").
    """
    mods = _query_modifiers(state, "uncounterable")
    if not mods:
        return False, None
    target_card = state.get_card(stack_entry_card_id)
    if target_card is None:
        return False, None
    for mod in mods:
        params = mod.get("params") or {}
        applies_to = _normalize_applies_to(params.get("applies_to", "own"))
        controller = mod.get("controller", -1)
        if applies_to == "own" and target_card.controller == controller:
            return True, mod.get("card_name", "?")
        if applies_to == "all":
            return True, mod.get("card_name", "?")
    return False, None


# ============================================================
# additional_land_drops (Azusa, Exploration)
# ============================================================


def additional_land_drops_available(
    state: GameState, player_id: int,
) -> int:
    """Sum of bonus land drops granted to `player_id` this turn.
    Caller adds to the default 1 land drop / turn to get the effective
    cap. Azusa: 2 additional (3 total). Exploration: 1 additional (2 total).
    """
    total = 0
    mods = _query_modifiers(state, "additional_land_drops")
    for mod in mods:
        params = mod.get("params") or {}
        applies_to = _normalize_applies_to(params.get("applies_to", "own"))
        controller = mod.get("controller", -1)
        if applies_to == "own" and controller != player_id:
            continue
        if applies_to == "opp" and controller == player_id:
            continue
        bonus = int(params.get("additional_drops_per_turn", 0) or 0)
        total += bonus
    return total


# ============================================================
# attack_tax (Propaganda, Ghostly Prison)
# ============================================================


def effective_attack_tax(
    state: GameState, attacker_card_id: str, attacker_controller_id: int,
    defending_player_id: int,
) -> int:
    """Returns the mana cost the attacker's controller must pay to
    declare `attacker_card_id` attacking `defending_player_id`. 0 if
    no taxes apply.

    Iter-11 simplification: tax_mana is treated as generic ({2} for
    Propaganda/Ghostly Prison).
    """
    mods = _query_modifiers(state, "attack_tax")
    total = 0
    for mod in mods:
        params = mod.get("params") or {}
        # Tax applies if the modifier's controller is the defending
        # player AND the modifier targets attackers of that defender.
        controller = mod.get("controller", -1)
        applies_to_atk_of = params.get("applies_to_attackers_of", "controller")
        if applies_to_atk_of == "controller" and controller != defending_player_id:
            continue
        per_attacker = bool(params.get("per_attacker", True))
        if not per_attacker:
            continue
        tax = int(params.get("tax_mana", 0) or 0)
        total += tax
    return total


# ============================================================
# additional_mana_when_land_taps (Utopia Sprawl, Wild Growth)
# ============================================================


def extra_mana_from_aura_on_land(
    state: GameState, land_card_id: str,
) -> List[str]:
    """Returns the extra mana colors a land produces because of
    Wild-Growth-style auras attached to it.

    Iter-11 stub: returns empty list. The static_modifier params
    have `pending_iter12` flagged, meaning the aura-attachment
    relationship isn't tracked yet in v11's registry. Iter-12+
    populates attached_land_ids on the modifier params and this
    function reads them.
    """
    mods = _query_modifiers(state, "additional_mana_when_land_taps")
    out: List[str] = []
    for mod in mods:
        params = mod.get("params") or {}
        if params.get("pending_iter12"):
            continue
        attached_land_ids = params.get("attached_land_ids") or []
        if land_card_id in attached_land_ids:
            color = params.get("mana_color")
            if color:
                out.append(color)
    return out
