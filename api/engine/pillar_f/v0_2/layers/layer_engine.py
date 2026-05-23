"""Continuous-effect layer engine per CR 613.

Iter-10 implements the canonical 7 layers + 4 sublayers in layer 7:

  1. Copy effects.
  2. Control-changing.
  3. Text-changing (rare — iter 10 ships minimal Mind Bend stub support).
  4. Type/subtype/supertype changes.
  5. Color changes.
  6. Ability adding/removing (anthems, keyword grants, Humility).
  7. Power/toughness:
     7a. Base P/T setting (printed values OR effects that set P/T to
         specific values).
     7b. CDA (characteristic-defining abilities — Tarmogoyf,
         Mortivore).
     7c. P/T modifications (anthems +1/+1, Glorious Anthem).
     7d. P/T switches (Inverter of Truth).

CR 613.7 timestamp + dependency ordering: iter-10 collapses to
insertion-order within each layer (= timestamp order in practice
since effects are added in chronological order). Dependency handling
is iter-11+ scope; iter-10 documents the simplification.

Effect application contract:
  - Each ContinuousEffect carries `effect_fn_name` keyed into
    `_LAYER_EFFECT_REGISTRY`.
  - Effect function signature:
      `(state, effect, characteristics_table) → None`
    Mutates the characteristics_table dict in-place. `target_pattern`
    on the effect selects which permanents to apply to.

  - `apply_continuous_effects(state)` builds a fresh Characteristics
    table from PRINTED card values, then walks layers 1→7 applying
    each registered effect in insertion order.
"""
from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from api.engine.pillar_f.v0_2.state import (
    Card, ContinuousEffect, GameState,
)
from api.engine.pillar_f.v0_2.layers.characteristics import Characteristics


LAYER_ENGINE_VERSION = "pillar_f_v0_2_layer_engine_v1"

# Effect function signature: (state, effect, characteristics_table) → None.
LayerEffectFn = Callable[[GameState, ContinuousEffect, Dict[str, Characteristics]], None]

_LAYER_EFFECT_REGISTRY: Dict[str, LayerEffectFn] = {}


def register_layer_effect(name: str, fn: LayerEffectFn) -> None:
    _LAYER_EFFECT_REGISTRY[name] = fn


def get_layer_effect(name: str) -> Optional[LayerEffectFn]:
    return _LAYER_EFFECT_REGISTRY.get(name)


# ============================================================
# Type parsing helpers
# ============================================================


def parse_type_line(type_line: str) -> Tuple[List[str], List[str], List[str]]:
    """Returns (supertypes, types, subtypes). Type line format:
    'Supertype Type1 Type2 — Subtype1 Subtype2'."""
    supertypes_list = ["Legendary", "Basic", "Snow", "World", "Tribal", "Ongoing"]
    types_list = ["Creature", "Artifact", "Enchantment", "Land", "Sorcery",
                  "Instant", "Planeswalker", "Battle"]
    if not type_line:
        return [], [], []
    if "—" in type_line:
        left, right = type_line.split("—", 1)
        subtypes = [s.strip() for s in right.split() if s.strip()]
    else:
        left = type_line
        subtypes = []
    tokens = [t.strip() for t in left.split() if t.strip()]
    supertypes = [t for t in tokens if t in supertypes_list]
    types = [t for t in tokens if t in types_list]
    return supertypes, types, subtypes


def reassemble_type_line(supertypes: List[str], types: List[str],
                         subtypes: List[str]) -> str:
    left = " ".join(supertypes + types).strip()
    if subtypes:
        return f"{left} — {' '.join(subtypes)}".strip()
    return left


# ============================================================
# Snapshot construction + main entry point
# ============================================================


def _initial_snapshot(card: Card) -> Characteristics:
    """Start from printed values."""
    supertypes, types, subtypes = parse_type_line(card.type_line)
    return Characteristics(
        card_id=card.card_id,
        name=card.name,
        type_line=card.type_line,
        subtypes=list(card.subtypes) if card.subtypes else list(subtypes),
        supertypes=supertypes,
        types=types,
        colors=list(card.colors),
        color_identity=list(card.color_identity),
        keywords=list(card.keywords),
        abilities=[],  # abilities are oracle-text-derived; iter-10 leaves empty
        power=card.power_int(),
        toughness=card.toughness_int(),
        loyalty=int(card.loyalty) if card.loyalty and card.loyalty.lstrip("-").isdigit() else 0,
        controller=card.controller,
        is_copy_of_card_id=None,
    )


# Layer ordering: list of (layer_int, sublayer_or_None).
_LAYER_ORDER: List[Tuple[int, Optional[str]]] = [
    (1, None), (2, None), (3, None), (4, None), (5, None), (6, None),
    (7, "a"), (7, "b"), (7, "c"), (7, "d"),
]


def apply_continuous_effects(state: GameState) -> Dict[str, Characteristics]:
    """Compute fresh Characteristics for every battlefield permanent.
    Returns a dict {card_id → Characteristics}.

    Walks layers 1→7 (with sublayers 7a/7b/7c/7d). Within each layer,
    effects apply in insertion order in state.continuous_effects (=
    timestamp order in practice). Dependency resolution (CR 613.7c)
    is iter-11+ scope; iter-10 documents the simplification.
    """
    # Step 1: initial snapshot from printed values for every battlefield permanent.
    table: Dict[str, Characteristics] = {}
    for ps in state.players:
        for cid in ps.zones.battlefield:
            card = state.get_card(cid)
            if card is None:
                continue
            table[cid] = _initial_snapshot(card)

    # Step 2: walk layers + apply effects.
    for layer, sublayer in _LAYER_ORDER:
        for ce in state.continuous_effects:
            if ce.layer != layer:
                continue
            if layer == 7 and ce.sublayer != sublayer:
                continue
            fn = get_layer_effect(ce.effect_fn_name)
            if fn is None:
                continue
            try:
                fn(state, ce, table)
            except Exception:
                # Iter-10: silently skip effect-fn errors so a buggy
                # effect doesn't break the whole snapshot. Iter 11+
                # adds structured error reporting.
                pass

    # Step 3: apply +1/+1 counters in layer 7d-equivalent (CR 613.4 —
    # counters apply in their own sub-process between layers 7 and SBAs).
    # Iter-10 simplification: apply at end of layer pipeline so they're
    # part of the final P/T.
    for cid, chars in table.items():
        card = state.get_card(cid)
        if card is None:
            continue
        plus_counters = card.counters.get("+1/+1", 0)
        minus_counters = card.counters.get("-1/-1", 0)
        net = plus_counters - minus_counters
        if net != 0:
            chars.power += net
            chars.toughness += net

    return table


# ============================================================
# Built-in layer effects (iter-10 minimal set)
# ============================================================

# Helper: select which permanents this effect targets.

def _select_targets(
    state: GameState,
    target_pattern: Dict[str, Any],
    table: Dict[str, Characteristics],
) -> List[Characteristics]:
    """Return list of Characteristics for permanents matching the
    target pattern. Supported pattern keys (iter-10 minimal set):
      - "card_id": specific card_id (single target)
      - "controller": player_id
      - "subtype": match by subtype (case-insensitive)
      - "type": match by type ("Creature", etc.)
      - "all_creatures_controller": player_id (all creatures controlled
        by this player)
      - "all_creatures": True (all creatures, all players)
      - "name": exact card name match
    """
    out: List[Characteristics] = []
    if "card_id" in target_pattern:
        if target_pattern["card_id"] in table:
            out.append(table[target_pattern["card_id"]])
        return out
    if target_pattern.get("all_creatures"):
        for chars in table.values():
            if "Creature" in chars.types:
                out.append(chars)
        return out
    if "all_creatures_controller" in target_pattern:
        pid = target_pattern["all_creatures_controller"]
        for chars in table.values():
            if "Creature" in chars.types and chars.controller == pid:
                out.append(chars)
        return out
    # Generic filter: walk all and check predicates.
    for chars in table.values():
        ok = True
        if "controller" in target_pattern and chars.controller != target_pattern["controller"]:
            ok = False
        if "subtype" in target_pattern:
            sub = target_pattern["subtype"]
            if not any(s.lower() == sub.lower() for s in chars.subtypes):
                ok = False
        if "type" in target_pattern:
            if target_pattern["type"] not in chars.types:
                ok = False
        if "name" in target_pattern and chars.name != target_pattern["name"]:
            ok = False
        if ok:
            out.append(chars)
    return out


# ---- Layer 1: Copy effects ----

def _l1_clone_of(state: GameState, effect: ContinuousEffect,
                 table: Dict[str, Characteristics]) -> None:
    """Clone copies the printed (or layer-1) characteristics of another
    permanent. target_pattern: {"card_id": clone_card_id,
                                 "copy_target_card_id": "..."}.
    """
    clone_id = effect.target_pattern.get("card_id")
    copy_target_id = effect.target_pattern.get("copy_target_card_id")
    if not clone_id or not copy_target_id:
        return
    if clone_id not in table:
        return
    # Source can be a battlefield permanent OR a card definition
    # (for graveyard-target clones). Iter-10 only handles battlefield.
    if copy_target_id not in table:
        # Source not on battlefield (or not yet snapshotted) — fall back
        # to source card's printed values.
        src_card = state.get_card(copy_target_id)
        if src_card is None:
            return
        src_chars = _initial_snapshot(src_card)
    else:
        src_chars = table[copy_target_id]
    # Copy fields from source onto clone (controller stays the clone's).
    clone = table[clone_id]
    clone.name = src_chars.name
    clone.type_line = src_chars.type_line
    clone.subtypes = list(src_chars.subtypes)
    clone.supertypes = list(src_chars.supertypes)
    clone.types = list(src_chars.types)
    clone.colors = list(src_chars.colors)
    clone.color_identity = list(src_chars.color_identity)
    clone.keywords = list(src_chars.keywords)
    clone.abilities = list(src_chars.abilities)
    clone.power = src_chars.power
    clone.toughness = src_chars.toughness
    clone.loyalty = src_chars.loyalty
    clone.is_copy_of_card_id = copy_target_id


register_layer_effect("clone_of", _l1_clone_of)


# ---- Layer 2: Control-changing ----

def _l2_change_control(state: GameState, effect: ContinuousEffect,
                       table: Dict[str, Characteristics]) -> None:
    """Mind Control / Threaten: set controller of target_pattern.card_id
    to effect.controller (or target_pattern.new_controller if specified)."""
    cid = effect.target_pattern.get("card_id")
    new_controller = effect.target_pattern.get("new_controller",
                                                effect.controller)
    if cid in table:
        table[cid].controller = new_controller


register_layer_effect("change_control", _l2_change_control)


# ---- Layer 4: Type/subtype/supertype changes ----

def _l4_add_subtype(state: GameState, effect: ContinuousEffect,
                    table: Dict[str, Characteristics]) -> None:
    """Add a subtype to matching permanents. target_pattern with
    extra key 'add_subtype': str."""
    targets = _select_targets(state, effect.target_pattern, table)
    new_st = effect.target_pattern.get("add_subtype")
    if not new_st:
        return
    for chars in targets:
        if new_st not in chars.subtypes:
            chars.subtypes.append(new_st)
            chars.type_line = reassemble_type_line(
                chars.supertypes, chars.types, chars.subtypes,
            )


def _l4_remove_supertype(state: GameState, effect: ContinuousEffect,
                         table: Dict[str, Characteristics]) -> None:
    """Mind Bend / similar: remove a supertype like Legendary.
    target_pattern with 'remove_supertype': str."""
    targets = _select_targets(state, effect.target_pattern, table)
    st = effect.target_pattern.get("remove_supertype")
    if not st:
        return
    for chars in targets:
        if st in chars.supertypes:
            chars.supertypes.remove(st)
            chars.type_line = reassemble_type_line(
                chars.supertypes, chars.types, chars.subtypes,
            )


register_layer_effect("add_subtype", _l4_add_subtype)
register_layer_effect("remove_supertype", _l4_remove_supertype)


# ---- Layer 5: Color changes ----

def _l5_set_color(state: GameState, effect: ContinuousEffect,
                  table: Dict[str, Characteristics]) -> None:
    """Set colors of matching permanents. target_pattern with
    'new_colors': List[str]."""
    targets = _select_targets(state, effect.target_pattern, table)
    new_colors = effect.target_pattern.get("new_colors")
    if not isinstance(new_colors, list):
        return
    for chars in targets:
        chars.colors = list(new_colors)


register_layer_effect("set_color", _l5_set_color)


# ---- Layer 6: Ability adding/removing ----

def _l6_grant_keyword(state: GameState, effect: ContinuousEffect,
                      table: Dict[str, Characteristics]) -> None:
    """Grant a keyword to matching permanents. target_pattern with
    'grant_keyword': str."""
    targets = _select_targets(state, effect.target_pattern, table)
    kw = effect.target_pattern.get("grant_keyword")
    if not kw:
        return
    for chars in targets:
        if not chars.has_keyword(kw):
            chars.keywords.append(kw)


def _l6_lose_all_abilities(state: GameState, effect: ContinuousEffect,
                           table: Dict[str, Characteristics]) -> None:
    """Humility-style: strip all abilities + keywords from matching
    creatures."""
    targets = _select_targets(state, effect.target_pattern, table)
    for chars in targets:
        chars.keywords = []
        chars.abilities = []


register_layer_effect("grant_keyword", _l6_grant_keyword)
register_layer_effect("lose_all_abilities", _l6_lose_all_abilities)


# ---- Layer 7a: Base P/T setting ----

def _l7a_set_base_pt(state: GameState, effect: ContinuousEffect,
                     table: Dict[str, Characteristics]) -> None:
    """Set base P/T to specific values. target_pattern with
    'set_power': int, 'set_toughness': int. Humility-style:
    'becomes a 1/1 with no abilities' uses layer 6 (lose_all_abilities)
    + layer 7b (CDA set to 1/1)."""
    targets = _select_targets(state, effect.target_pattern, table)
    p = effect.target_pattern.get("set_power")
    t = effect.target_pattern.get("set_toughness")
    for chars in targets:
        if p is not None:
            chars.power = int(p)
        if t is not None:
            chars.toughness = int(t)


register_layer_effect("set_base_pt", _l7a_set_base_pt)


# ---- Layer 7b: CDA ----

def _l7b_cda_set_pt(state: GameState, effect: ContinuousEffect,
                    table: Dict[str, Characteristics]) -> None:
    """CDA — characteristic-defining ability that sets P/T based on
    game state. target_pattern with 'pt_function': callable name
    keyed in a small CDA registry. iter-10 ships 2 CDA functions:
    'tarmogoyf' (P=card-types-in-all-graveyards, T=P+1) and
    'mortivore' (P/T = creatures-in-all-graveyards)."""
    cid = effect.target_pattern.get("card_id")
    fn_name = effect.target_pattern.get("pt_function")
    if not cid or cid not in table or not fn_name:
        return
    fn = _CDA_REGISTRY.get(fn_name)
    if fn is None:
        return
    p, t = fn(state, cid)
    table[cid].power = p
    table[cid].toughness = t


def _cda_tarmogoyf(state: GameState, cid: str) -> Tuple[int, int]:
    type_set: Set[str] = set()
    for ps in state.players:
        for gy_cid in ps.zones.graveyard:
            card = state.get_card(gy_cid)
            if card is None:
                continue
            _, types, _ = parse_type_line(card.type_line)
            type_set.update(types)
    p = len(type_set)
    return p, p + 1


def _cda_mortivore(state: GameState, cid: str) -> Tuple[int, int]:
    n = 0
    for ps in state.players:
        for gy_cid in ps.zones.graveyard:
            card = state.get_card(gy_cid)
            if card is None:
                continue
            _, types, _ = parse_type_line(card.type_line)
            if "Creature" in types:
                n += 1
    return n, n


_CDA_REGISTRY: Dict[str, Callable[[GameState, str], Tuple[int, int]]] = {
    "tarmogoyf": _cda_tarmogoyf,
    "mortivore": _cda_mortivore,
}


register_layer_effect("cda_set_pt", _l7b_cda_set_pt)


# ---- Layer 7c: P/T modifications ----

def _l7c_anthem(state: GameState, effect: ContinuousEffect,
                table: Dict[str, Characteristics]) -> None:
    """Glorious Anthem / Honor of the Pure / etc. Adds +X/+Y to
    matching creatures. target_pattern with 'p_mod', 't_mod' ints."""
    targets = _select_targets(state, effect.target_pattern, table)
    p_mod = int(effect.target_pattern.get("p_mod", 0) or 0)
    t_mod = int(effect.target_pattern.get("t_mod", 0) or 0)
    for chars in targets:
        chars.power += p_mod
        chars.toughness += t_mod


register_layer_effect("anthem_pt_mod", _l7c_anthem)


# ---- Layer 7d: P/T switches ----

def _l7d_switch_pt(state: GameState, effect: ContinuousEffect,
                   table: Dict[str, Characteristics]) -> None:
    """Inverter of Truth-style: switch P and T of matching permanents."""
    targets = _select_targets(state, effect.target_pattern, table)
    for chars in targets:
        chars.power, chars.toughness = chars.toughness, chars.power


register_layer_effect("switch_pt", _l7d_switch_pt)
