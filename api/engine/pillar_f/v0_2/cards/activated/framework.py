"""Activated-ability framework: per-card activated-ability registry +
helpers for the common patterns (tap-for-mana, fetch-and-shuffle, equip).

The iter-10 substrate provides:
  - `register_resolver(name, fn)` — registers what happens when an
    activated/triggered ability resolves off the stack.
  - `push_to_stack(state, ...)` — puts an activated ability on the
    stack with a payment dict (caller is responsible for cost-payment
    validation in iter-10; iter-11+ wires automated cost-payment).

What this module adds on top of the substrate:
  - `register_activated_ability(card_name, ability_key, fn)` — maps
    (card_name, ability_key) → ability metadata for lookup. ability_key
    is a short string identifying which ability on the card (cards can
    have multiple — Mind Stone has "tap_mana" + "sac_draw").
  - `get_activated_abilities(card_name)` — list of registered keys.
  - `build_activation_payload(card_name, ability_key, **kwargs)` —
    constructs the payment dict that push_to_stack expects, with the
    resolver name baked in. Wraps the substrate's contract.

Per-card modules call:
  1. `register_resolver("act_<card_slug>_<ability_key>", resolve_fn)`
     to wire the EFFECT side.
  2. `register_activated_ability(card_name, ability_key, metadata)`
     to wire the METADATA side (so test code + the eventual cast-
     and-activate plumbing can look up the resolver name + cost spec).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple


@dataclass
class ActivatedAbilityMeta:
    """Metadata for a single activated ability on a card. Holds the
    cost spec (mana, tap, sacrifice, pay-life) + the resolver name the
    substrate's stack uses when the ability resolves.

    Iter-10's cost-payment is caller-controlled: the test (or eventual
    cast-and-activate plumbing) is responsible for setting `tapped =
    True` on the source card before pushing, paying mana from the pool,
    sacrificing additional cards, paying life, etc. The resolver only
    handles the EFFECT side.
    """
    card_name: str
    ability_key: str            # short id: "tap_mana", "equip",
                                # "fetch_basic", "sac_draw", etc.
    resolver_name: str          # registered via register_resolver
    cost_mana: str = ""         # e.g., "{1}{G}" or "{0}"
    cost_tap: bool = False      # {T} tap cost
    cost_sacrifice_self: bool = False
    cost_sacrifice_other: str = ""  # e.g., "creature you control"
    cost_pay_life: int = 0
    cost_discard: int = 0
    description: str = ""
    # Sorcery-speed only? (Most planeswalker activations.)
    sorcery_speed: bool = False


# Registry: (card_name, ability_key) → ActivatedAbilityMeta
_ACTIVATED_REGISTRY: Dict[Tuple[str, str], ActivatedAbilityMeta] = {}


def register_activated_ability(meta: ActivatedAbilityMeta) -> None:
    """Register the metadata for an activated ability. Key is
    (card_name, ability_key). Re-registering overwrites; per-card
    modules register once at import time."""
    _ACTIVATED_REGISTRY[(meta.card_name, meta.ability_key)] = meta


def get_activated_ability(
    card_name: str, ability_key: str,
) -> Optional[ActivatedAbilityMeta]:
    return _ACTIVATED_REGISTRY.get((card_name, ability_key))


def get_activated_abilities_for_card(card_name: str) -> List[ActivatedAbilityMeta]:
    return [m for (cn, _ak), m in _ACTIVATED_REGISTRY.items() if cn == card_name]


def all_registered_card_names() -> List[str]:
    return sorted({cn for (cn, _ak) in _ACTIVATED_REGISTRY.keys()})


def build_activation_payload(
    card_name: str, ability_key: str, **extra: Any,
) -> Dict[str, Any]:
    """Build the payment dict that push_to_stack expects. Returns
    {"resolver": <resolver_name>, **extra}. Raises KeyError if the
    (card_name, ability_key) isn't registered."""
    meta = _ACTIVATED_REGISTRY.get((card_name, ability_key))
    if meta is None:
        raise KeyError(
            f"No activated ability registered for ({card_name!r}, {ability_key!r})",
        )
    payload: Dict[str, Any] = {"resolver": meta.resolver_name}
    payload.update(extra)
    return payload


# ============================================================
# Common-resolver factories — reduce per-card boilerplate
# ============================================================


def add_mana_resolver(color_letter: str, amount: int = 1):
    """Returns a resolver fn that adds `amount` mana of `color_letter`
    to the controller's pool. Suitable for any "tap for {X} mana of
    color C" ability (Sol Ring + 2C, Ancient Tomb + 2C, basics + 1,
    etc.)."""
    from api.engine.pillar_f.v0_2.state import GameState, StackEntry

    def fn(state: GameState, entry: StackEntry) -> None:
        pid = entry.controller
        if not (0 <= pid < len(state.players)):
            return None
        pool = state.players[pid].mana_pool
        # Multi-color sequences not supported by this factory — caller
        # should compose multiple resolvers or register a custom one.
        current = getattr(pool, color_letter, 0)
        setattr(pool, color_letter, current + amount)
        return None
    return fn


def add_multiple_mana_resolver(production: Dict[str, int]):
    """Like add_mana_resolver but for multi-color or color-pair
    production. e.g., {"C": 2} for Sol Ring, {"R": 1, "G": 1} for a
    fictional dual-color tap."""
    from api.engine.pillar_f.v0_2.state import GameState, StackEntry

    def fn(state: GameState, entry: StackEntry) -> None:
        pid = entry.controller
        if not (0 <= pid < len(state.players)):
            return None
        pool = state.players[pid].mana_pool
        for color, amt in production.items():
            current = getattr(pool, color, 0)
            setattr(pool, color, current + amt)
        return None
    return fn


def add_any_color_resolver():
    """Returns a resolver fn that reads `entry.payment["color"]` (a
    single-letter WUBRGC) and adds 1 mana of that color. Defaults to C
    if not specified. Used by Birds of Paradise, Chrome Mox after
    imprint, Lotus Petal, Talismans (any-color side), etc."""
    from api.engine.pillar_f.v0_2.state import GameState, StackEntry

    def fn(state: GameState, entry: StackEntry) -> None:
        pid = entry.controller
        if not (0 <= pid < len(state.players)):
            return None
        color = entry.payment.get("color", "C")
        if color not in ("W", "U", "B", "R", "G", "C"):
            color = "C"
        pool = state.players[pid].mana_pool
        setattr(pool, color, getattr(pool, color, 0) + 1)
        return None
    return fn


def add_commander_color_identity_resolver():
    """Tap-for-color-in-commander-identity (Arcane Signet, Command
    Tower, Path of Ancestry). Reads `entry.payment["color"]`; the
    caller is responsible for ensuring the color is in their commander's
    color identity (iter-10 doesn't validate — the LLM policy layer in
    iter-11+ will)."""
    return add_any_color_resolver()


def fetchland_resolver(allowed_subtypes: Tuple[str, ...], *,
                       basic_only: bool = True):
    """Fetchland tutor: sacrifice has already been paid (caller moves
    the fetch land to graveyard before resolution). Search library for
    a land with `allowed_subtypes` membership; put onto battlefield
    (tapped or untapped per the printed text — most fetchlands say
    "onto the battlefield" without "tapped").

    `allowed_subtypes` example: ("Forest", "Mountain") for Wooded
    Foothills. `basic_only=True` restricts to Basic-supertype cards;
    set False for shocks/duals tutoring (not present in standard
    fetchlands but useful for Misty Rainforest-shaped tutoring).
    """
    from api.engine.pillar_f.v0_2.state import GameState, StackEntry

    def fn(state: GameState, entry: StackEntry) -> None:
        pid = entry.controller
        if not (0 <= pid < len(state.players)):
            return None
        for cid in state.players[pid].zones.library:
            card = state.get_card(cid)
            if card is None:
                continue
            if not card.is_land():
                continue
            if basic_only and not card.is_legendary() and "Basic" not in card.type_line:
                continue
            # Match by subtype list overlap.
            subs = set(card.subtypes or []) | set(
                _parse_subtypes_from_type_line(card.type_line),
            )
            if not subs.intersection(allowed_subtypes):
                continue
            state.move_card(
                cid, from_player=pid, from_zone="library",
                to_player=pid, to_zone="battlefield",
            )
            # Iter-11+: shuffle library here. Iter-10 stubs the shuffle.
            return None
        return None
    return fn


def _parse_subtypes_from_type_line(type_line: str) -> List[str]:
    if "—" not in type_line:
        return []
    return [s.strip() for s in type_line.split("—", 1)[1].split() if s.strip()]


def equip_resolver(equip_keyword: str = "equip"):
    """Equipment equip-cost activated ability. Targets a creature; sets
    the equipment's `attached_to` field + appends the equipment's
    card_id to the creature's `attached_by` list. Detaches from any
    prior creature first (an Equipment can only be attached to one
    creature at a time — CR 301.5).

    `entry.targets[0]` = target creature card_id.
    """
    from api.engine.pillar_f.v0_2.state import GameState, StackEntry

    def fn(state: GameState, entry: StackEntry) -> None:
        if not entry.targets:
            return None
        equipment_cid = entry.card_id
        target_cid = entry.targets[0]
        equipment = state.get_card(equipment_cid) if equipment_cid else None
        target = state.get_card(target_cid)
        if equipment is None or target is None:
            return None
        if not target.is_creature():
            return None
        # Detach from prior creature.
        if equipment.attached_to:
            prior = state.get_card(equipment.attached_to)
            if prior is not None and equipment_cid in prior.attached_by:
                prior.attached_by.remove(equipment_cid)
        equipment.attached_to = target_cid
        if equipment_cid not in target.attached_by:
            target.attached_by.append(equipment_cid)
        return None
    return fn
