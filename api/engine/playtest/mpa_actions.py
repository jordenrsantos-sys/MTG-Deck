"""
mpa_actions — Phase 5b.1b.

Legal action enumeration for the MTG Playing Agent. Given a GameState +
seat with priority, returns the list of legal actions that seat can take.

Scope of this minimal viable shipment:
  - Pass priority
  - Play land (sorcery-speed, max 1 per turn, must be active player main phase)
  - Cast creature/artifact/enchantment from hand (sorcery-speed, mana cost
    payable from untapped lands + any open mana)
  - Cast commander from command zone (mana + commander tax)
  - Activate basic-land mana abilities (no stack, instant-speed)
  - Declare attackers (when active player in declare_attackers step)
  - Declare blockers (when defending player in declare_blockers step)

NOT yet handled (subsequent ships):
  - Instant-speed casts on stack (response timing)
  - Activated abilities with cost beyond mana
  - Triggered abilities
  - Modal/X/optional choices
  - Targeting with restrictions
  - Cost reduction effects
  - Spell copy / madness / cascade / suspend / etc.

Architectural rules served (DESIGN_DECISIONS.md):
  - 1.4 Pilot competence: legal action enumeration is the substrate. Without
    this, no policy can choose legal plays.
  - 1.4 Anti-bias: enumeration is symmetric — given the same state, both seats
    receive identical action sets.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from .mpa_game_state import (
    CardInGame, GameState, PlayerState, Phase, Step, Zone,
)


class ActionType(str, Enum):
    PASS_PRIORITY = "pass_priority"
    PLAY_LAND = "play_land"
    CAST_FROM_HAND = "cast_from_hand"
    CAST_COMMANDER = "cast_commander"
    TAP_FOR_MANA = "tap_for_mana"
    DECLARE_ATTACKER = "declare_attacker"
    DECLARE_BLOCKER = "declare_blocker"


@dataclass
class Action:
    type: ActionType
    seat_index: int
    source_instance_id: Optional[int] = None  # the card being acted on
    target_instance_id: Optional[int] = None  # for blockers, etc.
    notes: str = ""


def enumerate_legal_actions(state: GameState, seat_index: int) -> List[Action]:
    """Return the list of legal actions for `seat_index` given current state.

    Always includes PASS_PRIORITY. Other actions added only if their gating
    conditions hold.

    Performance: <1ms for typical mid-game state (<100 permanents). Caller
    should not call this in tight loops; called once per priority window.
    """
    actions: List[Action] = [Action(type=ActionType.PASS_PRIORITY, seat_index=seat_index)]

    if state.game_over:
        return actions

    # Priority must rest with this seat for non-mana actions
    has_priority = state.priority_player_index == seat_index
    is_active = state.active_player_index == seat_index
    is_main_phase = state.phase in (Phase.PRECOMBAT_MAIN, Phase.POSTCOMBAT_MAIN)
    stack_empty = len(state.stack) == 0

    player = state.players[seat_index]

    # ---- Mana abilities (no priority needed for activation under CR 605.3) ----
    if has_priority or True:  # mana abilities can be activated without holding priority
        for permanent in player.battlefield:
            if permanent.is_land() and not permanent.tapped:
                actions.append(Action(
                    type=ActionType.TAP_FOR_MANA,
                    seat_index=seat_index,
                    source_instance_id=permanent.instance_id,
                    notes=f"Tap {permanent.name} for mana",
                ))

    # ---- Sorcery-speed actions (active player, main phase, empty stack, priority) ----
    if has_priority and is_active and is_main_phase and stack_empty:
        # Play land (max 1 per turn)
        if state.lands_played_this_turn_by_seat.get(seat_index, 0) < 1:
            for card in player.hand:
                if card.is_land():
                    actions.append(Action(
                        type=ActionType.PLAY_LAND,
                        seat_index=seat_index,
                        source_instance_id=card.instance_id,
                        notes=f"Play {card.name}",
                    ))

        # Cast from hand — heuristic: only sorcery-speed cards here (creatures,
        # sorceries, artifacts, enchantments, planeswalkers). Instants are
        # cast-anywhere but we don't yet handle response timing.
        available_mana = _available_mana(state, seat_index)
        for card in player.hand:
            if card.is_land():
                continue
            if _can_pay_cost(card, available_mana):
                actions.append(Action(
                    type=ActionType.CAST_FROM_HAND,
                    seat_index=seat_index,
                    source_instance_id=card.instance_id,
                    notes=f"Cast {card.name}",
                ))

        # Cast commander (mana + tax)
        for card in player.command_zone:
            tax = player.commander_tax_paid
            if _can_pay_cost(card, available_mana, extra_generic=tax):
                actions.append(Action(
                    type=ActionType.CAST_COMMANDER,
                    seat_index=seat_index,
                    source_instance_id=card.instance_id,
                    notes=f"Cast commander {card.name} (tax {tax})",
                ))

    # ---- Combat actions ----
    if has_priority and is_active and state.step == Step.DECLARE_ATTACKERS:
        # Active player's creatures that are untapped and not summoning sick can attack
        for c in player.battlefield:
            if c.is_creature() and not c.tapped and not c.summoning_sick:
                actions.append(Action(
                    type=ActionType.DECLARE_ATTACKER,
                    seat_index=seat_index,
                    source_instance_id=c.instance_id,
                    notes=f"Attack with {c.name}",
                ))

    if has_priority and not is_active and state.step == Step.DECLARE_BLOCKERS:
        # Defending player's untapped creatures can block declared attackers
        attackers = [
            obj for obj in state.stack
            if obj.get("type") == "attack" and obj.get("controller_seat") != seat_index
        ]
        for c in player.battlefield:
            if c.is_creature() and not c.tapped:
                for atk in attackers:
                    actions.append(Action(
                        type=ActionType.DECLARE_BLOCKER,
                        seat_index=seat_index,
                        source_instance_id=c.instance_id,
                        target_instance_id=atk.get("source_instance_id"),
                        notes=f"Block with {c.name}",
                    ))

    return actions


# ============================================================
# Mana cost evaluation (minimal viable)
# ============================================================


def _available_mana(state: GameState, seat_index: int) -> Dict[str, int]:
    """Total mana available to this seat — pool + untapped basic lands.

    Returns a dict {color: count} where color is one of W/U/B/R/G/C.
    Generic mana is handled separately (any of the above counts as generic).
    """
    pool = dict(state.mana_pool_by_seat.get(seat_index, {}))
    for k in ("W", "U", "B", "R", "G", "C"):
        pool.setdefault(k, 0)
    # Add untapped lands (assumes basic-land mana — Mountain → R, etc.)
    for c in state.players[seat_index].battlefield:
        if c.is_land() and not c.tapped:
            color = _land_color(c)
            if color:
                pool[color] = pool.get(color, 0) + 1
            else:
                pool["C"] = pool.get("C", 0) + 1
    return pool


def _land_color(card: CardInGame) -> Optional[str]:
    name = (card.name or "").strip()
    if name in ("Mountain", "Snow-Covered Mountain"):
        return "R"
    if name in ("Island", "Snow-Covered Island"):
        return "U"
    if name in ("Plains", "Snow-Covered Plains"):
        return "W"
    if name in ("Swamp", "Snow-Covered Swamp"):
        return "B"
    if name in ("Forest", "Snow-Covered Forest"):
        return "G"
    # Could parse produced_mana from card data if available
    return None


def _can_pay_cost(
    card: CardInGame,
    available: Dict[str, int],
    extra_generic: int = 0,
) -> bool:
    """Mana-cost check using card.mana_cost when set (precise color check)
    or falling back to cmc + color_identity approximation."""
    if isinstance(card.mana_cost, str) and card.mana_cost.strip():
        try:
            from .mpa_mana_cost import parse_mana_cost, can_pay
            from copy import copy
            parsed = parse_mana_cost(card.mana_cost)
            if extra_generic > 0:
                parsed = copy(parsed)
                parsed.generic = parsed.generic + extra_generic
            return can_pay(parsed, available)
        except Exception:
            pass
    total = sum(available.values())
    needed = (card.cmc or 0) + extra_generic
    if total < needed:
        return False
    for color in (card.color_identity or ()):
        if color in ("W", "U", "B", "R", "G") and available.get(color, 0) < 1:
            return False
    return True