"""State-based actions per CR 704.

SBAs run between every priority pass and again after each one fires
(loop until no more SBA fires). Iter 10 implements the core 8:

  1. Player with 0 or less life loses.
  2. Player with 10+ poison counters loses (iter 10 skips poison).
  3. Player who attempted to draw from empty library loses.
  4. Creature with 0 or less toughness dies.
  5. Creature with damage marked ≥ toughness dies.
  6. Planeswalker with 0 loyalty dies (→ graveyard).
  7. Legend rule: 2+ legendary permanents same controller + name → all but one die.
  8. Commander damage: player with 21+ from a single commander loses.

Auras with no valid target unattach (CR 704.5n) — iter 10 stub: simply
move the aura to its owner's graveyard.

Each SBA returns one or more mutations. Mutations are applied via
DieEvent / LifeChangeEvent / etc. flowing through apply_replacements
so e.g. Rest in Peace's exile-instead-of-graveyard fires correctly.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from api.engine.pillar_f.v0_2.state import (
    GameState, Card,
)
from api.engine.pillar_f.v0_2.replacement.events import (
    DieEvent, LifeChangeEvent,
)
from api.engine.pillar_f.v0_2.replacement.replacement import apply_replacements


SBA_VERSION = "pillar_f_v0_2_sba_v1"

# Commander damage lethal threshold per CR 903.10a.
COMMANDER_DAMAGE_LETHAL = 21


def check_state_based_actions(state: GameState) -> List[Dict[str, Any]]:
    """Run one pass of all state-based actions. Mutates state. Returns
    a list of action dicts describing what fired (for tests + logs).
    Caller should re-call until the return list is empty (CR 704.3).
    """
    actions: List[Dict[str, Any]] = []

    # 1. + 3. + 8. Player loss conditions.
    for ps in state.players:
        if ps.has_lost:
            continue
        if ps.life_total <= 0:
            ps.has_lost = True
            actions.append({"action": "player_loses_life_0",
                            "player_id": ps.player_id,
                            "life_total": ps.life_total})
            continue
        if ps.has_drawn_from_empty_library:
            ps.has_lost = True
            actions.append({"action": "player_loses_empty_library",
                            "player_id": ps.player_id})
            continue
        for cmdr_oracle, dmg in ps.commander_damage_taken_from.items():
            if dmg >= COMMANDER_DAMAGE_LETHAL:
                ps.has_lost = True
                actions.append({"action": "player_loses_commander_damage",
                                "player_id": ps.player_id,
                                "commander_oracle": cmdr_oracle,
                                "damage": dmg})
                break
        if ps.has_lost:
            continue

    # 4. + 5. Creatures dying. Build a list of cards to die (process
    # together so SBAs that fire on multiple creatures simultaneously
    # don't generate cascading DieEvents one-at-a-time).
    creatures_to_die: List[Card] = []
    for ps in state.players:
        if ps.has_lost:
            continue
        for cid in list(ps.zones.battlefield):
            card = state.get_card(cid)
            if card is None or not card.is_creature():
                continue
            t = card.toughness_int()
            if t <= 0:
                creatures_to_die.append(card)
                actions.append({"action": "creature_dies_zero_toughness",
                                "card_id": card.card_id})
            elif card.damage_marked >= t:
                creatures_to_die.append(card)
                actions.append({"action": "creature_dies_lethal_damage",
                                "card_id": card.card_id,
                                "damage": card.damage_marked,
                                "toughness": t})

    # 6. Planeswalkers with 0 loyalty.
    pw_to_die: List[Card] = []
    for ps in state.players:
        if ps.has_lost:
            continue
        for cid in list(ps.zones.battlefield):
            card = state.get_card(cid)
            if card is None or not card.is_planeswalker():
                continue
            loyalty = card.counters.get("loyalty", 0)
            if loyalty <= 0:
                pw_to_die.append(card)
                actions.append({"action": "planeswalker_dies_zero_loyalty",
                                "card_id": card.card_id})

    # 7. Legend rule. For each player, group their legendaries by name;
    # 2+ with same name → all but one go to graveyard (controller's choice
    # for which to keep; iter 10 keeps the first encountered).
    legendaries_to_die: List[Card] = []
    for ps in state.players:
        if ps.has_lost:
            continue
        by_name: Dict[str, List[Card]] = defaultdict(list)
        for cid in ps.zones.battlefield:
            card = state.get_card(cid)
            if card is None or not card.is_legendary():
                continue
            by_name[card.name].append(card)
        for name, cards in by_name.items():
            if len(cards) > 1:
                # Keep first; rest die.
                for extra in cards[1:]:
                    legendaries_to_die.append(extra)
                    actions.append({"action": "legend_rule_dies",
                                    "card_id": extra.card_id,
                                    "name": name})

    # Send all dying things through DieEvent → apply replacements → move to GY.
    for card in creatures_to_die + pw_to_die + legendaries_to_die:
        _send_to_graveyard(state, card, cause=(
            "lethal_damage" if card.damage_marked >= card.toughness_int()
            else "sba"
        ))

    # 9. Auras attached to invalid targets. Iter 10 stub: aura whose
    # attached_to no longer points to a valid battlefield card → unattach
    # + move aura to owner's graveyard.
    auras_to_gy: List[Card] = []
    all_battlefield_ids: Set[str] = set()
    for ps in state.players:
        all_battlefield_ids.update(ps.zones.battlefield)
    for ps in state.players:
        for cid in list(ps.zones.battlefield):
            card = state.get_card(cid)
            if card is None:
                continue
            if "aura" in (card.subtypes or []) or "Aura" in (card.subtypes or []):
                if card.attached_to and card.attached_to not in all_battlefield_ids:
                    auras_to_gy.append(card)
                    actions.append({"action": "aura_falls_off",
                                    "card_id": card.card_id})
    for card in auras_to_gy:
        _send_to_graveyard(state, card, cause="sba")

    return actions


def _send_to_graveyard(state: GameState, card: Card, *, cause: str) -> None:
    """Move card from battlefield to its owner's graveyard, routing
    through a DieEvent so replacement effects (Rest in Peace, Leyline
    of the Void) can redirect to exile or other zones."""
    event = DieEvent(card_id=card.card_id, controller=card.controller,
                     cause=cause)
    apply_replacements(state, event, affected_controller=card.controller)
    # Determine destination zone: replacement may have set instead_zone.
    dest_zone = event.instead_zone or "graveyard"
    owner_id = card.owner
    # Remove from current zone.
    for ps in state.players:
        ps.zones.remove_card(card.card_id)
    # Add to destination.
    state.players[owner_id].zones.add_card(card.card_id, dest_zone)
    # Reset card state (damage marked clears on zone change).
    card.damage_marked = 0
    card.tapped = False
    card.attached_to = None
    card.attached_by = []
    card.counters = {}
    card.summoning_sick = False


def run_sba_loop(state: GameState, *, max_iterations: int = 50) -> List[Dict[str, Any]]:
    """Repeatedly run check_state_based_actions until no actions fire
    (CR 704.3 — SBAs run until nothing more is checking). Returns the
    flat list of all actions taken across all iterations."""
    all_actions: List[Dict[str, Any]] = []
    safety = max_iterations
    while safety > 0:
        safety -= 1
        actions = check_state_based_actions(state)
        if not actions:
            break
        all_actions.extend(actions)
    # Check game-over: only 1 player remains alive → that player wins.
    alive = [p for p in state.players if not p.has_lost]
    if len(alive) == 1 and not state.game_over:
        state.game_over = True
        state.winner_player_id = alive[0].player_id
    elif len(alive) == 0 and not state.game_over:
        state.game_over = True  # draw — no winner
    return all_actions
