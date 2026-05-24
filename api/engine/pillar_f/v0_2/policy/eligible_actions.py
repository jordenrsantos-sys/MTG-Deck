"""Engine-side legal-action computation.

Given a GameState + player_id, returns a list of action dicts the
player may legally take RIGHT NOW. The LLM picks one via the main-phase
prompt; the engine applies it.

Iter-11 (this task) ships a minimal implementation that supports the
substrate's existing resolver registry (noop, deal_damage_to_player,
draw_cards) + basic play-land + activate-ability. Per-card oracle
compilation (v11, parallel) will extend the eligible-action set with
full card mechanics in iter-12+.

Eligible action contract (each dict):
  {
    "action_type": "pass_priority" | "play_land" | "cast_spell"
                 | "activate_ability",
    "card_id": Optional[str],     # source card for cast/activate/play
    "ability_idx": Optional[int],  # which ability for activated
    "targets": List[Any],         # already-picked targets (LLM may
                                  # alter via downstream parser, but
                                  # default is single-target-required)
    "payment": Dict[str, Any],    # mana payment + resolver name
    "description": str,           # human-readable for prompt
  }

A pass_priority entry is ALWAYS included (legal at every priority
window).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from api.engine.pillar_f.v0_2.state import GameState, Step


ELIGIBLE_ACTIONS_VERSION = "pillar_f_v0_2_policy_eligible_actions_v1"


# Steps where casting sorceries / playing lands / activating sorcery-
# speed abilities is legal. Per CR 305 + 307, "you may play a land
# during your main phase if the stack is empty, you have priority,
# and you haven't already played the land for the turn."
_MAIN_PHASE_STEPS = {Step.MAIN_1, Step.MAIN_2}


def compute_eligible_actions(
    state: GameState, player_id: int,
) -> List[Dict[str, Any]]:
    """Returns the list of legal action dicts for `player_id` right
    now. Always includes pass_priority. May include play_land /
    cast_spell / activate_ability depending on game state.

    Iter-10 substrate constraints:
    - cast_spell only emits actions for cards in player's hand whose
      resolver is one of the registered minimal resolvers
      ({noop, deal_damage_to_player, draw_cards}) OR cards with no
      mana_cost requirement (free-spell stubs).
    - activate_ability skipped iter-10 (no card carries activated
      abilities in the substrate's minimal effect registry; v11
      ships these).
    - play_land emits actions for any Land-type card in hand if
      lands_played_this_turn < 1 AND it's an active-player main phase
      AND the stack is empty.
    """
    actions: List[Dict[str, Any]] = []

    # Pass priority — always legal.
    actions.append({
        "action_type": "pass_priority",
        "card_id": None,
        "ability_idx": None,
        "targets": [],
        "payment": {},
        "description": "Pass priority.",
    })

    if not (0 <= player_id < len(state.players)):
        return actions
    player = state.players[player_id]
    if player.has_lost:
        return actions

    is_active = (state.active_player == player_id)
    is_main_phase_stack_empty = (
        is_active and state.step in _MAIN_PHASE_STEPS and not state.stack
    )

    # ---- play_land ----
    if is_main_phase_stack_empty and player.lands_played_this_turn < 1:
        for cid in player.zones.hand:
            card = state.get_card(cid)
            if card is None:
                continue
            if card.is_land():
                actions.append({
                    "action_type": "play_land",
                    "card_id": cid,
                    "ability_idx": None,
                    "targets": [],
                    "payment": {},
                    "description": f"Play land: {card.name}",
                })

    # ---- cast_spell ----
    # Iter-10 limitation: only cards whose payment.resolver maps to a
    # registered substrate resolver can be cast. Cards carry an
    # iter-10-compatible payment template via `oracle_text` parsing OR
    # via per-card hardcoded entries in v11.
    #
    # For iter-11's policy work, we accept cards that have an
    # `iter10_resolver` annotation (set at deck-build time by the
    # hardcoded simple decks). Cards lacking the annotation can't be
    # cast through this path; they stay in hand. v11 will populate
    # _RESOLVERS for real cards.
    can_cast_sorcery = is_main_phase_stack_empty
    for cid in player.zones.hand:
        card = state.get_card(cid)
        if card is None or card.is_land():
            continue
        annotation = _read_iter10_annotation(card)
        if annotation is None:
            continue
        is_instant = "instant" in (card.type_line or "").lower()
        # Sorceries only on own main phase with empty stack.
        if not is_instant and not can_cast_sorcery:
            continue
        # Iter-10: mana payment is a no-op (LLM doesn't pay; cost-tracker
        # tracks the LLM-call cost, not the in-game mana cost). Engine
        # accepts the spell on faith. v11+ wires real mana payment.
        targets = annotation.get("default_targets") or []
        # Sub-C Phase 2: counterspell-family cards carry a
        # `target_stack_top: True` flag. When the stack is non-empty,
        # resolve target to the top entry's id so the counter can
        # actually point at something. If the stack is empty, the
        # counter has no legal target and we skip emitting this cast
        # action (LLM can't cast a counter with no target).
        if annotation.get("target_stack_top"):
            if not state.stack:
                continue
            top_entry = state.stack[-1]
            targets = [top_entry.entry_id]
        actions.append({
            "action_type": "cast_spell",
            "card_id": cid,
            "ability_idx": None,
            "targets": list(targets),
            "payment": dict(annotation.get("payment") or {}),
            "description": (
                f"Cast {card.name} "
                f"({annotation.get('description', '?')})"
            ),
        })

    return actions


def _read_iter10_annotation(card) -> Optional[Dict[str, Any]]:
    """Read the iter-10-policy annotation off a card. Cards in the
    hardcoded simple decks carry an `iter10_annotation` attribute set
    at deck-build time. Per-card oracle compilation (v11) will replace
    this with proper rules-text parsing.
    """
    return getattr(card, "iter10_annotation", None)


def apply_action(
    state: GameState, player_id: int, action: Dict[str, Any],
) -> None:
    """Apply the chosen action to state. Mutates state in place.

    Caller is responsible for invoking the priority loop + stack
    resolution after this returns; apply_action just performs the
    state mutation for the player's choice (push to stack for spells,
    move card from hand→battlefield for lands, etc.).
    """
    if not (0 <= player_id < len(state.players)):
        return
    player = state.players[player_id]
    atype = action.get("action_type")

    if atype == "pass_priority":
        # Engine's priority loop records the pass; apply_action is a
        # no-op for pass.
        return

    if atype == "play_land":
        cid = action.get("card_id")
        if not cid or cid not in player.zones.hand:
            return
        state.move_card(
            cid, from_player=player_id, from_zone="hand",
            to_player=player_id, to_zone="battlefield",
        )
        player.lands_played_this_turn += 1
        return

    if atype == "cast_spell":
        from api.engine.pillar_f.v0_2.stack import push_to_stack
        cid = action.get("card_id")
        if not cid or cid not in player.zones.hand:
            return
        # Move spell card from hand to stack (CR 601.2a — casting
        # moves the spell to the stack as it's cast).
        # Iter-10: we leave the card in hand temporarily and push a
        # stack entry referencing it; on resolution, the card moves
        # to graveyard (instant/sorcery) or battlefield (permanent).
        # For sub-B unit tests, the noop resolver doesn't need this
        # cleanup. v11+ wires the move-on-cast properly.
        push_to_stack(
            state,
            card_id=cid,
            controller=player_id,
            entry_type="spell",
            targets=action.get("targets") or [],
            payment=action.get("payment") or {"resolver": "noop"},
            description=action.get("description") or "Cast spell",
        )
        player.spells_cast_this_turn += 1
        # Iter-10 simplification: move card to graveyard immediately
        # (no on-resolution zone-change handling yet). v11+ wires the
        # proper move-on-resolution.
        player.zones.hand.remove(cid)
        player.zones.graveyard.append(cid)
        return

    # activate_ability: iter-10 stub — skipped.
    return
