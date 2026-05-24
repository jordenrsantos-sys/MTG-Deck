"""LLM-driven combat attackers + blockers wiring.

Sub-C Phase 1. Owns the substrate-bridge for sub-B Phase 9 gate 5.

Workflow per combat phase:

  1. eligible_attackers = filter active_player's battlefield by
     combat.can_attack(card, chars)
  2. attack_targets = alive opponents + their planeswalkers
  3. LLM-pick attackers via sub-B's attackers prompt
  4. Convert + validate, on failure re-prompt (up to 2); on 3rd failure
     fallback = no attack
  5. For each defending player who has incoming attackers:
       a. eligible_blockers = their untapped creatures (not the active
          player; not summoning-sick check necessary for blockers since
          haste doesn't matter for blocking, BUT defender of course
          blocks normally)
       b. LLM-pick blockers via sub-B's blockers prompt
       c. Convert + validate, on failure re-prompt; on 3rd fail = no
          block
  6. Hand AttackerDeclaration list + BlockerAssignment list to
     substrate's run_combat_phase

LLM client + cost tracker are SHARED with the priority responder so
combat calls bill into the same per-turn / per-game ceilings.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from api.engine.pillar_f.v0_2.state import Card, GameState
from api.engine.pillar_f.v0_2.layers import (
    apply_continuous_effects, Characteristics,
)
from api.engine.pillar_f.v0_2.combat import (
    AttackerDeclaration, BlockerAssignment, CombatState,
    can_attack, run_combat_phase,
)
from api.engine.pillar_f.v0_2.policy.cost import CostTracker
from api.engine.pillar_f.v0_2.policy.prompts import (
    ATTACKERS_SYSTEM_PROMPT, BLOCKERS_SYSTEM_PROMPT,
    build_attackers_prompt, build_blockers_prompt,
    compact_view,
)
from api.engine.pillar_f.v0_2.policy.parsers import (
    parse_attackers_response, parse_blockers_response,
    AttackersResponse, BlockersResponse,
)


COMBAT_DECIDER_VERSION = "pillar_f_v0_2_playtest_combat_decider_v1"

# Token budgets per sub-B scoping doc section 2b.
DEFAULT_MAX_INPUT_TOKENS = 5000
DEFAULT_MAX_OUTPUT_TOKENS = 800
MAX_REPROMPTS = 2  # -> at most 3 calls per decision


@dataclass
class CombatDecisionRecord:
    """Diagnostics for one combat-phase decision (attackers OR one
    defending player's blockers)."""
    phase: str                    # "attackers" or "blockers"
    player_id: int                # decider's seat
    turn_number: int
    eligible_count: int
    llm_calls_made: int
    parse_failures: int
    fallback_used: bool
    final_count: int              # attackers or blockers chosen
    rationale: str = ""


def _untapped_creatures(
    state: GameState, player_id: int,
) -> List[Tuple[Card, Characteristics]]:
    """Return (card, characteristics) pairs for untapped creatures on
    `player_id`'s battlefield."""
    if not (0 <= player_id < len(state.players)):
        return []
    chars_table = apply_continuous_effects(state)
    out: List[Tuple[Card, Characteristics]] = []
    for cid in state.players[player_id].zones.battlefield:
        card = state.get_card(cid)
        if card is None or card.tapped:
            continue
        chars = chars_table.get(cid)
        if chars is None or "Creature" not in chars.types:
            continue
        out.append((card, chars))
    return out


def compute_eligible_attackers(
    state: GameState, active_player: int,
) -> List[Dict[str, Any]]:
    """Build the eligible_attackers list the sub-B attackers prompt
    consumes. Filters by substrate's can_attack."""
    out: List[Dict[str, Any]] = []
    for card, chars in _untapped_creatures(state, active_player):
        if not can_attack(card, chars):
            continue
        out.append({
            "card_id": card.card_id,
            "name": card.name,
            "power": card.power_int(),
            "toughness": card.toughness_int(),
            "keywords": list(chars.keywords or []),
            "description": (
                f"{card.name} {card.power_int()}/{card.toughness_int()}"
                + (f" {', '.join(chars.keywords)}" if chars.keywords else "")
            ),
        })
    return out


def compute_attack_targets(
    state: GameState, active_player: int,
) -> List[Dict[str, Any]]:
    """Build the attack_targets list: alive opponents + their
    planeswalkers."""
    out: List[Dict[str, Any]] = []
    for pid in range(len(state.players)):
        if pid == active_player:
            continue
        player = state.players[pid]
        if player.has_lost:
            continue
        out.append({
            "target_id": pid,
            "kind": "player",
            "description": f"P{pid} (life={player.life_total})",
        })
        # Iter-11+: enumerate planeswalkers on player's battlefield.
        for cid in player.zones.battlefield:
            card = state.get_card(cid)
            if card is None or not card.is_planeswalker():
                continue
            out.append({
                "target_id": card.card_id,
                "kind": "planeswalker",
                "description": (
                    f"{card.name} (P{pid}'s planeswalker, "
                    f"loyalty={card.loyalty or '?'})"
                ),
            })
    return out


def compute_eligible_blockers(
    state: GameState, defending_player: int,
) -> List[Dict[str, Any]]:
    """Build the eligible_blockers list for ONE defending player. Per CR
    509.1: blockers must be untapped + creatures. Summoning-sick is OK
    for blocking (only attacking + activating tap abilities is
    restricted)."""
    out: List[Dict[str, Any]] = []
    for card, chars in _untapped_creatures(state, defending_player):
        out.append({
            "card_id": card.card_id,
            "name": card.name,
            "power": card.power_int(),
            "toughness": card.toughness_int(),
            "keywords": list(chars.keywords or []),
            "description": (
                f"{card.name} {card.power_int()}/{card.toughness_int()}"
                + (f" {', '.join(chars.keywords)}" if chars.keywords else "")
            ),
        })
    return out


def compute_attackers_to_block(
    combat_state: CombatState, defending_player: int,
    state: GameState,
) -> List[Dict[str, Any]]:
    """Build the attackers_to_block list shown in a defending player's
    blockers prompt — only attackers targeting THIS defender (player or
    their planeswalker)."""
    out: List[Dict[str, Any]] = []
    chars_table = apply_continuous_effects(state)
    for idx, decl in enumerate(combat_state.attackers):
        target = decl.target
        # Player target.
        target_pid: Optional[int] = None
        if isinstance(target, int):
            target_pid = target
        else:
            # Planeswalker — find its controller.
            tcard = state.get_card(target) if isinstance(target, str) else None
            if tcard is not None:
                target_pid = tcard.controller
        if target_pid != defending_player:
            continue
        attacker_card = state.get_card(decl.attacker_card_id)
        if attacker_card is None:
            continue
        chars = chars_table.get(decl.attacker_card_id)
        keywords = list(chars.keywords or []) if chars else []
        out.append({
            "attacker_index": idx,
            "card_id": decl.attacker_card_id,
            "name": attacker_card.name,
            "power": attacker_card.power_int(),
            "toughness": attacker_card.toughness_int(),
            "keywords": keywords,
            "description": (
                f"{attacker_card.name} "
                f"{attacker_card.power_int()}/{attacker_card.toughness_int()}"
                + (f" {', '.join(keywords)}" if keywords else "")
            ),
        })
    return out


def make_llm_attacker_decider(
    *,
    llm_client: Any,
    cost_tracker: CostTracker,
    politics_state_by_player: Optional[Dict[int, Dict[str, Any]]] = None,
    deck_archetype_hint_by_player: Optional[Dict[int, str]] = None,
    decision_log: Optional[List[CombatDecisionRecord]] = None,
) -> Callable[[GameState, int], List[AttackerDeclaration]]:
    """Return a callable (state, active_player) -> List[AttackerDeclaration].

    Asks the LLM to pick attackers + targets via sub-B's attackers
    prompt; validates via substrate.can_attack; re-prompts up to 2x on
    parse / validation failure; falls back to no-attack on 3rd failure.
    """
    if politics_state_by_player is None:
        politics_state_by_player = {}
    if deck_archetype_hint_by_player is None:
        deck_archetype_hint_by_player = {}
    if decision_log is None:
        decision_log = []

    def _decide(
        state: GameState, active_player: int,
    ) -> List[AttackerDeclaration]:
        eligible = compute_eligible_attackers(state, active_player)
        targets = compute_attack_targets(state, active_player)
        record = CombatDecisionRecord(
            phase="attackers", player_id=active_player,
            turn_number=state.turn_number,
            eligible_count=len(eligible),
            llm_calls_made=0, parse_failures=0,
            fallback_used=False, final_count=0,
        )
        if not eligible or not targets:
            decision_log.append(record)
            return []
        # Cost guardrails: if game halted or this player in fallback,
        # no attack.
        if cost_tracker.game_halted_for_cost:
            record.fallback_used = True
            decision_log.append(record)
            return []
        if cost_tracker.is_player_in_fallback(active_player, state.turn_number):
            record.fallback_used = True
            decision_log.append(record)
            return []
        if not llm_client.is_available():
            record.fallback_used = True
            decision_log.append(record)
            return []

        view = state.perspective_view(viewer_player_id=active_player)
        compact = compact_view(view, viewer_player_id=active_player)
        politics_ctx = politics_state_by_player.get(active_player)
        archetype = deck_archetype_hint_by_player.get(active_player)

        last_error: Optional[str] = None
        parsed: Optional[AttackersResponse] = None
        for _ in range(MAX_REPROMPTS + 1):
            prompt = build_attackers_prompt(
                compact, eligible, targets,
                politics_context=politics_ctx,
                deck_archetype_hint=archetype,
                last_error_message=last_error,
            )
            result = llm_client.call_with_budget(
                system=ATTACKERS_SYSTEM_PROMPT, user=prompt,
                max_input_tokens=DEFAULT_MAX_INPUT_TOKENS,
                max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS,
            )
            record.llm_calls_made += 1
            cost_tracker.record_call(
                player_id=active_player, turn_number=state.turn_number,
                cost_usd=getattr(result, "cost_usd", 0.0),
                purpose="combat_attackers",
            )
            if cost_tracker.game_halted_for_cost:
                record.fallback_used = True
                decision_log.append(record)
                return []
            if cost_tracker.is_player_in_fallback(
                active_player, state.turn_number,
            ):
                record.fallback_used = True
                decision_log.append(record)
                return []
            if not result.ok:
                last_error = (
                    f"LLM call failed: {result.error_message}. "
                    f"Pick valid JSON only."
                )
                record.parse_failures += 1
                continue
            cand, err = parse_attackers_response(
                result.text or "", eligible, targets,
            )
            if cand is not None:
                parsed = cand
                record.rationale = cand.rationale or ""
                break
            last_error = err or "Unknown parse error."
            record.parse_failures += 1
        if parsed is None:
            record.fallback_used = True
            decision_log.append(record)
            return []

        # Convert parsed response to AttackerDeclaration list, and
        # validate each against can_attack one more time (defense-in-
        # depth; eligible list already filtered).
        declarations: List[AttackerDeclaration] = []
        chars_table = apply_continuous_effects(state)
        for entry in parsed.attackers:
            elig = eligible[entry.attacker_index]
            target_info = targets[entry.target_index]
            atk_cid = elig["card_id"]
            card = state.get_card(atk_cid)
            if card is None:
                continue
            chars = chars_table.get(atk_cid)
            if chars is None or not can_attack(card, chars):
                continue
            # target_id is int (player) or str (planeswalker card_id).
            declarations.append(AttackerDeclaration(
                attacker_card_id=atk_cid,
                target=target_info["target_id"],
            ))
        record.final_count = len(declarations)
        decision_log.append(record)
        return declarations

    return _decide


def make_llm_blocker_decider(
    *,
    llm_client: Any,
    cost_tracker: CostTracker,
    politics_state_by_player: Optional[Dict[int, Dict[str, Any]]] = None,
    decision_log: Optional[List[CombatDecisionRecord]] = None,
) -> Callable[
    [GameState, CombatState, int],
    List[BlockerAssignment],
]:
    """Return a callable (state, combat_state, defending_player) ->
    List[BlockerAssignment]. ONE call per defending player.

    Asks the LLM to pick blockers + assignment order via sub-B's
    blockers prompt; validates each blocker; re-prompts on failure;
    fallback = no block (take all damage).
    """
    if politics_state_by_player is None:
        politics_state_by_player = {}
    if decision_log is None:
        decision_log = []

    def _decide(
        state: GameState, combat_state: CombatState,
        defending_player: int,
    ) -> List[BlockerAssignment]:
        eligible = compute_eligible_blockers(state, defending_player)
        attackers_to_block = compute_attackers_to_block(
            combat_state, defending_player, state,
        )
        record = CombatDecisionRecord(
            phase="blockers", player_id=defending_player,
            turn_number=state.turn_number,
            eligible_count=len(eligible),
            llm_calls_made=0, parse_failures=0,
            fallback_used=False, final_count=0,
        )
        # No attackers incoming OR no blockers available -> no decision.
        if not attackers_to_block:
            decision_log.append(record)
            return []
        if not eligible:
            decision_log.append(record)
            return []
        if cost_tracker.game_halted_for_cost:
            record.fallback_used = True
            decision_log.append(record)
            return []
        if cost_tracker.is_player_in_fallback(
            defending_player, state.turn_number,
        ):
            record.fallback_used = True
            decision_log.append(record)
            return []
        if not llm_client.is_available():
            record.fallback_used = True
            decision_log.append(record)
            return []

        view = state.perspective_view(viewer_player_id=defending_player)
        compact = compact_view(view, viewer_player_id=defending_player)
        politics_ctx = politics_state_by_player.get(defending_player)

        last_error: Optional[str] = None
        parsed: Optional[BlockersResponse] = None
        for _ in range(MAX_REPROMPTS + 1):
            prompt = build_blockers_prompt(
                compact, eligible, attackers_to_block,
                politics_context=politics_ctx,
                last_error_message=last_error,
            )
            result = llm_client.call_with_budget(
                system=BLOCKERS_SYSTEM_PROMPT, user=prompt,
                max_input_tokens=DEFAULT_MAX_INPUT_TOKENS,
                max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS,
            )
            record.llm_calls_made += 1
            cost_tracker.record_call(
                player_id=defending_player, turn_number=state.turn_number,
                cost_usd=getattr(result, "cost_usd", 0.0),
                purpose="combat_blockers",
            )
            if cost_tracker.game_halted_for_cost:
                record.fallback_used = True
                decision_log.append(record)
                return []
            if cost_tracker.is_player_in_fallback(
                defending_player, state.turn_number,
            ):
                record.fallback_used = True
                decision_log.append(record)
                return []
            if not result.ok:
                last_error = (
                    f"LLM call failed: {result.error_message}. "
                    f"Pick valid JSON only."
                )
                record.parse_failures += 1
                continue
            cand, err = parse_blockers_response(
                result.text or "", eligible, attackers_to_block,
            )
            if cand is not None:
                parsed = cand
                record.rationale = cand.rationale or ""
                break
            last_error = err or "Unknown parse error."
            record.parse_failures += 1
        if parsed is None:
            record.fallback_used = True
            decision_log.append(record)
            return []

        # Convert parsed blocks -> BlockerAssignment list.
        # parsed.blocks: List[BlockAssignmentParsed]
        #   attacker_index = index INTO attackers_to_block
        #   blocker_indices = indices INTO eligible
        assignments: List[BlockerAssignment] = []
        for blk in parsed.blocks:
            attacker_info = attackers_to_block[blk.attacker_index]
            blocker_ids: List[str] = []
            for bi in blk.blocker_indices:
                blocker_ids.append(eligible[bi]["card_id"])
            if not blocker_ids:
                continue
            assignments.append(BlockerAssignment(
                attacker_card_id=attacker_info["card_id"],
                blocker_card_ids=blocker_ids,
            ))
        record.final_count = len(assignments)
        decision_log.append(record)
        return assignments

    return _decide


def run_llm_combat_phase(
    state: GameState, active_player: int,
    *,
    attacker_decider: Callable[[GameState, int], List[AttackerDeclaration]],
    blocker_decider: Callable[
        [GameState, CombatState, int], List[BlockerAssignment],
    ],
) -> Tuple[CombatState, List[Dict[str, Any]]]:
    """End-to-end LLM-driven combat phase.

    1. attacker_decider picks attackers.
    2. If any attackers: blocker_decider runs once per defending
       player (any opponent with at least one incoming attacker).
    3. Hand declarations to substrate's run_combat_phase.

    Returns (combat_state, action_log).
    """
    attackers = attacker_decider(state, active_player)
    if not attackers:
        # No attack -- empty combat.
        return CombatState(), []
    # Identify defending players (unique opponents with at least one
    # incoming attacker).
    defending_pids: List[int] = []
    seen: set = set()
    for decl in attackers:
        t = decl.target
        if isinstance(t, int):
            pid = t
        else:
            tcard = state.get_card(t) if isinstance(t, str) else None
            pid = tcard.controller if tcard is not None else -1
        if pid >= 0 and pid not in seen:
            defending_pids.append(pid)
            seen.add(pid)
    # Sub-step 1: declare attackers via substrate.
    from api.engine.pillar_f.v0_2.combat import declare_attackers as _decl_atk
    cs = _decl_atk(state, attackers)
    # Sub-step 2: each defender picks blockers; accumulate assignments.
    all_assignments: List[BlockerAssignment] = []
    for dpid in defending_pids:
        if state.players[dpid].has_lost:
            continue
        dec_assignments = blocker_decider(state, cs, dpid)
        all_assignments.extend(dec_assignments)
    # Sub-step 3: hand all assignments to substrate.
    from api.engine.pillar_f.v0_2.combat import (
        declare_blockers as _decl_blk,
        first_strike_phase_active, deal_combat_damage,
    )
    _decl_blk(state, all_assignments, combat_state=cs)
    actions: List[Dict[str, Any]] = []
    if first_strike_phase_active(cs, state):
        actions.extend(deal_combat_damage(state, cs, is_first_strike=True))
    actions.extend(deal_combat_damage(state, cs, is_first_strike=False))
    return cs, actions
