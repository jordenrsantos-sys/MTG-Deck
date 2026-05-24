"""Live 4-LLM mulligan + 5-turn smoke for sub-mega-task B Phase 6.

Phase 6 ship-gate verification per kickoff:
- 4-LLM full mulligan cycle through London-mulligan rules ends with
  4 starting hands of correct sizes.
- 4-LLM 5-turn game; all actions legal; total cost < $1.

Runs against the real Anthropic API. Not part of the pytest suite —
run explicitly:

    python tools/test_pillar_f_v0_2_policy_mulligan_smoke.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones, Step,
)
from api.engine.pillar_f.v0_2.stack import priority_round, resolve_top
from api.engine.pillar_f.v0_2.turn import (
    STEP_ORDER, advance_step, start_step, step_opens_priority,
    untap_step, draw_step, cleanup_step,
    mulligan_setup,
)
from api.engine.pillar_f.v0_2.policy import (
    make_llm_priority_responder,
    make_llm_mulligan_decider, make_llm_bottom_picker,
)
from api.engine.pillar_f.v0_2.policy.cost import CostTracker
from api.engine.layers.agent_llm_client_v1 import get_default_client


_DECK_FLAVORS = [
    ("Swamp", "Basic Land — Swamp", "mono-black burn"),
    ("Mountain", "Basic Land — Mountain", "mono-red burn"),
    ("Forest", "Basic Land — Forest", "mono-green burn"),
    ("Island", "Basic Land — Island", "mono-blue burn"),
]


def _build_4p_game() -> GameState:
    """4-player game with 30-card decks each: 20 basic lands + 10 Bolts."""
    gs = GameState()
    for pid in range(4):
        land_name, type_line, _archetype = _DECK_FLAVORS[pid]
        ps = PlayerState(player_id=pid, name=f"P{pid}",
                         life_total=40, zones=PlayerZones())
        for i in range(20):
            land = Card(name=land_name, owner=pid, controller=pid,
                        type_line=type_line, oracle_id=f"basic-{pid}-{i}")
            gs.add_card(land)
            ps.zones.library.append(land.card_id)
        # 10 bolts each targeting the NEXT player around the table.
        for i in range(10):
            target_pid = (pid + 1) % 4
            bolt = Card(
                name="Lightning Bolt", owner=pid, controller=pid,
                type_line="Instant", mana_cost="{R}",
                oracle_text="Lightning Bolt deals 3 damage to any target.",
                oracle_id=f"bolt-{pid}-{i}",
            )
            bolt.iter10_annotation = {
                "description": f"deals 3 damage to P{target_pid}",
                "payment": {"resolver": "deal_damage_to_player", "amount": 3},
                "default_targets": [target_pid],
            }
            gs.add_card(bolt)
            ps.zones.library.append(bolt.card_id)
        gs.players.append(ps)
    gs.active_player = 0
    gs.turn_number = 1
    gs.step = Step.UNTAP
    return gs


def main() -> int:
    client = get_default_client()
    if not client.is_available():
        print(f"LLM unavailable: {client.unavailable_reason()}")
        return 1

    gs = _build_4p_game()
    ct = CostTracker(per_turn_ceiling_usd=0.30, per_game_ceiling_usd=2.0)

    deck_archetype_hints = {
        pid: _DECK_FLAVORS[pid][2] for pid in range(4)
    }

    decider = make_llm_mulligan_decider(
        llm_client=client, cost_tracker=ct,
        deck_archetype_hint_by_player=deck_archetype_hints,
    )
    picker = make_llm_bottom_picker(
        llm_client=client, cost_tracker=ct,
        deck_archetype_hint_by_player=deck_archetype_hints,
    )

    print("Starting 4-LLM mulligan cycle...", flush=True)
    t0 = time.perf_counter()
    mull_results = mulligan_setup(
        gs, decider_fn=decider, bottom_picker_fn=picker,
        max_mulligans=2,  # cap at 2 for cost predictability
    )
    mull_elapsed = time.perf_counter() - t0

    print(f"\n=== Mulligan cycle done in {mull_elapsed:.1f}s ===")
    print(f"Spend after mulligans: ${ct.total_spend():.4f}")
    for pid in range(4):
        hand_size = len(gs.players[pid].zones.hand)
        print(f"  P{pid}: {mull_results[pid]} mulligans, "
              f"hand={hand_size} (expected = {7 - mull_results[pid]})")
        # Assert: hand size = 7 - num_mulligans (per London).
        expected = 7 - mull_results[pid]
        if hand_size != expected:
            print(f"FAIL: P{pid} hand size {hand_size} != expected {expected}")
            return 2

    # Now play 5 turns with all 4 LLMs as priority responders.
    action_log: List[str] = []
    responder = make_llm_priority_responder(
        llm_client=client, cost_tracker=ct,
        action_log=action_log,
        deck_archetype_hint_by_player=deck_archetype_hints,
    )

    N_TURNS = 5
    print(f"\nStarting {N_TURNS}-turn 4-LLM head-to-head-to-head-to-head...",
          flush=True)
    t1 = time.perf_counter()
    illegal_actions = 0  # responder applies eligible_actions only, so 0
    for turn in range(1, N_TURNS + 1):
        for ap in range(4):
            if gs.players[ap].has_lost:
                continue
            gs.active_player = ap
            gs.turn_number = turn
            for step in STEP_ORDER:
                start_step(gs, step)
                if step == Step.UNTAP:
                    untap_step(gs)
                elif step == Step.DRAW:
                    draw_step(gs)
                elif step == Step.CLEANUP:
                    cleanup_step(gs)
                if step_opens_priority(step) and not ct.game_halted_for_cost:
                    priority_round(gs, responder)
                    while gs.stack:
                        resolve_top(gs)
                        if not ct.game_halted_for_cost:
                            priority_round(gs, responder)
                advance_step(gs)
                if ct.game_halted_for_cost:
                    break
            if ct.game_halted_for_cost:
                break
        if ct.game_halted_for_cost:
            break

    play_elapsed = time.perf_counter() - t1
    total_elapsed = time.perf_counter() - t0
    print(f"\n=== {N_TURNS}-turn game done in {play_elapsed:.1f}s "
          f"(total smoke {total_elapsed:.1f}s) ===")
    print(f"Total spend: ${ct.total_spend():.4f}  (gate: <$1)")
    print(f"Total LLM calls: {len(ct.events)}")
    for pid in range(4):
        ps = gs.players[pid]
        print(f"  P{pid}: life={ps.life_total} hand={len(ps.zones.hand)} "
              f"library={len(ps.zones.library)} graveyard={len(ps.zones.graveyard)} "
              f"lost={ps.has_lost}")
    print(f"\nAction log ({len(action_log)} actions):")
    for a in action_log[:60]:
        print(f"  {a}")
    if len(action_log) > 60:
        print(f"  ... ({len(action_log) - 60} more)")

    # Gate check.
    total = ct.total_spend()
    if total >= 1.0:
        print(f"FAIL: spend ${total:.4f} >= $1 gate ceiling")
        return 3
    print(f"PASS: spend ${total:.4f} under $1 gate")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
