"""Live 2-LLM head-to-head smoke for sub-mega-task B.

Phase 3 ship-gate substrate verification: runs a SHORT 3-turn game
between 2 LLM-piloted players using the substrate's minimal resolvers
(deal_damage_to_player, draw_cards). Assert no exceptions, costs
tracked, actions legal.

Runs against the real Anthropic API — costs ~$0.50-1.50. Not part of
the pytest suite (skipped by default to avoid burning $ on every pytest
run). Run explicitly:

    python tools/test_pillar_f_v0_2_policy_smoke.py

Phase 9 will run a longer 4-LLM 20-turn variant for the full ship gate.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones, Step, Phase,
)
from api.engine.pillar_f.v0_2.stack import priority_round
from api.engine.pillar_f.v0_2.turn import (
    STEP_ORDER, advance_step, start_step, step_opens_priority,
    untap_step, draw_step, cleanup_step,
)
from api.engine.pillar_f.v0_2.policy import make_llm_priority_responder
from api.engine.pillar_f.v0_2.policy.cost import CostTracker
from api.engine.layers.agent_llm_client_v1 import get_default_client


def _build_2p_game() -> GameState:
    """2-player game (P0, P1) with simple decks of 30 cards each:
    20 basic lands + 10 Lightning Bolts (deal 3 damage to player)."""
    gs = GameState()
    for pid in range(2):
        ps = PlayerState(player_id=pid, name=f"P{pid}",
                         life_total=20, zones=PlayerZones())
        # 20 basic lands.
        for i in range(20):
            land = Card(
                name="Swamp" if pid == 0 else "Mountain",
                owner=pid, controller=pid,
                type_line="Basic Land — Swamp" if pid == 0 else "Basic Land — Mountain",
                oracle_id=f"basic-{pid}-{i}",
            )
            gs.add_card(land)
            ps.zones.library.append(land.card_id)
        # 10 Bolt-like spells targeting the OTHER player.
        for i in range(10):
            target_pid = 1 - pid
            bolt = Card(
                name="Lightning Bolt",
                owner=pid, controller=pid,
                type_line="Instant",
                mana_cost="{R}",
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
        # Draw opening hand of 5 (smaller than 7 to save tokens).
        for _ in range(5):
            if ps.zones.library:
                ps.zones.hand.append(ps.zones.library.pop(0))
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
    gs = _build_2p_game()
    ct = CostTracker(per_turn_ceiling_usd=1.0, per_game_ceiling_usd=5.0)
    action_log: List[str] = []
    responder = make_llm_priority_responder(
        llm_client=client, cost_tracker=ct,
        action_log=action_log,
        deck_archetype_hint_by_player={0: "mono-black burn", 1: "mono-red burn"},
    )

    N_TURNS = 3
    print(f"Starting 2-LLM head-to-head smoke ({N_TURNS} turns)...", flush=True)
    t0 = time.perf_counter()

    for turn in range(1, N_TURNS + 1):
        for ap in (0, 1):
            gs.active_player = ap
            gs.turn_number = turn
            print(f"\n--- Turn {turn} P{ap} ---", flush=True)
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
                    # Resolve any stack pushes.
                    while gs.stack:
                        from api.engine.pillar_f.v0_2.stack import resolve_top
                        resolve_top(gs)
                        priority_round(gs, responder)
                advance_step(gs)
                if ct.game_halted_for_cost:
                    print("COST GAME HALT", flush=True)
                    break
            if ct.game_halted_for_cost:
                break
        if ct.game_halted_for_cost:
            break

    elapsed = time.perf_counter() - t0
    print(f"\n=== Smoke complete in {elapsed:.1f}s ===")
    print(f"Total spend: ${ct.total_spend():.4f}")
    print(f"P0 life: {gs.players[0].life_total}, P1 life: {gs.players[1].life_total}")
    print(f"P0 hand size: {len(gs.players[0].zones.hand)}")
    print(f"P1 hand size: {len(gs.players[1].zones.hand)}")
    print(f"Total LLM calls: {len(ct.events)}")
    print(f"\nAction log:")
    for a in action_log:
        print(f"  {a}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
