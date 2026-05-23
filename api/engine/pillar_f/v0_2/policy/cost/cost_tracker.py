"""Cost tracker — accumulates per-player + per-game LLM spend.

Implements scoping doc section 6 + 10 risk mitigation.

Per-turn cost ceiling: $0.30 default. When a player's per-turn spend
exceeds this, subsequent priority windows in that turn use the cheap-
fallback responder (always pass).

Per-game cost ceiling: $10 default. When total game spend exceeds
this, game halts with `GAME_COST_CEILING_EXCEEDED` event; whoever has
the most life wins by default.

The CostTracker is attached to the game via a side-channel (passed
into responder factory). It's NOT part of GameState because GameState
is the rules-engine source of truth; cost is an out-of-band concern
specific to the LLM policy layer.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


COST_TRACKER_VERSION = "pillar_f_v0_2_policy_cost_tracker_v1"

# Default ceilings per scoping doc.
DEFAULT_PER_TURN_CEILING_USD = 0.30
DEFAULT_PER_GAME_CEILING_USD = 10.00


@dataclass
class CostTracker:
    """Per-game LLM spend accumulator.

    Per-player + per-turn buckets so guardrails can fire at the right
    granularity. Total game spend is sum of per-player totals.
    """
    per_turn_ceiling_usd: float = DEFAULT_PER_TURN_CEILING_USD
    per_game_ceiling_usd: float = DEFAULT_PER_GAME_CEILING_USD
    # Per-player spend: {player_id: {turn_number: cost, ...}}.
    spend_by_player_turn: Dict[int, Dict[int, float]] = field(default_factory=dict)
    # Event log for diagnostics.
    events: List[Dict[str, Any]] = field(default_factory=list)
    # Per-player fallback state: once a player hits the per-turn ceiling
    # on a given turn, they're flagged for cheap-fallback for the
    # remainder of THAT turn. Reset at turn rollover.
    fallback_until_turn_end: Dict[int, int] = field(default_factory=dict)
    # Whether the per-game ceiling was hit (engine should halt the game).
    game_halted_for_cost: bool = False

    def record_call(
        self, *, player_id: int, turn_number: int, cost_usd: float,
        purpose: str = "",
    ) -> None:
        """Record one LLM call's cost. Triggers per-turn + per-game
        ceiling checks."""
        if cost_usd < 0:
            return
        bucket = self.spend_by_player_turn.setdefault(player_id, {})
        bucket[turn_number] = bucket.get(turn_number, 0.0) + cost_usd
        self.events.append({
            "player_id": player_id, "turn_number": turn_number,
            "cost_usd": cost_usd, "purpose": purpose,
        })
        # Per-turn check.
        if bucket[turn_number] > self.per_turn_ceiling_usd:
            self.fallback_until_turn_end[player_id] = turn_number
            self.events.append({
                "event": "COST_CEILING_HIT", "player_id": player_id,
                "turn_number": turn_number,
                "per_turn_spend": bucket[turn_number],
                "ceiling": self.per_turn_ceiling_usd,
            })
        # Per-game check.
        if self.total_spend() > self.per_game_ceiling_usd:
            self.game_halted_for_cost = True
            self.events.append({
                "event": "GAME_COST_CEILING_EXCEEDED",
                "total_spend": self.total_spend(),
                "ceiling": self.per_game_ceiling_usd,
            })

    def spend_for_player(self, player_id: int) -> float:
        bucket = self.spend_by_player_turn.get(player_id) or {}
        return sum(bucket.values())

    def spend_for_player_turn(self, player_id: int, turn_number: int) -> float:
        bucket = self.spend_by_player_turn.get(player_id) or {}
        return bucket.get(turn_number, 0.0)

    def total_spend(self) -> float:
        return sum(
            self.spend_for_player(pid)
            for pid in self.spend_by_player_turn.keys()
        )

    def is_player_in_fallback(self, player_id: int, current_turn: int) -> bool:
        """Returns True iff this player's per-turn ceiling was hit on
        the current turn. Reset on turn rollover (different turn = no
        fallback unless re-triggered)."""
        until_turn = self.fallback_until_turn_end.get(player_id)
        return until_turn == current_turn

    def reset_fallbacks_for_turn(self, turn_number: int) -> None:
        """Clear per-turn fallback flags whose `until_turn` was the
        previous turn. Caller (engine) invokes this at untap_step."""
        to_clear = [
            pid for pid, t in self.fallback_until_turn_end.items()
            if t < turn_number
        ]
        for pid in to_clear:
            self.fallback_until_turn_end.pop(pid, None)
