"""Dataclasses for the Stage 2 orchestrator + per-game runner.

Per sub-C scoping doc sections 2 + 3 + 5c.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ORCHESTRATOR_TYPES_VERSION = "pillar_f_v0_2_playtest_types_v1"


# ============================================================
# Deck representation
# ============================================================


@dataclass
class StageTwoDeck:
    """Minimal deck description for the Stage 2 orchestrator.

    Iter-11 simple decks: commander_name + mainboard list of card names
    (~30-99 entries) + archetype_hint string for the LLM prompt's
    deck_archetype slot.
    """
    deck_id: str               # e.g. "krenko-b4-2026-05-23"
    commander_name: str        # e.g. "Krenko, Mob Boss"
    mainboard: List[str] = field(default_factory=list)
    archetype_hint: str = ""   # e.g. "mono-red goblin tribal"
    bracket: str = "B3"        # power bracket label per Pillar A

    def __post_init__(self) -> None:
        if not self.deck_id:
            raise ValueError("StageTwoDeck.deck_id is required")
        if not self.commander_name:
            raise ValueError("StageTwoDeck.commander_name is required")


# ============================================================
# Per-game configuration
# ============================================================


@dataclass
class StageTwoGameConfig:
    """Per-game configuration. Built by the cycle runner per game in
    the N=30 cycle."""
    seed: int                                       # for reproducibility
    decks: List[StageTwoDeck]                       # 4 decks indexed by player_id
    deck_under_test_pid: int = 0                    # which seat is the deck-under-test
    max_turns: int = 25                             # halt if no win by then
    max_mulligans: int = 2                          # cap per sub-B Phase 6 pattern
    per_turn_cost_ceiling_usd: float = 0.30
    per_game_cost_ceiling_usd: float = 5.0
    llm_temperature: float = 0.8                    # variance for replay diversity
    enable_combat: bool = True                      # gates combat hook glue use
    starting_life: int = 40                         # Commander default

    def __post_init__(self) -> None:
        if len(self.decks) != 4:
            raise ValueError(
                f"StageTwoGameConfig requires exactly 4 decks, got {len(self.decks)}"
            )
        if not (0 <= self.deck_under_test_pid < 4):
            raise ValueError(
                f"deck_under_test_pid must be 0-3, got {self.deck_under_test_pid}"
            )


# ============================================================
# Per-game result
# ============================================================


@dataclass
class StageTwoGameResult:
    """Aggregated diagnostics from one game. Used by the cycle
    aggregator (Phase 4) to compute win_rate + politics_summary +
    cost_summary."""
    game_idx: int                                   # within the cycle
    seed: int
    deck_under_test_pid: int
    deck_ids: List[str]                             # per player_id
    winner_pid: Optional[int] = None                # None if max-turns tie
    turns_run: int = 0
    halted_for_cost: bool = False
    halted_reason: Optional[str] = None             # "max_turns" | "cost" | "win" | None
    elimination_order: List[Tuple[int, int, str]] = field(default_factory=list)
    # ^ (player_id, turn_when_eliminated, cause_string)
    final_life_totals: Dict[int, int] = field(default_factory=dict)
    final_threat_vectors: Dict[int, Dict[int, float]] = field(default_factory=dict)
    # ^ viewer_pid -> opponent_pid -> threat score
    politics_summary: Dict[str, Any] = field(default_factory=dict)
    action_log: List[str] = field(default_factory=list)
    combat_decisions_log: List[Dict[str, Any]] = field(default_factory=list)
    total_spend_usd: float = 0.0
    total_llm_calls: int = 0
    fallback_events: List[Dict[str, Any]] = field(default_factory=list)
    elapsed_seconds: float = 0.0
    notes: List[str] = field(default_factory=list)


# ============================================================
# Per-cycle configuration
# ============================================================


@dataclass
class StageTwoCycleConfig:
    """N-game cycle configuration. Phase 4 implements run_stage_two_cycle."""
    deck_under_test: StageTwoDeck
    control_pool: List[StageTwoDeck]                # 3+ controls drawn from
    n_games: int = 30
    parallelism: int = 1                            # iter-11 starts serial; Phase 4 raises
    output_dir: Optional[Path] = None               # per-game logs + cycle report
    cycle_cost_ceiling_usd: float = 200.0           # Phase 6 default
    max_turns: int = 25
    max_mulligans: int = 2
    per_turn_cost_ceiling_usd: float = 0.30
    per_game_cost_ceiling_usd: float = 5.0
    starting_life: int = 40
    seed_base: int = 1                              # game N uses seed_base + N
