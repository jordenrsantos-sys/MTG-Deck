"""N-game cycle runner.

Sub-C Phase 4 + Phase 6. Per scoping doc section 5a.

run_stage_two_cycle(cycle_config, llm_client) -> StageTwoReport:
  - For game_idx in range(n_games):
      - Build per-game config from cycle config (seed = seed_base + idx;
        decks = [deck_under_test, *3 controls picked from pool]).
      - Run single game via run_single_game.
      - Persist per-game JSON to output_dir/game_<idx:03d>.json.
      - Track cumulative spend; halt + write partial report if
        cycle_cost_ceiling exceeded.
  - Aggregate via aggregate_cycle.
  - Write cycle_report.md + cycle.json to output_dir.
  - Return the StageTwoReport.

Parallelism: iter-11 ships serial (parallelism=1). asyncio-driven
concurrency is iter-12+ work (cost-bounded by API rate limits anyway).
"""
from __future__ import annotations

import random
import time
from pathlib import Path
from typing import Any, List, Optional

from api.engine.pillar_f.v0_2.playtest.orchestrator import (
    StageTwoDeck, StageTwoGameConfig, StageTwoGameResult,
    StageTwoCycleConfig, run_single_game,
)
from api.engine.pillar_f.v0_2.playtest.aggregation import (
    StageTwoReport, aggregate_cycle,
)
from api.engine.pillar_f.v0_2.playtest.reports import (
    write_cycle_report_json, write_cycle_report_markdown,
    write_per_game_json,
)


CYCLE_RUNNER_VERSION = "pillar_f_v0_2_playtest_cycle_runner_v1"


def _pick_control_decks(
    control_pool: List[StageTwoDeck], deck_under_test: StageTwoDeck,
    rng: random.Random,
) -> List[StageTwoDeck]:
    """Pick 3 control decks from the pool. Per scoping doc section 2d
    (the Mode A default): controls should NOT all stack on the same
    color as the deck-under-test, to avoid color-stacking the table.

    Iter-11 simplification: just sample without replacement if pool
    has >=3; else cycle through. Color-anti-stack heuristic deferred
    to iter-12+ -- the pool curator is expected to provide reasonable
    diversity at dispatch time.
    """
    pool = list(control_pool)
    if len(pool) >= 3:
        return rng.sample(pool, 3)
    # Pool < 3: rotate as needed.
    out: List[StageTwoDeck] = []
    while len(out) < 3:
        for d in pool:
            out.append(d)
            if len(out) == 3:
                break
    return out


def _build_per_game_config(
    cycle_config: StageTwoCycleConfig, game_idx: int,
    rng: random.Random,
) -> StageTwoGameConfig:
    """Build a per-game config from the cycle config + game index."""
    controls = _pick_control_decks(
        cycle_config.control_pool, cycle_config.deck_under_test, rng,
    )
    # Deck under test goes at pid 0; controls at pid 1/2/3.
    decks = [cycle_config.deck_under_test, *controls]
    seed = cycle_config.seed_base + game_idx
    return StageTwoGameConfig(
        seed=seed,
        decks=decks,
        deck_under_test_pid=0,
        max_turns=cycle_config.max_turns,
        max_mulligans=cycle_config.max_mulligans,
        per_turn_cost_ceiling_usd=cycle_config.per_turn_cost_ceiling_usd,
        per_game_cost_ceiling_usd=cycle_config.per_game_cost_ceiling_usd,
        starting_life=cycle_config.starting_life,
    )


def run_stage_two_cycle(
    cycle_config: StageTwoCycleConfig,
    *,
    llm_client: Any,
    progress_callback: Optional[Any] = None,
) -> StageTwoReport:
    """Run a full Stage 2 cycle.

    Args:
        cycle_config: see StageTwoCycleConfig dataclass.
        llm_client: AnthropicClient or compatible mock.
        progress_callback: optional `(game_idx, total, partial_result) ->
            None` for streaming progress to caller (UI / log).

    Returns: StageTwoReport (also written to disk if
        cycle_config.output_dir is set).
    """
    rng = random.Random(cycle_config.seed_base)
    game_results: List[StageTwoGameResult] = []
    cost_to_date = 0.0
    halted_for_cycle_cost = False

    output_dir = cycle_config.output_dir
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)

    for game_idx in range(cycle_config.n_games):
        if cost_to_date >= cycle_config.cycle_cost_ceiling_usd:
            halted_for_cycle_cost = True
            break
        per_game_config = _build_per_game_config(
            cycle_config, game_idx, rng,
        )
        result = run_single_game(per_game_config, llm_client=llm_client)
        result.game_idx = game_idx
        game_results.append(result)
        cost_to_date += result.total_spend_usd
        if output_dir is not None:
            write_per_game_json(
                result,
                output_dir / f"game_{game_idx:03d}.json",
            )
        if progress_callback is not None:
            try:
                progress_callback(game_idx, cycle_config.n_games, result)
            except Exception:
                pass

    report = aggregate_cycle(
        deck_under_test_id=cycle_config.deck_under_test.deck_id,
        deck_under_test_archetype=cycle_config.deck_under_test.archetype_hint,
        game_results=game_results,
        halted_for_cycle_cost=halted_for_cycle_cost,
    )

    if output_dir is not None:
        write_cycle_report_json(report, output_dir / "cycle.json")
        write_cycle_report_markdown(report, output_dir / "cycle_report.md")

    return report
