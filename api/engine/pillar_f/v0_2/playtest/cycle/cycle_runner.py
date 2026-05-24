"""N-game cycle runner.

Sub-C Phase 4 + Phase 6 + mega-task v15 Phase 1. Per scoping doc
section 5a.

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

Parallelism (v15 Phase 1): when cycle_config.parallelism > 1, games
run in waves of size `parallelism` via asyncio.gather +
asyncio.to_thread (run_single_game is sync; the LLM client inside it
uses asyncio.run() per call, so the inner event loop is independent
across threads). Rate-limit fallback drops concurrency to 1 for the
remainder of the cycle after observed rate-limit failures.
"""
from __future__ import annotations

import asyncio
import random
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

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


CYCLE_RUNNER_VERSION = "pillar_f_v0_2_playtest_cycle_runner_v2"  # v15: parallel

# v15 Phase 1: rate-limit fallback drops concurrency to serial after
# this many consecutive rate-limit-style failures.
_RATE_LIMIT_FALLBACK_STREAK = 3


def _looks_like_rate_limit(result: Any) -> bool:
    """Best-effort detection of rate-limit / overload failures in a
    StageTwoGameResult. Reads fallback_events emitted by the
    CostTracker + responder-level error_code via the action_log.
    Iter-15: simple substring check on fallback_events for now;
    iter-16+ can refine with structured signals."""
    if not result:
        return False
    fbacks = getattr(result, "fallback_events", []) or []
    for ev in fbacks:
        evt = (ev.get("event") or "").upper() if isinstance(ev, dict) else ""
        if "RATE" in evt or "OVERLOAD" in evt:
            return True
    return False


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


def _run_one_game(
    cycle_config: StageTwoCycleConfig, game_idx: int,
    rng_seed: int, llm_client: Any,
) -> StageTwoGameResult:
    """Wrapper around run_single_game so asyncio.to_thread can call
    it. Builds per-game config from the cycle config + game index.

    Note: rng is per-call (seeded deterministically) so games run in
    parallel waves don't share RNG state. The cycle-level
    deck-selection RNG happens BEFORE this helper is called.
    """
    # Iter-15 design: deck selection is deterministic per game_idx
    # because the per-game seed is derived from game_idx. We do NOT
    # share the cycle-level rng across parallel games; instead, each
    # game seeds its own rng from seed_base + game_idx so re-runs
    # are reproducible regardless of parallel scheduling.
    per_game_rng = random.Random(rng_seed)
    per_game_config = _build_per_game_config(
        cycle_config, game_idx, per_game_rng,
    )
    result = run_single_game(per_game_config, llm_client=llm_client)
    result.game_idx = game_idx
    return result


async def _run_wave_async(
    cycle_config: StageTwoCycleConfig,
    indices: List[int], llm_client: Any,
) -> List[StageTwoGameResult]:
    """Run one wave of `len(indices)` games concurrently via
    asyncio.gather + asyncio.to_thread (run_single_game is sync but
    safe to call from multiple threads -- each invocation creates its
    own GameState, CostTracker, politics dicts; the LLM client's
    asyncio.run() inside each call uses an independent event loop
    per thread)."""
    tasks = [
        asyncio.to_thread(
            _run_one_game, cycle_config, idx,
            cycle_config.seed_base + idx, llm_client,
        )
        for idx in indices
    ]
    return await asyncio.gather(*tasks)


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

    v15 Phase 1: when cycle_config.parallelism > 1, games run in waves
    of size `parallelism` via asyncio.gather + asyncio.to_thread. The
    cost-ceiling check fires between waves (parallel games launched in
    a wave can collectively overshoot the ceiling by one wave's worth
    of spend before the cycle halts -- acceptable trade-off for
    wallclock speedup).
    """
    game_results: List[StageTwoGameResult] = []
    cost_to_date = 0.0
    halted_for_cycle_cost = False
    cycle_events: List[Dict[str, Any]] = []
    active_concurrency = max(1, cycle_config.parallelism)
    rate_limit_streak = 0

    output_dir = cycle_config.output_dir
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)

    game_idx = 0
    while game_idx < cycle_config.n_games:
        if cost_to_date >= cycle_config.cycle_cost_ceiling_usd:
            halted_for_cycle_cost = True
            cycle_events.append({
                "event": "CYCLE_COST_HALT",
                "at_game_idx": game_idx,
                "cost_to_date": cost_to_date,
                "ceiling": cycle_config.cycle_cost_ceiling_usd,
                "games_completed": len(game_results),
                "games_remaining_skipped": cycle_config.n_games - game_idx,
            })
            break

        # Plan the next wave.
        wave_size = min(
            active_concurrency, cycle_config.n_games - game_idx,
        )
        wave_indices = list(range(game_idx, game_idx + wave_size))

        if wave_size == 1:
            # Serial path -- avoids asyncio.run overhead.
            wave_results = [_run_one_game(
                cycle_config, wave_indices[0],
                cycle_config.seed_base + wave_indices[0], llm_client,
            )]
        else:
            wave_results = asyncio.run(_run_wave_async(
                cycle_config, wave_indices, llm_client,
            ))

        # Per-wave aggregation: persist + cost-track + rate-limit-check.
        wave_had_rate_limit = False
        for result in wave_results:
            game_results.append(result)
            cost_to_date += result.total_spend_usd
            if output_dir is not None:
                write_per_game_json(
                    result,
                    output_dir / f"game_{result.game_idx:03d}.json",
                )
            if progress_callback is not None:
                try:
                    progress_callback(
                        result.game_idx, cycle_config.n_games, result,
                    )
                except Exception:
                    pass
            if _looks_like_rate_limit(result):
                wave_had_rate_limit = True

        if wave_had_rate_limit:
            rate_limit_streak += 1
            if (rate_limit_streak >= _RATE_LIMIT_FALLBACK_STREAK
                    and active_concurrency > 1):
                cycle_events.append({
                    "event": "RATE_LIMIT_FALLBACK_TO_SERIAL",
                    "at_game_idx": game_idx + wave_size,
                    "streak": rate_limit_streak,
                    "prev_concurrency": active_concurrency,
                })
                active_concurrency = 1
        else:
            rate_limit_streak = 0

        game_idx += wave_size

    report = aggregate_cycle(
        deck_under_test_id=cycle_config.deck_under_test.deck_id,
        deck_under_test_archetype=cycle_config.deck_under_test.archetype_hint,
        game_results=game_results,
        halted_for_cycle_cost=halted_for_cycle_cost,
    )
    # Attach cycle events (CYCLE_COST_HALT, etc.) to cost_summary
    # for telemetry. Phase 6 contract.
    report.cost_summary["cycle_events"] = cycle_events
    report.cost_summary["cycle_cost_ceiling_usd"] = (
        cycle_config.cycle_cost_ceiling_usd
    )
    report.cost_summary["halted_for_cycle_cost"] = halted_for_cycle_cost

    if output_dir is not None:
        write_cycle_report_json(report, output_dir / "cycle.json")
        write_cycle_report_markdown(report, output_dir / "cycle_report.md")

    return report
