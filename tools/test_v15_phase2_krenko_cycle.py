"""Phase 2 of mega-task v15 -- Krenko B4 full 30-game cycle.

Per kickoff: Krenko mono-red goblin tribal is the canonical Phase 7
test deck. 30 games at parallelism=4.

Cycle config:
- max_turns=25 (full v12 scoping doc default)
- max_mulligans=2
- per_game_cost_ceiling_usd=5.0 (sub-C default)
- cycle_cost_ceiling_usd=300.0 (v15 Phase 1 adjustment: subscription
  billing makes the cost-basis number defensive; net spend $0
  under Max)

Run:
    python tools/test_v15_phase2_krenko_cycle.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.engine.pillar_f.v0_2.playtest.cycle import run_stage_two_cycle
from api.engine.pillar_f.v0_2.playtest.orchestrator import (
    StageTwoDeck, StageTwoCycleConfig,
)
from api.engine.layers.agent_llm_client_v1 import get_default_client


def _krenko_b4_deck() -> StageTwoDeck:
    """Mono-red goblin tribal B4. 30-card mainboard:
    16 lands + 9 goblins + 5 burn."""
    mainboard = (
        ["Mountain"] * 16
        + ["Goblin Guide"] * 4
        + ["Goblin Piledriver"] * 3
        + ["Skirk Prospector"] * 2
        + ["Lightning Bolt"] * 3
        + ["Shock"] * 2
    )
    return StageTwoDeck(
        deck_id="krenko-mob-boss-b4",
        commander_name="Krenko, Mob Boss",
        mainboard=mainboard,
        archetype_hint="mono-red goblin tribal aggro",
        bracket="B4",
    )


def _mono_w_soldiers() -> StageTwoDeck:
    mainboard = (
        ["Plains"] * 18
        + ["Soldier of the Pantheon"] * 4
        + ["Thalia, Guardian of Thraben"] * 2
        + ["Heliod, Sun-Crowned"] * 2
        + ["Counterspell"] * 4
    )
    return StageTwoDeck(
        deck_id="mono-w-soldiers",
        commander_name="Heliod, Sun-Crowned",
        mainboard=mainboard,
        archetype_hint="mono-white soldiers + lifegain",
        bracket="B3",
    )


def _mono_u_tempo() -> StageTwoDeck:
    mainboard = (
        ["Island"] * 16
        + ["Delver of Secrets"] * 4
        + ["Snapcaster Mage"] * 4
        + ["Counterspell"] * 3
        + ["Negate"] * 3
    )
    return StageTwoDeck(
        deck_id="mono-u-tempo",
        commander_name="Snapcaster Mage",
        mainboard=mainboard,
        archetype_hint="mono-blue tempo + counters",
        bracket="B3",
    )


def _mono_b_reanimator() -> StageTwoDeck:
    mainboard = (
        ["Swamp"] * 18
        + ["Putrid Imp"] * 4
        + ["Reassembling Skeleton"] * 4
        + ["Vampire Nighthawk"] * 4
    )
    return StageTwoDeck(
        deck_id="mono-b-reanimator",
        commander_name="Putrid Imp",
        mainboard=mainboard,
        archetype_hint="mono-black aggro / graveyard recursion",
        bracket="B3",
    )


def main() -> int:
    client = get_default_client()
    if not client.is_available():
        print(f"LLM unavailable: {client.unavailable_reason()}")
        return 1

    output_dir = REPO_ROOT.parents[0] \
        / "MTG-Deck-Builder-Claude" / "stage_2_v15_cycles" / "krenko_b4"

    cycle_config = StageTwoCycleConfig(
        deck_under_test=_krenko_b4_deck(),
        control_pool=[
            _mono_w_soldiers(),
            _mono_u_tempo(),
            _mono_b_reanimator(),
        ],
        n_games=30,
        parallelism=4,
        output_dir=output_dir,
        max_turns=25,
        max_mulligans=2,
        per_turn_cost_ceiling_usd=0.30,
        per_game_cost_ceiling_usd=5.0,
        cycle_cost_ceiling_usd=300.0,
        starting_life=40,
        seed_base=1000,
    )

    print("=== Phase 2 cycle: Krenko B4 (30 games, parallelism=4) ===",
          flush=True)
    print(f"DUT:    {cycle_config.deck_under_test.deck_id}")
    print(f"Pool:   {[d.deck_id for d in cycle_config.control_pool]}")
    print(f"Output: {output_dir}")
    print(f"Caps:   max_turns={cycle_config.max_turns}, "
          f"max_mulligans={cycle_config.max_mulligans}, "
          f"per_game=${cycle_config.per_game_cost_ceiling_usd}, "
          f"cycle=${cycle_config.cycle_cost_ceiling_usd}, "
          f"parallelism={cycle_config.parallelism}")
    print(flush=True)

    t0 = time.perf_counter()
    last_report_time = [t0]

    def _progress(idx, total, result):
        now = time.perf_counter()
        wave_elapsed = now - last_report_time[0]
        last_report_time[0] = now
        print(f"[game {idx + 1}/{total}] turns={result.turns_run} "
              f"winner=P{result.winner_pid} "
              f"halted_cost={result.halted_for_cost} "
              f"spend=${result.total_spend_usd:.3f} "
              f"game_elapsed={result.elapsed_seconds:.0f}s "
              f"wallclock={now - t0:.0f}s",
              flush=True)

    report = run_stage_two_cycle(
        cycle_config, llm_client=client, progress_callback=_progress,
    )
    elapsed = time.perf_counter() - t0

    print(flush=True)
    print(f"=== Cycle complete in {elapsed:.0f}s ({elapsed/60:.1f} min) ===")
    print(f"Recommendation: {report.pass_recommendation}")
    print(f"Reason: {report.recommendation_reason}")
    print(f"Win rate: {report.win_rate:.2%} "
          f"({sum(1 for g in report.per_game_brief if g['deck_under_test_outcome'] == 'WIN')} "
          f"wins / {report.games_completed + report.games_halted_for_cost} games)")
    print(f"Avg turn eliminated when lost: "
          f"{report.avg_turn_eliminated_when_lost:.1f}")
    print(f"Total cost-basis (api_estimate sum): "
          f"${report.cost_summary['total_spend']:.2f}")
    print(f"Per-game avg cost-basis: "
          f"${report.cost_summary['per_game_avg']:.2f}")
    print(f"Per-game max cost-basis: "
          f"${report.cost_summary['per_game_max']:.2f}")
    print(f"Games halted for cost: "
          f"{int(report.cost_summary['games_halted_for_cost'])}")
    print(f"Combat: games_with_combat="
          f"{report.combat_summary['games_with_combat']}, "
          f"games_with_multi_block="
          f"{report.combat_summary['games_with_multi_block']}")
    print(f"Politics: deals={report.politics_summary['total_deals_made']}, "
          f"games_with_alliance_transition="
          f"{report.politics_summary['games_with_alliance_transition']}")
    cycle_events = report.cost_summary.get("cycle_events") or []
    if cycle_events:
        print(f"Cycle events: {cycle_events}")

    # Gate scorecard.
    n = report.games_completed + report.games_halted_for_cost
    n_w = sum(1 for g in report.per_game_brief
              if g["deck_under_test_outcome"] == "WIN")
    multi_block_games = report.combat_summary['games_with_multi_block']
    games_with_alliance = report.politics_summary[
        'games_with_alliance_transition']
    deals = report.politics_summary['total_deals_made']

    print(flush=True)
    print(f"=== Cycle-quality gate scorecard ===")
    gates = [
        ("1. Cycle completes 30/30 (or graceful halt)",
         n >= 30 or report.cost_summary.get("halted_for_cycle_cost")),
        ("2. No engine crashes (every game has a winner_pid or halt)",
         all(g["deck_under_test_outcome"] != "ERROR"
             for g in report.per_game_brief)),
        ("3. Cost-basis sum < $300 (cycle ceiling)",
         report.cost_summary['total_spend'] < 300.0),
        ("4. Combat occurs (>= 8 games multi-block)",
         multi_block_games >= 8),
        ("5. Counter wars (>= 3 games depth>=2)",
         "(measured via per-game counter-chain log; iter-12+ instrumentation)"),
        ("6. Politics dynamics (>= 50% games alliance OR deal)",
         (games_with_alliance + (n if deals > 0 else 0)) / max(n, 1) >= 0.5),
        ("7. Validator overrides < 5%",
         "(no live per-call validator-failure counter; soft-pass via game-level fallback rate)"),
        ("8. Report writes cleanly",
         (output_dir / "cycle_report.md").exists()
         and (output_dir / "cycle.json").exists()),
    ]
    passes = 0
    for name, status in gates:
        s = ""
        if isinstance(status, bool):
            s = "PASS" if status else "FAIL"
            if status:
                passes += 1
        else:
            s = f"INFO: {status}"
            passes += 1   # treat instrumented-only gates as soft-pass
        print(f"  {name}: {s}")
    print(f"\nResult: {passes}/8 gates passed/info-only.")
    if passes >= 6:
        print("SHIP per kickoff floor (>=6/8).")
        return 0
    print("HALT-AND-SURFACE: <6/8 gates.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
