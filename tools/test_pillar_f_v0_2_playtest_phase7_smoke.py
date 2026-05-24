"""Phase 7 of mega-task v12 -- 3-game mini-smoke (REDUCED scope, $20 cap).

Per kickoff Phase 7 spec, reduced from 30 games to 3 games due to
$20 budget cap (user dispatched after Phase 3). Demonstrates the
Stage 2 harness end-to-end live but does NOT establish a
statistically meaningful win-rate.

Decks (all in iter-11 card factory coverage):
- Deck under test (P0): Krenko mono-red goblin tribal.
- 3 controls: mono-W lifegain (Heliod), mono-U tempo (Snapcaster),
  mono-B reanimator (Putrid Imp).

Caps (tighter than scoping default to stay within $20):
- max_turns = 8 (vs scoping 25)
- max_mulligans = 1 (vs scoping 2)
- per_game_cost_ceiling = $3
- cycle_cost_ceiling = $15

Expected cost: $8-15.

Run:
    python tools/test_pillar_f_v0_2_playtest_phase7_smoke.py
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


def _krenko_deck() -> StageTwoDeck:
    """Mono-red goblin tribal, 30 cards. Iter-11 card factory has the
    creature stats + Lightning Bolt instant for these names."""
    mainboard = (
        ["Mountain"] * 16
        + ["Goblin Guide"] * 3
        + ["Goblin Piledriver"] * 2
        + ["Skirk Prospector"] * 2
        + ["Battle Cry Goblin"] * 2
        + ["Lightning Bolt"] * 3
        + ["Shock"] * 2
    )
    return StageTwoDeck(
        deck_id="krenko-mono-red-2026-05-23",
        commander_name="Krenko, Mob Boss",
        mainboard=mainboard,
        archetype_hint="mono-red goblin tribal",
        bracket="B3",
    )


def _heliod_deck() -> StageTwoDeck:
    """Mono-W lifegain control."""
    mainboard = (
        ["Plains"] * 18
        + ["Soldier of the Pantheon"] * 4
        + ["Thalia, Guardian of Thraben"] * 2
        + ["Heliod, Sun-Crowned"] * 2
        + ["Counterspell"] * 4  # Cross-color stub for variety
    )
    return StageTwoDeck(
        deck_id="heliod-mono-w-control",
        commander_name="Heliod, Sun-Crowned",
        mainboard=mainboard,
        archetype_hint="mono-white lifegain control",
        bracket="B3",
    )


def _snapcaster_deck() -> StageTwoDeck:
    """Mono-U tempo with counterspells."""
    mainboard = (
        ["Island"] * 16
        + ["Delver of Secrets"] * 4
        + ["Snapcaster Mage"] * 4
        + ["Counterspell"] * 3
        + ["Negate"] * 3
    )
    return StageTwoDeck(
        deck_id="snapcaster-mono-u-tempo",
        commander_name="Snapcaster Mage",
        mainboard=mainboard,
        archetype_hint="mono-blue tempo control",
        bracket="B3",
    )


def _imp_deck() -> StageTwoDeck:
    """Mono-B reanimator."""
    mainboard = (
        ["Swamp"] * 18
        + ["Putrid Imp"] * 4
        + ["Reassembling Skeleton"] * 4
        + ["Vampire Nighthawk"] * 4
    )
    return StageTwoDeck(
        deck_id="imp-mono-b-reanimator",
        commander_name="Putrid Imp",
        mainboard=mainboard,
        archetype_hint="mono-black aggro / reanimator",
        bracket="B3",
    )


def main() -> int:
    client = get_default_client()
    if not client.is_available():
        print(f"LLM unavailable: {client.unavailable_reason()}")
        return 1

    output_dir = REPO_ROOT / "api" / "engine" / "data" / "agent" \
        / "stage_2_phase7_mini_smoke_2026-05-23"

    cycle_config = StageTwoCycleConfig(
        deck_under_test=_krenko_deck(),
        control_pool=[
            _heliod_deck(),
            _snapcaster_deck(),
            _imp_deck(),
        ],
        n_games=3,
        output_dir=output_dir,
        max_turns=8,                                # tight
        max_mulligans=1,
        per_turn_cost_ceiling_usd=0.30,
        per_game_cost_ceiling_usd=3.0,              # tight
        cycle_cost_ceiling_usd=15.0,                # cap
        starting_life=40,
        seed_base=1,
    )

    print("=== Phase 7 mini-smoke (3 games, $15 cap) ===", flush=True)
    print(f"DUT:    {cycle_config.deck_under_test.deck_id}")
    print(f"Pool:   {[d.deck_id for d in cycle_config.control_pool]}")
    print(f"Output: {output_dir}")
    print(f"Caps:   max_turns={cycle_config.max_turns}, "
          f"max_mulligans={cycle_config.max_mulligans}, "
          f"per_game=${cycle_config.per_game_cost_ceiling_usd}, "
          f"cycle=${cycle_config.cycle_cost_ceiling_usd}")
    print(flush=True)

    t_start = time.perf_counter()

    def _progress(idx, total, result):
        print(f"[game {idx + 1}/{total}] turns={result.turns_run} "
              f"winner=P{result.winner_pid} "
              f"halted_cost={result.halted_for_cost} "
              f"spend=${result.total_spend_usd:.3f} "
              f"elapsed={result.elapsed_seconds:.0f}s",
              flush=True)

    report = run_stage_two_cycle(
        cycle_config, llm_client=client, progress_callback=_progress,
    )
    elapsed = time.perf_counter() - t_start

    print(flush=True)
    print(f"=== Cycle complete in {elapsed:.0f}s ===")
    print(f"Recommendation: {report.pass_recommendation}")
    print(f"Reason: {report.recommendation_reason}")
    print(f"Win rate: {report.win_rate:.2%} "
          f"({sum(1 for g in report.per_game_brief if g['deck_under_test_outcome'] == 'WIN')} "
          f"wins / {report.games_completed + report.games_halted_for_cost} games)")
    print(f"Avg turn eliminated when lost: "
          f"{report.avg_turn_eliminated_when_lost:.1f}")
    print(f"Total spend: ${report.cost_summary['total_spend']:.2f} "
          f"(per-game avg ${report.cost_summary['per_game_avg']:.2f})")
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

    print(flush=True)
    print(f"Per-game brief:")
    for g in report.per_game_brief:
        print(f"  game_{g['game_idx']:03d}: "
              f"{g['deck_under_test_outcome']} "
              f"turn={g['turns_run']} "
              f"life={g['final_life_totals']} "
              f"spend=${g['spend_usd']:.3f}")

    print(flush=True)
    print(f"Artifacts written to {output_dir}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
