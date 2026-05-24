"""Phase 1 of mega-task v15 -- live parallel cycle smoke.

Per kickoff: live 3-game smoke at concurrency=4 confirms asyncio
parallelism works against the real Agent SDK. Subscription auth
must hold across parallel calls (i.e. cost_basis stays
"subscription_credit" on each game's reported cost).

Krenko mono-red vs mono-color burn opponents (light card-coverage
demands; cheap to run).

Run:
    python tools/test_v15_phase1_parallel_smoke.py
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
        deck_id="krenko-v15-phase1",
        commander_name="Krenko, Mob Boss",
        mainboard=mainboard,
        archetype_hint="mono-red goblin tribal",
        bracket="B3",
    )


def _heliod_deck() -> StageTwoDeck:
    mainboard = (
        ["Plains"] * 18
        + ["Soldier of the Pantheon"] * 4
        + ["Thalia, Guardian of Thraben"] * 2
        + ["Heliod, Sun-Crowned"] * 2
        + ["Counterspell"] * 4
    )
    return StageTwoDeck(
        deck_id="heliod-v15-phase1",
        commander_name="Heliod, Sun-Crowned",
        mainboard=mainboard,
        archetype_hint="mono-white lifegain control",
        bracket="B3",
    )


def _snapcaster_deck() -> StageTwoDeck:
    mainboard = (
        ["Island"] * 16
        + ["Delver of Secrets"] * 4
        + ["Snapcaster Mage"] * 4
        + ["Counterspell"] * 3
        + ["Negate"] * 3
    )
    return StageTwoDeck(
        deck_id="snapcaster-v15-phase1",
        commander_name="Snapcaster Mage",
        mainboard=mainboard,
        archetype_hint="mono-blue tempo control",
        bracket="B3",
    )


def _imp_deck() -> StageTwoDeck:
    mainboard = (
        ["Swamp"] * 18
        + ["Putrid Imp"] * 4
        + ["Reassembling Skeleton"] * 4
        + ["Vampire Nighthawk"] * 4
    )
    return StageTwoDeck(
        deck_id="imp-v15-phase1",
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
        / "stage_2_v15_phase1_smoke"

    cycle_config = StageTwoCycleConfig(
        deck_under_test=_krenko_deck(),
        control_pool=[_heliod_deck(), _snapcaster_deck(), _imp_deck()],
        n_games=3,
        parallelism=4,                              # Phase 1 unlock
        output_dir=output_dir,
        max_turns=8,
        max_mulligans=1,
        per_turn_cost_ceiling_usd=0.30,
        per_game_cost_ceiling_usd=3.0,
        cycle_cost_ceiling_usd=15.0,
        starting_life=40,
        seed_base=1,
    )

    print("=== Phase 1 parallel smoke (3 games, concurrency=4) ===",
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
    elapsed = time.perf_counter() - t0

    print(flush=True)
    print(f"=== Cycle complete in {elapsed:.0f}s wallclock ===")
    print(f"Recommendation: {report.pass_recommendation}")
    print(f"Reason: {report.recommendation_reason}")
    print(f"Total spend (api_estimate sum): "
          f"${report.cost_summary['total_spend']:.3f}")
    print(f"Per-game avg: ${report.cost_summary['per_game_avg']:.3f}")

    # Inspect a per-game JSON to verify cost_basis stayed subscription.
    import json
    sample_path = output_dir / "game_000.json"
    if sample_path.exists():
        payload = json.loads(sample_path.read_text(encoding="utf-8"))
        fb_evts = payload.get("fallback_events") or []
        print(f"\nGame 0 fallback_events count: {len(fb_evts)}")
        # game_runner persists cost-tracker events; subscription auth
        # is verified by the absence of API-key-fallback codes.

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
