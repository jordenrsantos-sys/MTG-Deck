"""Phase 4 of mega-task v12 -- cycle runner + aggregation + report writer.

Coverage per kickoff Phase 4 gates:
- aggregator computes correct win_rate from synthetic results.
- aggregator picks GREEN/YELLOW/RED recommendation per thresholds.
- aggregator marks INCOMPLETE when cycle halted for cost.
- cycle runner halts correctly on cycle_cost_ceiling.
- cycle runner writes per-game JSON + cycle_report.md + cycle.json.
- report writer produces expected markdown structure.

Live 30-game cycle smoke deferred to Phase 7 (reduced to 3-game
mini-smoke at $20 budget cap).
"""
from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from api.engine.pillar_f.v0_2.playtest.aggregation import (
    StageTwoReport, aggregate_cycle,
    GREEN_WINRATE, YELLOW_WINRATE,
    GREEN_AVG_TURN_ELIMINATED, YELLOW_AVG_TURN_ELIMINATED,
)
from api.engine.pillar_f.v0_2.playtest.cycle import run_stage_two_cycle
from api.engine.pillar_f.v0_2.playtest.orchestrator import (
    StageTwoDeck, StageTwoGameResult, StageTwoCycleConfig,
)
from api.engine.pillar_f.v0_2.playtest.reports import (
    write_cycle_report_markdown, write_cycle_report_json,
)


# ============================================================
# Helpers
# ============================================================


def _synth_result(
    *,
    game_idx: int = 0,
    seed: int = 1,
    winner_pid: Optional[int] = 0,
    deck_under_test_pid: int = 0,
    turns_run: int = 12,
    halted_for_cost: bool = False,
    elimination_order: Optional[List[tuple]] = None,
    spend: float = 0.5,
) -> StageTwoGameResult:
    """Build a synthetic StageTwoGameResult for aggregator testing."""
    return StageTwoGameResult(
        game_idx=game_idx,
        seed=seed,
        deck_under_test_pid=deck_under_test_pid,
        deck_ids=[f"d{i}" for i in range(4)],
        winner_pid=winner_pid,
        turns_run=turns_run,
        halted_for_cost=halted_for_cost,
        halted_reason="win" if winner_pid is not None and not halted_for_cost
                      else ("cost" if halted_for_cost else "max_turns"),
        elimination_order=elimination_order or [],
        final_life_totals={0: 30, 1: 20, 2: 0, 3: -5},
        final_threat_vectors={},
        politics_summary={
            "alliance_distribution": {
                0: {"ally": 1, "neutral": 1, "rival": 1},
                1: {"ally": 0, "neutral": 2, "rival": 1},
                2: {"ally": 0, "neutral": 3, "rival": 0},
                3: {"ally": 0, "neutral": 3, "rival": 0},
            },
            "deals_made_count": 1,
            "deals_honored_count": 0,
            "damage_log_count": 8,
        },
        action_log=[],
        combat_decisions_log=[],
        total_spend_usd=spend,
        total_llm_calls=100,
        fallback_events=[],
        elapsed_seconds=120.0,
    )


# ============================================================
# Aggregator tests
# ============================================================


class AggregatorWinRateTests(unittest.TestCase):
    def test_all_wins_yields_perfect_winrate(self) -> None:
        results = [
            _synth_result(game_idx=i, winner_pid=0, turns_run=15)
            for i in range(10)
        ]
        report = aggregate_cycle(
            deck_under_test_id="krenko-test",
            deck_under_test_archetype="mono-red burn",
            game_results=results,
        )
        self.assertEqual(report.win_rate, 1.0)
        self.assertEqual(report.pass_recommendation, "GREEN")

    def test_zero_wins_yields_zero_winrate(self) -> None:
        # All wins go to P1, deck-under-test (P0) loses early.
        results = [
            _synth_result(
                game_idx=i, winner_pid=1, turns_run=5,
                elimination_order=[(0, 4, "life_total_zero")],
            )
            for i in range(10)
        ]
        report = aggregate_cycle(
            deck_under_test_id="d", deck_under_test_archetype="?",
            game_results=results,
        )
        self.assertEqual(report.win_rate, 0.0)
        self.assertEqual(report.pass_recommendation, "RED")

    def test_mixed_winrate_picks_yellow(self) -> None:
        # 2/10 wins (0.20) -- borderline YELLOW (gate at 0.20).
        results = []
        for i in range(2):
            results.append(_synth_result(
                game_idx=i, winner_pid=0, turns_run=14,
            ))
        for i in range(8):
            results.append(_synth_result(
                game_idx=i+2, winner_pid=1, turns_run=10,
                elimination_order=[(0, 9, "life_total_zero")],
            ))
        report = aggregate_cycle(
            deck_under_test_id="d", deck_under_test_archetype="?",
            game_results=results,
        )
        self.assertAlmostEqual(report.win_rate, 0.20, places=2)
        # avg_turn_eliminated = 9 (only losses contribute);
        # 0.20 win-rate qualifies YELLOW per OR clause.
        self.assertEqual(report.pass_recommendation, "YELLOW")

    def test_incomplete_when_halted_for_cycle_cost(self) -> None:
        results = [_synth_result(game_idx=0, winner_pid=0, turns_run=20)]
        report = aggregate_cycle(
            deck_under_test_id="d", deck_under_test_archetype="?",
            game_results=results,
            halted_for_cycle_cost=True,
        )
        self.assertEqual(report.pass_recommendation, "INCOMPLETE")

    def test_avg_turn_eliminated_only_counts_losses(self) -> None:
        results = [
            _synth_result(game_idx=0, winner_pid=0, turns_run=20),
            _synth_result(
                game_idx=1, winner_pid=1, turns_run=14,
                elimination_order=[(0, 12, "life_total_zero")],
            ),
        ]
        report = aggregate_cycle(
            deck_under_test_id="d", deck_under_test_archetype="?",
            game_results=results,
        )
        # Only 1 loss, eliminated at turn 12.
        self.assertEqual(report.avg_turn_eliminated_when_lost, 12.0)

    def test_aggregates_politics_across_games(self) -> None:
        results = [_synth_result(game_idx=i) for i in range(3)]
        report = aggregate_cycle(
            deck_under_test_id="d", deck_under_test_archetype="?",
            game_results=results,
        )
        pol = report.politics_summary
        # Each synth has 1 deal_made; 3 games = 3 deals.
        self.assertEqual(pol["total_deals_made"], 3)
        # Each synth has P0 with ally=1+rival=1 -> 3 transitions across 3 games.
        self.assertEqual(pol["games_with_alliance_transition"], 3)


# ============================================================
# Report writer tests
# ============================================================


class ReportWriterTests(unittest.TestCase):
    def test_markdown_report_contains_required_sections(self) -> None:
        results = [_synth_result(game_idx=i, winner_pid=0) for i in range(3)]
        report = aggregate_cycle(
            deck_under_test_id="krenko-b4",
            deck_under_test_archetype="mono-red goblin tribal",
            game_results=results,
        )
        with tempfile.TemporaryDirectory() as td:
            md_path = Path(td) / "cycle_report.md"
            write_cycle_report_markdown(report, md_path)
            text = md_path.read_text(encoding="utf-8")
        self.assertIn("Stage 2 Validation Report -- krenko-b4", text)
        self.assertIn("mono-red goblin tribal", text)
        self.assertIn("## Win-rate breakdown", text)
        self.assertIn("## Damage analysis", text)
        self.assertIn("## Politics summary", text)
        self.assertIn("## Cost summary", text)
        self.assertIn("## Combat summary", text)
        self.assertIn("## Notable game logs", text)
        self.assertIn("## Recommendation", text)

    def test_json_report_round_trips(self) -> None:
        results = [_synth_result(game_idx=i) for i in range(2)]
        report = aggregate_cycle(
            deck_under_test_id="d", deck_under_test_archetype="?",
            game_results=results,
        )
        with tempfile.TemporaryDirectory() as td:
            jp = Path(td) / "cycle.json"
            write_cycle_report_json(report, jp)
            payload = json.loads(jp.read_text(encoding="utf-8"))
        self.assertEqual(payload["deck_under_test_id"], "d")
        self.assertIn("pass_recommendation", payload)
        self.assertIn("politics_summary", payload)
        self.assertEqual(len(payload["per_game_brief"]), 2)


# ============================================================
# Cycle runner integration
# ============================================================


# Re-use the Phase 3 MockLLMClient (default-passes-everywhere).
@dataclass
class MockCallResult:
    ok: bool = True
    text: str = ""
    cost_usd: float = 0.001
    error_code: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class MockLLMClient:
    responses: List[MockCallResult] = field(default_factory=list)
    calls: List[Dict[str, Any]] = field(default_factory=list)
    available: bool = True

    def is_available(self) -> bool:
        return self.available

    def call_with_budget(self, *, system, user, max_input_tokens,
                         max_output_tokens, **kwargs) -> MockCallResult:
        self.calls.append({"system_len": len(system), "user_len": len(user)})
        if self.responses:
            return self.responses.pop(0)
        if "MULLIGANS TAKEN" in user:
            return MockCallResult(ok=True, text='{"keep": true}', cost_usd=0.001)
        if "PUT_ON_BOTTOM" in user:
            import re
            m = re.search(r"PUT_ON_BOTTOM: (\d+)", user)
            n = int(m.group(1)) if m else 1
            cids = re.findall(r"card_id=(\S+)", user)
            picks = cids[:n]
            return MockCallResult(
                ok=True,
                text=('{"cards_to_bottom": [%s]}' %
                       ", ".join('"%s"' % c for c in picks)),
                cost_usd=0.001,
            )
        if "ELIGIBLE ATTACKERS" in user:
            return MockCallResult(ok=True, text='{"attackers": []}', cost_usd=0.001)
        if "INCOMING ATTACKERS" in user:
            return MockCallResult(ok=True, text='{"blocks": []}', cost_usd=0.001)
        return MockCallResult(
            ok=True,
            text='{"action_type": "pass_priority", "action_index": 0}',
            cost_usd=0.001,
        )


def _mono_red_burn_deck(deck_id: str) -> StageTwoDeck:
    mainboard = ["Mountain"] * 20 + ["Lightning Bolt"] * 10
    return StageTwoDeck(
        deck_id=deck_id, commander_name="Krenko, Mob Boss",
        mainboard=mainboard, archetype_hint="mono-red burn", bracket="B3",
    )


class CycleRunnerTests(unittest.TestCase):
    def test_3_game_cycle_completes_with_mocks(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cycle_config = StageTwoCycleConfig(
                deck_under_test=_mono_red_burn_deck("dut"),
                control_pool=[
                    _mono_red_burn_deck(f"ctrl-{i}") for i in range(3)
                ],
                n_games=3,
                output_dir=Path(td),
                max_turns=2,
                max_mulligans=0,
                per_game_cost_ceiling_usd=100.0,
                cycle_cost_ceiling_usd=100.0,
            )
            mock = MockLLMClient()
            report = run_stage_two_cycle(cycle_config, llm_client=mock)
        self.assertEqual(report.games_completed, 3)
        self.assertIn(report.pass_recommendation,
                      ["GREEN", "YELLOW", "RED"])  # not INCOMPLETE

    def test_cycle_writes_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)
            cycle_config = StageTwoCycleConfig(
                deck_under_test=_mono_red_burn_deck("dut"),
                control_pool=[
                    _mono_red_burn_deck(f"ctrl-{i}") for i in range(3)
                ],
                n_games=2,
                output_dir=td_path,
                max_turns=2,
                max_mulligans=0,
                per_game_cost_ceiling_usd=100.0,
                cycle_cost_ceiling_usd=100.0,
            )
            mock = MockLLMClient()
            run_stage_two_cycle(cycle_config, llm_client=mock)
            # Expected artifacts:
            self.assertTrue((td_path / "cycle.json").exists())
            self.assertTrue((td_path / "cycle_report.md").exists())
            self.assertTrue((td_path / "game_000.json").exists())
            self.assertTrue((td_path / "game_001.json").exists())

    def test_cycle_halts_on_cycle_cost_ceiling(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cycle_config = StageTwoCycleConfig(
                deck_under_test=_mono_red_burn_deck("dut"),
                control_pool=[
                    _mono_red_burn_deck(f"ctrl-{i}") for i in range(3)
                ],
                n_games=10,
                output_dir=Path(td),
                max_turns=2,
                max_mulligans=0,
                per_game_cost_ceiling_usd=100.0,
                cycle_cost_ceiling_usd=0.01,  # very tight
            )
            mock = MockLLMClient()
            report = run_stage_two_cycle(cycle_config, llm_client=mock)
        # Game 0 spent enough to trip the ceiling; game 1+ should be
        # skipped, report marked INCOMPLETE.
        self.assertLess(report.games_completed + report.games_halted_for_cost, 10)
        self.assertEqual(report.pass_recommendation, "INCOMPLETE")


if __name__ == "__main__":
    unittest.main()
