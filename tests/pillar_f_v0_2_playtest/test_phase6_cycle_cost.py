"""Phase 6 of mega-task v12 -- per-cycle cost ceiling + halt flow.

Coverage per kickoff Phase 6 gates:
- CYCLE_COST_CEILING_USD = 200.0 default exposed via StageTwoCycleConfig.
- Cycle accumulates per-game spend correctly.
- Ceiling triggers halt; report marked INCOMPLETE.
- Partial report writes correctly on halt (per-game JSONs through
  the halt point, cycle.json + cycle_report.md tagged INCOMPLETE).
- Per-cycle ceiling override via cycle_config.cycle_cost_ceiling_usd.
- CYCLE_COST_HALT event recorded in cycle report's cost_summary.

Sub-C's cycle_cost_ceiling layer is ABOVE sub-B's per-turn ($0.30) +
per-game ($5) ceilings. Phase 8 of v10 covers per-turn + per-game;
this file covers per-cycle.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from api.engine.pillar_f.v0_2.playtest.cycle import run_stage_two_cycle
from api.engine.pillar_f.v0_2.playtest.orchestrator import (
    StageTwoDeck, StageTwoCycleConfig,
)


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
    available: bool = True

    def is_available(self) -> bool:
        return self.available

    def call_with_budget(self, *, system, user, max_input_tokens,
                         max_output_tokens, **kwargs) -> MockCallResult:
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


def _simple_deck(deck_id: str) -> StageTwoDeck:
    return StageTwoDeck(
        deck_id=deck_id, commander_name="Krenko, Mob Boss",
        mainboard=["Mountain"] * 20 + ["Lightning Bolt"] * 10,
        archetype_hint="mono-red burn", bracket="B3",
    )


# ============================================================
# Defaults
# ============================================================


class CycleCostCeilingDefaultsTests(unittest.TestCase):
    def test_default_cycle_cost_ceiling_is_200(self) -> None:
        cfg = StageTwoCycleConfig(
            deck_under_test=_simple_deck("d"),
            control_pool=[_simple_deck("c")],
        )
        self.assertEqual(cfg.cycle_cost_ceiling_usd, 200.0)


# ============================================================
# Per-cycle ceiling halts the cycle
# ============================================================


class CycleCeilingHaltsTests(unittest.TestCase):
    def test_tight_ceiling_halts_early(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cycle_config = StageTwoCycleConfig(
                deck_under_test=_simple_deck("dut"),
                control_pool=[_simple_deck(f"c-{i}") for i in range(3)],
                n_games=10,
                output_dir=Path(td),
                max_turns=2,
                max_mulligans=0,
                per_game_cost_ceiling_usd=100.0,
                cycle_cost_ceiling_usd=0.01,  # halt after game 0
            )
            mock = MockLLMClient()
            report = run_stage_two_cycle(cycle_config, llm_client=mock)
        # Fewer than 10 games actually ran.
        total_games = report.games_completed + report.games_halted_for_cost
        self.assertLess(total_games, 10)
        self.assertEqual(report.pass_recommendation, "INCOMPLETE")

    def test_cycle_halt_emits_event(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cycle_config = StageTwoCycleConfig(
                deck_under_test=_simple_deck("dut"),
                control_pool=[_simple_deck(f"c-{i}") for i in range(3)],
                n_games=10,
                output_dir=Path(td),
                max_turns=2,
                max_mulligans=0,
                per_game_cost_ceiling_usd=100.0,
                cycle_cost_ceiling_usd=0.01,
            )
            mock = MockLLMClient()
            report = run_stage_two_cycle(cycle_config, llm_client=mock)
        cycle_events = report.cost_summary.get("cycle_events") or []
        halt_events = [e for e in cycle_events if e.get("event") == "CYCLE_COST_HALT"]
        self.assertEqual(len(halt_events), 1)
        halt = halt_events[0]
        self.assertIn("at_game_idx", halt)
        self.assertIn("cost_to_date", halt)
        self.assertEqual(halt["ceiling"], 0.01)
        self.assertGreater(halt["games_remaining_skipped"], 0)


# ============================================================
# Generous ceiling allows full cycle
# ============================================================


class CycleCeilingNotTrippedTests(unittest.TestCase):
    def test_generous_ceiling_lets_cycle_complete(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cycle_config = StageTwoCycleConfig(
                deck_under_test=_simple_deck("dut"),
                control_pool=[_simple_deck(f"c-{i}") for i in range(3)],
                n_games=3,
                output_dir=Path(td),
                max_turns=2,
                max_mulligans=0,
                per_game_cost_ceiling_usd=100.0,
                cycle_cost_ceiling_usd=1000.0,  # plenty
            )
            mock = MockLLMClient()
            report = run_stage_two_cycle(cycle_config, llm_client=mock)
        self.assertEqual(report.games_completed, 3)
        cycle_events = report.cost_summary.get("cycle_events") or []
        halt_events = [e for e in cycle_events if e.get("event") == "CYCLE_COST_HALT"]
        self.assertEqual(halt_events, [])
        self.assertEqual(report.cost_summary.get("halted_for_cycle_cost"), False)
        self.assertIn(report.pass_recommendation,
                      ["GREEN", "YELLOW", "RED"])


# ============================================================
# Override mechanism
# ============================================================


class CycleCeilingOverrideTests(unittest.TestCase):
    def test_override_via_cycle_config(self) -> None:
        """cycle_config.cycle_cost_ceiling_usd is the override path
        for high-stakes opt-in builds."""
        cfg = StageTwoCycleConfig(
            deck_under_test=_simple_deck("d"),
            control_pool=[_simple_deck("c")],
            cycle_cost_ceiling_usd=500.0,  # high-stakes override
        )
        self.assertEqual(cfg.cycle_cost_ceiling_usd, 500.0)


# ============================================================
# Partial artifacts on halt
# ============================================================


class CyclePartialArtifactsTests(unittest.TestCase):
    def test_per_game_json_written_through_halt_point(self) -> None:
        """Even on cost halt, the per-game JSONs through the halt
        point are persisted (so the user can inspect what DID run)."""
        with tempfile.TemporaryDirectory() as td:
            cycle_config = StageTwoCycleConfig(
                deck_under_test=_simple_deck("dut"),
                control_pool=[_simple_deck(f"c-{i}") for i in range(3)],
                n_games=5,
                output_dir=Path(td),
                max_turns=2,
                max_mulligans=0,
                per_game_cost_ceiling_usd=100.0,
                cycle_cost_ceiling_usd=0.01,
            )
            mock = MockLLMClient()
            run_stage_two_cycle(cycle_config, llm_client=mock)
            # At least game_000.json exists (the first game ran before
            # ceiling was checked at game 1).
            self.assertTrue((Path(td) / "game_000.json").exists())
            # cycle_report.md + cycle.json exist even on halt.
            self.assertTrue((Path(td) / "cycle.json").exists())
            self.assertTrue((Path(td) / "cycle_report.md").exists())

    def test_partial_report_marked_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cycle_config = StageTwoCycleConfig(
                deck_under_test=_simple_deck("dut"),
                control_pool=[_simple_deck(f"c-{i}") for i in range(3)],
                n_games=5,
                output_dir=Path(td),
                max_turns=2,
                max_mulligans=0,
                per_game_cost_ceiling_usd=100.0,
                cycle_cost_ceiling_usd=0.01,
            )
            mock = MockLLMClient()
            run_stage_two_cycle(cycle_config, llm_client=mock)
            report_json = json.loads(
                (Path(td) / "cycle.json").read_text(encoding="utf-8")
            )
        self.assertEqual(report_json["pass_recommendation"], "INCOMPLETE")


if __name__ == "__main__":
    unittest.main()
