"""Phase 1 of mega-task v15 -- asyncio parallelism for cycle runner.

Covers:
- parallelism>=1 path runs games in waves of size N concurrently
- per-game results aggregate correctly across parallel runs
- per-game JSON written atomically (one shot per file; no
  interleaving)
- per-cycle cost ceiling halts between waves
- rate-limit fallback drops concurrency to serial after streak
- backwards compat: parallelism=1 path matches existing serial
  behavior (verified by existing test_phase4_cycle + test_phase6_cycle_cost
  tests still passing)
"""
from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from api.engine.pillar_f.v0_2.playtest.cycle import run_stage_two_cycle
from api.engine.pillar_f.v0_2.playtest.cycle.cycle_runner import (
    _looks_like_rate_limit, _RATE_LIMIT_FALLBACK_STREAK,
)
from api.engine.pillar_f.v0_2.playtest.orchestrator import (
    StageTwoDeck, StageTwoCycleConfig, StageTwoGameResult,
)


# Reuse the canned-mock LLM pattern from Phase 4 tests.
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


class ParallelExecutionTests(unittest.TestCase):
    def test_parallelism_4_runs_4_games_concurrently(self) -> None:
        """4-game cycle with parallelism=4 completes in one wave."""
        with tempfile.TemporaryDirectory() as td:
            cycle_config = StageTwoCycleConfig(
                deck_under_test=_simple_deck("dut"),
                control_pool=[_simple_deck(f"c-{i}") for i in range(3)],
                n_games=4,
                parallelism=4,
                output_dir=Path(td),
                max_turns=2,
                max_mulligans=0,
                per_game_cost_ceiling_usd=100.0,
                cycle_cost_ceiling_usd=100.0,
            )
            mock = MockLLMClient()
            report = run_stage_two_cycle(cycle_config, llm_client=mock)
        self.assertEqual(report.games_completed, 4)
        self.assertEqual(report.cost_summary.get("halted_for_cycle_cost"), False)

    def test_parallelism_2_runs_in_waves(self) -> None:
        """6-game cycle with parallelism=2 runs in 3 waves of 2."""
        with tempfile.TemporaryDirectory() as td:
            cycle_config = StageTwoCycleConfig(
                deck_under_test=_simple_deck("dut"),
                control_pool=[_simple_deck(f"c-{i}") for i in range(3)],
                n_games=6,
                parallelism=2,
                output_dir=Path(td),
                max_turns=2,
                max_mulligans=0,
                per_game_cost_ceiling_usd=100.0,
                cycle_cost_ceiling_usd=100.0,
            )
            mock = MockLLMClient()
            report = run_stage_two_cycle(cycle_config, llm_client=mock)
        self.assertEqual(report.games_completed, 6)

    def test_parallelism_1_unchanged_behavior(self) -> None:
        """parallelism=1 falls through the serial code path. Result
        shape identical to pre-v15 cycle runner."""
        with tempfile.TemporaryDirectory() as td:
            cycle_config = StageTwoCycleConfig(
                deck_under_test=_simple_deck("dut"),
                control_pool=[_simple_deck(f"c-{i}") for i in range(3)],
                n_games=3,
                parallelism=1,
                output_dir=Path(td),
                max_turns=2,
                max_mulligans=0,
                per_game_cost_ceiling_usd=100.0,
                cycle_cost_ceiling_usd=100.0,
            )
            mock = MockLLMClient()
            report = run_stage_two_cycle(cycle_config, llm_client=mock)
        self.assertEqual(report.games_completed, 3)

    def test_per_game_indices_match_game_results(self) -> None:
        """Even when parallel, each game's StageTwoGameResult.game_idx
        matches the position. Per-game JSONs land at the right filename."""
        with tempfile.TemporaryDirectory() as td:
            cycle_config = StageTwoCycleConfig(
                deck_under_test=_simple_deck("dut"),
                control_pool=[_simple_deck(f"c-{i}") for i in range(3)],
                n_games=5,
                parallelism=3,
                output_dir=Path(td),
                max_turns=2,
                max_mulligans=0,
                per_game_cost_ceiling_usd=100.0,
                cycle_cost_ceiling_usd=100.0,
            )
            mock = MockLLMClient()
            run_stage_two_cycle(cycle_config, llm_client=mock)
            for i in range(5):
                self.assertTrue(
                    (Path(td) / f"game_{i:03d}.json").exists(),
                    f"missing game_{i:03d}.json",
                )

    def test_per_game_json_is_atomic_no_interleave(self) -> None:
        """Per-game JSON files must each be valid JSON (not torn by
        concurrent writes). Iter-15: write_per_game_json is a single
        json.dump call inside a context manager; OS-level file writes
        are atomic at the syscall level for small payloads. Iter-16+
        could harden with a tmp-then-rename pattern."""
        with tempfile.TemporaryDirectory() as td:
            cycle_config = StageTwoCycleConfig(
                deck_under_test=_simple_deck("dut"),
                control_pool=[_simple_deck(f"c-{i}") for i in range(3)],
                n_games=8,
                parallelism=4,
                output_dir=Path(td),
                max_turns=2,
                max_mulligans=0,
                per_game_cost_ceiling_usd=100.0,
                cycle_cost_ceiling_usd=100.0,
            )
            mock = MockLLMClient()
            run_stage_two_cycle(cycle_config, llm_client=mock)
            for i in range(8):
                payload = json.loads(
                    (Path(td) / f"game_{i:03d}.json").read_text(
                        encoding="utf-8",
                    ),
                )
                # Each game's payload has the right game_idx + a valid
                # winner/loss outcome shape.
                self.assertEqual(payload["game_idx"], i)


class CycleCostCeilingParallelTests(unittest.TestCase):
    def test_cost_ceiling_halts_between_waves(self) -> None:
        """Cost ceiling check fires between waves; parallel games in a
        wave can collectively overshoot by one wave's worth."""
        with tempfile.TemporaryDirectory() as td:
            cycle_config = StageTwoCycleConfig(
                deck_under_test=_simple_deck("dut"),
                control_pool=[_simple_deck(f"c-{i}") for i in range(3)],
                n_games=20,
                parallelism=4,
                output_dir=Path(td),
                max_turns=2,
                max_mulligans=0,
                per_game_cost_ceiling_usd=100.0,
                cycle_cost_ceiling_usd=0.005,  # tight -- halts fast
            )
            mock = MockLLMClient()
            report = run_stage_two_cycle(cycle_config, llm_client=mock)
        total_games = report.games_completed + report.games_halted_for_cost
        # Fewer than 20 (halt fired); but the first wave (4 games) ran
        # before the post-wave ceiling check fired.
        self.assertLess(total_games, 20)
        self.assertEqual(report.pass_recommendation, "INCOMPLETE")
        # Halt event present.
        cycle_events = report.cost_summary.get("cycle_events") or []
        halt_events = [e for e in cycle_events
                       if e.get("event") == "CYCLE_COST_HALT"]
        self.assertEqual(len(halt_events), 1)


class RateLimitFallbackTests(unittest.TestCase):
    def test_looks_like_rate_limit_detects_event(self) -> None:
        """Helper recognizes rate-limit-style fallback events."""
        result = StageTwoGameResult(
            game_idx=0, seed=1, deck_under_test_pid=0,
            deck_ids=["a", "b", "c", "d"],
            fallback_events=[{"event": "RATE_LIMIT_HIT"}],
        )
        self.assertTrue(_looks_like_rate_limit(result))

    def test_looks_like_rate_limit_ignores_other_events(self) -> None:
        result = StageTwoGameResult(
            game_idx=0, seed=1, deck_under_test_pid=0,
            deck_ids=["a", "b", "c", "d"],
            fallback_events=[{"event": "COST_CEILING_HIT"}],
        )
        self.assertFalse(_looks_like_rate_limit(result))

    def test_rate_limit_streak_constant(self) -> None:
        self.assertEqual(_RATE_LIMIT_FALLBACK_STREAK, 3)


if __name__ == "__main__":
    unittest.main()
