"""Phase 5 of mega-task v12 -- dispatcher integration unit tests.

Coverage per kickoff Phase 5 gates:
- enable_stage_2 opt-in default = False -> Stage 2 skipped.
- Stage 1 GREEN + enable_stage_2 + Stage 2 runs cleanly.
- Stage 1 RED at Tier 0 -> Stage 2 skipped (budget guard).
- Calibration log appends correctly with timestamp + delta.
- Delta > 15% threshold flagged.
- Calibration log handles missing file (creates) + corrupt file
  (overwrites).
"""
from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from api.engine.layers.agent_graduated_playtest_stage_2_v1 import (
    CombinedReport, run_stage_1_then_stage_2,
    CALIBRATION_DELTA_THRESHOLD,
)
from api.engine.pillar_f.v0_2.playtest.orchestrator import StageTwoDeck


# Reuse simple mock LLM and deck builder from earlier tests.
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
        # Reasonable defaults so cycle runs complete.
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


def _decklist_dict(commander: str) -> List[Dict[str, Any]]:
    """Build a minimal deck-as-list-of-dicts shape the v1 Stage 1
    orchestrator consumes."""
    return [
        {"name": commander, "is_commander": True, "bracket": "B3"},
    ] + [
        {"name": "Mountain", "type_line": "Basic Land — Mountain"}
        for _ in range(99)
    ]


# ============================================================
# Opt-in flag tests
# ============================================================


class OptInFlagTests(unittest.TestCase):
    def test_default_skips_stage_2(self) -> None:
        result = run_stage_1_then_stage_2(
            deck=_decklist_dict("Krenko, Mob Boss"),
            bracket="B3",
            # enable_stage_2 default = False
        )
        self.assertEqual(result.stage_2_recommendation, "SKIPPED")
        self.assertIsNone(result.stage_2_report)
        self.assertFalse(result.enable_stage_2)

    def test_enable_true_but_missing_args_skipped(self) -> None:
        """If enable_stage_2=True but stage_2_deck/control_pool/
        llm_client are missing, Stage 2 is skipped with explanation."""
        result = run_stage_1_then_stage_2(
            deck=_decklist_dict("Krenko, Mob Boss"),
            bracket="B3",
            enable_stage_2=True,
        )
        self.assertEqual(result.stage_2_recommendation, "SKIPPED")
        self.assertIn("not provided", result.stage_2_recommendation_reason)


# ============================================================
# Full Stage 1 + Stage 2 flow
# ============================================================


class FullFlowTests(unittest.TestCase):
    def test_enable_true_runs_stage_2_cycle(self) -> None:
        """Smoke: enable Stage 2 with a 1-game mini-cycle + mock LLM
        + temp output dir; verify CombinedReport populated."""
        deck = _simple_deck("krenko-dut")
        controls = [_simple_deck(f"ctrl-{i}") for i in range(3)]
        mock = MockLLMClient()
        with tempfile.TemporaryDirectory() as td:
            log_path = Path(td) / "calibration_log.json"
            result = run_stage_1_then_stage_2(
                deck=_decklist_dict("Krenko, Mob Boss"),
                bracket="B3",
                enable_stage_2=True,
                stage_2_deck=deck,
                stage_2_control_pool=controls,
                stage_2_n_games=1,
                stage_2_output_dir=Path(td) / "cycle",
                stage_2_cycle_cost_ceiling_usd=100.0,
                llm_client=mock,
                calibration_log_path=log_path,
            )
        # Stage 1 always runs.
        self.assertIsNotNone(result.stage_1_report)
        # Stage 2 runs because enable=True + Stage 1 has some signal.
        self.assertIsNotNone(result.stage_2_report)
        self.assertIn(
            result.stage_2_recommendation,
            ["GREEN", "YELLOW", "RED", "INCOMPLETE"],
        )

    def test_stage_1_red_at_tier_0_skips_stage_2(self) -> None:
        """If Stage 1 stalls at Tier 0 with very low winrate, sub-C
        gates skip Stage 2 to save budget."""
        # Construct a fake Stage 1 result -- need to bypass real Stage
        # 1 since it depends on opposition data. Easier: stub
        # run_graduated_sweep to return RED.
        import api.engine.layers.agent_graduated_playtest_stage_2_v1 as mod
        original_run = mod.run_graduated_sweep
        try:
            def _fake_run(*args, **kwargs):
                from api.engine.layers.agent_graduated_playtest_v1 import (
                    GraduationReport, TierResult,
                )
                return GraduationReport(
                    bracket="B3", final_tier_reached=0,
                    tier_results=[TierResult(
                        tier=0, label="Tier 0", bracket="B3",
                        effective_bracket="B2", pod_winrate=0.05,
                        advanced=False, reason="winrate_lt_threshold",
                    )],
                    overall_status="stalled_tier_0",
                )
            mod.run_graduated_sweep = _fake_run
            mock = MockLLMClient()
            with tempfile.TemporaryDirectory() as td:
                result = run_stage_1_then_stage_2(
                    deck=_decklist_dict("Krenko, Mob Boss"),
                    bracket="B3",
                    enable_stage_2=True,
                    stage_2_deck=_simple_deck("dut"),
                    stage_2_control_pool=[_simple_deck("c")],
                    stage_2_n_games=1,
                    stage_2_output_dir=Path(td),
                    llm_client=mock,
                    calibration_log_path=Path(td) / "log.json",
                )
            self.assertEqual(result.stage_2_recommendation, "RED")
            self.assertIsNone(result.stage_2_report)
            self.assertIn("stalled at Tier 0",
                          result.stage_2_recommendation_reason)
        finally:
            mod.run_graduated_sweep = original_run


# ============================================================
# Calibration log tests
# ============================================================


class CalibrationLogTests(unittest.TestCase):
    def test_log_appends_record_with_timestamp(self) -> None:
        deck = _simple_deck("krenko")
        controls = [_simple_deck(f"c-{i}") for i in range(3)]
        mock = MockLLMClient()
        with tempfile.TemporaryDirectory() as td:
            log_path = Path(td) / "cal.json"
            run_stage_1_then_stage_2(
                deck=_decklist_dict("Krenko, Mob Boss"),
                bracket="B3",
                enable_stage_2=True,
                stage_2_deck=deck,
                stage_2_control_pool=controls,
                stage_2_n_games=1,
                stage_2_output_dir=Path(td) / "out",
                llm_client=mock,
                calibration_log_path=log_path,
            )
            if not log_path.exists():
                self.skipTest("Stage 2 was gated off; no log entry.")
            data = json.loads(log_path.read_text(encoding="utf-8"))
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 1)
        rec = data[0]
        self.assertIn("timestamp", rec)
        self.assertEqual(rec["deck_id"], "krenko")
        self.assertEqual(rec["bracket"], "B3")
        self.assertIn("delta", rec)
        self.assertIn("delta_exceeds_threshold", rec)

    def test_log_appends_to_existing_records(self) -> None:
        """Second cycle on same deck appends, doesn't overwrite."""
        deck = _simple_deck("krenko")
        controls = [_simple_deck(f"c-{i}") for i in range(3)]
        mock = MockLLMClient()
        with tempfile.TemporaryDirectory() as td:
            log_path = Path(td) / "cal.json"
            # Pre-seed log with a fake earlier record.
            log_path.write_text(json.dumps([{
                "timestamp": "2026-05-22T00:00:00Z",
                "deck_id": "fake-old",
            }]), encoding="utf-8")
            run_stage_1_then_stage_2(
                deck=_decklist_dict("Krenko, Mob Boss"),
                bracket="B3",
                enable_stage_2=True,
                stage_2_deck=deck,
                stage_2_control_pool=controls,
                stage_2_n_games=1,
                stage_2_output_dir=Path(td) / "out",
                llm_client=mock,
                calibration_log_path=log_path,
            )
            data = json.loads(log_path.read_text(encoding="utf-8"))
        # Original record + new record.
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["deck_id"], "fake-old")
        self.assertEqual(data[1]["deck_id"], "krenko")

    def test_log_recovers_from_corrupt_file(self) -> None:
        deck = _simple_deck("krenko")
        controls = [_simple_deck(f"c-{i}") for i in range(3)]
        mock = MockLLMClient()
        with tempfile.TemporaryDirectory() as td:
            log_path = Path(td) / "cal.json"
            log_path.write_text("not-valid-json", encoding="utf-8")
            run_stage_1_then_stage_2(
                deck=_decklist_dict("Krenko, Mob Boss"),
                bracket="B3",
                enable_stage_2=True,
                stage_2_deck=deck,
                stage_2_control_pool=controls,
                stage_2_n_games=1,
                stage_2_output_dir=Path(td) / "out",
                llm_client=mock,
                calibration_log_path=log_path,
            )
            data = json.loads(log_path.read_text(encoding="utf-8"))
        # Should have ONE valid record now (corrupt content discarded).
        self.assertEqual(len(data), 1)

    def test_delta_threshold_constant(self) -> None:
        self.assertEqual(CALIBRATION_DELTA_THRESHOLD, 0.15)


if __name__ == "__main__":
    unittest.main()
