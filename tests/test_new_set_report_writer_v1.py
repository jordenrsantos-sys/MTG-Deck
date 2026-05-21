"""Mega-task v3 Phase 6 — LLM discovery report writer tests.

Verifies:
  - build_report_inputs correctly shapes pipeline data
  - _rank_impactful_cards ranks by max delta + combo participation
  - _rank_combo_pairs ranks by confidence
  - _archetype_winners_losers aggregates cumulative deltas
  - _primitive_dimension_coverage counts cards per dimension
  - Fallback report (no LLM) has all 5 sections
  - write_set_report returns OK envelope with valid markdown when LLM
    returns valid JSON
  - write_set_report falls back when LLM is unavailable
  - write_set_report falls back when LLM returns invalid JSON
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from api.engine.layers.new_set_report_writer_v1 import (
    ReportEnvelope,
    _archetype_winners_losers,
    _fallback_markdown,
    _rank_combo_pairs,
    _rank_impactful_cards,
    build_report_inputs,
    write_set_report,
)


def _mk_pipeline_data():
    """Synthetic 4-card pipeline output."""
    return {
        "released_at": "2026-05-01",
        "cards": [
            {"name": "Sac Outlet", "primitives": ["sac-outlet"]},
            {"name": "Persist Creature",
             "primitives": ["persist-creature", "death-trigger"]},
            {"name": "Mana Rock", "primitives": ["mana-positive-rock"]},
            {"name": "Vanilla", "primitives": []},
        ],
        "archetype_impacts": {
            "Sac Outlet": {
                "aristocrats": {
                    "delta": 0.08, "fits_role": "sac-outlet",
                    "matched_primitives": ["sac-outlet"],
                },
                "tribal": {"delta": 0.0, "matched_primitives": []},
            },
            "Persist Creature": {
                "aristocrats": {
                    "delta": 0.14, "fits_role": "death-trigger",
                    "matched_primitives": ["persist-creature", "death-trigger"],
                },
            },
            "Mana Rock": {
                "storm": {"delta": 0.04, "fits_role": "mana-positive-rock",
                          "matched_primitives": ["mana-positive-rock"]},
            },
            "Vanilla": {
                "tribal": {"delta": 0.0, "matched_primitives": []},
            },
        },
        "combo_pairs": [
            {
                "new_card": "Sac Outlet", "paired_with": "Persist Creature",
                "combo_pattern": "ontology_edge:sac-outlet<->persist-creature",
                "confidence": 1.0,
                "via_primitives": ["sac-outlet", "persist-creature"],
            },
            {
                "new_card": "Sac Outlet", "paired_with": "Some Existing Card",
                "combo_pattern": "canonical_pair:sac-outlet+death-trigger",
                "confidence": 0.7,
                "via_primitives": ["sac-outlet", "death-trigger"],
            },
        ],
    }


class BuildReportInputsTests(unittest.TestCase):
    def test_shape(self) -> None:
        inputs = build_report_inputs("tst", "Test Set", _mk_pipeline_data())
        self.assertEqual(inputs["set_code"], "tst")
        self.assertEqual(inputs["set_name"], "Test Set")
        self.assertEqual(inputs["card_count"], 4)
        self.assertIn("primitive_dimension_coverage", inputs)
        self.assertIn("most_impactful_cards", inputs)
        self.assertIn("top_combo_pairs", inputs)
        self.assertIn("archetype_winners_losers", inputs)

    def test_impactful_cards_ranked_by_composite(self) -> None:
        # Composite score = max_delta + 0.05 * combo_count.
        # Sac Outlet:       0.08 + 0.10 = 0.18 (combo_count=2 in fixture)
        # Persist Creature: 0.14 + 0.00 = 0.14
        # Mana Rock:        0.04 + 0.00 = 0.04
        # Vanilla:          0.00 + 0.00 = 0.00
        ranked = _rank_impactful_cards(_mk_pipeline_data())
        self.assertEqual(ranked[0]["name"], "Sac Outlet")
        self.assertEqual(ranked[1]["name"], "Persist Creature")
        self.assertEqual(ranked[2]["name"], "Mana Rock")

    def test_combo_pairs_ranked_by_confidence(self) -> None:
        ranked = _rank_combo_pairs(_mk_pipeline_data())
        self.assertEqual(ranked[0]["confidence"], 1.0)
        self.assertEqual(ranked[1]["confidence"], 0.7)


class WinnersLosersTests(unittest.TestCase):
    def test_aggregates_per_archetype(self) -> None:
        wl = _archetype_winners_losers(_mk_pipeline_data())
        # Aristocrats gets 0.08 + 0.14 = 0.22 cumulative.
        winners = {w["archetype"]: w for w in wl["winners"]}
        self.assertIn("aristocrats", winners)
        self.assertAlmostEqual(
            winners["aristocrats"]["cumulative_delta"], 0.22, places=2,
        )


class FallbackTests(unittest.TestCase):
    def test_fallback_has_all_5_sections(self) -> None:
        inputs = build_report_inputs("tst", "Test Set", _mk_pipeline_data())
        md = _fallback_markdown(inputs)
        self.assertIn("## Set overview", md)
        self.assertIn("## Most impactful new cards", md)
        self.assertIn("## New combo pairs", md)
        self.assertIn("## Archetype winners and losers", md)
        self.assertIn("## Suggested deck updates", md)

    def test_fallback_references_input_cards(self) -> None:
        inputs = build_report_inputs("tst", "Test Set", _mk_pipeline_data())
        md = _fallback_markdown(inputs)
        self.assertIn("Sac Outlet", md)
        self.assertIn("Persist Creature", md)


class WriteSetReportTests(unittest.TestCase):
    def test_falls_back_when_llm_unavailable(self) -> None:
        client = MagicMock()
        client.is_available.return_value = False
        env = write_set_report(
            "tst", "Test Set", _mk_pipeline_data(),
            llm_client=client,
        )
        self.assertEqual(env.status, "fallback")
        self.assertIn("## Set overview", env.markdown)

    def test_uses_llm_when_available_and_valid_json(self) -> None:
        client = MagicMock()
        client.is_available.return_value = True
        result = MagicMock()
        result.ok = True
        result.parsed_json = {
            "markdown": "## Set overview\n\nMocked LLM report content."
        }
        result.cost_usd = 0.05
        client.call_with_budget.return_value = result
        env = write_set_report(
            "tst", "Test Set", _mk_pipeline_data(),
            llm_client=client,
        )
        self.assertEqual(env.status, "ok")
        self.assertIn("Mocked LLM report content", env.markdown)
        self.assertEqual(env.cost_usd, 0.05)

    def test_falls_back_when_llm_returns_non_json(self) -> None:
        client = MagicMock()
        client.is_available.return_value = True
        result = MagicMock()
        result.ok = True
        result.parsed_json = None   # LLM returned non-JSON text
        result.cost_usd = 0.05
        result.error_code = None
        client.call_with_budget.return_value = result
        env = write_set_report(
            "tst", "Test Set", _mk_pipeline_data(),
            llm_client=client,
        )
        self.assertEqual(env.status, "failed")
        self.assertIn("## Set overview", env.markdown)  # fallback content


if __name__ == "__main__":
    unittest.main()
