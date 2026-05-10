"""Tests for tools/playtest/aggregator.py (Phase 5b stage 5b.8)."""
from __future__ import annotations

import unittest

from tools.playtest.aggregator import (
    AGGREGATOR_V1_VERSION,
    BRACKET_ORDER,
    CATEGORY_ENGINE_DISAGREES,
    CATEGORY_PLAYTEST_DISAGREES,
    CATEGORY_SELF_MISLABEL_CONFIRMED,
    CATEGORY_TRIPLE_AGREE,
    CATEGORY_TRIPLE_DISAGREE,
    aggregate_per_deck,
    build_drift_report_rows,
    category_counts,
    categorize_three_way,
    derive_playtest_bracket,
)


def _record(*, test_deck_id: str, target_bracket: str, win: bool, turns: int = 10,
            win_condition_type: str = "combat") -> dict:
    return {
        "test_deck_id": test_deck_id,
        "target_bracket": target_bracket,
        "winner_is_test_deck": win,
        "turns_to_resolution": turns,
        "win_condition_used": {"type": win_condition_type} if win else None,
    }


class AggregatorPerDeckTests(unittest.TestCase):
    def test_empty_records_returns_zero_stats(self) -> None:
        s = aggregate_per_deck("DECK_A", [])
        self.assertEqual(s["n_games"], 0)
        self.assertEqual(s["win_rate_overall"], 0.0)
        for b in BRACKET_ORDER:
            self.assertIsNone(s["win_rate_vs_bracket"][b])

    def test_aggregates_win_rate_overall_and_per_bracket(self) -> None:
        recs = [
            _record(test_deck_id="A", target_bracket="B3", win=True),
            _record(test_deck_id="A", target_bracket="B3", win=False),
            _record(test_deck_id="A", target_bracket="B5", win=False),
            _record(test_deck_id="A", target_bracket="B5", win=False),
        ]
        s = aggregate_per_deck("A", recs)
        self.assertEqual(s["n_games"], 4)
        self.assertEqual(s["win_rate_overall"], 0.25)
        self.assertEqual(s["win_rate_vs_bracket"]["B3"], 0.5)
        self.assertEqual(s["win_rate_vs_bracket"]["B5"], 0.0)

    def test_filters_records_by_deck_id(self) -> None:
        recs = [
            _record(test_deck_id="A", target_bracket="B3", win=True),
            _record(test_deck_id="B", target_bracket="B3", win=True),
        ]
        s = aggregate_per_deck("A", recs)
        self.assertEqual(s["n_games"], 1)


class AggregatorPlaytestBracketDerivationTests(unittest.TestCase):
    def test_derive_returns_bracket_closest_to_target_win_rate(self) -> None:
        # 25% target. B3=0.7 (far high), B4=0.25 (exact), B5=0.05 (far low).
        # B4 should win.
        rates = {"B1": None, "B2": None, "B3": 0.7, "B4": 0.25, "B5": 0.05}
        result = derive_playtest_bracket(rates)
        self.assertEqual(result, "B4")

    def test_derive_returns_none_when_all_brackets_none(self) -> None:
        rates = {b: None for b in BRACKET_ORDER}
        self.assertIsNone(derive_playtest_bracket(rates))


class AggregatorCategorizationTests(unittest.TestCase):
    def test_triple_agree(self) -> None:
        cat = categorize_three_way(self_reported="B3", engine_assigned="B3", playtest_bracket="B3")
        self.assertEqual(cat, CATEGORY_TRIPLE_AGREE)

    def test_engine_disagrees(self) -> None:
        # self == playtest != engine
        cat = categorize_three_way(self_reported="B3", engine_assigned="B2", playtest_bracket="B3")
        self.assertEqual(cat, CATEGORY_ENGINE_DISAGREES)

    def test_self_mislabel_confirmed(self) -> None:
        # engine == playtest != self
        cat = categorize_three_way(self_reported="B5", engine_assigned="B4", playtest_bracket="B4")
        self.assertEqual(cat, CATEGORY_SELF_MISLABEL_CONFIRMED)

    def test_playtest_disagrees(self) -> None:
        # engine == self != playtest
        cat = categorize_three_way(self_reported="B3", engine_assigned="B3", playtest_bracket="B5")
        self.assertEqual(cat, CATEGORY_PLAYTEST_DISAGREES)

    def test_triple_disagree(self) -> None:
        cat = categorize_three_way(self_reported="B1", engine_assigned="B3", playtest_bracket="B5")
        self.assertEqual(cat, CATEGORY_TRIPLE_DISAGREE)

    def test_none_inputs_default_to_triple_disagree(self) -> None:
        cat = categorize_three_way(self_reported=None, engine_assigned="B3", playtest_bracket="B3")
        self.assertEqual(cat, CATEGORY_TRIPLE_DISAGREE)


class AggregatorDriftReportTests(unittest.TestCase):
    def test_build_drift_report_rows_returns_extended_row_per_deck(self) -> None:
        summaries = [
            {
                "deck_id": "A",
                "n_games": 100,
                "win_rate_overall": 0.3,
                "win_rate_vs_bracket": {
                    "B1": None, "B2": 0.5, "B3": 0.25, "B4": 0.1, "B5": 0.0,
                },
            },
        ]
        self_by_deck = {"A": "B3"}
        engine_by_deck = {"A": "B3"}
        rows = build_drift_report_rows(
            per_deck_summaries=summaries,
            self_reported_by_deck=self_by_deck,
            engine_assigned_by_deck=engine_by_deck,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["deck_id"], "A")
        # B3 win_rate=0.25 = exactly target -> playtest_bracket=B3.
        self.assertEqual(rows[0]["playtest_bracket"], "B3")
        # Triple-agree.
        self.assertEqual(rows[0]["category"], CATEGORY_TRIPLE_AGREE)

    def test_category_counts(self) -> None:
        rows = [
            {"category": CATEGORY_TRIPLE_AGREE},
            {"category": CATEGORY_TRIPLE_AGREE},
            {"category": CATEGORY_ENGINE_DISAGREES},
        ]
        counts = category_counts(rows)
        self.assertEqual(counts[CATEGORY_TRIPLE_AGREE], 2)
        self.assertEqual(counts[CATEGORY_ENGINE_DISAGREES], 1)


if __name__ == "__main__":
    unittest.main()
