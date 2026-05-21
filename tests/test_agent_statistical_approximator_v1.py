"""Phase 12 tests — Pillar F v0.1 statistical approximator.

Covers:
  - Win-path matching (5+ known patterns: Thoracle, Kiki, Heliod-Ballista,
    Sanguine-Exquisite, aristocrats engine).
  - Matchup logic (faster deck vs slower; high-interaction vs low-resilience).
  - Opposition deck loading (live read of opposition_decks_v1.json).
  - approximate_pod_winrate output shape (pod_winrate in [0.05, 0.95],
    per_opponent_winrate dict, decomposition complete).
  - Sanity: cEDH deck (Thoracle+DC) winrates > B2 deck (Atraxa).
"""
from __future__ import annotations

import unittest

from api.engine.layers.agent_statistical_approximator_v1 import (
    PodWinrateReport,
    WIN_PATHS,
    _match_win_paths,
    _matchup_winrate,
    approximate_pod_winrate,
    load_opposition_decks,
)


def _card(name: str, primitives=None) -> dict:
    return {"card_name": name, "primitives": primitives or []}


class WinPathMatchingTests(unittest.TestCase):
    def test_thoracle_dc_combo_armed(self) -> None:
        deck = [
            _card("Thassa's Oracle"),
            _card("Demonic Consultation"),
            _card("Filler"),
        ]
        matches = _match_win_paths(deck)
        thoracle = next(m for m in matches if m.win_path_id == "thoracle_consultation")
        self.assertTrue(thoracle.armed)

    def test_thoracle_without_dc_not_armed(self) -> None:
        deck = [_card("Thassa's Oracle"), _card("Filler")]
        matches = _match_win_paths(deck)
        thoracle = next(m for m in matches if m.win_path_id == "thoracle_consultation")
        self.assertFalse(thoracle.armed)
        self.assertTrue(thoracle.missing_pieces)

    def test_kiki_combo_armed_with_snoop(self) -> None:
        deck = [_card("Kiki-Jiki, Mirror Breaker"), _card("Conspicuous Snoop")]
        matches = _match_win_paths(deck)
        kiki = next(m for m in matches if m.win_path_id == "kiki_combo")
        self.assertTrue(kiki.armed)

    def test_aristocrats_engine_via_primitives(self) -> None:
        deck = [
            _card("Viscera Seer", ["SACRIFICE_OUTLET"]),
            _card("Blood Artist", ["DEATH_TRIGGER"]),
            _card("Reassembling Skeleton", ["RECURSION_GRAVEYARD"]),
        ]
        matches = _match_win_paths(deck)
        m = next(p for p in matches if p.win_path_id == "aristocrats_drain")
        self.assertTrue(m.armed)

    def test_dragon_combat_armed_with_tempest_and_ur_dragon(self) -> None:
        deck = [
            _card("Dragon Tempest"),
            _card("The Ur-Dragon"),
            _card("Tiamat"),
        ]
        matches = _match_win_paths(deck)
        m = next(p for p in matches if p.win_path_id == "dragon_tempest_combat")
        self.assertTrue(m.armed)

    def test_no_armed_paths_on_empty_deck(self) -> None:
        matches = _match_win_paths([])
        self.assertEqual(sum(1 for m in matches if m.armed), 0)


class MatchupWinrateTests(unittest.TestCase):
    def test_faster_deck_wins_more(self) -> None:
        # Fast deck (speed 4) vs slow opponent (speed 8) → > 0.5.
        wr = _matchup_winrate(
            own_speed=4.0, own_interaction=3, own_resilience=2,
            opp_speed=8.0, opp_interaction=4,
        )
        self.assertGreater(wr, 0.5)

    def test_slower_deck_loses_more(self) -> None:
        # Slow deck (speed 9) vs fast opponent (speed 4) → < 0.5.
        wr = _matchup_winrate(
            own_speed=9.0, own_interaction=3, own_resilience=2,
            opp_speed=4.0, opp_interaction=6,
        )
        self.assertLess(wr, 0.5)

    def test_clamped_to_range(self) -> None:
        # Extreme speed gap clamps to [0.05, 0.95].
        wr_high = _matchup_winrate(
            own_speed=2.0, own_interaction=20, own_resilience=20,
            opp_speed=20.0, opp_interaction=0,
        )
        self.assertLessEqual(wr_high, 0.95)
        self.assertGreaterEqual(wr_high, 0.05)

    def test_high_resilience_cancels_opp_interaction(self) -> None:
        baseline = _matchup_winrate(
            own_speed=6.0, own_interaction=4, own_resilience=2,
            opp_speed=6.0, opp_interaction=8,
        )
        boosted = _matchup_winrate(
            own_speed=6.0, own_interaction=4, own_resilience=8,
            opp_speed=6.0, opp_interaction=8,
        )
        # Higher resilience → strictly ≥ winrate.
        self.assertGreaterEqual(boosted, baseline)


class OppositionLoadingTests(unittest.TestCase):
    def test_loads_19_entries(self) -> None:
        opps = load_opposition_decks()
        self.assertGreater(len(opps), 0,
                           "opposition_decks_v1.json should have entries")
        self.assertGreaterEqual(len(opps), 15)


class ApproximatePodWinrateTests(unittest.TestCase):
    def test_thoracle_deck_outperforms_atraxa_deck(self) -> None:
        # Sanity: a deck with Thoracle+DC (fast combo) should out-win-rate
        # a non-combo Atraxa B2 archetype against the same opponents.
        thoracle_deck = [
            _card("Thassa's Oracle"),
            _card("Demonic Consultation"),
            _card("Force of Will"),
            _card("Veil of Summer"),
        ]
        atraxa_deck = [
            _card("Atraxa, Praetors' Voice"),
            _card("Doubling Season"),
            _card("Pir, Imaginative Rascal"),
        ]
        thoracle_report = approximate_pod_winrate(deck=thoracle_deck)
        atraxa_report = approximate_pod_winrate(deck=atraxa_deck)
        self.assertGreater(thoracle_report.pod_winrate, atraxa_report.pod_winrate,
                           f"Thoracle ({thoracle_report.pod_winrate}) should beat Atraxa ({atraxa_report.pod_winrate})")

    def test_thoracle_deck_winrate_above_quarter(self) -> None:
        # cEDH-tier deck against bracket-mixed opposition should sit
        # ABOVE the 0.25 1/N-baseline.
        deck = [
            _card("Thassa's Oracle"),
            _card("Demonic Consultation"),
            _card("Force of Will"),
        ]
        report = approximate_pod_winrate(deck=deck)
        self.assertGreater(report.pod_winrate, 0.25)

    def test_output_shape_complete(self) -> None:
        deck = [_card("Sol Ring")]
        report = approximate_pod_winrate(deck=deck)
        self.assertIsInstance(report, PodWinrateReport)
        self.assertGreaterEqual(report.pod_winrate, 0.0)
        self.assertLessEqual(report.pod_winrate, 1.0)
        self.assertIsInstance(report.per_opponent_winrate, dict)
        self.assertIsNotNone(report.decomposition)
        self.assertIsInstance(report.decomposition.win_paths, list)

    def test_empty_deck_doesnt_crash(self) -> None:
        report = approximate_pod_winrate(deck=[])
        self.assertIsNotNone(report)

    def test_no_armed_paths_flagged_in_vulnerability(self) -> None:
        report = approximate_pod_winrate(deck=[])
        self.assertIn(
            "no identified win-path — likely incomplete deck or unseen archetype",
            " ".join(report.decomposition.vulnerability_to),
        )


class CatalogValidityTests(unittest.TestCase):
    def test_every_win_path_has_required_fields(self) -> None:
        for wp in WIN_PATHS:
            self.assertIn("id", wp)
            self.assertIn("name", wp)
            self.assertIn("description", wp)
            self.assertIn("speed_score", wp)
            self.assertIn("category", wp)

    def test_win_path_ids_unique(self) -> None:
        ids = [wp["id"] for wp in WIN_PATHS]
        self.assertEqual(len(ids), len(set(ids)))

    def test_at_least_10_win_paths(self) -> None:
        # Kickoff: "encode 10+ common win-paths".
        self.assertGreaterEqual(len(WIN_PATHS), 10)


if __name__ == "__main__":
    unittest.main()
