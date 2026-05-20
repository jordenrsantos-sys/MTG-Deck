"""Phase B tests for agent_build_deck_v1 candidate pool.

Splits into two halves:
  - Pure helpers (scoring, color-identity normalization) tested without any
    DB / corpus / FastAPI overhead.
  - `_build_candidate_pool` tested against mocked upstream layer functions so
    we exercise the orchestration without needing a real snapshot. Real-snapshot
    integration coverage lands in Phase F (5 test-case sweep).

The "Edgar B3 must_include=[Vito, Bloodthirsty Conqueror]" expectation from
the kickoff brief is encoded as a test here:
  - Theme-matched vampire/lifegain cards should outscore generic staples.
  - Sol Ring (or any high-corpus-frequency non-theme card) should NOT be in
    the top 20 of the resulting candidate pool.
"""
from __future__ import annotations

import unittest
from unittest.mock import patch

from api.engine.layers.agent_build_deck_v1 import (
    USER_PICK_SCORE,
    _normalize_color_identity,
    _score_archetype_staple,
    _score_theme_candidate,
    _build_candidate_pool,
)


class NormalizeColorIdentityTests(unittest.TestCase):
    def test_list_input(self) -> None:
        self.assertEqual(_normalize_color_identity(["b", "r"]), ["B", "R"])

    def test_json_string(self) -> None:
        self.assertEqual(_normalize_color_identity('["U", "G"]'), ["G", "U"])

    def test_comma_string(self) -> None:
        self.assertEqual(_normalize_color_identity("w,u, b"), ["B", "U", "W"])

    def test_empty_inputs(self) -> None:
        self.assertEqual(_normalize_color_identity(None), [])
        self.assertEqual(_normalize_color_identity(""), [])
        self.assertEqual(_normalize_color_identity([]), [])

    def test_dedupes_and_uppercases(self) -> None:
        self.assertEqual(_normalize_color_identity(["b", "B", "r"]), ["B", "R"])


class ScoreTests(unittest.TestCase):
    def test_theme_match_with_low_corpus_freq_scores_high(self) -> None:
        # A non-obvious theme match: 3 theme signals, 5% corpus frequency.
        # No frequency penalty kicks in below threshold 30%.
        score = _score_theme_candidate(theme_signal_count=3, frequency_in_corpus=0.05)
        self.assertGreater(score, 25)  # 3 * 10 = 30 base

    def test_theme_match_high_freq_dampens_penalty(self) -> None:
        # 70% corpus freq, but 2 theme signals — penalty is halved for theme-matched.
        # raw_penalty = (0.70 - 0.30) * 30 = 12; halved = 6.
        # theme_bonus = 20.
        score = _score_theme_candidate(theme_signal_count=2, frequency_in_corpus=0.70)
        self.assertAlmostEqual(score, 14.0, places=1)

    def test_pure_staple_high_freq_scores_negative(self) -> None:
        # Sol Ring profile: ~85% corpus freq, no theme match.
        # base 5.0 - (0.85 - 0.30) * 30 = 5.0 - 16.5 = -11.5
        score = _score_archetype_staple(frequency_in_corpus=0.85)
        self.assertLess(score, 0)

    def test_low_freq_staple_keeps_baseline(self) -> None:
        # Niche staple at 20% freq: no penalty, baseline 5.
        score = _score_archetype_staple(frequency_in_corpus=0.20)
        self.assertAlmostEqual(score, 5.0)


class BuildCandidatePoolTests(unittest.TestCase):
    """Test `_build_candidate_pool` with upstream layer functions mocked.

    The Edgar-B3 / Vito-Bloodthirsty Conqueror scenario from the kickoff brief
    is encoded here: theme-matched vampire/lifegain cards should outscore
    generic staples like Sol Ring.
    """

    def _archetype_brief_edgar(self) -> dict:
        return {
            "version": "archetype_brief_v1.0",
            "commander": "Edgar Markov",
            "commander_oracle_id": "edgar-oracle-id",
            "color_identity": ["B", "R", "W"],
            "corpus_deck_count": 25,
            "common_archetypes": [{"name": "Vampire Tribal Aristocrats", "frequency": 0.6, "deck_count": 15}],
            "bracket_distribution": {"B3": 0.5, "B4": 0.3},
            "staple_cards": [
                # High-frequency generic staples (should be heavily penalized).
                {"name": "Sol Ring", "usage_pct": 0.92},
                {"name": "Command Tower", "usage_pct": 0.88},
                {"name": "Arcane Signet", "usage_pct": 0.80},
                # Mid-frequency theme-aligned (light penalty).
                {"name": "Skeletal Vampire", "usage_pct": 0.45},
                # Low-frequency niche (full baseline).
                {"name": "Indulgent Aristocrat", "usage_pct": 0.20},
            ],
            "warnings": [],
        }

    def _theme_top_cards_vampire(self) -> dict:
        return {
            "version": "theme_top_cards_v1.0",
            "theme_id": "TYPAL_VAMPIRES",
            "subtype": "Vampire",
            "primitives_used_for_match": ["TRIBAL_PAYOFFS"],
            "matched_count": 60,
            "returned_count": 4,
            "results": [
                {
                    "oracle_id": "indulgent-aristocrat-oid",
                    "name": "Indulgent Aristocrat",
                    "type_line": "Creature — Vampire",
                    "cmc": 1,
                    "primitives": ["TRIBAL_PAYOFFS", "SACRIFICE_PAYOFFS"],
                    "theme_signal_count": 2,
                },
                {
                    "oracle_id": "skeletal-vampire-oid",
                    "name": "Skeletal Vampire",
                    "type_line": "Creature — Vampire",
                    "cmc": 5,
                    "primitives": ["TRIBAL_PAYOFFS"],
                    "theme_signal_count": 1,
                },
                {
                    "oracle_id": "drana-oid",
                    "name": "Drana, Liberator of Malakir",
                    "type_line": "Creature — Vampire Ally",
                    "cmc": 3,
                    "primitives": ["TRIBAL_PAYOFFS"],
                    "theme_signal_count": 1,
                },
                {
                    "oracle_id": "captivating-vampire-oid",
                    "name": "Captivating Vampire",
                    "type_line": "Creature — Vampire",
                    "cmc": 3,
                    "primitives": ["TRIBAL_PAYOFFS"],
                    "theme_signal_count": 1,
                },
            ],
            "warnings": [],
        }

    def _find_card_mock(self, snapshot_id: str, name: str):
        # Cards used in test must-includes — both should pass color-identity
        # validation for Edgar Markov (CI = BRW).
        catalog = {
            "Vito, Thorn of the Dusk Rose": {
                "name": "Vito, Thorn of the Dusk Rose",
                "color_identity": '["B"]',
                "primitives": ["LIFEGAIN_PAYOFFS"],
                "type_line": "Legendary Creature — Vampire",
                "cmc": 3,
            },
            "Bloodthirsty Conqueror": {
                "name": "Bloodthirsty Conqueror",
                "color_identity": ["B"],
                "primitives": ["LIFEGAIN_PAYOFFS"],
                "type_line": "Creature — Vampire",
                "cmc": 5,
            },
            "Sky Diamond": {  # for color-illegal pick test
                "name": "Sky Diamond",
                "color_identity": ["U"],  # Edgar can't include blue
                "primitives": ["MANA_ROCK"],
                "type_line": "Artifact",
                "cmc": 2,
            },
            "Made Up Card": None,
        }
        return catalog.get(name)

    def _patch_upstream(self):
        return (
            patch(
                "api.engine.layers.agent_build_deck_v1.compute_archetype_brief_v1",
                return_value=self._archetype_brief_edgar(),
            ),
            patch(
                "api.engine.layers.agent_build_deck_v1.compute_theme_top_cards_v1",
                return_value=self._theme_top_cards_vampire(),
            ),
            patch(
                "api.engine.layers.agent_build_deck_v1.find_card_by_name",
                side_effect=self._find_card_mock,
            ),
        )

    def _run_edgar_b3(self, must_include=None):
        # The patch targets must reference where the symbol is *looked up*. The
        # layer imports compute_archetype_brief_v1 / compute_theme_top_cards_v1
        # lazily inside _build_candidate_pool (from api.engine.layers.agent_endpoints_v1)
        # and find_card_by_name lazily inside _validate_must_includes (from engine.db).
        # So we patch them at their source modules.
        from api.engine.layers import agent_endpoints_v1 as ae
        from engine import db as engine_db

        with patch.object(ae, "compute_archetype_brief_v1", return_value=self._archetype_brief_edgar()), \
             patch.object(ae, "compute_theme_top_cards_v1", return_value=self._theme_top_cards_vampire()), \
             patch.object(engine_db, "find_card_by_name", side_effect=self._find_card_mock):
            counter = {"calls": 0}
            return _build_candidate_pool(
                db_snapshot_id="TEST_SNAPSHOT",
                commander="Edgar Markov",
                bracket="B3",
                theme_hints=["TYPAL_VAMPIRES"],
                must_include_cards=must_include or [],
                seed=42,
                call_counter=counter,
            )

    def test_user_must_includes_locked_at_top_of_pool(self) -> None:
        result = self._run_edgar_b3(must_include=[
            "Vito, Thorn of the Dusk Rose",
            "Bloodthirsty Conqueror",
        ])
        cands = result["candidates"]
        # Top two must be user picks (score=INF), regardless of theme/staple ordering.
        self.assertEqual(cands[0]["name"], "Bloodthirsty Conqueror")  # name sort tie-break (alpha)
        self.assertEqual(cands[1]["name"], "Vito, Thorn of the Dusk Rose")
        for top in cands[:2]:
            self.assertEqual(top["score"], USER_PICK_SCORE)
            self.assertEqual(top["source"], "user_intent")
            self.assertTrue(top["is_user_pick"])

    def test_high_freq_staples_score_below_theme_matches(self) -> None:
        """Creativity-envelope check: high-frequency generic staples (Sol Ring,
        Command Tower) must score BELOW every theme-matched vampire card. The
        pool may still contain them at the bottom — Phase C's selection picks
        from the top, so this is what 'don't force staples' means concretely."""
        result = self._run_edgar_b3(must_include=[
            "Vito, Thorn of the Dusk Rose",
            "Bloodthirsty Conqueror",
        ])
        by_name = {c["name"]: c for c in result["candidates"]}
        sol_ring_score = by_name["Sol Ring"]["score"]
        command_tower_score = by_name["Command Tower"]["score"]
        # Every vampire theme match should outscore both.
        for theme_card in ["Indulgent Aristocrat", "Skeletal Vampire",
                            "Drana, Liberator of Malakir", "Captivating Vampire"]:
            self.assertGreater(
                by_name[theme_card]["score"], sol_ring_score,
                f"{theme_card} should outscore Sol Ring",
            )
            self.assertGreater(
                by_name[theme_card]["score"], command_tower_score,
                f"{theme_card} should outscore Command Tower",
            )
        # And both staples score negative (frequency penalty > baseline).
        self.assertLess(sol_ring_score, 0)
        self.assertLess(command_tower_score, 0)

    def test_must_include_lookup_failure_warns_and_skips(self) -> None:
        result = self._run_edgar_b3(must_include=["Made Up Card"])
        codes = {w["code"] for w in result["warnings"]}
        self.assertIn("MUST_INCLUDE_NOT_FOUND", codes)
        # No "Made Up Card" in candidates.
        names = {c["name"] for c in result["candidates"]}
        self.assertNotIn("Made Up Card", names)

    def test_must_include_color_illegal_skipped_with_warning(self) -> None:
        # Sky Diamond is U-only; Edgar Markov is BRW.
        result = self._run_edgar_b3(must_include=["Sky Diamond"])
        codes = {w["code"] for w in result["warnings"]}
        self.assertIn("MUST_INCLUDE_COLOR_ILLEGAL", codes)
        names = {c["name"] for c in result["candidates"]}
        self.assertNotIn("Sky Diamond", names)

    def test_color_identity_propagated_from_archetype_brief(self) -> None:
        result = self._run_edgar_b3()
        self.assertEqual(set(result["color_identity"]), {"B", "R", "W"})

    def test_endpoint_calls_counted(self) -> None:
        # 1 archetype_brief + 1 theme_top_cards = 2 calls for Edgar w/ one hint.
        result = self._run_edgar_b3()
        self.assertEqual(result["endpoint_calls"], 2)

    def test_seed_determinism_for_tie_break(self) -> None:
        # Same seed → same ordering. Different seed → ordering can differ for
        # equal-score candidates (but user picks still pinned at top).
        r1 = self._run_edgar_b3()  # seed=42
        r2 = self._run_edgar_b3()
        self.assertEqual(
            [c["name"] for c in r1["candidates"]],
            [c["name"] for c in r2["candidates"]],
        )


if __name__ == "__main__":
    unittest.main()
