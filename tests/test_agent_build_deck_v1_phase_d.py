"""Phase D tests for agent_build_deck_v1 validation + swap-iteration loop.

Three groups:
  - `_deck_to_raw_text` — pure serializer.
  - `_compute_theme_coherence` — pure scoring; runs the brief's coherence
    formula (fraction of requested hints that matched classified themes).
  - `_validate_deck` and `_validate_and_iterate` — orchestration tested
    against mocked deck_analyze_v1 + deck_strength_check_v1 calls.

The 12-iteration inner cap and the 30-call endpoint budget (Fix 4 + Fix 5
from the kickoff patch) get their own assertions here so they can't drift
silently.
"""
from __future__ import annotations

import unittest
from unittest.mock import patch

from api.engine.layers.agent_build_deck_v1 import (
    ENDPOINT_CALL_BUDGET,
    MAX_SWAP_ITERATIONS,
    THEME_COHERENCE_TARGET,
    _compute_theme_coherence,
    _deck_to_raw_text,
    _validate_and_iterate,
    _validate_deck,
)


class DeckToRawTextTests(unittest.TestCase):
    def test_commander_block_then_deck_block(self) -> None:
        deck_body = [{"card_name": "Plains"}, {"card_name": "Sol Ring"}]
        text = _deck_to_raw_text("Edgar Markov", deck_body)
        self.assertIn("Commander\n1 Edgar Markov", text)
        self.assertIn("Deck\n1 Plains\n1 Sol Ring", text)

    def test_empty_body(self) -> None:
        text = _deck_to_raw_text("Edgar Markov", [])
        self.assertEqual(text, "Commander\n1 Edgar Markov\nDeck")


class ComputeThemeCoherenceTests(unittest.TestCase):
    def test_no_hints_is_trivially_coherent(self) -> None:
        self.assertEqual(_compute_theme_coherence([], []), 1.0)

    def test_all_hints_match(self) -> None:
        classified = [
            {"theme_id": "TYPAL_VAMPIRES", "name": "Vampire Tribal"},
            {"theme_id": "THEME_LIFEGAIN"},
        ]
        score = _compute_theme_coherence(["TYPAL_VAMPIRES", "THEME_LIFEGAIN"], classified)
        self.assertEqual(score, 1.0)

    def test_partial_match(self) -> None:
        classified = [{"theme_id": "TYPAL_VAMPIRES"}]
        score = _compute_theme_coherence(["TYPAL_VAMPIRES", "THEME_LIFEGAIN"], classified)
        self.assertEqual(score, 0.5)

    def test_no_classified_themes(self) -> None:
        self.assertEqual(_compute_theme_coherence(["TYPAL_VAMPIRES"], []), 0.0)

    def test_case_insensitive_substring_match(self) -> None:
        # Classifier returned typal_id with colon-suffixed subtype; hint is the
        # base id without the suffix.
        classified = [{"theme_id": "TYPAL_VAMPIRES:Vampire"}]
        self.assertEqual(_compute_theme_coherence(["typal_vampires"], classified), 1.0)


def _make_deck_100(commander="Edgar Markov") -> list:
    """Build a synthetic 100-card deck (commander + 99 unique stand-ins)."""
    deck = [{"card_name": commander, "reason": "Cmdr.", "source": "user_intent"}]
    for i in range(99):
        deck.append({"card_name": f"Card {i:02d}",
                     "reason": "filler",
                     "source": "agent_select"})
    return deck


def _analyze_stub(themes_classified=None, bracket_estimate=None):
    return {
        "version": "analyze_v1.0",
        "card_count": 100,
        "color_identity": ["B", "R", "W"],
        "deck_themes_v1": themes_classified or [],
        "bracket_estimate": bracket_estimate or {"bracket": "B3"},
        "warnings": [],
    }


def _strength_check_stub():
    return {
        "version": "strength_check_v1.3",
        "measurement_a": {
            "bracket_signal": "B3",
            "mean_similarity": 0.62,
            "nearest_neighbors": [{"corpus_id": "x"}, {"corpus_id": "y"}],
        },
        "warnings": [],
    }


class ValidateDeckTests(unittest.TestCase):
    def _patch_analyze(self, **kwargs):
        from api.engine.layers import deck_analyze_v1 as da
        return patch.object(
            da, "compute_deck_analyze_v1", return_value=_analyze_stub(**kwargs)
        )

    def _patch_strength(self):
        from api.engine.layers import deck_strength_check_v1 as sc
        return patch.object(
            sc, "compute_deck_strength_check_v1", return_value=_strength_check_stub()
        )

    def test_passing_deck_no_issues(self) -> None:
        deck = _make_deck_100()
        with self._patch_analyze(
            themes_classified=[{"theme_id": "TYPAL_VAMPIRES"}],
            bracket_estimate={"bracket": "B3"},
        ), self._patch_strength():
            findings = _validate_deck(
                deck=deck, commander="Edgar Markov", bracket="B3",
                theme_hints=["TYPAL_VAMPIRES"],
                db_snapshot_id="TEST", call_counter={"calls": 0},
            )
        self.assertEqual(findings["issues"], [])
        self.assertEqual(findings["theme_coherence_score"], 1.0)
        self.assertEqual(findings["endpoint_calls_made"], 2)

    def test_wrong_card_count_flagged(self) -> None:
        deck = _make_deck_100()[:50]
        with self._patch_analyze(), self._patch_strength():
            findings = _validate_deck(
                deck=deck, commander="Edgar Markov", bracket="B3",
                theme_hints=[], db_snapshot_id="TEST", call_counter={"calls": 0},
            )
        codes = {i["code"] for i in findings["issues"]}
        self.assertIn("DECK_SIZE_WRONG", codes)

    def test_singleton_violation_flagged(self) -> None:
        deck = _make_deck_100()
        deck[5] = {"card_name": "Card 03", "reason": "dup", "source": "x"}  # duplicate of Card 03
        with self._patch_analyze(), self._patch_strength():
            findings = _validate_deck(
                deck=deck, commander="Edgar Markov", bracket="B3",
                theme_hints=[], db_snapshot_id="TEST", call_counter={"calls": 0},
            )
        codes = {i["code"] for i in findings["issues"]}
        self.assertIn("SINGLETON_VIOLATION", codes)
        # Basics shouldn't trip singleton.
        deck[5] = {"card_name": "Plains", "reason": "basic", "source": "mana_base"}
        deck[6] = {"card_name": "Plains", "reason": "basic", "source": "mana_base"}
        with self._patch_analyze(), self._patch_strength():
            findings = _validate_deck(
                deck=deck, commander="Edgar Markov", bracket="B3",
                theme_hints=[], db_snapshot_id="TEST", call_counter={"calls": 0},
            )
        # No SINGLETON_VIOLATION for duplicate Plains.
        codes = {i["code"] for i in findings["issues"]}
        self.assertNotIn("SINGLETON_VIOLATION", codes)

    def test_theme_coherence_low_flagged(self) -> None:
        deck = _make_deck_100()
        with self._patch_analyze(
            themes_classified=[{"theme_id": "THEME_RAMP"}],  # not what was asked
            bracket_estimate={"bracket": "B3"},
        ), self._patch_strength():
            findings = _validate_deck(
                deck=deck, commander="Edgar Markov", bracket="B3",
                theme_hints=["TYPAL_VAMPIRES"],
                db_snapshot_id="TEST", call_counter={"calls": 0},
            )
        codes = {i["code"] for i in findings["issues"]}
        self.assertIn("THEME_COHERENCE_LOW", codes)
        self.assertLess(findings["theme_coherence_score"], THEME_COHERENCE_TARGET)

    def test_bracket_mismatch_flagged(self) -> None:
        deck = _make_deck_100()
        with self._patch_analyze(
            themes_classified=[],
            bracket_estimate={"bracket": "B5"},  # estimated higher than requested
        ), self._patch_strength():
            findings = _validate_deck(
                deck=deck, commander="Edgar Markov", bracket="B3",
                theme_hints=[], db_snapshot_id="TEST", call_counter={"calls": 0},
            )
        codes = {i["code"] for i in findings["issues"]}
        self.assertIn("BRACKET_MISMATCH", codes)
        offending = [i for i in findings["issues"] if i["code"] == "BRACKET_MISMATCH"][0]
        self.assertEqual(offending["estimated_bracket"], "B5")
        self.assertEqual(offending["requested_bracket"], "B3")

    def test_call_counter_increments(self) -> None:
        deck = _make_deck_100()
        counter = {"calls": 5}
        with self._patch_analyze(), self._patch_strength():
            _validate_deck(
                deck=deck, commander="Edgar Markov", bracket="B3",
                theme_hints=[], db_snapshot_id="TEST", call_counter=counter,
            )
        self.assertEqual(counter["calls"], 7)  # 5 + 1 analyze + 1 strength

    def test_budget_exceeded_short_circuits_analyze(self) -> None:
        deck = _make_deck_100()
        # Counter already at budget — analyze must not be called.
        counter = {"calls": ENDPOINT_CALL_BUDGET}
        with self._patch_analyze() as p_an, self._patch_strength() as p_sc:
            findings = _validate_deck(
                deck=deck, commander="Edgar Markov", bracket="B3",
                theme_hints=[], db_snapshot_id="TEST", call_counter=counter,
            )
        # Analyze wasn't called.
        p_an.assert_not_called()
        p_sc.assert_not_called()
        codes = {i["code"] for i in findings["issues"]}
        self.assertIn("BUDGET_EXCEEDED_BEFORE_ANALYZE", codes)


class ValidateAndIterateTests(unittest.TestCase):
    """Outer loop behavior: ≤12 iterations, ≤30 calls, swap on each failed pass."""

    def test_clean_deck_short_circuits_to_one_pass(self) -> None:
        deck = _make_deck_100()
        pool = {
            "candidates": [],
            "color_identity": ["B", "R", "W"],
            "archetype_brief": {},
        }
        from api.engine.layers import deck_analyze_v1 as da
        from api.engine.layers import deck_strength_check_v1 as sc
        with patch.object(da, "compute_deck_analyze_v1", return_value=_analyze_stub(
            themes_classified=[{"theme_id": "TYPAL_VAMPIRES"}],
            bracket_estimate={"bracket": "B3"},
        )), patch.object(sc, "compute_deck_strength_check_v1", return_value=_strength_check_stub()):
            counter = {"calls": 0}
            final_deck, findings, warnings = _validate_and_iterate(
                deck=deck, pool=pool, commander="Edgar Markov", bracket="B3",
                theme_hints=["TYPAL_VAMPIRES"],
                db_snapshot_id="TEST", call_counter=counter,
            )
        self.assertEqual(findings["issues"], [])
        self.assertEqual(counter["calls"], 2)
        # No SWAP_ITERATION warnings on a clean deck.
        codes = {w["code"] for w in warnings}
        self.assertNotIn("SWAP_ITERATION", codes)

    def test_singleton_violation_swapped_with_pool_candidate(self) -> None:
        deck = _make_deck_100()
        deck[5] = {"card_name": "Card 03", "reason": "dup", "source": "x"}  # dup
        pool = {
            "candidates": [
                {
                    "name": "Brand New Card",
                    "score": 5.0,
                    "source": "theme",
                    "rationale_components": ["Replacement for duplicate."],
                    "is_user_pick": False,
                },
            ],
            "color_identity": ["B", "R", "W"],
            "archetype_brief": {},
        }
        from api.engine.layers import deck_analyze_v1 as da
        from api.engine.layers import deck_strength_check_v1 as sc

        call_log = {"analyze": 0}

        def analyze_side_effect(**kwargs):
            call_log["analyze"] += 1
            # After the first call, the swap should have happened; report no
            # singleton violation on the second call.
            return _analyze_stub(themes_classified=[{"theme_id": "X"}], bracket_estimate={"bracket": "B3"})

        with patch.object(da, "compute_deck_analyze_v1", side_effect=analyze_side_effect), \
             patch.object(sc, "compute_deck_strength_check_v1", return_value=_strength_check_stub()):
            counter = {"calls": 0}
            final_deck, findings, warnings = _validate_and_iterate(
                deck=deck, pool=pool, commander="Edgar Markov", bracket="B3",
                theme_hints=[],  # no theme hints → no THEME_COHERENCE_LOW
                db_snapshot_id="TEST", call_counter=counter,
            )

        # Verify swap happened.
        codes = {w["code"] for w in warnings}
        self.assertIn("SWAP_ITERATION", codes)
        # The replacement is in the deck; the dup-instance was replaced.
        names = [c["card_name"] for c in final_deck]
        self.assertIn("Brand New Card", names)
        self.assertEqual(names.count("Card 03"), 1)

    def test_iteration_cap_at_12_for_persistent_failure(self) -> None:
        # Force the analyze call to always report a low theme coherence with
        # no possible swap — outer loop must bail (no actionable swap), NOT
        # exhaust the 12-iter cap.
        deck = _make_deck_100()
        pool = {
            "candidates": [],  # empty → no swap candidates available
            "color_identity": ["B", "R", "W"],
            "archetype_brief": {},
        }
        from api.engine.layers import deck_analyze_v1 as da
        from api.engine.layers import deck_strength_check_v1 as sc

        analyze_call_count = {"n": 0}

        def stuck_analyze(**kw):
            analyze_call_count["n"] += 1
            return _analyze_stub(
                themes_classified=[{"theme_id": "OTHER"}],  # never matches
                bracket_estimate={"bracket": "B3"},
            )

        with patch.object(da, "compute_deck_analyze_v1", side_effect=stuck_analyze), \
             patch.object(sc, "compute_deck_strength_check_v1", return_value=_strength_check_stub()):
            counter = {"calls": 0}
            _, _, warnings = _validate_and_iterate(
                deck=deck, pool=pool, commander="Edgar Markov", bracket="B3",
                theme_hints=["TYPAL_VAMPIRES"],
                db_snapshot_id="TEST", call_counter=counter,
            )

        # The loop should exit after the first validate finds a non-swappable
        # issue (THEME_COHERENCE_LOW with empty pool → no replacement).
        # That's ONE analyze + ONE strength call, then bail.
        self.assertLessEqual(analyze_call_count["n"], MAX_SWAP_ITERATIONS)
        # UNRESOLVED_* warnings should be emitted.
        codes = {w["code"] for w in warnings}
        self.assertTrue(any(c.startswith("UNRESOLVED_") for c in codes),
                        f"want some UNRESOLVED_* warning, got {codes}")

    def test_budget_exceeded_halts_loop(self) -> None:
        deck = _make_deck_100()
        pool = {
            "candidates": [],
            "color_identity": ["B", "R", "W"],
            "archetype_brief": {},
        }
        from api.engine.layers import deck_analyze_v1 as da
        from api.engine.layers import deck_strength_check_v1 as sc
        with patch.object(da, "compute_deck_analyze_v1", return_value=_analyze_stub()), \
             patch.object(sc, "compute_deck_strength_check_v1", return_value=_strength_check_stub()):
            counter = {"calls": ENDPOINT_CALL_BUDGET}  # already exhausted
            _, _, warnings = _validate_and_iterate(
                deck=deck, pool=pool, commander="Edgar Markov", bracket="B3",
                theme_hints=[], db_snapshot_id="TEST", call_counter=counter,
            )
        codes = {w["code"] for w in warnings}
        self.assertIn("ENDPOINT_BUDGET_EXCEEDED", codes)


if __name__ == "__main__":
    unittest.main()
