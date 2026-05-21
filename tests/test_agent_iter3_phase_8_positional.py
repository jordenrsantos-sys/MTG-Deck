"""Iter 3 Phase 8 tests — positional context engineering for C2.1.

Tests the new helpers:
  - `_primitive_tag_hint(primitives)` — maps a primitive list to a
    short tag like "ramp-mana", "draw-engine", "sac-outlet".
  - `_compute_positional_context(candidate, deck_in_context, pool)` —
    returns interacts_with_in_deck, pairs_with_not_yet_picked,
    primitive_tag_hint.
  - `_build_candidate_critic_user_prompt(..., deck_primitive_index=...)`
    — includes positional context block when the index is provided.
"""
from __future__ import annotations

import unittest

from api.engine.layers.agent_build_deck_v1 import (
    _build_candidate_critic_user_prompt,
    _compute_positional_context,
    _primitive_tag_hint,
)


class PrimitiveTagHintTests(unittest.TestCase):
    def test_mana_rock_to_ramp_mana(self) -> None:
        self.assertEqual(_primitive_tag_hint(["MANA_ROCK"]), "ramp-mana")

    def test_card_draw_burst_to_draw_burst(self) -> None:
        self.assertEqual(_primitive_tag_hint(["CARD_DRAW_BURST"]), "draw-burst")

    def test_card_draw_repeatable_to_draw_engine(self) -> None:
        self.assertEqual(_primitive_tag_hint(["CARD_DRAW_REPEATABLE"]), "draw-engine")

    def test_counterspell_to_counterspell(self) -> None:
        self.assertEqual(_primitive_tag_hint(["COUNTERSPELL_GENERIC"]), "counterspell")

    def test_boardwipe_to_removal_mass(self) -> None:
        self.assertEqual(_primitive_tag_hint(["BOARDWIPE_CREATURES"]), "removal-mass")

    def test_sac_outlet_to_sac_outlet(self) -> None:
        self.assertEqual(_primitive_tag_hint(["SACRIFICE_OUTLET"]), "sac-outlet")

    def test_wincon_combo_to_wincon_combo(self) -> None:
        self.assertEqual(_primitive_tag_hint(["WINCON_COMBO"]), "wincon-combo")

    def test_tribal_anchor_via_typal(self) -> None:
        self.assertEqual(_primitive_tag_hint(["TYPAL_VAMPIRES"]), "tribal-anchor")

    def test_unknown_falls_back_to_value(self) -> None:
        self.assertEqual(_primitive_tag_hint(["SOMETHING_RANDOM"]), "value")

    def test_empty_list_falls_back_to_value(self) -> None:
        self.assertEqual(_primitive_tag_hint([]), "value")

    def test_priority_ramp_beats_draw(self) -> None:
        # If a card has both MANA_ROCK and CARD_DRAW, ramp-mana wins
        # because of priority ordering in the helper.
        self.assertEqual(
            _primitive_tag_hint(["MANA_ROCK", "CARD_DRAW"]),
            "ramp-mana",
        )


class ComputePositionalContextTests(unittest.TestCase):
    def test_interacts_with_in_deck_by_primitive_overlap(self) -> None:
        # Vito has LIFEGAIN_PAYOFF; deck has Sanguine Bond (LIFEGAIN_PAYOFF).
        # Vito interacts with Sanguine Bond.
        candidate = {
            "name": "Vito", "primitives": ["LIFEGAIN_PAYOFF"],
        }
        deck = [
            {"card_name": "Sanguine Bond", "primitives": ["LIFEGAIN_PAYOFF"]},
            {"card_name": "Sol Ring", "primitives": ["MANA_ROCK"]},
        ]
        ctx = _compute_positional_context(
            candidate=candidate, deck_in_context=deck, candidate_pool=[],
        )
        self.assertIn("Sanguine Bond", ctx["interacts_with_in_deck"])
        self.assertNotIn("Sol Ring", ctx["interacts_with_in_deck"])

    def test_pairs_with_not_yet_picked_requires_two_overlap(self) -> None:
        candidate = {
            "name": "Card A",
            "primitives": ["LIFEGAIN_PAYOFF", "TYPAL_VAMPIRES", "MANA_ROCK"],
        }
        pool = [
            # Same 2 primitives → pair (LIFEGAIN_PAYOFF + TYPAL_VAMPIRES).
            {"name": "Card B", "primitives": ["LIFEGAIN_PAYOFF", "TYPAL_VAMPIRES"]},
            # Just 1 primitive overlap (MANA_ROCK) → not a pair (need >=2).
            {"name": "Card C", "primitives": ["MANA_ROCK", "TOKEN_PRODUCER"]},
        ]
        ctx = _compute_positional_context(
            candidate=candidate, deck_in_context=[], candidate_pool=pool,
        )
        self.assertIn("Card B", ctx["pairs_with_not_yet_picked"])
        self.assertNotIn("Card C", ctx["pairs_with_not_yet_picked"])

    def test_candidate_excluded_from_its_own_pairs(self) -> None:
        candidate = {
            "name": "Self Card",
            "primitives": ["LIFEGAIN_PAYOFF", "TYPAL_VAMPIRES"],
        }
        pool = [
            {"name": "Self Card", "primitives": ["LIFEGAIN_PAYOFF", "TYPAL_VAMPIRES"]},  # same
            {"name": "Other", "primitives": ["LIFEGAIN_PAYOFF", "TYPAL_VAMPIRES"]},
        ]
        ctx = _compute_positional_context(
            candidate=candidate, deck_in_context=[], candidate_pool=pool,
        )
        self.assertNotIn("Self Card", ctx["pairs_with_not_yet_picked"])
        self.assertIn("Other", ctx["pairs_with_not_yet_picked"])

    def test_caps_at_5_interactions_and_4_pairs(self) -> None:
        candidate = {"name": "C", "primitives": ["TYPAL_VAMPIRES"]}
        deck = [
            {"card_name": f"Deck {i}", "primitives": ["TYPAL_VAMPIRES"]}
            for i in range(20)
        ]
        pool = [
            {"name": f"Pool {i}", "primitives": ["TYPAL_VAMPIRES", "LIFEGAIN_PAYOFF"]}
            for i in range(20)
        ]
        # Candidate also has LIFEGAIN_PAYOFF to overlap with pool (2 prims).
        candidate["primitives"] = ["TYPAL_VAMPIRES", "LIFEGAIN_PAYOFF"]
        ctx = _compute_positional_context(
            candidate=candidate, deck_in_context=deck, candidate_pool=pool,
        )
        self.assertEqual(len(ctx["interacts_with_in_deck"]), 5)
        self.assertEqual(len(ctx["pairs_with_not_yet_picked"]), 4)

    def test_empty_primitives_no_interactions(self) -> None:
        candidate = {"name": "Bland", "primitives": []}
        deck = [{"card_name": "Other", "primitives": ["LIFEGAIN_PAYOFF"]}]
        ctx = _compute_positional_context(
            candidate=candidate, deck_in_context=deck, candidate_pool=deck,
        )
        self.assertEqual(ctx["interacts_with_in_deck"], [])
        self.assertEqual(ctx["pairs_with_not_yet_picked"], [])


class PromptBuilderIncludesPositionalContextTests(unittest.TestCase):
    def _build_args(self, deck_primitive_index=None):
        return dict(
            commander="Edgar Markov", bracket="B3",
            theme_hints=["TYPAL_VAMPIRES"], intent_analysis=None,
            current_deck_summary=[
                {"card_name": "Sanguine Bond", "source": "theme:vampires"},
            ],
            swappable_slots=[
                {"card_name": "Filler", "source": "theme:vampires"},
            ],
            candidate_pool=[
                {
                    "name": "Vito",
                    "type_line": "Creature - Vampire",
                    "cmc": 3,
                    "primitives": ["LIFEGAIN_PAYOFF"],
                    "rationale_components": ["theme:vampires"],
                    "oracle_text": "Whenever you gain life, each opponent loses that much life.",
                },
            ],
            bracket_policy_summary="Bracket B3: late combos only.",
            deck_primitive_index=deck_primitive_index,
        )

    def test_no_positional_block_when_index_none(self) -> None:
        # Backwards-compat path: deck_primitive_index=None → no
        # tag=/interacts_with= lines in the prompt.
        prompt = _build_candidate_critic_user_prompt(**self._build_args(None))
        self.assertNotIn("interacts_with=", prompt)
        self.assertNotIn("pairs_with=", prompt)

    def test_positional_block_when_index_provided(self) -> None:
        # Iter 5 Phase 2: the verbose "POSITIONAL CONTEXT (iter 3
        # Phase 8): ..." explainer was moved from the user prompt to
        # the system prompt (cached at the model level for token
        # efficiency). The per-candidate annotations (tag=, interacts_with=,
        # pairs_with=) remain in the user prompt when an index is
        # provided.
        index = [
            {"card_name": "Sanguine Bond", "primitives": ["LIFEGAIN_PAYOFF"]},
        ]
        prompt = _build_candidate_critic_user_prompt(**self._build_args(index))
        # Vito (LIFEGAIN_PAYOFF) should be tagged as lifegain-payoff and
        # interact with Sanguine Bond.
        self.assertIn("tag=lifegain-payoff", prompt)
        self.assertIn("interacts_with=", prompt)
        self.assertIn("Sanguine Bond", prompt)


if __name__ == "__main__":
    unittest.main()
