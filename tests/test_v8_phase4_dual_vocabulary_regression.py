"""
test_v8_phase4_dual_vocabulary_regression — Mega-task v8 Phase 4.

Phase 4's kickoff goal was to retire the dual-vocabulary patches added
in v7 (legacy lowercase-hyphenated + v2 UPPERCASE_UNDERSCORED) by
rebuilding `primitive_to_cards` from the v2 ontology source. The full
rebuild is a 3-5 day effort + risky data migration that exceeds v8's
budget envelope; the iter-10 dispatch will own the rebuild.

This file is the SAFETY NET: it asserts the dual-vocabulary aliases
STAY in place until iter 10 lands the rebuild. Removing them
prematurely (before the rebuild) re-introduces the iter-8 vocab-
mismatch failure (interaction_designer + win_con_coherence pattern
matches drop to 0 because consumer maps don't see the v2 names).

When iter 10 ships the rebuild, this test should be flipped: replace
the asserts with new ones that verify the legacy aliases are GONE
+ v2 aliases are the only entries. Reference: v7 Phase 1 + v7 Phase 8
commit messages for the original vocab-bridge work this protects.
"""
from __future__ import annotations

import unittest


class V8Phase4DualVocabularyRegressionTests(unittest.TestCase):
    """Iter-10 rebuild dispatch should flip these tests; until then they
    protect against premature removal of vocab-bridge aliases."""

    def test_classify_card_ramp_primitives_include_both_vocabs(self) -> None:
        from api.engine.layers.agent_build_deck_v1 import _RAMP_PRIMITIVES
        # Legacy v1 ontology in primitive_to_cards.
        self.assertIn("MANA_ROCK", _RAMP_PRIMITIVES)
        self.assertIn("MANA_RAMP_LAND_SEARCH", _RAMP_PRIMITIVES)
        # v6 Phase 3 ontology v2 in cards.primitives_v1_json.
        self.assertIn("RAMP_MANA", _RAMP_PRIMITIVES)
        self.assertIn("RAMP_LAND", _RAMP_PRIMITIVES)
        self.assertIn("MANA_FIXING", _RAMP_PRIMITIVES)

    def test_classify_card_draw_primitives_include_both_vocabs(self) -> None:
        from api.engine.layers.agent_build_deck_v1 import _DRAW_PRIMITIVES
        self.assertIn("CARD_DRAW_BURST", _DRAW_PRIMITIVES)   # legacy
        self.assertIn("DRAW_REPLACEMENT", _DRAW_PRIMITIVES)  # legacy
        self.assertIn("CARD_DRAW", _DRAW_PRIMITIVES)         # v2
        self.assertIn("CARD_SELECTION", _DRAW_PRIMITIVES)    # v2

    def test_classify_card_removal_primitives_include_both_vocabs(self) -> None:
        from api.engine.layers.agent_build_deck_v1 import _REMOVAL_PRIMITIVES
        self.assertIn("TARGETED_REMOVAL_CREATURE", _REMOVAL_PRIMITIVES)  # legacy
        self.assertIn("BOARDWIPE_CREATURES", _REMOVAL_PRIMITIVES)        # legacy
        self.assertIn("COUNTERSPELL", _REMOVAL_PRIMITIVES)               # v2
        self.assertIn("REMOVAL_SINGLE", _REMOVAL_PRIMITIVES)             # v2
        self.assertIn("BOARD_WIPE", _REMOVAL_PRIMITIVES)                 # v2

    def test_interaction_designer_primitives_to_category_has_both_vocabs(self) -> None:
        from api.engine.layers.interaction_designer_v1 import _PRIMITIVES_TO_CATEGORY
        # Legacy.
        self.assertEqual(_PRIMITIVES_TO_CATEGORY.get("counterspell-hard"), "counterspells")
        self.assertEqual(_PRIMITIVES_TO_CATEGORY.get("removal-mass-creatures"), "mass_removal")
        # v2 vocab.
        self.assertEqual(_PRIMITIVES_TO_CATEGORY.get("COUNTERSPELL"), "counterspells")
        self.assertEqual(_PRIMITIVES_TO_CATEGORY.get("BOARD_WIPE"), "mass_removal")
        self.assertEqual(_PRIMITIVES_TO_CATEGORY.get("REMOVAL_SINGLE"), "targeted_creature_removal")

    def test_win_con_patterns_include_both_vocabs(self) -> None:
        from api.engine.layers.win_con_coherence_v1 import _WIN_CON_PATTERNS
        counters = _WIN_CON_PATTERNS["counters_proliferate"]["primitive_sets"]
        # Legacy.
        self.assertIn({"proliferate-trigger"}, counters)
        # v2.
        self.assertIn({"PROLIFERATE"}, counters)
        self.assertIn({"COUNTER_SYNERGY"}, counters)
        combo = _WIN_CON_PATTERNS["combo_win"]["primitive_sets"]
        self.assertIn({"combo-assembly"}, combo)             # legacy
        self.assertIn({"INFINITE_COMBO"}, combo)             # v2

    def test_pillar_e_swap_ramp_prims_include_both_vocabs(self) -> None:
        from api.engine.layers.pillar_e_aggressive_swaps_v1 import _RAMP_PRIMS
        self.assertIn("MANA_ROCK", _RAMP_PRIMS)               # legacy
        self.assertIn("RAMP_MANA", _RAMP_PRIMS)               # v2

    def test_pillar_e_swap_win_con_enabler_prims_are_v2_canonical(self) -> None:
        # v8 Phase 3 added _WIN_CON_ENABLER_PRIMS using v2-canonical
        # vocabulary only. Once the inverted index ships v2, the rest
        # of the codebase should converge to this single-vocab pattern.
        from api.engine.layers.pillar_e_aggressive_swaps_v1 import _WIN_CON_ENABLER_PRIMS
        self.assertIn("INFINITE_COMBO", _WIN_CON_ENABLER_PRIMS)
        self.assertIn("PROLIFERATE", _WIN_CON_ENABLER_PRIMS)
        self.assertIn("SAC_OUTLET", _WIN_CON_ENABLER_PRIMS)
        # Legacy lowercase-hyphenated names SHOULD NOT be in this newer set.
        self.assertNotIn("combo-assembly", _WIN_CON_ENABLER_PRIMS)
        self.assertNotIn("proliferate-trigger", _WIN_CON_ENABLER_PRIMS)
        self.assertNotIn("sac-outlet", _WIN_CON_ENABLER_PRIMS)


if __name__ == "__main__":
    unittest.main()
