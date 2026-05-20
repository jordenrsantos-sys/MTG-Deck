"""Phase C tests for agent_build_deck_v1 selection layer.

Three test groups:
  - `_classify_card` — pure mapping from (type_line, primitives) to slot category.
  - `_combo_violates_bracket` — per-bracket combo policy (Fix 1 from kickoff patch).
  - `_select_deck` — end-to-end selection from a hand-built candidate pool.

Test cases here use synthetic combo pair indexes rather than the on-disk
`combo_brackets_v1.json` so policy assertions are deterministic regardless of
upstream Spellbook scrape state.
"""
from __future__ import annotations

import unittest

from api.engine.layers.agent_build_deck_v1 import (
    USER_PICK_SCORE,
    _adjust_slot_targets,
    _classify_card,
    _combo_violates_bracket,
    _count_existing_combo_pairs,
    _fill_mana_base,
    _select_deck,
)


class ClassifyCardTests(unittest.TestCase):
    def test_land_classification_wins_over_primitives(self) -> None:
        # Even if a land card carries MANA_ROCK in primitives (weird but possible),
        # type_line "Land" wins.
        self.assertEqual(
            _classify_card(name="Volcanic Island", type_line="Land",
                           primitives=["MANA_ROCK"]),
            "land",
        )

    def test_ramp_via_mana_rock(self) -> None:
        self.assertEqual(
            _classify_card(name="Sol Ring", type_line="Artifact",
                           primitives=["MANA_ROCK"]),
            "ramp",
        )

    def test_card_draw_primitive(self) -> None:
        self.assertEqual(
            _classify_card(name="Phyrexian Arena", type_line="Enchantment",
                           primitives=["CARD_DRAW_REPEATABLE"]),
            "card_draw",
        )

    def test_removal_primitive(self) -> None:
        self.assertEqual(
            _classify_card(name="Swords to Plowshares", type_line="Instant",
                           primitives=["TARGETED_REMOVAL_CREATURE"]),
            "removal",
        )

    def test_creature_default(self) -> None:
        self.assertEqual(
            _classify_card(name="Drana, Liberator of Malakir",
                           type_line="Creature — Vampire Ally",
                           primitives=["TRIBAL_PAYOFFS"]),
            "creature",
        )

    def test_flex_default_when_no_match(self) -> None:
        # Enchantment with no recognized primitive.
        self.assertEqual(
            _classify_card(name="Mystic Remora",
                           type_line="Enchantment",
                           primitives=[]),
            "flex",
        )


class SlotTargetsTests(unittest.TestCase):
    def test_tribal_archetype_increases_creature_count(self) -> None:
        brief = {"common_archetypes": [{"name": "Vampire Tribal Aggro"}]}
        targets = _adjust_slot_targets(brief)
        # Default creature target is 28; tribal bumps by 4.
        self.assertEqual(targets["creature"], 32)
        # Flex is reduced.
        self.assertEqual(targets["flex"], 1)

    def test_combo_archetype_shifts_to_wincons(self) -> None:
        brief = {"common_archetypes": [{"name": "Storm Combo"}]}
        targets = _adjust_slot_targets(brief)
        self.assertGreater(targets["win_condition"], 3)
        self.assertGreater(targets["card_draw"], 10)
        self.assertLess(targets["creature"], 28)

    def test_empty_archetypes_returns_defaults(self) -> None:
        targets = _adjust_slot_targets({})
        self.assertEqual(targets["creature"], 28)
        self.assertEqual(targets["land"], 36)


class FillManaBaseTests(unittest.TestCase):
    def test_mono_color_fills_with_one_basic(self) -> None:
        cards = _fill_mana_base(["W"], 5)
        self.assertEqual(len(cards), 5)
        for c in cards:
            self.assertEqual(c["card_name"], "Plains")
            self.assertEqual(c["source"], "mana_base")

    def test_multi_color_round_robins(self) -> None:
        cards = _fill_mana_base(["B", "R", "W"], 6)
        names = [c["card_name"] for c in cards]
        self.assertEqual(names.count("Swamp"), 2)
        self.assertEqual(names.count("Mountain"), 2)
        self.assertEqual(names.count("Plains"), 2)

    def test_colorless_fills_with_wastes(self) -> None:
        cards = _fill_mana_base([], 3)
        for c in cards:
            self.assertEqual(c["card_name"], "Wastes")


class ComboViolatesBracketTests(unittest.TestCase):
    """The brief's Fix 1 — bracket combo policy. User picks override; B4 caps at 3."""

    PAIR_INDEX = {
        # Late combo (S tag): allowed B3+.
        frozenset({"thassa's oracle", "demonic consultation"}):
            {"B3", "B4", "B5"},
        # Restricted infinite (R tag): allowed B4+.
        frozenset({"kiki-jiki, mirror breaker", "conspicuous snoop"}):
            {"B4", "B5"},
        # Easy infinite (E): allowed B1-B5.
        frozenset({"easy combo a", "easy combo b"}):
            {"B1", "B2", "B3", "B4", "B5"},
    }

    def test_b1_rejects_any_combo_pair(self) -> None:
        # Thoracle + Consult at B1: rejected because pair's brackets_allowed
        # ({B3,B4,B5}) doesn't include B1.
        violates, reason = _combo_violates_bracket(
            candidate_name="Demonic Consultation",
            selected_names_lower={"thassa's oracle"},
            user_pick_names_lower=set(),
            bracket="B1",
            pair_index=self.PAIR_INDEX,
            current_pair_count=0,
        )
        self.assertTrue(violates)
        self.assertIn("B1", reason or "")
        self.assertIn("pair allowed", (reason or "").lower())

    def test_b3_allows_late_combo(self) -> None:
        # Thoracle + Consult is S-tier (late) → allowed at B3.
        violates, _ = _combo_violates_bracket(
            candidate_name="Demonic Consultation",
            selected_names_lower={"thassa's oracle"},
            user_pick_names_lower=set(),
            bracket="B3",
            pair_index=self.PAIR_INDEX,
            current_pair_count=0,
        )
        self.assertFalse(violates)

    def test_b3_rejects_restricted_infinite(self) -> None:
        # Kiki+Snoop is R-tier → only B4/B5.
        violates, _ = _combo_violates_bracket(
            candidate_name="Conspicuous Snoop",
            selected_names_lower={"kiki-jiki, mirror breaker"},
            user_pick_names_lower=set(),
            bracket="B3",
            pair_index=self.PAIR_INDEX,
            current_pair_count=0,
        )
        self.assertTrue(violates)

    def test_b4_pair_cap_enforced(self) -> None:
        # B4 allows R-tier combos but caps total distinct pairs at 3.
        # Current count already at 3 → next combo-completing candidate rejected.
        violates, reason = _combo_violates_bracket(
            candidate_name="Conspicuous Snoop",
            selected_names_lower={"kiki-jiki, mirror breaker"},
            user_pick_names_lower=set(),
            bracket="B4",
            pair_index=self.PAIR_INDEX,
            current_pair_count=3,
        )
        self.assertTrue(violates)
        self.assertIn("cap", (reason or "").lower())

    def test_b5_unrestricted(self) -> None:
        # B5 = cEDH, no caps. Any combo allowed.
        violates, _ = _combo_violates_bracket(
            candidate_name="Demonic Consultation",
            selected_names_lower={"thassa's oracle"},
            user_pick_names_lower=set(),
            bracket="B5",
            pair_index=self.PAIR_INDEX,
            current_pair_count=10,
        )
        self.assertFalse(violates)

    def test_user_picks_both_halves_override(self) -> None:
        # User explicitly included both halves of a B5-only pair while building
        # B1 → still allowed (user override).
        violates, _ = _combo_violates_bracket(
            candidate_name="Conspicuous Snoop",
            selected_names_lower={"kiki-jiki, mirror breaker"},
            user_pick_names_lower={"conspicuous snoop", "kiki-jiki, mirror breaker"},
            bracket="B1",
            pair_index=self.PAIR_INDEX,
            current_pair_count=0,
        )
        self.assertFalse(violates)

    def test_user_pick_one_half_does_not_override(self) -> None:
        # User picked Kiki-Jiki (one half) at B1. Agent must NOT auto-complete
        # by adding Conspicuous Snoop. This is Fix 2 from the kickoff patch.
        violates, _ = _combo_violates_bracket(
            candidate_name="Conspicuous Snoop",
            selected_names_lower={"kiki-jiki, mirror breaker"},
            user_pick_names_lower={"kiki-jiki, mirror breaker"},  # only one half
            bracket="B1",
            pair_index=self.PAIR_INDEX,
            current_pair_count=0,
        )
        self.assertTrue(violates)


class CountExistingComboPairsTests(unittest.TestCase):
    PAIR_INDEX = {
        frozenset({"a", "b"}): {"B5"},
        frozenset({"c", "d"}): {"B5"},
        frozenset({"e", "f"}): {"B5"},
    }

    def test_no_pairs(self) -> None:
        self.assertEqual(
            _count_existing_combo_pairs(selected_names_lower={"a", "c"}, pair_index=self.PAIR_INDEX),
            0,
        )

    def test_one_complete_pair(self) -> None:
        self.assertEqual(
            _count_existing_combo_pairs(selected_names_lower={"a", "b", "c"}, pair_index=self.PAIR_INDEX),
            1,
        )

    def test_multiple_complete_pairs(self) -> None:
        self.assertEqual(
            _count_existing_combo_pairs(
                selected_names_lower={"a", "b", "c", "d", "e"},
                pair_index=self.PAIR_INDEX,
            ),
            2,
        )


class SelectDeckTests(unittest.TestCase):
    """End-to-end selection against synthetic candidate pools."""

    def _make_pool(self, *, user_picks=None, theme_creatures=20, ramp=15, draw=10,
                   removal=10, color_identity=("B", "R", "W"), archetype="Vampire Tribal"):
        """Build a synthetic candidate pool large enough to fill all slots."""
        candidates = []

        # User picks (score=INF).
        for n in (user_picks or []):
            candidates.append({
                "name": n,
                "score": USER_PICK_SCORE,
                "source": "user_intent",
                "rationale_components": ["User must-include."],
                "primitives": ["TRIBAL_PAYOFFS"],
                "type_line": "Creature — Vampire",
                "cmc": 3,
                "color_identity": ["B"],
                "is_user_pick": True,
                "is_combo_half": False,
            })

        # Theme creatures.
        for i in range(theme_creatures):
            candidates.append({
                "name": f"Theme Creature {i:02d}",
                "score": 20.0 - i * 0.1,
                "source": "theme:TYPAL_VAMPIRES",
                "rationale_components": [f"Theme TYPAL_VAMPIRES signal_count=2."],
                "primitives": ["TRIBAL_PAYOFFS"],
                "type_line": "Creature — Vampire",
                "cmc": (i % 5) + 1,
                "color_identity": list(color_identity),
                "is_user_pick": False,
            })

        # Ramp.
        for i in range(ramp):
            candidates.append({
                "name": f"Ramp Piece {i:02d}",
                "score": 15.0 - i * 0.1,
                "source": "theme",
                "rationale_components": ["Ramp source."],
                "primitives": ["MANA_ROCK"],
                "type_line": "Artifact",
                "cmc": 2,
                "color_identity": [],
                "is_user_pick": False,
            })

        # Card draw.
        for i in range(draw):
            candidates.append({
                "name": f"Draw {i:02d}",
                "score": 12.0 - i * 0.1,
                "source": "theme",
                "rationale_components": ["Draw engine."],
                "primitives": ["CARD_DRAW_REPEATABLE"],
                "type_line": "Enchantment",
                "cmc": 3,
                "color_identity": ["B"],
                "is_user_pick": False,
            })

        # Removal.
        for i in range(removal):
            candidates.append({
                "name": f"Removal {i:02d}",
                "score": 10.0 - i * 0.1,
                "source": "theme",
                "rationale_components": ["Targeted removal."],
                "primitives": ["TARGETED_REMOVAL_CREATURE"],
                "type_line": "Instant",
                "cmc": 2,
                "color_identity": ["W"],
                "is_user_pick": False,
            })

        return {
            "candidates": candidates,
            "color_identity": list(color_identity),
            "archetype_brief": {
                "common_archetypes": [{"name": archetype}],
                "staple_cards": [],
            },
            "warnings": [],
            "endpoint_calls": 2,
            "must_includes_resolved": list(user_picks or []),
            "must_includes_dropped": [],
        }

    def test_select_produces_exactly_99_cards(self) -> None:
        pool = self._make_pool()
        deck, _ = _select_deck(pool=pool, bracket="B3", commander="Edgar Markov")
        self.assertEqual(len(deck), 99)

    def test_user_picks_present_in_output(self) -> None:
        pool = self._make_pool(user_picks=["Vito, Thorn of the Dusk Rose",
                                            "Bloodthirsty Conqueror"])
        deck, _ = _select_deck(pool=pool, bracket="B3", commander="Edgar Markov")
        names = {c["card_name"] for c in deck}
        self.assertIn("Vito, Thorn of the Dusk Rose", names)
        self.assertIn("Bloodthirsty Conqueror", names)

    def test_basic_lands_fill_to_land_target(self) -> None:
        # No land candidates in pool → all lands must be basics. Edgar is BRW.
        pool = self._make_pool(color_identity=("B", "R", "W"))
        deck, _ = _select_deck(pool=pool, bracket="B3", commander="Edgar Markov")
        basic_names = {"Plains", "Swamp", "Mountain"}
        basic_count = sum(1 for c in deck if c["card_name"] in basic_names)
        # Target is 36 lands for tribal-shifted slot table.
        self.assertGreaterEqual(basic_count, 30, f"want >=30 basics, got {basic_count}")

    def test_colorless_commander_fills_with_wastes(self) -> None:
        pool = self._make_pool(color_identity=())
        deck, _ = _select_deck(pool=pool, bracket="B3", commander="Karn, Silver Golem")
        wastes_count = sum(1 for c in deck if c["card_name"] == "Wastes")
        self.assertGreater(wastes_count, 0)

    def test_every_card_has_non_empty_reason(self) -> None:
        pool = self._make_pool()
        deck, _ = _select_deck(pool=pool, bracket="B3", commander="Edgar Markov")
        for c in deck:
            self.assertTrue(c.get("reason"), f"missing reason on {c}")
            self.assertTrue(c.get("source"), f"missing source on {c}")

    def test_no_duplicates_among_non_basic_picks(self) -> None:
        pool = self._make_pool()
        deck, _ = _select_deck(pool=pool, bracket="B3", commander="Edgar Markov")
        basic_names = {"Plains", "Island", "Swamp", "Mountain", "Forest", "Wastes"}
        non_basics = [c["card_name"] for c in deck if c["card_name"] not in basic_names]
        self.assertEqual(len(non_basics), len(set(non_basics)),
                         "agent-picked non-basics must be singleton")


if __name__ == "__main__":
    unittest.main()
