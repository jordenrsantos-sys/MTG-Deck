"""Iter 3 Phase 1 tests — D2 prompt cap to 30 priority cards.

Covers `_select_priority_rewrite_cards`. Priority order:
  1. Commander
  2. Must-include cards
  3. Creative outliers (source contains 'creative_outlier')
  4. Cards in novel_combo_flags
  5. Highest-corpus-delta cards (NOT in top-30 staples, not basics)
  6. Backstop fill with any non-basic deck cards

Stopping point: cap (default 30).
"""
from __future__ import annotations

import unittest

from api.engine.layers.agent_build_deck_v1 import (
    _select_priority_rewrite_cards,
)


def _deck(*entries) -> list:
    """Helper: build a deck from (name, source) tuples or (name, source, reason)."""
    out = []
    for e in entries:
        if len(e) == 2:
            out.append({"card_name": e[0], "source": e[1], "reason": ""})
        elif len(e) == 3:
            out.append({"card_name": e[0], "source": e[1], "reason": e[2]})
    return out


def _make_archetype_brief(top_staples: list[str]) -> dict:
    """Make a brief whose top-30 staples are the given names (usage_pct
    descending so sort order matches)."""
    return {
        "staple_cards": [
            {"name": n, "usage_pct": 0.50 - i * 0.005}
            for i, n in enumerate(top_staples)
        ],
    }


class PriorityRewriteSelectionTests(unittest.TestCase):
    def test_commander_always_first(self) -> None:
        deck = _deck(
            ("Edgar Markov", "user_intent"),
            ("Sol Ring", "archetype_staple"),
            ("Vito", "user_intent"),
        )
        out = _select_priority_rewrite_cards(
            deck=deck, must_include_cards=[], novel_combo_flags=[],
            archetype_brief={}, cap=10,
        )
        self.assertEqual(out[0]["card_name"], "Edgar Markov")

    def test_must_includes_in_top_priorities(self) -> None:
        deck = _deck(
            ("Edgar Markov", "user_intent"),
            ("Sol Ring", "archetype_staple"),
            ("Vito", "user_intent"),
            ("Bloodthirsty Conqueror", "user_intent"),
        )
        out = _select_priority_rewrite_cards(
            deck=deck,
            must_include_cards=["Vito", "Bloodthirsty Conqueror"],
            novel_combo_flags=[],
            archetype_brief={},
            cap=10,
        )
        names = [c["card_name"] for c in out]
        # Commander first, then both must-includes (in deck order — Vito then BC).
        self.assertEqual(names[0], "Edgar Markov")
        self.assertIn("Vito", names[1:3])
        self.assertIn("Bloodthirsty Conqueror", names[1:3])

    def test_creative_outliers_after_must_includes(self) -> None:
        deck = _deck(
            ("Edgar Markov", "user_intent"),
            ("Sol Ring", "archetype_staple"),
            ("Cult of Skaab", "llm_candidate_critic|creative_outlier"),
            ("Vito", "user_intent"),
        )
        out = _select_priority_rewrite_cards(
            deck=deck,
            must_include_cards=["Vito"],
            novel_combo_flags=[],
            archetype_brief={},
            cap=10,
        )
        names = [c["card_name"] for c in out]
        # commander → must-include → creative_outlier — staple comes later
        self.assertEqual(names[0], "Edgar Markov")
        self.assertEqual(names[1], "Vito")
        self.assertEqual(names[2], "Cult of Skaab")

    def test_combo_flag_participants_included(self) -> None:
        deck = _deck(
            ("Edgar Markov", "user_intent"),
            ("Mirkwood Bats", "llm_wild_combo_discovery"),
            ("Vito", "user_intent"),
        )
        out = _select_priority_rewrite_cards(
            deck=deck,
            must_include_cards=[],
            novel_combo_flags=[
                {"cards": ["Mirkwood Bats", "Vito"], "applied_swap": True},
            ],
            archetype_brief={},
            cap=10,
        )
        names = [c["card_name"] for c in out]
        self.assertIn("Mirkwood Bats", names)
        self.assertIn("Vito", names)

    def test_top_30_staples_deprioritized(self) -> None:
        # Top-30 staples should fall behind corpus-delta picks.
        deck = _deck(
            ("Edgar Markov", "user_intent"),
            ("Sol Ring", "archetype_staple"),       # top staple — defer
            ("Command Tower", "archetype_staple"),   # top staple — defer
            ("Cordial Vampire", "theme:TYPAL_VAMPIRES"),  # not a top staple — prioritize
        )
        brief = _make_archetype_brief(["Sol Ring", "Command Tower"])
        out = _select_priority_rewrite_cards(
            deck=deck, must_include_cards=[], novel_combo_flags=[],
            archetype_brief=brief, cap=2,
        )
        names = [c["card_name"] for c in out]
        # Cap=2 leaves room for commander + one corpus-delta pick.
        self.assertEqual(names[0], "Edgar Markov")
        self.assertEqual(names[1], "Cordial Vampire")
        # Top staples NOT in the priority list at cap=2.
        self.assertNotIn("Sol Ring", names)
        self.assertNotIn("Command Tower", names)

    def test_basics_never_selected_by_delta_rule(self) -> None:
        deck = _deck(
            ("Edgar Markov", "user_intent"),
            ("Swamp", "mana_base"),
            ("Mountain", "mana_base"),
            ("Plains", "mana_base"),
        )
        out = _select_priority_rewrite_cards(
            deck=deck, must_include_cards=[], novel_combo_flags=[],
            archetype_brief={}, cap=10,
        )
        names = [c["card_name"] for c in out]
        # Only commander selected (basics never qualify via delta rule).
        self.assertEqual(names, ["Edgar Markov"])

    def test_cap_respected(self) -> None:
        # 100-card deck — cap should hold at the limit.
        deck = [{"card_name": "Edgar Markov", "source": "user_intent"}]
        for i in range(99):
            deck.append({"card_name": f"Card {i:02d}", "source": "theme:vampires"})
        out = _select_priority_rewrite_cards(
            deck=deck, must_include_cards=[], novel_combo_flags=[],
            archetype_brief={}, cap=30,
        )
        self.assertEqual(len(out), 30)

    def test_dedup_no_duplicate_card_across_priorities(self) -> None:
        # A card that's must-include AND a combo participant only appears once.
        deck = _deck(
            ("Edgar Markov", "user_intent"),
            ("Vito", "user_intent"),
        )
        out = _select_priority_rewrite_cards(
            deck=deck,
            must_include_cards=["Vito"],
            novel_combo_flags=[{"cards": ["Vito", "Mirkwood Bats"], "applied_swap": False}],
            archetype_brief={},
            cap=10,
        )
        names = [c["card_name"] for c in out]
        self.assertEqual(names.count("Vito"), 1)

    def test_empty_deck_returns_empty(self) -> None:
        out = _select_priority_rewrite_cards(
            deck=[], must_include_cards=["Vito"], novel_combo_flags=[],
            archetype_brief={}, cap=10,
        )
        self.assertEqual(out, [])

    def test_realistic_100_card_selects_30_useful_priorities(self) -> None:
        """Realistic shape: commander, 2 must-includes, 1 creative outlier,
        3 novel-combo participants, then 23 mid-priority cards, then 36
        basics. Verify the 30-priority list captures the high-value
        rewrite targets and stops at the cap."""
        deck = [{"card_name": "Edgar Markov", "source": "user_intent"}]
        deck.append({"card_name": "Vito", "source": "user_intent"})
        deck.append({"card_name": "Bloodthirsty Conqueror", "source": "user_intent"})
        deck.append({"card_name": "Cult of Skaab",
                     "source": "llm_candidate_critic|creative_outlier"})
        for n in ["Mirkwood Bats", "Sanguine Bond", "Exquisite Blood"]:
            deck.append({"card_name": n, "source": "llm_wild_combo_discovery"})
        for i in range(60):
            deck.append({"card_name": f"Picks {i:02d}", "source": "theme:vampires"})
        for _ in range(33):
            deck.append({"card_name": "Swamp", "source": "mana_base"})
        out = _select_priority_rewrite_cards(
            deck=deck,
            must_include_cards=["Vito", "Bloodthirsty Conqueror"],
            novel_combo_flags=[
                {"cards": ["Mirkwood Bats", "Vito"], "applied_swap": True},
                {"cards": ["Sanguine Bond", "Exquisite Blood"], "applied_swap": False},
            ],
            archetype_brief={"staple_cards": [{"name": "Sol Ring", "usage_pct": 0.9}]},
            cap=30,
        )
        names = [c["card_name"] for c in out]
        self.assertEqual(len(names), 30)
        # All high-value picks made it in.
        self.assertIn("Edgar Markov", names)
        self.assertIn("Vito", names)
        self.assertIn("Bloodthirsty Conqueror", names)
        self.assertIn("Cult of Skaab", names)
        self.assertIn("Mirkwood Bats", names)
        self.assertIn("Sanguine Bond", names)
        self.assertIn("Exquisite Blood", names)
        # No basics in the priority list.
        self.assertNotIn("Swamp", names)


if __name__ == "__main__":
    unittest.main()
