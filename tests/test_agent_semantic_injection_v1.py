"""Mega-task v6 Phase 2 — agent_semantic_injection_v1 unit tests.

Closes iter 6 success criterion #6 by guaranteeing N semantic-neighbor
cards via post-hoc deterministic injection. The module replaces the
failed score-boost (iter 5) + prompt-level "MUST SELECT" approaches per
the `feedback_pool_score_does_not_drive_llm_picking` learning.
"""
from __future__ import annotations

import unittest
from typing import Any, Dict, List, Optional

from api.engine.layers.agent_semantic_injection_v1 import (
    SEMANTIC_INJECTION_VERSION,
    SOURCE_TAG,
    inject_semantic_picks,
    resolve_n_target,
)


def _card(name: str, source: str = "agent_select", reason: str = "") -> Dict[str, Any]:
    return {"card_name": name, "source": source, "reason": reason or f"{name} reason"}


def _mock_neighbors(
    mapping: Dict[str, List[Dict[str, Any]]],
):
    """Returns a mock query_neighbors that returns canned per-anchor neighbors."""

    def _q(
        card_name: str,
        k: int = 20,
        color_identity_filter: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        return mapping.get(card_name, [])[:k]

    return _q


class ResolveNTargetTests(unittest.TestCase):
    def test_bracket_aware_targets(self):
        # v7 Phase 4: B3/B4 bumped from 3 to 4 to close iter-7 sweep
        # gap (voyage_semantic_avg landed at 2.2 vs ≥3 target).
        self.assertEqual(resolve_n_target("B1"), 2)
        self.assertEqual(resolve_n_target("B2"), 2)
        self.assertEqual(resolve_n_target("B3"), 4)
        self.assertEqual(resolve_n_target("B4"), 4)
        self.assertEqual(resolve_n_target("B5"), 4)

    def test_unknown_bracket_defaults_to_b3(self):
        # v7 Phase 4: B3 default is now 4 (was 3).
        self.assertEqual(resolve_n_target(""), 4)
        self.assertEqual(resolve_n_target("UNKNOWN"), 4)


class V7Phase4WidenedSwappableSetTests(unittest.TestCase):
    """v7 Phase 4: slot_fallback / agent_select / pillar_e_aggressive_swap
    sources are eligible swap-out targets (widens iter-6/7 swap set which
    only covered C2.2 wild discovery picks). Closes iter-7 sweep gap #1
    (voyage_semantic_avg 2.2 → ≥3 target)."""

    def test_slot_fallback_card_is_swappable(self):
        deck = [
            _card("Edgar Markov", source="user_intent"),
            _card("Sol Ring", source="archetype_staple"),
            _card("Some Ramp", source="slot_fallback:ramp"),
        ]
        anchors = ["Edgar Markov"]
        neighbors = {"Edgar Markov": [{"name": "Vampire Tactician", "similarity": 0.9}]}
        new_deck, swap_log = inject_semantic_picks(
            deck, anchors, ["B", "R", "W"], n_target=1,
            query_neighbors=_mock_neighbors(neighbors),
        )
        self.assertEqual(len(swap_log), 1)
        self.assertEqual(swap_log[0]["removed"], "Some Ramp")
        self.assertEqual(swap_log[0]["added"], "Vampire Tactician")

    def test_agent_select_card_is_swappable(self):
        deck = [
            _card("Edgar Markov", source="user_intent"),
            _card("Generic Pick", source="agent_select"),
        ]
        anchors = ["Edgar Markov"]
        neighbors = {"Edgar Markov": [{"name": "Vampire Tactician", "similarity": 0.9}]}
        new_deck, swap_log = inject_semantic_picks(
            deck, anchors, ["B", "R", "W"], n_target=1,
            query_neighbors=_mock_neighbors(neighbors),
        )
        self.assertEqual(len(swap_log), 1)
        self.assertEqual(swap_log[0]["removed"], "Generic Pick")

    def test_pillar_e_aggressive_swap_card_is_swappable(self):
        deck = [
            _card("Edgar Markov", source="user_intent"),
            _card("Phyrexian Arena", source="pillar_e_aggressive_swap"),
        ]
        anchors = ["Edgar Markov"]
        neighbors = {"Edgar Markov": [{"name": "Vampire Tactician", "similarity": 0.9}]}
        new_deck, swap_log = inject_semantic_picks(
            deck, anchors, ["B", "R", "W"], n_target=1,
            query_neighbors=_mock_neighbors(neighbors),
        )
        self.assertEqual(len(swap_log), 1)
        self.assertEqual(swap_log[0]["removed"], "Phyrexian Arena")

    def test_archetype_staple_still_protected(self):
        # Regression: archetype_staple cards must NEVER be swap targets
        # — they're the corpus baseline that anchors the deck.
        deck = [
            _card("Edgar Markov", source="user_intent"),
            _card("Sol Ring", source="archetype_staple"),  # Should NOT be swapped.
        ]
        anchors = ["Edgar Markov"]
        neighbors = {"Edgar Markov": [{"name": "Vampire Tactician", "similarity": 0.9}]}
        new_deck, swap_log = inject_semantic_picks(
            deck, anchors, ["B", "R", "W"], n_target=2,
            query_neighbors=_mock_neighbors(neighbors),
        )
        self.assertEqual(len(swap_log), 0)
        names = [c["card_name"] for c in new_deck]
        self.assertIn("Sol Ring", names)

    def test_user_intent_still_protected(self):
        deck = [
            _card("Edgar Markov", source="user_intent"),
            _card("Bloodthirsty Conqueror", source="user_intent"),  # Should NOT be swapped.
        ]
        anchors = ["Edgar Markov"]
        neighbors = {"Edgar Markov": [{"name": "Vampire Tactician", "similarity": 0.9}]}
        new_deck, swap_log = inject_semantic_picks(
            deck, anchors, ["B", "R", "W"], n_target=2,
            query_neighbors=_mock_neighbors(neighbors),
        )
        self.assertEqual(len(swap_log), 0)
        names = [c["card_name"] for c in new_deck]
        self.assertIn("Bloodthirsty Conqueror", names)


class InjectSemanticPicksTests(unittest.TestCase):

    def test_injects_n_target_neighbors_when_none_present(self):
        """Full pool scenario — none of the wild picks are semantic yet."""
        deck = [
            _card("Edgar Markov", source="user_intent"),
            _card("Sol Ring", source="archetype_staple"),
            _card("Wild Pick A", source="C2_2_wild_combo_discovery_added"),
            _card("Wild Pick B", source="C2_2_wild_combo_discovery_added"),
            _card("Wild Pick C", source="C2_2_wild_combo_discovery_added"),
            _card("Plains", source="mana_base"),
        ]
        neighbors = {
            "Edgar Markov": [
                {"name": "Neighbor1", "similarity": 0.85, "color_identity": ["B", "R", "W"]},
                {"name": "Neighbor2", "similarity": 0.80, "color_identity": ["B", "R", "W"]},
                {"name": "Neighbor3", "similarity": 0.78, "color_identity": ["B", "R", "W"]},
            ],
        }
        new_deck, swap_log = inject_semantic_picks(
            deck,
            anchor_cards=["Edgar Markov"],
            color_identity=["B", "R", "W"],
            n_target=3,
            query_neighbors=_mock_neighbors(neighbors),
        )
        # 3 cards injected → 3 swaps.
        self.assertEqual(len(swap_log), 3)
        # Source tag landed.
        injected_names = {e["added"] for e in swap_log}
        self.assertEqual(injected_names, {"Neighbor1", "Neighbor2", "Neighbor3"})
        injected_cards = [c for c in new_deck if c.get("source") == SOURCE_TAG]
        self.assertEqual(len(injected_cards), 3)
        # Deck size preserved.
        self.assertEqual(len(new_deck), len(deck))
        # Commander, archetype staple, basic land preserved.
        names_in_deck = {c["card_name"] for c in new_deck}
        self.assertIn("Edgar Markov", names_in_deck)
        self.assertIn("Sol Ring", names_in_deck)
        self.assertIn("Plains", names_in_deck)

    def test_partial_pool_already_has_some_semantic_cards(self):
        deck = [
            _card("Edgar Markov", source="user_intent"),
            _card("Existing Semantic", source="C2_2_wild_combo_discovery_added|from_semantic_neighbor"),
            _card("Wild Pick A", source="C2_2_wild_combo_discovery_added"),
            _card("Wild Pick B", source="C2_2_wild_combo_discovery_added"),
        ]
        neighbors = {
            "Edgar Markov": [
                {"name": "Neighbor1", "similarity": 0.85, "color_identity": ["B", "R", "W"]},
                {"name": "Neighbor2", "similarity": 0.80, "color_identity": ["B", "R", "W"]},
            ],
        }
        new_deck, swap_log = inject_semantic_picks(
            deck, ["Edgar Markov"], ["B", "R", "W"],
            n_target=3, query_neighbors=_mock_neighbors(neighbors),
        )
        # Already have 1, need 2 more.
        self.assertEqual(len(swap_log), 2)
        sem_count = sum(1 for c in new_deck if (
            "semantic_injection" in (c.get("source") or "")
            or "from_semantic_neighbor" in (c.get("source") or "")
        ))
        self.assertEqual(sem_count, 3)

    def test_all_anchors_already_in_deck_or_overlap(self):
        """When all neighbors are already in the deck, returns unchanged."""
        deck = [
            _card("Edgar Markov", source="user_intent"),
            _card("Neighbor1", source="C2_1_candidate_critic"),
            _card("Neighbor2", source="C2_1_candidate_critic"),
            _card("Wild Pick", source="C2_2_wild_combo_discovery_added"),
        ]
        neighbors = {
            "Edgar Markov": [
                {"name": "Neighbor1", "similarity": 0.85, "color_identity": ["B"]},
                {"name": "Neighbor2", "similarity": 0.80, "color_identity": ["B"]},
                {"name": "Edgar Markov", "similarity": 1.0, "color_identity": ["B"]},
            ],
        }
        new_deck, swap_log = inject_semantic_picks(
            deck, ["Edgar Markov"], ["B"],
            n_target=3, query_neighbors=_mock_neighbors(neighbors),
        )
        # All candidates filtered out (already in deck) → no swaps.
        self.assertEqual(swap_log, [])
        self.assertEqual(new_deck, deck)

    def test_color_identity_filter_via_mock(self):
        """The injection layer trusts query_neighbors to apply the color
        filter; this test asserts the color_identity arg is forwarded."""
        captured: Dict[str, Any] = {}

        def _q(card_name, k=20, color_identity_filter=None):
            captured["color_identity_filter"] = color_identity_filter
            return []

        inject_semantic_picks(
            [_card("Edgar Markov", source="user_intent")],
            anchor_cards=["Edgar Markov"],
            color_identity=["B", "R", "W"],
            n_target=3,
            query_neighbors=_q,
        )
        self.assertEqual(captured["color_identity_filter"], ["B", "R", "W"])

    def test_no_neighbors_returns_unchanged(self):
        """Voyage offline / empty pool → deck unchanged, no swap log."""
        deck = [
            _card("Edgar Markov", source="user_intent"),
            _card("Wild Pick", source="C2_2_wild_combo_discovery_added"),
        ]
        new_deck, swap_log = inject_semantic_picks(
            deck, ["Edgar Markov"], ["B"],
            n_target=3, query_neighbors=_mock_neighbors({}),
        )
        self.assertEqual(swap_log, [])
        self.assertEqual(new_deck, deck)

    def test_protects_must_includes_and_commander_and_lands(self):
        """Commander, user_intent must-includes, mana_base, and
        C2_1_candidate_critic picks must NEVER be swapped out."""
        deck = [
            _card("Edgar Markov", source="user_intent"),
            _card("Sol Ring", source="archetype_staple"),
            _card("Must Include Card", source="user_intent"),
            _card("C2.1 Pick", source="C2_1_candidate_critic"),
            _card("Plains", source="mana_base"),
            # The ONLY swappable card:
            _card("Wild Pick", source="C2_2_wild_combo_discovery_added"),
        ]
        neighbors = {
            "Edgar Markov": [
                {"name": f"Neighbor{i}", "similarity": 0.9 - 0.01 * i, "color_identity": ["B"]}
                for i in range(5)
            ],
        }
        new_deck, swap_log = inject_semantic_picks(
            deck, ["Edgar Markov", "Must Include Card"], ["B"],
            n_target=3, query_neighbors=_mock_neighbors(neighbors),
        )
        # Only ONE wild pick available to swap; can only inject 1.
        self.assertEqual(len(swap_log), 1)
        names = {c["card_name"] for c in new_deck}
        self.assertIn("Edgar Markov", names)
        self.assertIn("Must Include Card", names)
        self.assertIn("C2.1 Pick", names)
        self.assertIn("Plains", names)
        self.assertIn("Sol Ring", names)

    def test_forbidden_set_blocks_neighbors(self):
        deck = [
            _card("Edgar Markov", source="user_intent"),
            _card("Wild Pick", source="C2_2_wild_combo_discovery_added"),
        ]
        neighbors = {
            "Edgar Markov": [
                {"name": "Forbidden A", "similarity": 0.9, "color_identity": ["B"]},
                {"name": "Allowed B", "similarity": 0.85, "color_identity": ["B"]},
            ],
        }
        new_deck, swap_log = inject_semantic_picks(
            deck, ["Edgar Markov"], ["B"],
            n_target=3,
            forbidden_set={"Forbidden A"},
            query_neighbors=_mock_neighbors(neighbors),
        )
        self.assertEqual(len(swap_log), 1)
        self.assertEqual(swap_log[0]["added"], "Allowed B")

    def test_already_at_target_no_op(self):
        deck = [
            _card("Edgar Markov", source="user_intent"),
            _card("S1", source="semantic_injection"),
            _card("S2", source="semantic_injection"),
            _card("S3", source="semantic_injection"),
            _card("Wild", source="C2_2_wild_combo_discovery_added"),
        ]
        new_deck, swap_log = inject_semantic_picks(
            deck, ["Edgar Markov"], ["B"],
            n_target=3, query_neighbors=_mock_neighbors({
                "Edgar Markov": [{"name": "X", "similarity": 0.9, "color_identity": ["B"]}]
            }),
        )
        self.assertEqual(swap_log, [])
        self.assertEqual(new_deck, deck)

    def test_no_swappable_wild_picks_returns_unchanged(self):
        """If no C2.2 wild picks are present, nothing can be swapped."""
        deck = [
            _card("Edgar Markov", source="user_intent"),
            _card("Must A", source="user_intent"),
            _card("C2.1", source="C2_1_candidate_critic"),
            _card("Plains", source="mana_base"),
        ]
        neighbors = {
            "Edgar Markov": [
                {"name": "Neighbor1", "similarity": 0.9, "color_identity": ["B"]},
            ],
        }
        new_deck, swap_log = inject_semantic_picks(
            deck, ["Edgar Markov"], ["B"],
            n_target=3, query_neighbors=_mock_neighbors(neighbors),
        )
        self.assertEqual(swap_log, [])
        self.assertEqual(new_deck, deck)

    def test_query_neighbors_exception_falls_back_gracefully(self):
        """Voyage backend raising must not break the build."""
        deck = [
            _card("Edgar Markov", source="user_intent"),
            _card("Wild", source="C2_2_wild_combo_discovery_added"),
        ]

        def _q_raises(card_name, k=20, color_identity_filter=None):
            raise RuntimeError("Voyage backend offline")

        new_deck, swap_log = inject_semantic_picks(
            deck, ["Edgar Markov"], ["B"],
            n_target=3, query_neighbors=_q_raises,
        )
        # No neighbors returned from any anchor → unchanged.
        self.assertEqual(swap_log, [])
        self.assertEqual(new_deck, deck)

    def test_version_constant_present(self):
        self.assertTrue(SEMANTIC_INJECTION_VERSION.startswith("agent_semantic_injection_v1"))


if __name__ == "__main__":
    unittest.main()
