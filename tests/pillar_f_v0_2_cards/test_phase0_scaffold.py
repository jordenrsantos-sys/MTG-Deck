"""Phase 0 smoke tests: scaffold imports clean + the basic-land tap-mana
resolver actually mutates the controller's mana pool when invoked.

This file's purpose is "scaffold integrity": importing
`api.engine.pillar_f.v0_2.cards` must not raise, every subpackage must
register at least one entry in the substrate's resolver registry (the
ones that ship handlers in this commit), and the per-card top-500
metadata file must parse + cover at least one card per non-empty
handler-type bucket.
"""
from __future__ import annotations

import json
import unittest
from pathlib import Path

# Import the cards package — triggers all per-card registration.
import api.engine.pillar_f.v0_2.cards  # noqa: F401
from api.engine.pillar_f.v0_2.cards.simple.basic_lands import BASIC_LAND_RESOLVERS
from api.engine.pillar_f.v0_2.cards.simple.mana_dorks import MANA_DORK_RESOLVERS
from api.engine.pillar_f.v0_2.stack import push_to_stack, resolve_top
from api.engine.pillar_f.v0_2.stack.stack import get_resolver
from api.engine.pillar_f.v0_2.state import GameState, PlayerState, PlayerZones


TOP_500_JSON = (Path(__file__).parent.parent.parent / "api" / "engine"
                / "pillar_f" / "v0_2" / "cards" / "_meta"
                / "top_500_edh_cards.json")


def _empty_game() -> GameState:
    gs = GameState()
    for pid in range(4):
        gs.players.append(PlayerState(
            player_id=pid, name=f"P{pid}", life_total=40, zones=PlayerZones(),
        ))
    gs.active_player = 0
    return gs


class CardsPackageScaffold(unittest.TestCase):
    """Imports + registrations + top-500 metadata."""

    def test_cards_package_imports_clean(self) -> None:
        # Already imported at module level — assert the subpackages are
        # reachable.
        import api.engine.pillar_f.v0_2.cards as cards_pkg
        self.assertTrue(hasattr(cards_pkg, "CARDS_PKG_VERSION"))
        # Subpackages all reachable.
        from api.engine.pillar_f.v0_2.cards import (
            simple, etb, ltb, activated, continuous,
            replacement, triggered, spell, complex,
        )
        for sub in [simple, etb, ltb, activated, continuous,
                    replacement, triggered, spell, complex]:
            self.assertIsNotNone(sub.__doc__)

    def test_basic_land_resolvers_registered(self) -> None:
        # 5 colors × 2 (regular + snow-covered) = 10, + Wastes = 11.
        self.assertEqual(len(BASIC_LAND_RESOLVERS), 11)
        for color in ("W", "U", "B", "R", "G", "C"):
            fn = get_resolver(f"basic_tap_{color}")
            self.assertIsNotNone(fn, f"basic_tap_{color} not registered")

    def test_mana_dork_resolvers_registered(self) -> None:
        self.assertGreaterEqual(len(MANA_DORK_RESOLVERS), 8)
        for color in ("W", "U", "B", "R", "G"):
            self.assertIsNotNone(get_resolver(f"mana_dork_tap_{color}"))
        self.assertIsNotNone(get_resolver("mana_dork_tap_any"))


class BasicLandTapMana(unittest.TestCase):
    """Live behavior: a basic-land tap stack entry resolves and adds
    one mana of the right color to the controller's pool."""

    def test_forest_tap_adds_green_mana(self) -> None:
        gs = _empty_game()
        # Set controller = 0; push a "Forest's tap" activated ability.
        push_to_stack(
            gs, card_id=None, controller=0, entry_type="activated",
            payment={"resolver": "basic_tap_G"},
            description="Forest tap-mana",
        )
        resolve_top(gs)
        self.assertEqual(gs.players[0].mana_pool.G, 1)
        self.assertEqual(gs.players[0].mana_pool.total(), 1)

    def test_each_basic_color_adds_correct_bucket(self) -> None:
        gs = _empty_game()
        for color in ("W", "U", "B", "R", "G", "C"):
            push_to_stack(
                gs, card_id=None, controller=0, entry_type="activated",
                payment={"resolver": f"basic_tap_{color}"},
                description=f"Basic tap {color}",
            )
            resolve_top(gs)
        pool = gs.players[0].mana_pool
        self.assertEqual(pool.W, 1)
        self.assertEqual(pool.U, 1)
        self.assertEqual(pool.B, 1)
        self.assertEqual(pool.R, 1)
        self.assertEqual(pool.G, 1)
        self.assertEqual(pool.C, 1)
        self.assertEqual(pool.total(), 6)

    def test_any_color_dork_defaults_to_colorless(self) -> None:
        """Birds of Paradise / Noble Hierarch stub — when caller doesn't
        specify a color, default is C (iter-10 limitation; iter-11+
        adds LLM-driven color choice)."""
        gs = _empty_game()
        push_to_stack(
            gs, card_id=None, controller=0, entry_type="activated",
            payment={"resolver": "mana_dork_tap_any"},  # no color
            description="Birds of Paradise tap (default-colorless stub)",
        )
        resolve_top(gs)
        self.assertEqual(gs.players[0].mana_pool.C, 1)

    def test_any_color_dork_honors_caller_color(self) -> None:
        gs = _empty_game()
        push_to_stack(
            gs, card_id=None, controller=0, entry_type="activated",
            payment={"resolver": "mana_dork_tap_any", "color": "U"},
            description="Birds of Paradise → U",
        )
        resolve_top(gs)
        self.assertEqual(gs.players[0].mana_pool.U, 1)


class Top500Metadata(unittest.TestCase):
    """The top-500 JSON exists, parses, and matches expected shape."""

    def test_metadata_file_parses(self) -> None:
        self.assertTrue(TOP_500_JSON.exists(),
                        f"missing: {TOP_500_JSON}")
        d = json.loads(TOP_500_JSON.read_text(encoding="utf-8"))
        self.assertEqual(d["count"], 500)
        self.assertEqual(len(d["entries"]), 500)
        # Schema check on the first entry.
        e0 = d["entries"][0]
        for required in ("rank", "name", "oracle_id", "usage_rate",
                         "handler_type", "type_line", "oracle_text"):
            self.assertIn(required, e0)
        # First entry should be the most-played card.
        self.assertEqual(e0["rank"], 1)

    def test_handler_buckets_cover_top_500(self) -> None:
        d = json.loads(TOP_500_JSON.read_text(encoding="utf-8"))
        hist = d["bucket_histogram"]
        expected_buckets = {"simple", "etb", "ltb", "activated",
                            "continuous", "replacement", "triggered",
                            "spell", "complex"}
        # Every bucket name in the histogram is one of the 9 valid ones.
        for bucket in hist.keys():
            self.assertIn(bucket, expected_buckets)
        # At least the major buckets are non-empty.
        for required_nonempty in ("activated", "spell", "simple"):
            self.assertGreater(hist.get(required_nonempty, 0), 0)
        # Sum is 500.
        self.assertEqual(sum(hist.values()), 500)


if __name__ == "__main__":
    unittest.main()
