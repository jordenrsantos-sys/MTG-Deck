"""Mega-task v5 Phase 6 — Voyage color-identity filter edge-case tests.

Background: the original Phase 6 spec assumed
agent_semantic_retrieval_v1.query_neighbors had a color-filter bug that
caused iter 5's voyage_semantic_avg=1.8. Phase 5's venv recovery showed
the actual root cause was a dep gap (numpy/voyageai missing on the
upgraded-but-not-reinstalled Python 3.14 venv) — the filter logic itself
at lines 253-261 (set.issubset on uppercased color identities) is
correct on inspection and works correctly on live data after the dep fix.

These tests lock the contract in place so a future refactor can't
silently regress it. Covers the color-identity-filter shapes that real
commanders produce:

  - mono-color (Krenko = R)
  - 2-color (Yuriko = UB)
  - 3-color (Edgar = BRW)
  - 4-color (Atraxa = WUBG)
  - 5-color (Ur-Dragon = WUBRG)
  - colorless cards must NEVER be filtered out
  - empty / None filter must NOT filter at all
  - filter and stored identities must be case-insensitive
  - mixed-color cards must be kept iff every color is in the filter set
"""
from __future__ import annotations

import sqlite3
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from api.engine.layers import agent_semantic_retrieval_v1 as sr


def _pack(vec):
    return struct.pack(f"<{len(vec)}f", *vec)


# A spread of 11 cards covering every color-identity shape we care about.
# All are placed at orthogonal vectors so similarity ordering is stable
# and we can assert on membership without worrying about ranking.
_COLOR_FIXTURE_CARDS = [
    ("Colorless Engine",   "",        [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    ("Mono W Soldier",     "W",       [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    ("Mono U Wizard",      "U",       [0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    ("Mono B Vampire",     "B",       [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    ("Mono R Goblin",      "R",       [0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
    ("Mono G Druid",       "G",       [0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0]),
    ("UB Ninja",           "U,B",     [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0]),
    ("BRW Vampire Lord",   "B,R,W",   [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0]),
    ("WUBG Praetor",       "W,U,B,G", [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0]),
    ("WUBRG Dragon",       "W,U,B,R,G", [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0]),
    # A "Mono r" lowercase entry to stress-test case-insensitivity on the
    # stored identity side. Filter should match "R" identities even if the
    # DB happens to hold lower-case (defensive — current ingest uppercases).
    ("Lowercase r Ember",  "r",       [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.5]),
]

# A "query anchor" — vector positioned equidistant to every card so the
# ordering is dominated by the filter membership test, not by raw cosine.
_ANCHOR_VEC = [0.10, 0.10, 0.10, 0.10, 0.10, 0.10, 0.10, 0.10, 0.10, 0.10]


def _build_color_fixture_index(path: Path) -> None:
    con = sr._ensure_schema(path)
    try:
        # Anchor card the test will pass to query_neighbors.
        con.execute(
            "INSERT INTO card_embeddings VALUES (?,?,?,?,?,?,?)",
            ("Test Anchor", "", "Artifact", "Tap: tutor.", 1, "1993-12-01",
             _pack(_ANCHOR_VEC)),
        )
        for name, ci, vec in _COLOR_FIXTURE_CARDS:
            con.execute(
                "INSERT INTO card_embeddings VALUES (?,?,?,?,?,?,?)",
                (name, ci, "Creature - Test", "test text", 2, "2020-01-01",
                 _pack(vec)),
            )
        sr._meta_set(con, "snapshot_id", "phase6_color_fixture")
        sr._meta_set(con, "model", "test-model")
        sr._meta_set(con, "card_count", str(len(_COLOR_FIXTURE_CARDS) + 1))
        con.commit()
    finally:
        con.close()


class Phase6ColorIdentityEdgeCases(unittest.TestCase):
    """Lock the color_identity_filter contract for every real-commander shape."""

    def setUp(self) -> None:
        self._tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self._tmp.close()
        self._path = Path(self._tmp.name)
        _build_color_fixture_index(self._path)
        sr._CACHE["loaded_path"] = None
        self._patcher = patch.object(sr, "EMBEDDING_DB_PATH", self._path)
        self._patcher.start()

    def tearDown(self) -> None:
        self._patcher.stop()
        sr._CACHE["loaded_path"] = None
        self._path.unlink(missing_ok=True)

    def _names(self, filter_):
        out = sr.query_neighbors("Test Anchor", k=20, color_identity_filter=filter_)
        return {n["name"] for n in out}

    def test_no_filter_returns_every_card(self) -> None:
        """None / unset filter must NOT drop anything."""
        names = self._names(None)
        # All 11 fixture cards reachable (anchor excluded by self-match skip).
        self.assertEqual(len(names), 11,
                         f"no filter should return all 11 cards, got {sorted(names)}")

    def test_empty_list_filter_treated_as_no_filter(self) -> None:
        """An empty list passed for color_identity_filter does NOT drop
        anything — the call site uses `sorted(color_identity)` which can be
        [] for a colorless commander."""
        names = self._names([])
        # Note: the current implementation treats empty list as no filter
        # since `[] or None` is None semantics. This pins that behavior.
        self.assertEqual(len(names), 11,
                         f"empty filter should return all 11 cards, got {sorted(names)}")

    def test_mono_R_filter_krenko_shape(self) -> None:
        """Krenko, Mob Boss is mono-R. Subset of R: colorless + mono-R only.
        Multi-color and non-R cards must all be filtered out."""
        names = self._names(["R"])
        self.assertIn("Colorless Engine", names, "colorless must pass any filter")
        self.assertIn("Mono R Goblin", names, "mono-R must pass R filter")
        self.assertIn("Lowercase r Ember", names,
                      "stored lowercase 'r' must still match filter 'R' "
                      "(case-insensitive)")
        for off in ("Mono W Soldier", "Mono U Wizard", "Mono B Vampire",
                    "Mono G Druid", "UB Ninja", "BRW Vampire Lord",
                    "WUBG Praetor", "WUBRG Dragon"):
            self.assertNotIn(off, names,
                             f"{off} must not pass R-only filter")

    def test_two_color_UB_filter_yuriko_shape(self) -> None:
        """Yuriko, the Tiger's Shadow is UB. Subset of UB: colorless + U + B
        + UB. Crucially the UB hybrid 'UB Ninja' must pass — that was the
        case that scored 0 picks pre-venv-recovery on iter 5."""
        names = self._names(["U", "B"])
        for must in ("Colorless Engine", "Mono U Wizard", "Mono B Vampire",
                     "UB Ninja"):
            self.assertIn(must, names,
                          f"{must} must pass UB filter (subset)")
        for off in ("Mono W Soldier", "Mono R Goblin", "Mono G Druid",
                    "BRW Vampire Lord", "WUBG Praetor", "WUBRG Dragon",
                    "Lowercase r Ember"):
            self.assertNotIn(off, names,
                             f"{off} must not pass UB filter "
                             f"(contains a color outside UB)")

    def test_three_color_BRW_filter_edgar_shape(self) -> None:
        """Edgar Markov is BRW. Subset of BRW: colorless + B + R + W +
        the BRW hybrid card. U-, G-, or 4-color identities must drop."""
        names = self._names(["B", "R", "W"])
        for must in ("Colorless Engine", "Mono W Soldier", "Mono B Vampire",
                     "Mono R Goblin", "BRW Vampire Lord",
                     "Lowercase r Ember"):
            self.assertIn(must, names,
                          f"{must} must pass BRW filter")
        for off in ("Mono U Wizard", "Mono G Druid", "UB Ninja",
                    "WUBG Praetor", "WUBRG Dragon"):
            self.assertNotIn(off, names,
                             f"{off} must not pass BRW filter")

    def test_four_color_WUBG_filter_atraxa_shape(self) -> None:
        """Atraxa, Praetors' Voice is WUBG. Subset rule keeps colorless +
        every mono-color subset of WUBG + UB hybrid + WUBG itself; drops
        anything that touches R."""
        names = self._names(["W", "U", "B", "G"])
        for must in ("Colorless Engine", "Mono W Soldier", "Mono U Wizard",
                     "Mono B Vampire", "Mono G Druid", "UB Ninja",
                     "WUBG Praetor"):
            self.assertIn(must, names,
                          f"{must} must pass WUBG filter")
        for off in ("Mono R Goblin", "Lowercase r Ember",
                    "BRW Vampire Lord", "WUBRG Dragon"):
            self.assertNotIn(off, names,
                             f"{off} must not pass WUBG filter (contains R)")

    def test_five_color_WUBRG_filter_ur_dragon_shape(self) -> None:
        """The Ur-Dragon is WUBRG. The filter is the full color set so
        every fixture card except the test anchor self should pass."""
        names = self._names(["W", "U", "B", "R", "G"])
        self.assertEqual(len(names), 11,
                         f"5-color filter should return all 11 cards, "
                         f"got {sorted(names)}")

    def test_filter_is_case_insensitive_on_filter_side(self) -> None:
        """Filter passed in lowercase still matches uppercase stored ids."""
        names = self._names(["b"])
        self.assertIn("Mono B Vampire", names,
                      "lowercase 'b' filter must still match stored 'B' id")

    def test_colorless_cards_pass_under_every_filter_shape(self) -> None:
        """A colorless artifact (empty color_identity) must NEVER be
        filtered out, regardless of the filter set's contents."""
        for f in (["W"], ["U"], ["B"], ["R"], ["G"],
                  ["U", "B"], ["B", "R", "W"],
                  ["W", "U", "B", "G"], ["W", "U", "B", "R", "G"]):
            self.assertIn(
                "Colorless Engine", self._names(f),
                f"colorless must pass filter={f}",
            )


if __name__ == "__main__":
    unittest.main()
