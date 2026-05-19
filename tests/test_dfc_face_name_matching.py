"""Regression tests for DFC / Adventure card-name matching.

Two surfaces were broken by exact-name-only matching (2026-05-18 incidents):

  1. `engine.db.find_card_by_name` — given a face name like
     'Tovolar, Dire Overlord', returned None because the cards table
     stores the canonical Scryfall name
     'Tovolar, Dire Overlord // Tovolar, the Midnight Scourge'.
     Consequence: `commander_archetype_brief_v1` returned
     `color_identity: []` for any DFC commander.

  2. `api.engine.deck_complete_engine_v1` candidate dedupe — checked
     `name in working_cards` with strict string equality, so a candidate
     'Decadent Dragon' (face-only) bypassed the dedupe and got added to
     a deck that already had 'Decadent Dragon // Expensive Taste'.
     Consequence: duplicate-physical-card additions surfaced as
     COMMANDER_DUPLICATE_NONBASIC violations on the next Complete pass.

Both fixes are now in place. These tests pin the new behavior so the
matching can't regress to exact-name-only.
"""
from __future__ import annotations

import unittest

from api.engine.deck_complete_engine_v1 import _face_variants


class FaceVariantsHelperTest(unittest.TestCase):
    """Pure-Python helper that powers the deck_complete dedupe."""

    def test_simple_card_returns_singleton_set(self) -> None:
        self.assertEqual(_face_variants("Sol Ring"), {"Sol Ring"})

    def test_dfc_returns_full_plus_both_faces(self) -> None:
        self.assertEqual(
            _face_variants("Decadent Dragon // Expensive Taste"),
            {
                "Decadent Dragon // Expensive Taste",
                "Decadent Dragon",
                "Expensive Taste",
            },
        )

    def test_face_only_input_is_singleton(self) -> None:
        # We don't try to expand face → full via DB here; that's
        # find_card_by_name's job. The dedupe set built from existing
        # deck cards is the side that contributes face halves.
        self.assertEqual(_face_variants("Decadent Dragon"), {"Decadent Dragon"})

    def test_dedupe_works_when_existing_is_full_and_candidate_is_face(self) -> None:
        existing_variants = _face_variants("Decadent Dragon // Expensive Taste")
        candidate_variants = _face_variants("Decadent Dragon")
        self.assertTrue(existing_variants & candidate_variants,
                        "face-only candidate must overlap with existing full DFC name")

    def test_dedupe_works_when_existing_is_face_and_candidate_is_full(self) -> None:
        existing_variants = _face_variants("Decadent Dragon")
        candidate_variants = _face_variants("Decadent Dragon // Expensive Taste")
        self.assertTrue(existing_variants & candidate_variants,
                        "full-DFC candidate must overlap with existing face-only deck card")

    def test_dedupe_works_via_back_face(self) -> None:
        existing_variants = _face_variants("Decadent Dragon // Expensive Taste")
        candidate_variants = _face_variants("Expensive Taste")
        self.assertTrue(existing_variants & candidate_variants,
                        "back-face candidate must overlap with full DFC name")

    def test_unrelated_dfcs_do_not_overlap(self) -> None:
        a = _face_variants("Decadent Dragon // Expensive Taste")
        b = _face_variants("Monster Manual // Zoological Study")
        self.assertEqual(a & b, set(),
                         "unrelated DFCs must not share any variant")


class FindCardByNameDFCTest(unittest.TestCase):
    """Live-DB test against the production snapshot. Skips if the production
    DB isn't available (e.g. in CI with only the guardrails fixture)."""

    PROD_SNAPSHOT = "20260217_190902_tagpass_20260222"

    def _find(self, name: str):
        from engine.db import find_card_by_name
        return find_card_by_name(self.PROD_SNAPSHOT, name)

    def setUp(self) -> None:
        # Skip if the production card row doesn't exist (CI / minimal-fixture env)
        if self._find("Sol Ring") is None:
            self.skipTest("production snapshot not available in this environment")

    def test_face_name_resolves_dfc_commander(self) -> None:
        """Tovolar — DFC werewolf commander. Caller passes the front face
        only; matcher must resolve to the canonical row with its real
        color_identity."""
        card = self._find("Tovolar, Dire Overlord")
        self.assertIsNotNone(card, "DFC commander front-face must resolve")
        self.assertEqual(card.get("name"),
                         "Tovolar, Dire Overlord // Tovolar, the Midnight Scourge",
                         "must prefer the canonical row (distinct faces) over data-artifact rows")
        # The bug surface — color_identity was [] before the fix.
        import json as _json
        ci_raw = card.get("color_identity")
        if isinstance(ci_raw, str):
            ci_raw = _json.loads(ci_raw)
        self.assertEqual(sorted(c.upper() for c in ci_raw), ["G", "R"],
                         f"Tovolar's color identity must populate (was [] before fix); got {ci_raw}")

    def test_face_name_resolves_adventure_card(self) -> None:
        """Adventure cards have the same '// '-joined name shape."""
        card = self._find("Decadent Dragon")
        self.assertIsNotNone(card, "Adventure front-face must resolve")
        self.assertEqual(card.get("name"), "Decadent Dragon // Expensive Taste")

    def test_back_face_also_resolves(self) -> None:
        """The Adventure side of the card should also match."""
        card = self._find("Expensive Taste")
        self.assertIsNotNone(card, "Adventure back-face must resolve to the parent DFC row")
        self.assertEqual(card.get("name"), "Decadent Dragon // Expensive Taste")

    def test_exact_full_dfc_name_still_works(self) -> None:
        """The pre-fix exact-match path is preserved."""
        card = self._find("Decadent Dragon // Expensive Taste")
        self.assertIsNotNone(card)
        self.assertEqual(card.get("name"), "Decadent Dragon // Expensive Taste")

    def test_non_dfc_card_unaffected(self) -> None:
        card = self._find("Sol Ring")
        self.assertIsNotNone(card)
        self.assertEqual(card.get("name"), "Sol Ring")

    def test_nonexistent_card_returns_none(self) -> None:
        self.assertIsNone(self._find("Not A Real Card Name xyz"))

    def test_face_name_with_competing_stub_prefers_real_card(self) -> None:
        """Bloomvine Regent (Tarkir: Dragonstorm Omen front face) appears as
        a face on TWO DB rows: the real card 'Bloomvine Regent // Claim
        Territory' and a stub 'Bloomvine Regent // Bloomvine Regent' with
        type_line 'Card // Card'. The ORDER BY in find_card_by_name must
        rank the real card first (distinct faces preferred over identical
        faces) so the front-face lookup returns the right one deterministically."""
        card = self._find("Bloomvine Regent")
        self.assertIsNotNone(card)
        self.assertEqual(card.get("name"), "Bloomvine Regent // Claim Territory")

    def test_scavenger_regent_face_resolves_to_real_omen_card(self) -> None:
        """Same pattern as Bloomvine — second Tarkir Omen Dragon."""
        card = self._find("Scavenger Regent")
        self.assertIsNotNone(card)
        self.assertEqual(card.get("name"), "Scavenger Regent // Exude Toxin")


class DecklistResolveDFCDisambiguationTest(unittest.TestCase):
    """Regression tests against `decklist_resolve_v1` — the separate resolver
    that builds its own face index from card names. Bug: the cards table
    ships ~2,030 stub rows shaped 'X // X' with type_line 'Card // Card'
    (placeholder oracle_ids) which polluted the face index, producing
    CARD_NAME_AMBIGUOUS when a user typed a front-face name that also had
    a stub. Fix: skip stub rows from face indexing in `_load_cards_index`.
    """

    PROD_SNAPSHOT = "20260217_190902_tagpass_20260222"

    def setUp(self) -> None:
        from engine.db import find_card_by_name
        if find_card_by_name(self.PROD_SNAPSHOT, "Sol Ring") is None:
            self.skipTest("production snapshot not available in this environment")

    def _resolve(self, names):
        from api.engine.decklist_resolve_v1 import resolve_parsed_decklist
        items = [{"name_raw": n, "count": 1, "line_no": i + 1}
                 for i, n in enumerate(names)]
        return resolve_parsed_decklist({"items": items}, self.PROD_SNAPSHOT, name_overrides_v1=[])

    def test_bloomvine_regent_resolves_unambiguously(self) -> None:
        """Front-face lookup must return exactly one candidate (not AMBIGUOUS)."""
        r = self._resolve(["Bloomvine Regent"])
        self.assertEqual(len(r["resolved_cards"]), 1,
                         f"expected unique resolution; got unknowns={r.get('unknowns')}")
        self.assertEqual(
            r["resolved_cards"][0]["name"],
            "Bloomvine Regent // Claim Territory",
        )
        self.assertEqual(r["unknowns"], [])

    def test_scavenger_regent_resolves_unambiguously(self) -> None:
        r = self._resolve(["Scavenger Regent"])
        self.assertEqual(len(r["resolved_cards"]), 1)
        self.assertEqual(
            r["resolved_cards"][0]["name"],
            "Scavenger Regent // Exude Toxin",
        )

    def test_back_face_of_omen_card_resolves(self) -> None:
        """Back face of Tarkir Omen card (the Sorcery side) should also
        resolve to the same DFC parent."""
        r = self._resolve(["Claim Territory"])
        self.assertEqual(len(r["resolved_cards"]), 1)
        self.assertEqual(
            r["resolved_cards"][0]["name"],
            "Bloomvine Regent // Claim Territory",
        )

    def test_decadent_dragon_adventure_still_resolves(self) -> None:
        """Sanity: the prior fix (split/adventure DFC) still works through
        this resolver."""
        r = self._resolve(["Decadent Dragon"])
        self.assertEqual(len(r["resolved_cards"]), 1)
        self.assertEqual(
            r["resolved_cards"][0]["name"],
            "Decadent Dragon // Expensive Taste",
        )

    def test_tovolar_transform_dfc_still_resolves(self) -> None:
        """Sanity: transform-layout DFC (werewolf commander) still works."""
        r = self._resolve(["Tovolar, Dire Overlord"])
        self.assertEqual(len(r["resolved_cards"]), 1)
        self.assertEqual(
            r["resolved_cards"][0]["name"],
            "Tovolar, Dire Overlord // Tovolar, the Midnight Scourge",
        )

    def test_truly_nonexistent_card_returns_not_found(self) -> None:
        """If a name genuinely doesn't exist in the DB, the resolver should
        report CARD_NOT_FOUND with an empty candidates list — NOT mask the
        missing-data condition as AMBIGUOUS."""
        r = self._resolve(["Definitely Not A Real Card xyz123"])
        self.assertEqual(r["resolved_cards"], [])
        self.assertEqual(len(r["unknowns"]), 1)
        self.assertEqual(r["unknowns"][0]["reason_code"], "CARD_NOT_FOUND")
        self.assertEqual(r["unknowns"][0]["candidates"], [])


if __name__ == "__main__":
    unittest.main()
