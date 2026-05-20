"""v1.7.2 Stage 1 — Deck-combo insights engine layer.

v1.7 Stage 2 wired `combo_enabler_reasons_v1` to annotate engine-added
cards that complete a 2-card combo. The v1.7 Cowork browser-walk
(2026-05-16) found near-zero production coverage because the engine's
primitive-coverage-driven completion rarely picks a combo partner —
the post-hoc annotator has nothing to flag.

v1.7.2 closes the visibility gap with a NEW layer that scans the
FINAL completed deck (commander + initial deckText + engine adds)
against the v2 pair index + outcome pack and surfaces TWO insights:

  1. `detected_combos_v1` — pairs where BOTH halves are present in
     the final deck. UI message: "Your deck contains X + Y → outcome".
  2. `missing_partners_v1` — pairs where EXACTLY ONE half is present
     in the final deck AND the partner is NOT being added. UI message:
     "Add Y to enable a combo with X → outcome".

Fixture pair (confirmed against the Stage 1.5 outcome pack):
  - Storm-Kiln Artist  a145ff8c-5812-4bcb-bd16-9839dc25121d
  - Haze of Rage       f17d0fb8-c157-43b8-be26-f5ba4c6aed14
  - variant_id 3940-5195 → "Infinite colored mana; Infinitely powerful
    creatures…; Infinite magecraft triggers; Infinite storm count;
    Infinite Treasure tokens"

Storm-Kiln Artist has exactly 4 partners with outcomes in the pack
(variants 326-5195, 2655-5195, 3718-5195, 3940-5195) — the
missing-partner test asserts ≥4.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


FIXTURE_SNAPSHOT_ID = "DECK_COMBO_INSIGHTS_TEST_SNAPSHOT"

STORM_KILN_ARTIST_ORACLE = "a145ff8c-5812-4bcb-bd16-9839dc25121d"
HAZE_OF_RAGE_ORACLE = "f17d0fb8-c157-43b8-be26-f5ba4c6aed14"
TARGET_VARIANT_ID = "3940-5195"

# Pillar A.7 alignment fixture — Old Gnawbone + Hellkite Charger is
# the Spellbook O/core_plus combo (variant 1800-3398) that exposed the
# divergence between the bracket verifier (combo_brackets_v1.json,
# auto-bumped) and the UI insights layer (returned detected_combos_v1:
# []). After the fix both surfaces share the same dataset and detect
# the pair.
OLD_GNAWBONE_ORACLE = "d4f28a4b-d821-4132-bdec-4c528318f8e2"
HELLKITE_CHARGER_ORACLE = "dff3f4c7-f792-4ca3-9b0a-0738e70664d9"
OLD_GNAWBONE_VARIANT_ID = "1800-3398"

# Storm-Kiln Artist's 4 known combo partners. Pillar A.7 alignment
# switched the insights layer's data source to combo_brackets_v1.json,
# so the partner NAMES below now mirror that file's `card_names` for
# the four SKA variants (combo_size==2, no-extra-prerequisite):
#   - variant 2655-5195 → Fury Storm
#   - variant 326-5195  → Chain of Smog
#   - variant 3718-5195 → Chain of Acid
#   - variant 3940-5195 → Haze of Rage
# Oracle_ids are synthetic — the test asserts ≥4 missing-partner
# entries + the shape contract, not which exact oracle_ids resolve.
SKA_PARTNERS = [
    ("0b1a27bd-bb98-44f8-8357-666fabfeabf0", "Chain of Acid"),    # variant 3718-5195
    ("371fa9e3-5432-4f2f-89d4-55061b0b4e57", "Fury Storm"),       # variant 2655-5195
    ("ea14c26b-bf2f-48b4-b879-6e63069ded1f", "Chain of Smog"),    # variant 326-5195
    (HAZE_OF_RAGE_ORACLE, "Haze of Rage"),                         # variant 3940-5195
]


def _create_combo_insights_fixture_db(tmp_dir: Path) -> Path:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    db_path = (tmp_dir / "deck_combo_insights_fixture.sqlite").resolve()
    schema_sql = (
        Path(__file__).resolve().parents[1] / "schemas" / "schema.sql"
    ).read_text(encoding="utf-8")
    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(schema_sql)
        con.execute(
            "INSERT INTO snapshots (snapshot_id, created_at, source, "
            "scryfall_bulk_uri, scryfall_bulk_updated_at, manifest_json) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (FIXTURE_SNAPSHOT_ID, "2026-01-01T00:00:00+00:00",
             "pytest_deck_combo_insights", "local://", "2026-01-01T00:00:00+00:00", "{}"),
        )
        rows = [
            # Storm-Kiln Artist (the deck-side combo half)
            (FIXTURE_SNAPSHOT_ID, STORM_KILN_ARTIST_ORACLE, "Storm-Kiln Artist",
             "{1}{R}{R}", 3.0, "Creature — Devil Artificer", "[\"R\"]", "[\"R\"]", "{}", "[]"),
        ]
        for oid, name in SKA_PARTNERS:
            rows.append((FIXTURE_SNAPSHOT_ID, oid, name,
                         "{R}", 1.0, "Sorcery", "[\"R\"]", "[\"R\"]", "{}", "[]"))
        # Pillar A.7 repro fixture — Old Gnawbone + Hellkite Charger
        # (the Spellbook O/core_plus combo variant 1800-3398). Both
        # cards mono-red color_identity so the test commander
        # (Krenko, mono-red) is legal for them.
        rows.append((FIXTURE_SNAPSHOT_ID, OLD_GNAWBONE_ORACLE,
                     "Old Gnawbone", "{4}{G}{G}", 6.0,
                     "Legendary Creature — Dragon", "[\"G\"]", "[\"G\"]", "{}", "[]"))
        rows.append((FIXTURE_SNAPSHOT_ID, HELLKITE_CHARGER_ORACLE,
                     "Hellkite Charger", "{4}{R}{R}", 6.0,
                     "Creature — Dragon", "[\"R\"]", "[\"R\"]", "{}", "[]"))
        # Commander (mono-red so all SKA partners are color-legal).
        rows.append((FIXTURE_SNAPSHOT_ID, "11111111-1111-1111-1111-111111111111",
                     "Krenko, Mob Boss", "{2}{R}{R}", 4.0,
                     "Legendary Creature — Goblin Warrior", "[\"R\"]", "[\"R\"]", "{}", "[]"))
        # A 5-color commander row for the Old Gnawbone + Hellkite
        # Charger repro test (Gnawbone is mono-green, Charger is
        # mono-red — the simplest legal commander is The Ur-Dragon,
        # the spec-cited deck's commander).
        rows.append((FIXTURE_SNAPSHOT_ID, "22222222-2222-2222-2222-222222222222",
                     "The Ur-Dragon", "{4}{W}{U}{B}{R}{G}", 9.0,
                     "Legendary Creature — Dragon Avatar",
                     "[\"W\",\"U\",\"B\",\"R\",\"G\"]",
                     "[\"W\",\"U\",\"B\",\"R\",\"G\"]", "{}", "[]"))
        con.executemany(
            "INSERT INTO cards (snapshot_id, oracle_id, name, mana_cost, cmc, "
            "type_line, colors, color_identity, legalities_json, primitives_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        con.commit()
    finally:
        con.close()
    return db_path


@contextmanager
def _set_db_env(db_path: Path) -> Iterator[None]:
    previous = os.environ.get("MTG_ENGINE_DB_PATH")
    os.environ["MTG_ENGINE_DB_PATH"] = str(db_path.resolve())
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("MTG_ENGINE_DB_PATH", None)
        else:
            os.environ["MTG_ENGINE_DB_PATH"] = previous


class DeckComboInsightsV1Tests(unittest.TestCase):
    _tmp_dir_ctx: tempfile.TemporaryDirectory[str] | None = None
    _db_env_ctx = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls._tmp_dir_ctx = tempfile.TemporaryDirectory()
        db_path = _create_combo_insights_fixture_db(Path(cls._tmp_dir_ctx.name))
        cls._db_env_ctx = _set_db_env(db_path)
        cls._db_env_ctx.__enter__()

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            if cls._db_env_ctx is not None:
                cls._db_env_ctx.__exit__(None, None, None)
                cls._db_env_ctx = None
        finally:
            if cls._tmp_dir_ctx is not None:
                cls._tmp_dir_ctx.cleanup()
                cls._tmp_dir_ctx = None
            super().tearDownClass()

    def test_detected_combo_when_both_halves_present(self) -> None:
        from api.engine.layers.deck_combo_insights_v1 import (
            compute_deck_combo_insights_v1,
        )

        result = compute_deck_combo_insights_v1(
            db_snapshot_id=FIXTURE_SNAPSHOT_ID,
            commander_names=["Krenko, Mob Boss"],
            deck_cards_after_completion=["Storm-Kiln Artist", "Haze of Rage"],
        )

        self.assertIsInstance(result, dict)
        detected = result.get("detected_combos_v1") or []
        missing = result.get("missing_partners_v1") or []

        # The detected combo for variant 3940-5195 must surface.
        ska_haze_entries = [
            e for e in detected
            if isinstance(e, dict)
            and {e.get("card_a_name"), e.get("card_b_name")}
                == {"Storm-Kiln Artist", "Haze of Rage"}
        ]
        self.assertEqual(
            len(ska_haze_entries), 1,
            f"Expected exactly one detected_combos entry for the SKA + Haze pair; "
            f"got {len(ska_haze_entries)}. detected_combos_v1={detected!r}",
        )
        entry = ska_haze_entries[0]
        self.assertEqual(entry.get("variant_id"), TARGET_VARIANT_ID)
        self.assertIn("Infinite", entry.get("combo_outcome_label") or "")
        self.assertIn("storm count", entry.get("combo_outcome_label") or "")

        # When BOTH halves are in the deck, the SKA+Haze pair must NOT
        # appear in missing_partners_v1 (it's not missing — it's there).
        for m in missing:
            if not isinstance(m, dict):
                continue
            self.assertNotEqual(
                {m.get("present_card_name"), m.get("partner_card_name")},
                {"Storm-Kiln Artist", "Haze of Rage"},
                f"SKA+Haze should not be in missing_partners_v1 when both halves present; "
                f"missing entry = {m!r}",
            )

    def test_missing_partners_when_only_one_half_present(self) -> None:
        from api.engine.layers.deck_combo_insights_v1 import (
            compute_deck_combo_insights_v1,
        )

        result = compute_deck_combo_insights_v1(
            db_snapshot_id=FIXTURE_SNAPSHOT_ID,
            commander_names=["Krenko, Mob Boss"],
            deck_cards_after_completion=["Storm-Kiln Artist"],
        )

        detected = result.get("detected_combos_v1") or []
        missing = result.get("missing_partners_v1") or []

        # No combo half pairing yet (only Storm-Kiln Artist present).
        self.assertEqual(
            detected, [],
            f"With only one half present, detected_combos_v1 should be empty. got={detected!r}",
        )

        # Storm-Kiln Artist should surface ALL 4 of its known partners
        # (the outcome pack carries exactly 4 SKA variants).
        ska_missing = [
            m for m in missing
            if isinstance(m, dict)
            and m.get("present_card_name") == "Storm-Kiln Artist"
        ]
        self.assertGreaterEqual(
            len(ska_missing), 4,
            f"Expected ≥4 SKA missing-partner entries; got {len(ska_missing)}. "
            f"missing entries: {[m.get('partner_card_name') for m in ska_missing]}",
        )

        # Each missing entry carries the structured payload the UI panel
        # needs (variant_id + outcome label + partner identifiers).
        for m in ska_missing:
            self.assertIn("variant_id", m)
            self.assertIn("combo_outcome_label", m)
            self.assertIn("partner_card_oracle_id", m)
            self.assertIn("partner_card_name", m)
            self.assertIn("present_card_oracle_id", m)
            self.assertEqual(m.get("present_card_name"), "Storm-Kiln Artist")

    def test_old_gnawbone_hellkite_charger_detected(self) -> None:
        """Pillar A.7 alignment repro: Old Gnawbone + Hellkite Charger
        (Spellbook O/core_plus combo, variant 1800-3398) MUST surface
        in detected_combos_v1. Before the fix, the v2 oracle_id-keyed
        pair index missed this pair (returned []) while the bracket
        verifier's combo_brackets_v1.json dataset correctly auto-bumped
        for it — divergent datasets. After the fix both layers consume
        the same 3679-pair filtered subset."""
        from api.engine.layers.deck_combo_insights_v1 import (
            compute_deck_combo_insights_v1,
        )

        result = compute_deck_combo_insights_v1(
            db_snapshot_id=FIXTURE_SNAPSHOT_ID,
            commander_names=["The Ur-Dragon"],
            deck_cards_after_completion=["Old Gnawbone", "Hellkite Charger"],
        )

        detected = result.get("detected_combos_v1") or []
        entries = [
            e for e in detected
            if isinstance(e, dict)
            and {e.get("card_a_name"), e.get("card_b_name")}
            == {"Old Gnawbone", "Hellkite Charger"}
        ]
        self.assertEqual(
            len(entries), 1,
            f"Expected the Old Gnawbone + Hellkite Charger pair in "
            f"detected_combos_v1 after Pillar A.7 alignment; got "
            f"{len(entries)} matching entries. detected={detected!r}",
        )
        entry = entries[0]
        self.assertEqual(entry.get("variant_id"), OLD_GNAWBONE_VARIANT_ID)
        # Label sourcing — outcomes pack carries a non-empty label for
        # 1800-3398; if it's ever stripped the results-array fallback
        # joins the bracket entry's `results` (which mentions infinite
        # combat phases). Either path must yield a non-empty label.
        label = entry.get("combo_outcome_label") or ""
        self.assertTrue(
            isinstance(label, str) and label.strip() != "",
            f"combo_outcome_label must be non-empty; got {label!r}",
        )

    def test_no_false_positives_when_deck_has_no_combo_pairs(self) -> None:
        """Pillar A.7: a deck with cards that do NOT participate in any
        combo_brackets pair must surface zero detected combos. Krenko
        alone (commander only) + a single non-combo basic land row
        triggers zero matches in the index — the commander's own
        Krenko + Intruder Alarm pair is in the bracket index, but
        Intruder Alarm is absent from the fixture so the missing-
        partner entry gets dropped during oracle_id resolution."""
        from api.engine.layers.deck_combo_insights_v1 import (
            compute_deck_combo_insights_v1,
        )

        result = compute_deck_combo_insights_v1(
            db_snapshot_id=FIXTURE_SNAPSHOT_ID,
            commander_names=["Krenko, Mob Boss"],
            # Storm-Kiln Artist is excluded — Krenko's only fixture-
            # resolvable combo partner is Intruder Alarm, which the
            # fixture intentionally does not provide, so neither half
            # of any combo is fully resolvable for this deck.
            deck_cards_after_completion=[],
        )

        detected = result.get("detected_combos_v1") or []
        self.assertEqual(
            detected, [],
            f"No combos present → detected_combos_v1 must be []. got={detected!r}",
        )


if __name__ == "__main__":
    unittest.main()
