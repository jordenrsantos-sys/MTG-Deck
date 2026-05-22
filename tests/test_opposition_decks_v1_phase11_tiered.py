"""Mega-task v5 Phase 11 — Tiered opposition registry tests.

Phase 11 expanded `opposition_decks_v1.json` with the new `opposition_tier`
field (0 = precon-equivalent, 1 = mid-tier, 2 = high-tier / cEDH for B5)
and added new entries to reach 3+ entries per (bracket B2-B5, tier 0/1/2).
B1 keeps mid-tier-only (per kickoff: "drop B1 if precons aren't well-
distinguished").

These tests lock the new schema and the loader helpers:
  - Every entry has `opposition_tier` populated (no missing/None).
  - Tier values are in {0, 1, 2}.
  - Every (B2-B5) × (tier 0,1,2) has at least 3 entries.
  - `filter_by_tier` and `filter_by_bracket_and_tier` return correct entries.
  - Pre-Phase-11 entries (no tier field) default to tier 1 in the loader.
"""
from __future__ import annotations

import json
import unittest
from pathlib import Path

from api.engine.playtest.opposition_decks_v1 import (
    load_registry, registry_summary,
    filter_by_tier, filter_by_bracket_and_tier,
)


REGISTRY_PATH = (
    Path(__file__).resolve().parents[1]
    / "api" / "engine" / "data" / "playtest" / "opposition_decks_v1.json"
)


def _raw_registry() -> dict:
    with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


class TieredSchemaTest(unittest.TestCase):
    def test_every_entry_has_opposition_tier(self) -> None:
        reg = _raw_registry()
        missing = [
            e.get("corpus_id") for e in reg["entries"]
            if "opposition_tier" not in e
        ]
        self.assertEqual(missing, [],
                         f"entries missing opposition_tier: {missing}")

    def test_tier_values_in_set_0_1_2(self) -> None:
        reg = _raw_registry()
        bad = [
            (e.get("corpus_id"), e.get("opposition_tier"))
            for e in reg["entries"]
            if e.get("opposition_tier") not in (0, 1, 2)
        ]
        self.assertEqual(bad, [], f"invalid opposition_tier values: {bad}")

    def test_schema_version_advertises_tiered(self) -> None:
        reg = _raw_registry()
        self.assertIn("schema_version", reg)
        self.assertEqual(reg["schema_version"], "opposition_v1.1_tiered")

    def test_tier_definitions_documented(self) -> None:
        reg = _raw_registry()
        defs = reg.get("tier_definitions") or {}
        for k in ("0", "1", "2"):
            self.assertIn(k, defs, f"tier definition for {k!r} missing")
            self.assertGreater(len(defs[k].strip()), 20,
                               f"tier {k} definition too short to be useful")


class TieredCoverageTest(unittest.TestCase):
    """Kickoff requires 3+ entries per (B2-B5, tier 0/1/2). B1 keeps mid-
    tier-only (per kickoff: drop precons there)."""

    def test_each_b2_b5_tier_has_at_least_3_entries(self) -> None:
        missing: list[str] = []
        for bracket in ("B2", "B3", "B4", "B5"):
            for tier in (0, 1, 2):
                entries = filter_by_bracket_and_tier(bracket, tier)
                if len(entries) < 3:
                    missing.append(f"{bracket} tier{tier}: only {len(entries)}")
        self.assertEqual(missing, [], f"undersized cells: {missing}")

    def test_total_entries_36_or_more(self) -> None:
        reg = _raw_registry()
        # 4 brackets × 3 tiers × 3 = 36 minimum from Phase 11, plus B1
        # mid-tier entries from the legacy 19.
        self.assertGreaterEqual(len(reg["entries"]), 36)


class FilterHelpersTest(unittest.TestCase):
    def test_filter_by_tier_returns_only_matching_tier(self) -> None:
        for t in (0, 1, 2):
            entries = filter_by_tier(t)
            self.assertGreater(len(entries), 0, f"tier {t} should have entries")
            for e in entries:
                self.assertEqual(int(e.get("opposition_tier", -1)), t)

    def test_filter_by_bracket_and_tier_intersection(self) -> None:
        entries = filter_by_bracket_and_tier("B3", 0)
        self.assertGreater(len(entries), 0)
        for e in entries:
            self.assertEqual(e["bracket"], "B3")
            self.assertEqual(int(e["opposition_tier"]), 0)

    def test_filter_by_unknown_bracket_returns_empty(self) -> None:
        self.assertEqual(filter_by_bracket_and_tier("BX", 0), [])

    def test_filter_by_non_int_tier_returns_empty(self) -> None:
        # Defensive: non-int input gracefully handled.
        self.assertEqual(filter_by_tier("0"), [])  # type: ignore[arg-type]
        self.assertEqual(filter_by_bracket_and_tier("B3", "0"), [])  # type: ignore[arg-type]


class RegistrySummaryTest(unittest.TestCase):
    def test_summary_includes_tier_breakdown(self) -> None:
        s = registry_summary()
        self.assertIn("per_bracket_tier_count", s)
        # B3 should have all three tiers represented.
        b3 = s["per_bracket_tier_count"].get("B3", {})
        for t in ("tier0", "tier1", "tier2"):
            self.assertIn(t, b3, f"B3 missing {t}")
            self.assertGreaterEqual(b3[t], 3)

    def test_summary_total_matches_registry_size(self) -> None:
        s = registry_summary()
        reg = _raw_registry()
        self.assertEqual(s["total_entries"], len(reg["entries"]))


class BackwardCompatTest(unittest.TestCase):
    def test_pre_phase11_entry_without_tier_defaults_to_tier_1(self) -> None:
        """If a future ingest produces an entry without opposition_tier,
        filter_by_tier(1) should still surface it (defensive default)."""
        # We can't mutate the on-disk file inside this test, but we can
        # exercise the default path by calling the helper directly on a
        # synthetic entry shape.
        from api.engine.playtest import opposition_decks_v1 as mod
        # Snapshot + monkey-patch _cache for this test only.
        original = mod._cache
        try:
            mod._cache = {
                "version": "test",
                "entries": [
                    {"corpus_id": "x", "commander": "X", "bracket": "B3",
                     "archetype_hint": "x", "role_tag": "x"}
                ],
            }
            r = filter_by_tier(1)
            self.assertEqual(len(r), 1, "tier-less entry should default to tier 1")
        finally:
            mod._cache = original


if __name__ == "__main__":
    unittest.main()
