"""Tests for import_url_v1 (Engine-4A URL importer registry + dispatch)."""
from __future__ import annotations

import unittest
from typing import Any, Dict

from api.import_url_v1 import (
    IMPORT_URL_V1_VERSION,
    REASON_BAD_RESPONSE_SHAPE,
    REASON_INVALID_URL,
    REASON_NO_DECK_ID,
    REASON_SOURCE_DEFERRED,
    REASON_UNKNOWN_SOURCE,
    STATUS_DEFERRED,
    STATUS_FETCH_ERROR,
    STATUS_INVALID_REQUEST,
    STATUS_OK,
    STATUS_PARSE_ERROR,
    SOURCE_REGISTRY,
    _identify_source,
    _parse_archidekt,
    import_from_url,
    list_supported_sources,
)


def _archidekt_payload(cards):
    """Minimal Archidekt API response stub for parser tests."""
    return {"id": 8000000, "cards": cards}


def _archidekt_card(*, name, oracle_id, quantity=1, categories=None):
    return {
        "quantity": quantity,
        "categories": list(categories or []),
        "card": {"oracleCard": {"uid": oracle_id, "name": name}},
    }


class SourceRegistryTests(unittest.TestCase):
    def test_archidekt_active(self) -> None:
        spec = SOURCE_REGISTRY["archidekt"]
        self.assertTrue(spec.enabled)
        self.assertIsNone(spec.deferred_reason)

    def test_moxfield_deferred_with_cloudflare_reason(self) -> None:
        spec = SOURCE_REGISTRY["moxfield"]
        self.assertFalse(spec.enabled)
        self.assertIn("cloudflare", (spec.deferred_reason or "").lower())

    def test_edhrec_deferred_with_unverified_reason(self) -> None:
        spec = SOURCE_REGISTRY["edhrec"]
        self.assertFalse(spec.enabled)
        self.assertIn("unverified", (spec.deferred_reason or "").lower())

    def test_list_supported_sources_sorted_alphabetically(self) -> None:
        sources = list_supported_sources()
        names = [s["name"] for s in sources]
        self.assertEqual(names, sorted(names))


class IdentifySourceTests(unittest.TestCase):
    def test_archidekt_url_matches(self) -> None:
        result = _identify_source("https://archidekt.com/decks/8000000/")
        self.assertIsNotNone(result)
        spec, groups = result  # type: ignore[misc]
        self.assertEqual(spec.name, "archidekt")
        self.assertEqual(groups["deck_id"], "8000000")

    def test_moxfield_url_matches_but_returns_deferred(self) -> None:
        result = _identify_source("https://moxfield.com/decks/zNVMaTl9JEStdcSt1Q12pg")
        self.assertIsNotNone(result)
        spec, _ = result  # type: ignore[misc]
        self.assertEqual(spec.name, "moxfield")

    def test_unknown_url_returns_none(self) -> None:
        self.assertIsNone(_identify_source("https://example.com/whatever"))
        self.assertIsNone(_identify_source(""))
        self.assertIsNone(_identify_source(None))  # type: ignore[arg-type]


class ArchidektParserTests(unittest.TestCase):
    def test_parses_commander_and_deck_cards(self) -> None:
        payload = _archidekt_payload([
            _archidekt_card(
                name="Atraxa, Praetors' Voice", oracle_id="atraxa-uuid",
                categories=["Commander"],
            ),
            _archidekt_card(name="Sol Ring", oracle_id="sol-ring-uuid"),
            _archidekt_card(name="Forest", oracle_id="forest-uuid", quantity=10),
        ])
        out = _parse_archidekt(payload)
        self.assertIsNone(out["parse_error"])
        deck = out["deck"]
        self.assertEqual(deck["commander"]["name"], "Atraxa, Praetors' Voice")
        self.assertEqual(deck["commander"]["oracle_id"], "atraxa-uuid")
        self.assertEqual(len(deck["cards"]), 2)
        self.assertEqual(deck["format"], "commander")
        forest = next(c for c in deck["cards"] if c["name"] == "Forest")
        self.assertEqual(forest["count"], 10)

    def test_drops_entries_missing_name_and_surfaces_in_unknowns(self) -> None:
        payload = _archidekt_payload([
            {"quantity": 1, "categories": [], "card": {"oracleCard": {"uid": "x"}}},
            _archidekt_card(name="Sol Ring", oracle_id="sol-ring-uuid"),
        ])
        out = _parse_archidekt(payload)
        self.assertEqual(len(out["unknowns"]), 1)
        self.assertEqual(out["unknowns"][0]["name"], "<missing_name>")
        self.assertEqual(len(out["deck"]["cards"]), 1)

    def test_handles_missing_cards_array(self) -> None:
        out = _parse_archidekt({})
        self.assertIsNotNone(out["parse_error"])
        self.assertIsNone(out["deck"])


class ImportFromUrlDispatchTests(unittest.TestCase):
    def test_archidekt_url_with_stub_fetcher_returns_ok(self) -> None:
        def stub_fetcher(url: str) -> Dict[str, Any]:
            self.assertIn("archidekt.com/api/decks/8000000", url)
            return _archidekt_payload([
                _archidekt_card(name="Atraxa, Praetors' Voice", oracle_id="ax",
                                categories=["Commander"]),
                _archidekt_card(name="Sol Ring", oracle_id="sr"),
            ])

        result = import_from_url(
            "https://archidekt.com/decks/8000000/", fetcher=stub_fetcher,
        )
        self.assertEqual(result["status"], STATUS_OK)
        self.assertEqual(result["source"], "archidekt")
        self.assertEqual(result["deck"]["commander"]["name"], "Atraxa, Praetors' Voice")
        self.assertEqual(result["version"], IMPORT_URL_V1_VERSION)

    def test_moxfield_url_returns_deferred_without_fetching(self) -> None:
        called = []
        def should_not_be_called(url: str) -> Dict[str, Any]:
            called.append(url)
            return {}

        result = import_from_url(
            "https://moxfield.com/decks/zNVMaTl9JEStdcSt1Q12pg",
            fetcher=should_not_be_called,
        )
        self.assertEqual(result["status"], STATUS_DEFERRED)
        self.assertEqual(result["source"], "moxfield")
        self.assertEqual(result["reason_code"], REASON_SOURCE_DEFERRED)
        self.assertEqual(called, [])

    def test_edhrec_url_returns_deferred(self) -> None:
        result = import_from_url("https://edhrec.com/commanders/atraxa-praetors-voice")
        self.assertEqual(result["status"], STATUS_DEFERRED)
        self.assertEqual(result["source"], "edhrec")

    def test_unknown_url_returns_invalid_request(self) -> None:
        result = import_from_url("https://example.com/foo")
        self.assertEqual(result["status"], STATUS_INVALID_REQUEST)
        self.assertEqual(result["reason_code"], REASON_UNKNOWN_SOURCE)

    def test_empty_url_returns_invalid_request(self) -> None:
        result = import_from_url("")
        self.assertEqual(result["status"], STATUS_INVALID_REQUEST)
        self.assertEqual(result["reason_code"], REASON_INVALID_URL)

    def test_parser_failure_surfaces_as_parse_error(self) -> None:
        def bad_payload_fetcher(url: str) -> Dict[str, Any]:
            return {"unexpected": "shape"}  # missing cards[]

        result = import_from_url(
            "https://archidekt.com/decks/8000000/", fetcher=bad_payload_fetcher,
        )
        self.assertEqual(result["status"], STATUS_PARSE_ERROR)
        self.assertEqual(result["reason_code"], REASON_BAD_RESPONSE_SHAPE)

    def test_fetcher_http_error_surfaces_as_fetch_error(self) -> None:
        import urllib.error
        def http_404_fetcher(url: str) -> Dict[str, Any]:
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)  # type: ignore[arg-type]

        result = import_from_url(
            "https://archidekt.com/decks/99999999999/", fetcher=http_404_fetcher,
        )
        self.assertEqual(result["status"], STATUS_FETCH_ERROR)
        self.assertIn("404", result["reason_code"])

    def test_determinism_same_url_same_payload_same_result(self) -> None:
        def stub(url):
            return _archidekt_payload([
                _archidekt_card(name="Sol Ring", oracle_id="sr",
                                categories=["Commander"]),
            ])
        r1 = import_from_url("https://archidekt.com/decks/8000000/", fetcher=stub)
        r2 = import_from_url("https://archidekt.com/decks/8000000/", fetcher=stub)
        self.assertEqual(r1, r2)


if __name__ == "__main__":
    unittest.main()
