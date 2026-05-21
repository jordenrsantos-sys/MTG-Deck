"""Mega-task v3 Phase 1 — Scryfall set-release watcher tests.

Verifies:
  - find_new_sets correctly filters by known-codes + released_at
  - load/save_known_set_codes round-trip + idempotent
  - fetch_set_index handles 429 / 5xx with backoff
  - fetch_set_index parses the Scryfall response envelope
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from api.engine.integrations import scryfall_sets_watcher_v1 as ssw


def _set(code, released_at, name=None, card_count=0):
    return {
        "code": code, "released_at": released_at,
        "name": name or code.upper(), "card_count": card_count,
    }


class FindNewSetsTests(unittest.TestCase):
    def test_no_new_when_all_known(self) -> None:
        idx = [_set("a1", "2023-01-01"), _set("a2", "2023-06-01")]
        known = {"a1", "a2"}
        self.assertEqual(ssw.find_new_sets(idx, known, "2026-05-21"), [])

    def test_finds_new_set_past_release_date(self) -> None:
        idx = [_set("a1", "2023-01-01"), _set("new", "2026-01-01")]
        known = {"a1"}
        result = ssw.find_new_sets(idx, known, "2026-05-21")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["code"], "new")

    def test_skips_unreleased_future_sets(self) -> None:
        # Set's released_at is in the future — don't process it.
        idx = [_set("future", "2030-01-01")]
        result = ssw.find_new_sets(idx, set(), "2026-05-21")
        self.assertEqual(result, [])

    def test_skips_entries_without_released_at(self) -> None:
        idx = [{"code": "test", "released_at": ""},
               _set("ok", "2024-01-01")]
        result = ssw.find_new_sets(idx, set(), "2026-05-21")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["code"], "ok")

    def test_normalizes_codes_to_lowercase(self) -> None:
        idx = [_set("BLB", "2024-08-01")]
        known = {"blb"}
        result = ssw.find_new_sets(idx, known, "2026-05-21")
        self.assertEqual(result, [])


class LedgerRoundTripTests(unittest.TestCase):
    def test_save_then_load_returns_same_codes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "codes.json"
            codes = {"abc", "def", "blb"}
            ssw.save_known_set_codes(codes, path=path)
            loaded = ssw.load_known_set_codes(path=path)
            self.assertEqual(loaded, codes)

    def test_load_returns_empty_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "nonexistent.json"
            self.assertEqual(ssw.load_known_set_codes(path=path), set())

    def test_save_is_atomic_via_temp_rename(self) -> None:
        # Save twice; second write should fully replace first contents.
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "codes.json"
            ssw.save_known_set_codes({"a"}, path=path)
            ssw.save_known_set_codes({"b"}, path=path)
            self.assertEqual(ssw.load_known_set_codes(path=path), {"b"})


class FetchSetIndexTests(unittest.TestCase):
    def test_parses_data_envelope(self) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "data": [_set("a", "2024-01-01"), _set("b", "2024-06-01")]
        }
        mock_resp.raise_for_status = MagicMock()
        result = ssw.fetch_set_index(http_get=lambda url, **kw: mock_resp)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["code"], "a")

    def test_retries_on_429(self) -> None:
        call_count = {"n": 0}

        def http_get(url, **kw):
            call_count["n"] += 1
            mock = MagicMock()
            if call_count["n"] < 3:
                mock.status_code = 429
            else:
                mock.status_code = 200
                mock.json.return_value = {"data": [_set("x", "2024-01-01")]}
                mock.raise_for_status = MagicMock()
            return mock

        with patch("time.sleep"):  # don't actually sleep
            result = ssw.fetch_set_index(http_get=http_get)
        self.assertEqual(call_count["n"], 3)
        self.assertEqual(result[0]["code"], "x")

    def test_retries_on_5xx(self) -> None:
        call_count = {"n": 0}

        def http_get(url, **kw):
            call_count["n"] += 1
            mock = MagicMock()
            if call_count["n"] == 1:
                mock.status_code = 503
            else:
                mock.status_code = 200
                mock.json.return_value = {"data": []}
                mock.raise_for_status = MagicMock()
            return mock

        with patch("time.sleep"):
            result = ssw.fetch_set_index(http_get=http_get)
        self.assertEqual(call_count["n"], 2)
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
