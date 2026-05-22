"""Tests for the incremental vectorizer in deck_strength_check_v1._ensure_vectors.

Pre-incremental, every batch ingest reset _CORPUS_VECTORS=[] forcing the next
_ensure_vectors call to re-vectorize the whole corpus (O(N) per ingest). At
4000+ entries the rebuild took 30-60s and caused the archetype_brief flake
observed during the manual sweeps. The incremental implementation tracks
already-vectorized corpus_ids per snapshot and only computes deltas.
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import api.engine.layers.deck_analyze_v1 as analyze_mod
import api.engine.layers.deck_strength_check_v1 as sc


SNAPSHOT_ID = "TEST_SNAPSHOT_INC"


def _mk_entry(cid: str, commander: str = "Test Commander") -> dict:
    return {
        "corpus_id": cid,
        "commander": commander,
        "archetype": "Test Archetype",
        "bracket": "B3",
        "source": "test_incremental_vectors",
        "decklist": ["Sol Ring", "Command Tower"] + [f"Card {i}" for i in range(50)],
    }


def _fake_analyze(*, db_snapshot_id, commander, raw_decklist_text, include_debug=False):
    """Plausible compute_deck_analyze_v1 stub — returns the same shape the real
    function returns but with hand-rolled values so vector cache populates."""
    return {
        "primitive_density": {"creature": 0.3, "instant": 0.1},
        "subtype_density": {},
        "card_count": 52,
    }


class IncrementalVectorsTest(unittest.TestCase):
    """Validates the corpus_id-diff incremental vectorization behavior."""

    def setUp(self) -> None:
        import tempfile
        from pathlib import Path
        # Snapshot module-level state so the test is isolated
        self._original_corpus_raw = sc._CORPUS_RAW
        self._original_vectors = sc._CORPUS_VECTORS
        # Mega-task v5 Phase 5: redirect persistent cache path to a per-test
        # tmp file so _ensure_vectors's disk-checkpoint writes never touch
        # the production cache (which would corrupt a parallel warmup run).
        self._original_cache_path = sc._CORPUS_VECTORS_PATH
        self._tmpdir = tempfile.mkdtemp(prefix="strength_test_")
        sc._CORPUS_VECTORS_PATH = Path(self._tmpdir) / "test_cache.json"
        sc._CORPUS_RAW = {
            "version": "test_incremental",
            "decks": [_mk_entry(f"test_id_{i}", f"Commander {i}") for i in range(5)],
        }
        sc._CORPUS_VECTORS = []

    def tearDown(self) -> None:
        import shutil
        sc._CORPUS_RAW = self._original_corpus_raw
        sc._CORPUS_VECTORS = self._original_vectors
        sc._CORPUS_VECTORS_PATH = self._original_cache_path
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_initial_call_vectorizes_all_then_added_entry_vectorizes_only_one(self) -> None:
        mock_analyze = MagicMock(side_effect=_fake_analyze)
        with patch.object(analyze_mod, "compute_deck_analyze_v1", mock_analyze), \
             patch.object(sc, "_load_corpus", lambda: None):
            # Initial call: vectorizes all 5 corpus entries
            cached = sc._ensure_vectors(SNAPSHOT_ID)
            self.assertEqual(len(cached), 5,
                             "initial call should return 5 vectorized entries")
            self.assertEqual(mock_analyze.call_count, 5,
                             "initial call should invoke compute_deck_analyze_v1 5 times")
            self.assertEqual(len(sc._CORPUS_VECTORS), 5,
                             "module-level cache should hold 5 entries after initial vectorize")

            # Mutate _CORPUS_RAW directly to simulate a fresh ingest
            sc._CORPUS_RAW["decks"].append(_mk_entry("test_id_NEW", "Commander New"))

            # Reset the mock counter to isolate the second call
            mock_analyze.reset_mock()

            # Second call: should vectorize ONLY the new entry — not re-process the 5
            cached2 = sc._ensure_vectors(SNAPSHOT_ID)
            self.assertEqual(len(cached2), 6,
                             "second call should return all 6 vectors (5 cached + 1 new)")
            self.assertEqual(mock_analyze.call_count, 1,
                             f"second call should invoke analyze EXACTLY 1 time "
                             f"(only the new entry), got {mock_analyze.call_count}. "
                             f"This is the regression that broke at-scale ingest.")
            self.assertEqual(len(sc._CORPUS_VECTORS), 6,
                             "module-level cache should hold 6 entries after incremental vectorize")

    def test_third_call_with_no_new_entries_invokes_analyze_zero_times(self) -> None:
        mock_analyze = MagicMock(side_effect=_fake_analyze)
        with patch.object(analyze_mod, "compute_deck_analyze_v1", mock_analyze), \
             patch.object(sc, "_load_corpus", lambda: None):
            sc._ensure_vectors(SNAPSHOT_ID)
            mock_analyze.reset_mock()
            cached = sc._ensure_vectors(SNAPSHOT_ID)
            self.assertEqual(len(cached), 5)
            self.assertEqual(mock_analyze.call_count, 0,
                             "idempotent call with no corpus changes should not invoke analyze")

    def test_different_snapshot_triggers_full_revectorize(self) -> None:
        """Per-snapshot caching: switching snapshot IDs re-vectorizes from scratch
        for the new snapshot but preserves the old snapshot's cache."""
        mock_analyze = MagicMock(side_effect=_fake_analyze)
        with patch.object(analyze_mod, "compute_deck_analyze_v1", mock_analyze), \
             patch.object(sc, "_load_corpus", lambda: None):
            sc._ensure_vectors("SNAP_A")
            self.assertEqual(mock_analyze.call_count, 5)

            mock_analyze.reset_mock()
            cached_b = sc._ensure_vectors("SNAP_B")
            self.assertEqual(len(cached_b), 5,
                             "new snapshot should return 5 vectors (re-vectorized)")
            self.assertEqual(mock_analyze.call_count, 5,
                             "switching snapshot must re-vectorize all entries for the new snapshot")
            self.assertEqual(len(sc._CORPUS_VECTORS), 10,
                             "cache holds entries for both snapshots (5 + 5)")

    def test_return_value_only_includes_requested_snapshot(self) -> None:
        """When multiple snapshots are in the cache, _ensure_vectors returns
        only the entries for the requested snapshot."""
        mock_analyze = MagicMock(side_effect=_fake_analyze)
        with patch.object(analyze_mod, "compute_deck_analyze_v1", mock_analyze), \
             patch.object(sc, "_load_corpus", lambda: None):
            sc._ensure_vectors("SNAP_A")
            sc._ensure_vectors("SNAP_B")
            cached_a = sc._ensure_vectors("SNAP_A")
            self.assertEqual(len(cached_a), 5)
            self.assertTrue(all(v.get("_snapshot") == "SNAP_A" for v in cached_a),
                            "all returned entries must be for the requested snapshot")


class PersistentVectorCacheTest(unittest.TestCase):
    """Mega-task v5 Phase 5: validate the disk-persistent vector cache.

    Pre-Phase-5 the cache was in-memory only, so any fresh process re-
    vectorized the entire corpus on first strength_check call (~110 min for
    13K entries). The fix persists `_CORPUS_VECTORS` to disk; `_load_corpus`
    reads it back, and `_ensure_vectors` checkpoints during long runs.
    """

    def setUp(self) -> None:
        import tempfile
        from pathlib import Path
        # Snapshot module-level state
        self._original_corpus_raw = sc._CORPUS_RAW
        self._original_vectors = sc._CORPUS_VECTORS
        self._original_cache_path = sc._CORPUS_VECTORS_PATH
        # Per-test isolated cache file
        self._tmpdir = tempfile.mkdtemp(prefix="phase5_cache_")
        sc._CORPUS_VECTORS_PATH = Path(self._tmpdir) / "test_cache.json"
        sc._CORPUS_RAW = {
            "version": "test_persistent",
            "decks": [_mk_entry(f"test_id_{i}", f"Commander {i}") for i in range(3)],
        }
        sc._CORPUS_VECTORS = []

    def tearDown(self) -> None:
        import shutil
        sc._CORPUS_RAW = self._original_corpus_raw
        sc._CORPUS_VECTORS = self._original_vectors
        sc._CORPUS_VECTORS_PATH = self._original_cache_path
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_ensure_vectors_persists_to_disk(self) -> None:
        """After _ensure_vectors finishes, the cache file exists on disk and
        contains all vectorized entries — so a future process can read it."""
        import json
        mock_analyze = MagicMock(side_effect=_fake_analyze)
        with patch.object(analyze_mod, "compute_deck_analyze_v1", mock_analyze), \
             patch.object(sc, "_load_corpus", lambda: None):
            sc._ensure_vectors(SNAPSHOT_ID)
        self.assertTrue(sc._CORPUS_VECTORS_PATH.exists(),
                        "cache file should be written after vectorization")
        with open(sc._CORPUS_VECTORS_PATH, "r", encoding="utf-8") as f:
            persisted = json.load(f)
        self.assertEqual(len(persisted), 3,
                         "disk cache should hold all 3 vectorized entries")
        self.assertTrue(all(v.get("_snapshot") == SNAPSHOT_ID for v in persisted),
                        "every disk-cached vector must have _snapshot field set")
        self.assertTrue(all(v.get("corpus_id") for v in persisted),
                        "every disk-cached vector must have corpus_id field set")

    def test_load_corpus_reads_persisted_cache(self) -> None:
        """A fresh _CORPUS_VECTORS (simulating new process) gets pre-populated
        from disk on _load_corpus() if a cache file exists."""
        import json
        # Pre-seed a cache file
        seeded = [{
            "_snapshot": SNAPSHOT_ID, "corpus_id": f"seeded_{i}",
            "commander": "X", "archetype": "Y", "bracket": "B2",
            "source": "test", "primitive_density": {}, "subtype_density": {},
            "card_count": 50,
        } for i in range(7)]
        sc._CORPUS_VECTORS_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(sc._CORPUS_VECTORS_PATH, "w", encoding="utf-8") as f:
            json.dump(seeded, f)

        # Simulate a fresh process: empty in-memory cache, then _load_corpus
        # should populate from disk. We have to actually invoke _load_corpus
        # so the test does NOT patch it out (unlike the other tests).
        sc._CORPUS_VECTORS = []
        # Make the real _load_corpus see a "valid corpus" — patch _CORPUS_PATH
        # to point to a temp JSON file shaped like a real corpus.
        corpus_json_path = sc._CORPUS_VECTORS_PATH.parent / "corpus_v1.json"
        with open(corpus_json_path, "w", encoding="utf-8") as f:
            json.dump({"version": "test", "decks": []}, f)
        with patch.object(sc, "_CORPUS_PATH", corpus_json_path):
            sc._load_corpus()
        self.assertEqual(len(sc._CORPUS_VECTORS), 7,
                         "fresh _load_corpus should populate from disk cache")

    def test_load_corpus_tolerates_missing_cache_file(self) -> None:
        """When the cache file doesn't exist (first-ever run), _load_corpus
        sets _CORPUS_VECTORS to []. No crash."""
        import json
        # Ensure no cache file on disk
        if sc._CORPUS_VECTORS_PATH.exists():
            sc._CORPUS_VECTORS_PATH.unlink()
        sc._CORPUS_VECTORS = []
        corpus_json_path = sc._CORPUS_VECTORS_PATH.parent / "corpus_v1.json"
        with open(corpus_json_path, "w", encoding="utf-8") as f:
            json.dump({"version": "test", "decks": []}, f)
        with patch.object(sc, "_CORPUS_PATH", corpus_json_path):
            sc._load_corpus()  # must not raise
        self.assertEqual(sc._CORPUS_VECTORS, [],
                         "missing cache file should leave vectors empty, not raise")

    def test_load_corpus_tolerates_malformed_cache_file(self) -> None:
        """Garbage / partially-written cache file should fall back to empty
        rather than crashing the engine. (Atomic writes prevent this in
        practice, but defense-in-depth.)"""
        sc._CORPUS_VECTORS_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(sc._CORPUS_VECTORS_PATH, "w", encoding="utf-8") as f:
            f.write("{not valid json at all")
        sc._CORPUS_VECTORS = []
        import json
        corpus_json_path = sc._CORPUS_VECTORS_PATH.parent / "corpus_v1.json"
        with open(corpus_json_path, "w", encoding="utf-8") as f:
            json.dump({"version": "test", "decks": []}, f)
        with patch.object(sc, "_CORPUS_PATH", corpus_json_path):
            sc._load_corpus()
        self.assertEqual(sc._CORPUS_VECTORS, [],
                         "malformed cache file should fall back to empty, not raise")


if __name__ == "__main__":
    unittest.main()
