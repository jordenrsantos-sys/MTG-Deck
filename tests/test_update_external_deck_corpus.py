from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import tools.update_external_deck_corpus as updater
from api.engine.curated_pack_manifest_v1 import (
    iter_runtime_pack_entries,
    load_curated_pack_manifest_v1,
)


def _archidekt_payload(
    *,
    deck_id: int,
    commander_oracle_ids: list[str],
    deck_card_oracle_ids: list,  # list of oracle_id strings or (oracle_id, qty) tuples
    bracket: int | None = None,
    name: str | None = None,
    description: str | None = None,
    extra_categories: list[dict] | None = None,
    inject_card_with_missing_oracle: bool = False,
) -> dict:
    """Build a mock Archidekt /api/decks/{id}/ response payload.

    Mirrors the response shape captured during 5a.2.1 shape discovery against
    deck 8000000: top-level {id, name, edhBracket, description, categories,
    cards}; cards[].card.oracleCard.uid carries the oracle UUID; commanders
    are identified by "Commander" in cards[].categories.
    """
    payload: dict = {
        "id": deck_id,
        "name": name if name is not None else "Test Deck",
        "edhBracket": bracket,
        "description": description if description is not None else "",
        "categories": [
            {"id": 1, "name": "Commander", "isPremier": True, "includedInDeck": True, "includedInPrice": True},
            {"id": 2, "name": "Creature", "isPremier": False, "includedInDeck": True, "includedInPrice": True},
            {"id": 3, "name": "Land", "isPremier": False, "includedInDeck": True, "includedInPrice": True},
            {"id": 4, "name": "Sideboard", "isPremier": False, "includedInDeck": False, "includedInPrice": False},
        ],
        "cards": [],
    }
    if extra_categories:
        payload["categories"].extend(extra_categories)

    next_id = 1000
    for cmdr_oid in commander_oracle_ids:
        payload["cards"].append({
            "id": next_id,
            "categories": ["Commander"],
            "quantity": 1,
            "card": {
                "oracleCard": {
                    "uid": cmdr_oid,
                    "name": f"Commander_{cmdr_oid[:6]}" if cmdr_oid else "Commander_Unknown",
                },
            },
        })
        next_id += 1

    for entry in deck_card_oracle_ids:
        if isinstance(entry, tuple):
            oid, qty = entry
        else:
            oid, qty = entry, 1
        payload["cards"].append({
            "id": next_id,
            "categories": ["Creature"],
            "quantity": qty,
            "card": {
                "oracleCard": {
                    "uid": oid,
                    "name": f"Card_{oid[:6]}",
                },
            },
        })
        next_id += 1

    if inject_card_with_missing_oracle:
        payload["cards"].append({
            "id": next_id,
            "categories": ["Creature"],
            "quantity": 1,
            "card": {
                "oracleCard": {
                    "uid": None,
                    "name": "Mystery Card With No Oracle Id",
                },
            },
        })

    return payload


class UpdateExternalDeckCorpusTests(unittest.TestCase):
    def test_main_with_no_deck_ids_writes_initial_empty_pack(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            exit_code = updater.main(["--output-dir", str(output_dir)])
            self.assertEqual(exit_code, updater.EXIT_SUCCESS)
            pack_path = output_dir / updater.EXTERNAL_DECKS_PACK_FILENAME
            self.assertTrue(pack_path.is_file())
            payload = json.loads(pack_path.read_text(encoding="utf-8"))
            self.assertEqual(payload.get("version"), updater.EXTERNAL_DECKS_V1_VERSION)
            self.assertEqual(payload.get("decks"), [])
            self.assertEqual(payload.get("source"), "none")

    def test_main_dry_run_does_not_write_files(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            payload = _archidekt_payload(
                deck_id=8000001,
                commander_oracle_ids=["uuid-cmdr-alpha"],
                deck_card_oracle_ids=["uuid-card-a", "uuid-card-b"],
                bracket=3,
            )
            with patch.object(updater, "_fetch_json", return_value=payload):
                exit_code = updater.main([
                    "--output-dir", str(output_dir),
                    "--deck-ids", "8000001",
                    "--dry-run",
                ])
            self.assertEqual(exit_code, updater.EXIT_SUCCESS)
            self.assertFalse((output_dir / updater.EXTERNAL_DECKS_PACK_FILENAME).exists())

    def test_normalize_archidekt_deck_extracts_shape_with_oracle_ids(self) -> None:
        payload = _archidekt_payload(
            deck_id=8000002,
            commander_oracle_ids=["uuid-cmdr"],
            deck_card_oracle_ids=["uuid-x", "uuid-y", "uuid-z"],
            bracket=4,
            name="Test Deck",
            description="A test deck",
        )
        normalized, unresolved = updater._normalize_archidekt_deck(
            payload,
            fetched_at_utc="2026-05-08T00:00:00Z",
            engine_version_at_ingest="post-2D",
        )
        self.assertEqual(unresolved, [])
        self.assertIsNotNone(normalized)
        self.assertEqual(normalized["deck_id"], "ARCHIDEKT_8000002")
        self.assertEqual(normalized["source"], "archidekt")
        self.assertEqual(normalized["source_url"], "https://archidekt.com/decks/8000002/")
        self.assertEqual(normalized["commander_oracle_ids"], ["uuid-cmdr"])
        self.assertEqual(normalized["deck_oracle_ids"], ["uuid-x", "uuid-y", "uuid-z"])
        self.assertEqual(normalized["self_reported_bracket"], "B4")
        self.assertIsNone(normalized["engine_assigned_bracket"])
        self.assertEqual(normalized["fetched_at_utc"], "2026-05-08T00:00:00Z")
        self.assertEqual(normalized["source_metadata"]["engine_version_at_ingest"], "post-2D")
        self.assertEqual(normalized["source_metadata"]["archidekt_deck_id"], "8000002")
        self.assertEqual(normalized["source_metadata"]["name"], "Test Deck")
        self.assertEqual(normalized["source_metadata"]["description_excerpt"], "A test deck")

    def test_normalize_drops_when_card_oracle_id_missing(self) -> None:
        # Archidekt analog of "unresolvable card": entry with oracleCard.uid=None.
        payload = _archidekt_payload(
            deck_id=8000003,
            commander_oracle_ids=["uuid-cmdr"],
            deck_card_oracle_ids=["uuid-known"],
            inject_card_with_missing_oracle=True,
        )
        normalized, unresolved = updater._normalize_archidekt_deck(
            payload,
            fetched_at_utc="2026-05-08T00:00:00Z",
            engine_version_at_ingest="post-2D",
        )
        self.assertIsNone(normalized)
        self.assertIn("Mystery Card With No Oracle Id", unresolved)

    def test_normalize_drops_when_commander_missing(self) -> None:
        payload = _archidekt_payload(
            deck_id=8000004,
            commander_oracle_ids=[],  # no commander
            deck_card_oracle_ids=["uuid-a"],
        )
        normalized, _ = updater._normalize_archidekt_deck(
            payload,
            fetched_at_utc="2026-05-08T00:00:00Z",
            engine_version_at_ingest="post-2D",
        )
        self.assertIsNone(normalized)

    def test_dedup_hash_includes_self_reported_bracket(self) -> None:
        # Decision 4: same card list at different brackets should NOT collapse.
        h_b3 = updater._compute_dedup_hash(
            commander_oracle_id="uuid-cmdr",
            deck_oracle_ids=["uuid-a", "uuid-b"],
            self_reported_bracket="B3",
        )
        h_b5 = updater._compute_dedup_hash(
            commander_oracle_id="uuid-cmdr",
            deck_oracle_ids=["uuid-a", "uuid-b"],
            self_reported_bracket="B5",
        )
        h_none = updater._compute_dedup_hash(
            commander_oracle_id="uuid-cmdr",
            deck_oracle_ids=["uuid-a", "uuid-b"],
            self_reported_bracket=None,
        )
        self.assertNotEqual(h_b3, h_b5)
        self.assertNotEqual(h_b3, h_none)
        self.assertNotEqual(h_b5, h_none)

    def test_dedup_hash_deterministic_across_card_order(self) -> None:
        h_one = updater._compute_dedup_hash(
            commander_oracle_id="uuid-cmdr",
            deck_oracle_ids=["uuid-a", "uuid-b", "uuid-c"],
            self_reported_bracket="B3",
        )
        h_two = updater._compute_dedup_hash(
            commander_oracle_id="uuid-cmdr",
            deck_oracle_ids=["uuid-c", "uuid-a", "uuid-b"],
            self_reported_bracket="B3",
        )
        self.assertEqual(h_one, h_two)

    def test_build_decks_payload_dedup_collapses_identical_dedup_hashes(self) -> None:
        deck_a = {"deck_id": "ARCHIDEKT_8000010", "dedup_hash": "hash-shared"}
        deck_b = {"deck_id": "ARCHIDEKT_8000011", "dedup_hash": "hash-shared"}
        deck_c = {"deck_id": "ARCHIDEKT_8000012", "dedup_hash": "hash-distinct"}
        result = updater._build_decks_payload(
            [deck_a, deck_b, deck_c],
            source="archidekt",
            generated_from="/api/decks/",
        )
        self.assertEqual(len(result["decks"]), 2)
        self.assertEqual(
            sorted(d["dedup_hash"] for d in result["decks"]),
            ["hash-distinct", "hash-shared"],
        )

    def test_main_idempotent_re_run_produces_byte_identical_pack(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            payload = _archidekt_payload(
                deck_id=8000005,
                commander_oracle_ids=["uuid-cmdr"],
                deck_card_oracle_ids=["uuid-a", "uuid-b", "uuid-c"],
                bracket=3,
            )
            with (
                patch.object(updater, "_fetch_json", return_value=payload),
                patch.object(updater, "_utcnow_iso", return_value="2026-05-08T00:00:00Z"),
            ):
                first = updater.main([
                    "--output-dir", str(output_dir),
                    "--deck-ids", "8000005",
                    "--no-update-manifest",
                ])
            self.assertEqual(first, updater.EXIT_SUCCESS)
            pack_path = output_dir / updater.EXTERNAL_DECKS_PACK_FILENAME
            first_text = pack_path.read_text(encoding="utf-8")

            with (
                patch.object(updater, "_fetch_json", return_value=payload),
                patch.object(updater, "_utcnow_iso", return_value="2026-05-08T00:00:00Z"),
            ):
                second = updater.main([
                    "--output-dir", str(output_dir),
                    "--deck-ids", "8000005",
                    "--no-update-manifest",
                ])
            self.assertEqual(second, updater.EXIT_SUCCESS)
            second_text = pack_path.read_text(encoding="utf-8")

            self.assertEqual(first_text, second_text)

    def test_existing_pack_preserved_when_all_fetches_fail(self) -> None:
        # Operational safety: when the Archidekt API is unreachable AND the pack
        # already has real data, don't overwrite the populated pack with a
        # freshly-derived empty one. Rerun once the API is back will produce
        # real data. Exit code is PARTIAL_FAILURE so cron monitoring catches it.
        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            pack_path = output_dir / updater.EXTERNAL_DECKS_PACK_FILENAME
            existing = '{"decks":[{"deck_id":"PRESERVE_ME"}]}\n'
            pack_path.write_text(existing, encoding="utf-8")

            with patch.object(updater, "_fetch_json", side_effect=RuntimeError("simulated API down")):
                exit_code = updater.main([
                    "--output-dir", str(output_dir),
                    "--deck-ids", "8000099",
                ])
            self.assertEqual(exit_code, updater.EXIT_PARTIAL_FAILURE)
            self.assertEqual(pack_path.read_text(encoding="utf-8"), existing)

    def test_exit_code_partial_failure_when_decks_drop(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            payload = _archidekt_payload(
                deck_id=8000006,
                commander_oracle_ids=["uuid-cmdr"],
                deck_card_oracle_ids=["uuid-known"],
                inject_card_with_missing_oracle=True,
            )
            with patch.object(updater, "_fetch_json", return_value=payload):
                exit_code = updater.main([
                    "--output-dir", str(output_dir),
                    "--deck-ids", "8000006",
                    "--no-update-manifest",
                ])
            self.assertEqual(exit_code, updater.EXIT_PARTIAL_FAILURE)

    def test_extract_archidekt_bracket_handles_int_and_null(self) -> None:
        # Archidekt's edhBracket is an int 1-5 or null. Unlike Moxfield it
        # does NOT accept string forms; the spec's prior assumption of a
        # noisy/inconsistent field was outdated — discovery confirmed clean
        # int-or-null semantics.
        self.assertEqual(updater._extract_archidekt_bracket({"edhBracket": 1}), "B1")
        self.assertEqual(updater._extract_archidekt_bracket({"edhBracket": 3}), "B3")
        self.assertEqual(updater._extract_archidekt_bracket({"edhBracket": 5}), "B5")
        self.assertIsNone(updater._extract_archidekt_bracket({"edhBracket": None}))
        self.assertIsNone(updater._extract_archidekt_bracket({"edhBracket": 0}))
        self.assertIsNone(updater._extract_archidekt_bracket({"edhBracket": 7}))
        self.assertIsNone(updater._extract_archidekt_bracket({"edhBracket": "3"}))
        self.assertIsNone(updater._extract_archidekt_bracket({}))

    def test_iter_runtime_pack_entries_skips_calibration_only(self) -> None:
        # Live-manifest test: calibration-only entries (external_decks_v1 from
        # 5a.2 + pilot_personalities_v1 from 5b.4) are filtered out by the
        # runtime helper. The gap grows as Phase 5b adds more calibration
        # packs; the assertion checks the contract structurally, not by count.
        all_entries = load_curated_pack_manifest_v1()["packs"]
        runtime_entries = iter_runtime_pack_entries()
        self.assertGreaterEqual(len(all_entries) - len(runtime_entries), 1)
        runtime_pack_ids = {e["pack_id"] for e in runtime_entries}
        self.assertNotIn("external_decks_v1", runtime_pack_ids)
        all_pack_ids = {e["pack_id"] for e in all_entries}
        self.assertIn("external_decks_v1", all_pack_ids)

    def test_engine_version_pin_captured_in_source_metadata(self) -> None:
        payload = _archidekt_payload(
            deck_id=8000007,
            commander_oracle_ids=["uuid-cmdr"],
            deck_card_oracle_ids=["uuid-a"],
        )
        normalized, _ = updater._normalize_archidekt_deck(
            payload,
            fetched_at_utc="2026-05-08T00:00:00Z",
            engine_version_at_ingest="custom-pin-v42",
        )
        self.assertIsNotNone(normalized)
        self.assertEqual(normalized["source_metadata"]["engine_version_at_ingest"], "custom-pin-v42")

    # ------------------------------------------------------------------
    # Manifest auto-refresh (5a.2.2)
    # ------------------------------------------------------------------

    def _write_test_manifest(self, manifest_path: Path, *, external_decks_sha: str) -> str:
        """Write a small, format-compatible manifest with two entries — one
        unrelated (sentinel for 'preserve other entries byte-identical')
        plus the external_decks_v1 entry under test."""
        payload = {
            "packs": [
                {
                    "load_order": 10,
                    "pack_id": "sentinel_other_pack",
                    "pack_version": "sentinel_v1",
                    "path": "api/engine/data/sentinel/sentinel.json",
                    "sha256": "0" * 64,
                },
                {
                    "calibration_only": True,
                    "load_order": 900,
                    "pack_id": "external_decks_v1",
                    "pack_version": "external_decks_v1",
                    "path": "api/engine/data/calibration/external_decks_v1.json",
                    "sha256": external_decks_sha,
                },
            ],
            "version": "curated_pack_manifest_v1",
        }
        text = json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
        manifest_path.write_text(text, encoding="utf-8")
        return text

    def test_successful_live_fetch_rewrites_manifest_sha256_in_lockstep(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp_root = Path(tmp_dir)
            output_dir = tmp_root / "calibration"
            output_dir.mkdir()
            manifest_path = tmp_root / "curated_pack_manifest_v1.json"
            stale_sha = "8" * 64
            self._write_test_manifest(manifest_path, external_decks_sha=stale_sha)

            payload = _archidekt_payload(
                deck_id=8000020,
                commander_oracle_ids=["uuid-cmdr"],
                deck_card_oracle_ids=["uuid-a", "uuid-b"],
                bracket=3,
            )
            with (
                patch.object(updater, "_fetch_json", return_value=payload),
                patch.object(updater, "_utcnow_iso", return_value="2026-05-08T00:00:00Z"),
            ):
                exit_code = updater.main([
                    "--output-dir", str(output_dir),
                    "--deck-ids", "8000020",
                    "--manifest-path", str(manifest_path),
                ])
            self.assertEqual(exit_code, updater.EXIT_SUCCESS)

            pack_path = output_dir / updater.EXTERNAL_DECKS_PACK_FILENAME
            self.assertTrue(pack_path.is_file())
            new_pack_sha = updater._sha256_text(pack_path.read_text(encoding="utf-8"))
            self.assertNotEqual(new_pack_sha, stale_sha)

            updated_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            packs = updated_manifest["packs"]
            external_entries = [e for e in packs if e["pack_id"] == "external_decks_v1"]
            self.assertEqual(len(external_entries), 1)
            self.assertEqual(external_entries[0]["sha256"], new_pack_sha)

            # Sentinel entry byte-identical (other manifest entries preserved).
            sentinel_entries = [e for e in packs if e["pack_id"] == "sentinel_other_pack"]
            self.assertEqual(len(sentinel_entries), 1)
            self.assertEqual(sentinel_entries[0]["sha256"], "0" * 64)
            self.assertEqual(sentinel_entries[0]["load_order"], 10)
            self.assertEqual(sentinel_entries[0]["path"], "api/engine/data/sentinel/sentinel.json")

            # Entry ordering preserved (sentinel first, external_decks_v1 second).
            self.assertEqual([e["pack_id"] for e in packs], ["sentinel_other_pack", "external_decks_v1"])

            # external_decks_v1 entry preserves all other fields byte-identical.
            entry = external_entries[0]
            self.assertEqual(entry["calibration_only"], True)
            self.assertEqual(entry["load_order"], 900)
            self.assertEqual(entry["pack_version"], "external_decks_v1")
            self.assertEqual(entry["path"], "api/engine/data/calibration/external_decks_v1.json")

    def test_dry_run_skips_both_pack_and_manifest_write(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp_root = Path(tmp_dir)
            output_dir = tmp_root / "calibration"
            output_dir.mkdir()
            manifest_path = tmp_root / "curated_pack_manifest_v1.json"
            initial_text = self._write_test_manifest(manifest_path, external_decks_sha="8" * 64)
            initial_mtime_ns = manifest_path.stat().st_mtime_ns

            payload = _archidekt_payload(
                deck_id=8000021,
                commander_oracle_ids=["uuid-cmdr"],
                deck_card_oracle_ids=["uuid-a"],
            )
            with patch.object(updater, "_fetch_json", return_value=payload):
                exit_code = updater.main([
                    "--output-dir", str(output_dir),
                    "--deck-ids", "8000021",
                    "--manifest-path", str(manifest_path),
                    "--dry-run",
                ])
            self.assertEqual(exit_code, updater.EXIT_SUCCESS)
            self.assertFalse((output_dir / updater.EXTERNAL_DECKS_PACK_FILENAME).exists())
            self.assertEqual(manifest_path.read_text(encoding="utf-8"), initial_text)
            self.assertEqual(manifest_path.stat().st_mtime_ns, initial_mtime_ns)

    def test_all_fetches_failed_safety_path_skips_both_pack_and_manifest_write(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp_root = Path(tmp_dir)
            output_dir = tmp_root / "calibration"
            output_dir.mkdir()
            pack_path = output_dir / updater.EXTERNAL_DECKS_PACK_FILENAME
            existing_pack = '{"decks":[{"deck_id":"PRESERVE_ME"}]}\n'
            pack_path.write_text(existing_pack, encoding="utf-8")

            manifest_path = tmp_root / "curated_pack_manifest_v1.json"
            initial_text = self._write_test_manifest(manifest_path, external_decks_sha="8" * 64)

            with patch.object(updater, "_fetch_json", side_effect=RuntimeError("simulated API down")):
                exit_code = updater.main([
                    "--output-dir", str(output_dir),
                    "--deck-ids", "8000099",
                    "--manifest-path", str(manifest_path),
                ])
            self.assertEqual(exit_code, updater.EXIT_PARTIAL_FAILURE)
            self.assertEqual(pack_path.read_text(encoding="utf-8"), existing_pack)
            self.assertEqual(manifest_path.read_text(encoding="utf-8"), initial_text)

    def test_idempotent_rerun_does_not_rewrite_manifest_when_pack_unchanged(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp_root = Path(tmp_dir)
            output_dir = tmp_root / "calibration"
            output_dir.mkdir()
            manifest_path = tmp_root / "curated_pack_manifest_v1.json"
            self._write_test_manifest(manifest_path, external_decks_sha="8" * 64)

            payload = _archidekt_payload(
                deck_id=8000022,
                commander_oracle_ids=["uuid-cmdr"],
                deck_card_oracle_ids=["uuid-a"],
                bracket=3,
            )
            with (
                patch.object(updater, "_fetch_json", return_value=payload),
                patch.object(updater, "_utcnow_iso", return_value="2026-05-08T00:00:00Z"),
            ):
                first = updater.main([
                    "--output-dir", str(output_dir),
                    "--deck-ids", "8000022",
                    "--manifest-path", str(manifest_path),
                ])
            self.assertEqual(first, updater.EXIT_SUCCESS)
            after_first_manifest = manifest_path.read_text(encoding="utf-8")
            after_first_mtime_ns = manifest_path.stat().st_mtime_ns

            with (
                patch.object(updater, "_fetch_json", return_value=payload),
                patch.object(updater, "_utcnow_iso", return_value="2026-05-08T00:00:00Z"),
            ):
                second = updater.main([
                    "--output-dir", str(output_dir),
                    "--deck-ids", "8000022",
                    "--manifest-path", str(manifest_path),
                ])
            self.assertEqual(second, updater.EXIT_SUCCESS)
            after_second_manifest = manifest_path.read_text(encoding="utf-8")

            # Manifest content byte-identical across reruns.
            self.assertEqual(after_first_manifest, after_second_manifest)
            # And the file wasn't even rewritten — mtime preserved (helper
            # short-circuits on the byte-identical-pack guard).
            self.assertEqual(manifest_path.stat().st_mtime_ns, after_first_mtime_ns)


if __name__ == "__main__":
    unittest.main()
