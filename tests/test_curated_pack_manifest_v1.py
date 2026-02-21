from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import api.engine.curated_pack_manifest_v1 as curated


class CuratedPackManifestV1Tests(unittest.TestCase):
    def _manifest_entry(
        self,
        *,
        pack_id: str,
        pack_version: str,
        path: str,
        sha256: str = "a" * 64,
        load_order: int = 0,
    ) -> dict:
        return {
            "pack_id": pack_id,
            "pack_version": pack_version,
            "path": path,
            "sha256": sha256,
            "load_order": load_order,
        }

    def test_load_normalizes_and_sorts_deterministically(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            manifest_path = Path(tmp_dir) / "curated_pack_manifest_v1.json"
            manifest_payload = {
                "version": curated.CURATED_PACK_MANIFEST_V1_VERSION,
                "packs": [
                    self._manifest_entry(
                        pack_id="beta",
                        pack_version="v2",
                        path="api\\engine\\data\\combos\\b.json",
                        load_order=20,
                    ),
                    self._manifest_entry(
                        pack_id="alpha",
                        pack_version="v1",
                        path="./api/engine/data/combos/a.json",
                        load_order=10,
                    ),
                ],
            }
            manifest_path.write_text(json.dumps(manifest_payload), encoding="utf-8")

            first = curated.load_curated_pack_manifest_v1(manifest_path=manifest_path)
            second = curated.load_curated_pack_manifest_v1(manifest_path=manifest_path)

        self.assertEqual(first, second)
        self.assertEqual(first.get("version"), curated.CURATED_PACK_MANIFEST_V1_VERSION)

        packs = first.get("packs") if isinstance(first.get("packs"), list) else []
        self.assertEqual(
            [
                (
                    row.get("pack_id"),
                    row.get("pack_version"),
                    row.get("load_order"),
                    row.get("path"),
                )
                for row in packs
            ],
            [
                ("alpha", "v1", 10, "api/engine/data/combos/a.json"),
                ("beta", "v2", 20, "api/engine/data/combos/b.json"),
            ],
        )

    def test_load_rejects_absolute_paths(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            manifest_path = Path(tmp_dir) / "curated_pack_manifest_v1.json"
            manifest_payload = {
                "version": curated.CURATED_PACK_MANIFEST_V1_VERSION,
                "packs": [
                    self._manifest_entry(
                        pack_id="bad",
                        pack_version="v1",
                        path="C:/absolute/path.json",
                    ),
                ],
            }
            manifest_path.write_text(json.dumps(manifest_payload), encoding="utf-8")

            with self.assertRaises(RuntimeError) as raised:
                curated.load_curated_pack_manifest_v1(manifest_path=manifest_path)

        self.assertTrue(str(raised.exception).startswith("CURATED_PACK_MANIFEST_V1_INVALID:"))

    def test_validate_manifest_hashes_detects_mismatch(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            repo_root = Path(tmp_dir)
            pack_path = repo_root / "api" / "engine" / "data" / "combos" / "sample.json"
            pack_path.parent.mkdir(parents=True, exist_ok=True)
            pack_path.write_text('{"sample":1}', encoding="utf-8")

            expected_sha = hashlib.sha256(pack_path.read_bytes()).hexdigest()
            manifest_path = repo_root / "curated_pack_manifest_v1.json"
            manifest_payload = {
                "version": curated.CURATED_PACK_MANIFEST_V1_VERSION,
                "packs": [
                    self._manifest_entry(
                        pack_id="sample_pack",
                        pack_version="v1",
                        path="api/engine/data/combos/sample.json",
                        sha256=expected_sha,
                        load_order=1,
                    )
                ],
            }
            manifest_path.write_text(json.dumps(manifest_payload), encoding="utf-8")

            with patch.object(curated, "_REPO_ROOT", repo_root):
                curated.validate_manifest_hashes(manifest_path=manifest_path)

                pack_path.write_text('{"sample":2}', encoding="utf-8")
                with self.assertRaises(RuntimeError) as raised:
                    curated.validate_manifest_hashes(manifest_path=manifest_path)

        self.assertTrue(str(raised.exception).startswith("CURATED_PACK_MANIFEST_V1_SHA256_MISMATCH:"))

    def test_resolve_pack_entry_prefers_latest_when_version_not_specified(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            manifest_path = Path(tmp_dir) / "curated_pack_manifest_v1.json"
            manifest_payload = {
                "version": curated.CURATED_PACK_MANIFEST_V1_VERSION,
                "packs": [
                    self._manifest_entry(
                        pack_id="bundle",
                        pack_version="v1",
                        path="api/engine/data/a.json",
                        load_order=1,
                    ),
                    self._manifest_entry(
                        pack_id="bundle",
                        pack_version="v2",
                        path="api/engine/data/b.json",
                        load_order=2,
                    ),
                ],
            }
            manifest_path.write_text(json.dumps(manifest_payload), encoding="utf-8")

            selected = curated.resolve_pack_entry(pack_id="bundle", manifest_path=manifest_path)

        self.assertEqual(selected.get("pack_version"), "v2")
        self.assertEqual(selected.get("path"), "api/engine/data/b.json")

    def test_collect_taxonomy_pack_refs_filters_and_sorts(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            manifest_path = Path(tmp_dir) / "curated_pack_manifest_v1.json"
            manifest_payload = {
                "version": curated.CURATED_PACK_MANIFEST_V1_VERSION,
                "packs": [
                    self._manifest_entry(
                        pack_id="taxonomy_primitive_mappings",
                        pack_version="taxonomy_v1_23",
                        path="taxonomy/packs/taxonomy_v1_23/map.json",
                        load_order=30,
                    ),
                    self._manifest_entry(
                        pack_id="taxonomy_primitives",
                        pack_version="taxonomy_v1_23",
                        path="taxonomy/packs/taxonomy_v1_23/primitives.json",
                        load_order=20,
                    ),
                    self._manifest_entry(
                        pack_id="taxonomy_primitives",
                        pack_version="taxonomy_v1_22",
                        path="taxonomy/packs/taxonomy_v1_22/primitives.json",
                        load_order=19,
                    ),
                ],
            }
            manifest_path.write_text(json.dumps(manifest_payload), encoding="utf-8")

            refs = curated.collect_taxonomy_pack_refs(
                taxonomy_version="taxonomy_v1_23",
                manifest_path=manifest_path,
            )

        self.assertEqual(
            [
                (row.get("pack_id"), row.get("pack_version"), row.get("load_order"))
                for row in refs
            ],
            [
                ("taxonomy_primitives", "taxonomy_v1_23", 20),
                ("taxonomy_primitive_mappings", "taxonomy_v1_23", 30),
            ],
        )


if __name__ == "__main__":
    unittest.main()
