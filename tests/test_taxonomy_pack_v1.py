from __future__ import annotations

import hashlib
import json
import unittest

from taxonomy.taxonomy_pack_v1 import TAXONOMY_PACK_V1_VERSION, build_taxonomy_pack_v1


class TaxonomyPackV1Tests(unittest.TestCase):
    def test_build_pack_has_deterministic_shape_and_hash(self) -> None:
        pack = build_taxonomy_pack_v1(
            {
                "taxonomy_source_id": "taxonomy_workbook_v1.xlsx",
                "tag_taxonomy_version": "taxonomy_v1_23",
                "generator_version": "taxonomy_exporter_v2",
                "rulespec_rules": [
                    {"primitive_id": "CARD_DRAW", "rule_id": "R2", "pattern": "draw"},
                    {"rule_id": "R1", "primitive_id": "RAMP_MANA", "pattern": "add"},
                ],
                "rulespec_facets": [
                    {"facet_value": "ramp", "facet_key": "role"},
                    {"facet_key": "speed", "facet_value": "fast"},
                ],
                "primitives": [
                    {"primitive_id": "RAMP_MANA", "description": "mana accel"},
                    {"primitive_id": "CARD_DRAW", "description": "draw cards"},
                ],
            }
        )

        self.assertEqual(
            list(pack.keys()),
            ["version", "taxonomy_source_id", "created_from", "hashes", "counts", "payload"],
        )
        self.assertEqual(pack.get("version"), TAXONOMY_PACK_V1_VERSION)
        self.assertEqual(pack.get("taxonomy_source_id"), "taxonomy_workbook_v1.xlsx")

        created_from = pack.get("created_from") if isinstance(pack.get("created_from"), dict) else {}
        self.assertEqual(created_from.get("tag_taxonomy_version"), "taxonomy_v1_23")
        self.assertEqual(created_from.get("generator_version"), "taxonomy_exporter_v2")

        counts = pack.get("counts") if isinstance(pack.get("counts"), dict) else {}
        self.assertEqual(counts.get("tags"), 2)
        self.assertEqual(counts.get("primitives"), 2)
        self.assertEqual(counts.get("facets"), 2)
        self.assertEqual(counts.get("edges"), 2)

        payload = pack.get("payload") if isinstance(pack.get("payload"), dict) else {}
        self.assertEqual(payload.get("tag_ids"), ["CARD_DRAW", "RAMP_MANA"])
        self.assertEqual(
            payload.get("edges"),
            [
                {"primitive_id": "RAMP_MANA", "rule_id": "R1"},
                {"primitive_id": "CARD_DRAW", "rule_id": "R2"},
            ],
        )

        hashes = pack.get("hashes") if isinstance(pack.get("hashes"), dict) else {}
        pack_sha = hashes.get("pack_sha256")
        self.assertIsInstance(pack_sha, str)
        self.assertNotEqual(pack_sha, "")

        hash_input = dict(pack)
        hash_input["hashes"] = dict(hash_input.get("hashes") or {})
        hash_input["hashes"]["pack_sha256"] = ""
        expected_sha = hashlib.sha256(
            json.dumps(hash_input, separators=(",", ":"), sort_keys=True, ensure_ascii=False).encode("utf-8")
        ).hexdigest()
        self.assertEqual(pack_sha, expected_sha)

    def test_build_pack_is_deterministic_for_same_semantics(self) -> None:
        input_a = {
            "taxonomy_source_id": "taxonomy_workbook_v1.xlsx",
            "tag_taxonomy_version": "taxonomy_v1_23",
            "generator_version": "taxonomy_exporter_v2",
            "rulespec_rules": [
                {"rule_id": "R2", "primitive_id": "CARD_DRAW", "pattern": "draw"},
                {"rule_id": "R1", "primitive_id": "RAMP_MANA", "pattern": "add"},
            ],
            "rulespec_facets": [
                {"facet_key": "speed", "facet_value": "fast"},
                {"facet_key": "role", "facet_value": "ramp"},
            ],
            "primitives": [
                {"primitive_id": "CARD_DRAW"},
                {"primitive_id": "RAMP_MANA"},
            ],
        }
        input_b = {
            "generator_version": "taxonomy_exporter_v2",
            "tag_taxonomy_version": "taxonomy_v1_23",
            "taxonomy_source_id": "taxonomy_workbook_v1.xlsx",
            "primitives": [
                {"primitive_id": "RAMP_MANA"},
                {"primitive_id": "CARD_DRAW"},
            ],
            "rulespec_facets": [
                {"facet_value": "ramp", "facet_key": "role"},
                {"facet_value": "fast", "facet_key": "speed"},
            ],
            "rulespec_rules": [
                {"primitive_id": "RAMP_MANA", "pattern": "add", "rule_id": "R1"},
                {"primitive_id": "CARD_DRAW", "rule_id": "R2", "pattern": "draw"},
            ],
        }

        first = build_taxonomy_pack_v1(input_a)
        second = build_taxonomy_pack_v1(input_b)

        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
