from __future__ import annotations

import sqlite3
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from api.engine.layers.seed_synergy_detection_v1 import SEED_SYNERGY_DETECTION_V1_VERSION
from api.engine.pipeline_build import run_build_pipeline


TEST_SNAPSHOT_ID = "TEST_SNAPSHOT_2D_0001"


class _BuildResponse(dict):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


# Aristocrats fixture: commander + 8 deck cards tagged with sac / death / token
# primitives. Per the post-2C.2 algorithm with MAX_BIND=8, the resulting
# primitive counts push THEME_ARISTOCRATS over its classify_threshold of
# score>=18.
#
# Counts after pipeline_build's slot_ids_by_primitive construction:
#   SAC_OUTLET_FREE: 5 (commander + 4 deck cards)
#   DEATH_PAYOFF_DRAIN: 4
#   SAC_OUTLET_COSTED: 4
#   TOKEN_PRODUCTION_CREATURE: 4
# THEME_ARISTOCRATS score = 2*(5+4) + 3*4 + 1*4 + 1*0 = 18 + 12 + 4 = 34
# Bands @ MAX_BIND=8: low=18, med=36, high=54 → 34 lands LOW.
_ARISTOCRATS_CARDS = [
    "Aristo Card 1",
    "Aristo Card 2",
    "Aristo Card 3",
    "Aristo Card 4",
    "Aristo Card 5",
    "Aristo Card 6",
    "Aristo Card 7",
    "Aristo Card 8",
]

_ORACLE_BY_NAME = {
    "Aristo Commander": "oracle_aristo_commander",
    "Aristo Card 1": "oracle_aristo_card_1",
    "Aristo Card 2": "oracle_aristo_card_2",
    "Aristo Card 3": "oracle_aristo_card_3",
    "Aristo Card 4": "oracle_aristo_card_4",
    "Aristo Card 5": "oracle_aristo_card_5",
    "Aristo Card 6": "oracle_aristo_card_6",
    "Aristo Card 7": "oracle_aristo_card_7",
    "Aristo Card 8": "oracle_aristo_card_8",
}

_PRIMITIVES_BY_ORACLE = {
    "oracle_aristo_commander": ["SAC_OUTLET_FREE"],
    "oracle_aristo_card_1": ["SAC_OUTLET_FREE", "DEATH_PAYOFF_DRAIN"],
    "oracle_aristo_card_2": ["SAC_OUTLET_FREE", "DEATH_PAYOFF_DRAIN"],
    "oracle_aristo_card_3": ["SAC_OUTLET_FREE", "DEATH_PAYOFF_DRAIN"],
    "oracle_aristo_card_4": ["SAC_OUTLET_FREE", "DEATH_PAYOFF_DRAIN"],
    "oracle_aristo_card_5": ["SAC_OUTLET_COSTED", "TOKEN_PRODUCTION_CREATURE"],
    "oracle_aristo_card_6": ["SAC_OUTLET_COSTED", "TOKEN_PRODUCTION_CREATURE"],
    "oracle_aristo_card_7": ["SAC_OUTLET_COSTED", "TOKEN_PRODUCTION_CREATURE"],
    "oracle_aristo_card_8": ["SAC_OUTLET_COSTED", "TOKEN_PRODUCTION_CREATURE"],
}


class PipelineSeedSynergyDetectionV1Tests(unittest.TestCase):
    def _build_request(self) -> SimpleNamespace:
        return SimpleNamespace(
            db_snapshot_id=TEST_SNAPSHOT_ID,
            profile_id="focused",
            bracket_id="B3",
            format="commander",
            commander="Aristo Commander",
            cards=_ARISTOCRATS_CARDS,
            engine_patches_v0=[],
        )

    def _find_card_by_name_side_effect(self, snapshot_id: str, name: str) -> dict | None:
        _ = snapshot_id
        oracle = _ORACLE_BY_NAME.get(name)
        if oracle is None:
            return None
        if name == "Aristo Commander":
            return {
                "name": name,
                "oracle_id": oracle,
                "color_identity": ["B"],
                "legalities": {"commander": "legal"},
                "type_line": "Legendary Creature - Human",
            }
        return {
            "name": name,
            "oracle_id": oracle,
            "color_identity": ["B"],
            "legalities": {"commander": "legal"},
            "type_line": "Creature - Human",
        }

    def _bulk_get_card_tags_payload(self) -> dict:
        return {
            oracle: {
                "primitive_ids": prims,
                "ruleset_version": "ruleset_v_test",
                "evidence": {"matches": []},
            }
            for oracle, prims in _PRIMITIVES_BY_ORACLE.items()
        }

    def test_pipeline_reports_seed_synergy_detection_with_aristocrats_match(self) -> None:
        stub_api_main = types.ModuleType("api.main")
        stub_api_main.BuildResponse = _BuildResponse

        preflight_payload = {
            "version": "snapshot_preflight_v1",
            "snapshot_id": TEST_SNAPSHOT_ID,
            "status": "OK",
            "errors": [],
            "checks": {
                "snapshot_exists": True,
                "manifest_present": True,
                "tags_compiled": True,
                "schema_ok": True,
            },
        }

        with (
            patch.dict(sys.modules, {"api.main": stub_api_main}),
            patch("api.engine.pipeline_build.cards_db_connect", side_effect=lambda: sqlite3.connect(":memory:")),
            patch("api.engine.pipeline_build.run_snapshot_preflight_v1", return_value=preflight_payload),
            patch("api.engine.pipeline_build.resolve_runtime_taxonomy_version", return_value="taxonomy_v_test"),
            patch("api.engine.pipeline_build.resolve_runtime_ruleset_version", return_value="ruleset_v_test"),
            patch("api.engine.pipeline_build.run_snapshot_preflight", return_value={"status": "OK"}),
            patch("api.engine.pipeline_build.is_legal_commander_card", return_value=(True, "legal")),
            patch("api.engine.pipeline_build.find_card_by_name", side_effect=self._find_card_by_name_side_effect),
            patch("api.engine.pipeline_build.suggest_card_names", return_value=[]),
            patch("api.engine.pipeline_build.ensure_tag_tables", return_value=None),
            patch(
                "api.engine.pipeline_build.bulk_get_card_tags",
                return_value=self._bulk_get_card_tags_payload(),
            ),
        ):
            payload = run_build_pipeline(req=self._build_request(), conn=None, repo_root_path=None)

        self.assertIsInstance(payload, dict)
        result = payload.get("result") if isinstance(payload.get("result"), dict) else {}

        # Sub-task 2D wiring: seed_synergy_detection_v1 result is in the response.
        seed_synergy_payload = result.get("seed_synergy_detection_v1")
        self.assertIsInstance(seed_synergy_payload, dict)
        self.assertEqual(seed_synergy_payload.get("version"), SEED_SYNERGY_DETECTION_V1_VERSION)
        status = seed_synergy_payload.get("status")
        self.assertIn(status, ("OK", "WARN"))

        # The detection envelope's payload key must be present per sub-task-1's contract.
        detection = seed_synergy_payload.get("seed_synergy_detection_v1")
        self.assertIsInstance(detection, dict)

        # bracket_id round-trip from the request.
        self.assertEqual(detection.get("bracket_id"), "B3")

        # The aristocrats fixture should produce a non-empty matched_themes list
        # with THEME_ARISTOCRATS in it (specific theme_id from themes_v1_5).
        matched_themes = detection.get("matched_themes")
        self.assertIsInstance(matched_themes, list)
        self.assertGreater(len(matched_themes), 0)
        theme_ids = {m.get("theme_id") for m in matched_themes if isinstance(m, dict)}
        self.assertIn("THEME_ARISTOCRATS", theme_ids)

        # Every match has the expected shape per sub-task-1's output contract.
        for match in matched_themes:
            self.assertIsInstance(match, dict)
            self.assertIsInstance(match.get("theme_id"), str)
            self.assertIsInstance(match.get("score"), float)
            self.assertIn(match.get("confidence"), ("HIGH", "MED", "LOW"))

        # Sibling layer didn't break: engine_requirement_detection_v1 result is also present.
        engine_req_payload = result.get("engine_requirement_detection_v1")
        self.assertIsInstance(engine_req_payload, dict)
        self.assertIn(engine_req_payload.get("status"), ("OK", "WARN", "SKIP"))


if __name__ == "__main__":
    unittest.main()
