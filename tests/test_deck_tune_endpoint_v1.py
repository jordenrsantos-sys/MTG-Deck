from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tests.decklist_fixture_harness import (
    DECKLIST_FIXTURE_SNAPSHOT_ID,
    create_decklist_fixture_db,
    set_decklist_fixture_env,
)

try:
    from fastapi.testclient import TestClient
    from api.main import app

    _IMPORT_ERROR: Exception | None = None
except Exception as exc:  # pragma: no cover - environment-dependent dependency loading
    TestClient = None
    app = None
    _IMPORT_ERROR = exc


class DeckTuneEndpointV1Tests(unittest.TestCase):
    _tmp_dir_ctx: tempfile.TemporaryDirectory[str] | None = None
    _db_env_ctx = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        if _IMPORT_ERROR is not None:
            return

        cls._tmp_dir_ctx = tempfile.TemporaryDirectory()
        db_path = create_decklist_fixture_db(Path(cls._tmp_dir_ctx.name))
        cls._db_env_ctx = set_decklist_fixture_env(db_path)
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

    def test_tune_unknowns_block_build_and_tuning(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": "Unknown Card Name",
            "format": "commander",
            "profile_id": "focused",
            "bracket_id": "B3",
            "mulligan_model_id": "NORMAL",
            "max_swaps": 5,
        }

        with (
            patch.dict(os.environ, {"MTG_ENGINE_DEV_METRICS": "0"}, clear=False),
            patch("api.main.run_build_pipeline") as mocked_run_build,
            patch("api.main.run_deck_tune_engine_v1") as mocked_run_tune,
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            response = client.post("/deck/tune_v1", json=payload)

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("status"), "UNKNOWN_PRESENT")
        self.assertEqual(body.get("recommended_swaps_v1"), [])
        self.assertEqual(body.get("baseline_summary_v1"), {})
        self.assertIsInstance(body.get("request_hash_v1"), str)
        self.assertNotIn("dev_metrics_v1", body)
        mocked_run_build.assert_not_called()
        mocked_run_tune.assert_not_called()

    def test_tune_happy_path_invokes_build_then_engine(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": """
Commander
1 Krenko, Mob Boss
Deck
1 Sol Ring
1 Arcane Signet
""",
            "format": "commander",
            "profile_id": "focused",
            "bracket_id": "B2",
            "mulligan_model_id": "NORMAL",
            "max_swaps": 2,
        }

        mocked_build_payload = {
            "status": "OK",
            "deck_size_total": 3,
            "result": {},
        }
        mocked_tune_payload = {
            "version": "deck_tune_engine_v1",
            "status": "OK",
            "baseline_summary_v1": {
                "build_status": "OK",
                "deck_size_total": 3,
            },
            "recommended_swaps_v1": [
                {
                    "cut_name": "Arcane Signet",
                    "add_name": "Sol Ring",
                    "reasons_v1": ["ADD_PRIMITIVE_COVERAGE"],
                    "delta_summary_v1": {
                        "total_score_delta_v1": 1.25,
                        "coherence_delta_v1": 0.25,
                        "primitive_coverage_delta_v1": 1,
                        "gc_compliance_preserved_v1": True,
                    },
                }
            ],
        }

        with (
            patch.dict(os.environ, {"MTG_ENGINE_DEV_METRICS": "0"}, clear=False),
            patch("api.main.run_build_pipeline", return_value=mocked_build_payload) as mocked_run_build,
            patch("api.main.run_deck_tune_engine_v1", return_value=mocked_tune_payload) as mocked_run_tune,
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            response = client.post("/deck/tune_v1", json=payload)

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("status"), "OK")
        self.assertEqual(body.get("tune_engine_version"), "deck_tune_engine_v1")
        self.assertNotIn("dev_metrics_v1", body)

        swaps = body.get("recommended_swaps_v1") if isinstance(body.get("recommended_swaps_v1"), list) else []
        self.assertEqual(len(swaps), 1)
        self.assertEqual(swaps[0].get("cut_name"), "Arcane Signet")
        self.assertEqual(swaps[0].get("add_name"), "Sol Ring")

        mocked_run_build.assert_called_once()
        mocked_run_tune.assert_called_once()

    def test_tune_dev_metrics_included_when_flag_enabled(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": """
Commander
1 Krenko, Mob Boss
Deck
1 Sol Ring
1 Arcane Signet
""",
            "format": "commander",
            "profile_id": "focused",
            "bracket_id": "B2",
            "mulligan_model_id": "NORMAL",
            "max_swaps": 2,
        }

        mocked_build_payload = {
            "status": "OK",
            "deck_size_total": 3,
            "result": {},
        }
        mocked_tune_payload = {
            "version": "deck_tune_engine_v1",
            "status": "OK",
            "baseline_summary_v1": {
                "build_status": "OK",
                "deck_size_total": 3,
            },
            "recommended_swaps_v1": [
                {
                    "cut_name": "Arcane Signet",
                    "add_name": "Sol Ring",
                    "reasons_v1": ["ADD_PRIMITIVE_COVERAGE"],
                    "delta_summary_v1": {
                        "total_score_delta_v1": 1.25,
                        "coherence_delta_v1": 0.25,
                        "primitive_coverage_delta_v1": 1,
                        "gc_compliance_preserved_v1": True,
                    },
                }
            ],
            "evaluation_summary_v1": {
                "swap_evaluations_total": 7,
            },
            "dev_metrics_v1": {
                "candidate_pool_count": 12,
                "swap_eval_count": 7,
                "swap_eval_ms_total": 3.5,
                "candidate_pool_ms": 0.9,
                "candidate_pool_breakdown_v1": {
                    "sql_query_ms": 0.4,
                    "python_filter_ms": 0.3,
                    "color_check_ms": 0.1,
                    "gc_check_ms": 0.1,
                    "total_candidates_seen": 20,
                    "total_candidates_returned": 12,
                },
                "evaluation_cap_hit": False,
            },
        }

        with (
            patch.dict(os.environ, {"MTG_ENGINE_DEV_METRICS": "1"}, clear=False),
            patch("api.main.run_build_pipeline", return_value=mocked_build_payload),
            patch("api.main.run_deck_tune_engine_v1", return_value=mocked_tune_payload),
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            response = client.post("/deck/tune_v1", json=payload)

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertIsInstance(body.get("dev_metrics_v1"), dict)
        metrics = body.get("dev_metrics_v1")
        self.assertEqual(metrics.get("swap_eval_count"), 7)
        self.assertEqual(metrics.get("swap_eval_ms_total"), 3.5)
        self.assertIn("baseline_build_ms", metrics)
        self.assertIn("candidate_pool_ms", metrics)
        self.assertIsInstance(metrics.get("candidate_pool_breakdown_v1"), dict)
        breakdown = metrics.get("candidate_pool_breakdown_v1")
        self.assertEqual(breakdown.get("sql_query_ms"), 0.4)
        self.assertEqual(breakdown.get("total_candidates_seen"), 20)
        self.assertIn("total_ms", metrics)


if __name__ == "__main__":
    unittest.main()
