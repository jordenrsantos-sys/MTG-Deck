from __future__ import annotations

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


class DeckValidateEndpointTests(unittest.TestCase):
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

    def test_validate_clean_then_build_succeeds(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        validate_payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": """
Commander
1 Krenko, Mob Boss
Deck
1 Sol Ring
Arcane Signet
""",
            "format": "commander",
            "profile_id": "focused",
            "bracket_id": "B2",
        }

        with TestClient(app, raise_server_exceptions=False) as client:
            validate_response = client.post("/deck/validate", json=validate_payload)
            self.assertEqual(validate_response.status_code, 200)

            validate_body = validate_response.json()
            self.assertEqual(validate_body.get("status"), "OK")
            self.assertEqual(validate_body.get("unknowns"), [])
            self.assertEqual(validate_body.get("violations_v1"), [])

            request_hash_v1 = validate_body.get("request_hash_v1")
            self.assertIsInstance(request_hash_v1, str)
            self.assertEqual(len(request_hash_v1), 64)

            canonical_input = (
                validate_body.get("canonical_deck_input")
                if isinstance(validate_body.get("canonical_deck_input"), dict)
                else {}
            )
            self.assertEqual(canonical_input.get("commander"), "Krenko, Mob Boss")
            self.assertEqual(canonical_input.get("cards"), ["Sol Ring", "Arcane Signet"])

            build_response = client.post("/build", json=canonical_input)
            self.assertEqual(build_response.status_code, 200)

            build_body = build_response.json()
            self.assertIsInstance(build_body, dict)
            self.assertIn("status", build_body)
            self.assertIn("result", build_body)
            self.assertEqual(build_body.get("request_hash_v1"), request_hash_v1)

    def test_validate_unknown_does_not_call_build_pipeline(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        validate_payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": "Unknown Card Name",
            "format": "commander",
        }

        with (
            patch("api.main.run_build_pipeline") as mocked_run_build,
            TestClient(app, raise_server_exceptions=False) as client,
        ):
            response = client.post("/deck/validate", json=validate_payload)

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("status"), "UNKNOWN_PRESENT")
        self.assertIsInstance(body.get("violations_v1"), list)
        self.assertIsInstance(body.get("request_hash_v1"), str)
        mocked_run_build.assert_not_called()

    def test_validate_request_hash_same_input_stable_and_changes_when_card_order_changes(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        ordered_payload = {
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
        }
        reordered_payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": """
Commander
1 Krenko, Mob Boss
Deck
1 Arcane Signet
1 Sol Ring
""",
            "format": "commander",
            "profile_id": "focused",
            "bracket_id": "B2",
        }

        with TestClient(app, raise_server_exceptions=False) as client:
            first = client.post("/deck/validate", json=ordered_payload)
            second = client.post("/deck/validate", json=ordered_payload)
            reordered = client.post("/deck/validate", json=reordered_payload)

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(reordered.status_code, 200)

        first_hash = first.json().get("request_hash_v1")
        second_hash = second.json().get("request_hash_v1")
        reordered_hash = reordered.json().get("request_hash_v1")

        self.assertEqual(first_hash, second_hash)
        self.assertNotEqual(first_hash, reordered_hash)

    def test_validate_override_resolves_ambiguous_name(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": """
Commander
1 Krenko, Mob Boss
Deck
1 Twin Name
""",
            "format": "commander",
            "name_overrides_v1": [
                {
                    "name_raw": "Twin Name",
                    "resolved_oracle_id": "ORA_AMB_001",
                }
            ],
        }

        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.post("/deck/validate", json=payload)

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("status"), "OK")
        self.assertEqual(body.get("unknowns"), [])

    def test_validate_invalid_override_marks_unknown_override_invalid(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": """
Commander
1 Krenko, Mob Boss
Deck
1 Twin Name
""",
            "format": "commander",
            "name_overrides_v1": [
                {
                    "name_raw": "Twin Name",
                    "resolved_oracle_id": "ORA_DOES_NOT_EXIST",
                }
            ],
        }

        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.post("/deck/validate", json=payload)

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body.get("status"), "UNKNOWN_PRESENT")
        unknowns = body.get("unknowns") if isinstance(body.get("unknowns"), list) else []
        self.assertEqual(len(unknowns), 1)
        self.assertEqual(unknowns[0].get("reason_code"), "OVERRIDE_INVALID")

    def test_validate_request_hash_changes_when_override_changes(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        base_payload = {
            "db_snapshot_id": DECKLIST_FIXTURE_SNAPSHOT_ID,
            "raw_decklist_text": """
Commander
1 Krenko, Mob Boss
Deck
1 Twin Name
""",
            "format": "commander",
        }
        override_payload = {
            **base_payload,
            "name_overrides_v1": [
                {
                    "name_raw": "Twin Name",
                    "resolved_oracle_id": "ORA_AMB_001",
                }
            ],
        }

        with TestClient(app, raise_server_exceptions=False) as client:
            base_response = client.post("/deck/validate", json=base_payload)
            override_response = client.post("/deck/validate", json=override_payload)

        self.assertEqual(base_response.status_code, 200)
        self.assertEqual(override_response.status_code, 200)

        self.assertNotEqual(
            base_response.json().get("request_hash_v1"),
            override_response.json().get("request_hash_v1"),
        )


if __name__ == "__main__":
    unittest.main()
