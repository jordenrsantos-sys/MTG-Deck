from __future__ import annotations

import unittest

try:
    from api.main import DeckValidateResponse

    _IMPORT_ERROR: Exception | None = None
except Exception as exc:  # pragma: no cover - environment-dependent dependency loading
    DeckValidateResponse = None
    _IMPORT_ERROR = exc


class DeckValidateSchemaV1Tests(unittest.TestCase):
    def test_deck_validate_response_schema_is_typed_and_strict(self) -> None:
        if _IMPORT_ERROR is not None:
            self.skipTest(f"FastAPI integration dependencies unavailable: {_IMPORT_ERROR}")

        schema = DeckValidateResponse.model_json_schema()

        self.assertEqual(schema.get("type"), "object")
        self.assertEqual(schema.get("additionalProperties"), False)

        required = schema.get("required") if isinstance(schema.get("required"), list) else []
        self.assertEqual(
            required,
            [
                "status",
                "db_snapshot_id",
                "format",
                "canonical_deck_input",
                "unknowns",
                "violations_v1",
                "request_hash_v1",
                "parse_version",
                "resolve_version",
                "ingest_version",
            ],
        )

        properties = schema.get("properties") if isinstance(schema.get("properties"), dict) else {}
        canonical_schema = properties.get("canonical_deck_input") if isinstance(properties.get("canonical_deck_input"), dict) else {}
        unknowns_schema = properties.get("unknowns") if isinstance(properties.get("unknowns"), dict) else {}
        violations_schema = properties.get("violations_v1") if isinstance(properties.get("violations_v1"), dict) else {}

        self.assertEqual(canonical_schema.get("$ref"), "#/$defs/CanonicalDeckInputV1")
        self.assertEqual(
            (unknowns_schema.get("items") or {}).get("$ref") if isinstance(unknowns_schema.get("items"), dict) else None,
            "#/$defs/DecklistUnknownV1",
        )
        self.assertEqual(
            (violations_schema.get("items") or {}).get("$ref") if isinstance(violations_schema.get("items"), dict) else None,
            "#/$defs/DeckValidateViolationV1",
        )

        defs = schema.get("$defs") if isinstance(schema.get("$defs"), dict) else {}
        for model_name in (
            "CanonicalDeckInputV1",
            "DecklistUnknownV1",
            "DecklistUnknownCandidateV1",
            "DeckValidateViolationV1",
        ):
            model_schema = defs.get(model_name) if isinstance(defs.get(model_name), dict) else {}
            self.assertEqual(
                model_schema.get("additionalProperties"),
                False,
                f"{model_name} must be strict (additionalProperties=false)",
            )


if __name__ == "__main__":
    unittest.main()
