from __future__ import annotations

import unittest

from api.engine.layers.commander_dependency_v2 import (
    COMMANDER_DEPENDENCY_V2_VERSION,
    run_commander_dependency_v2,
)


class CommanderDependencyV2DeterminismTests(unittest.TestCase):
    def _engine_requirements_payload(self) -> dict:
        return {
            "version": "engine_requirement_detection_v1",
            "status": "OK",
            "engine_requirements_v1": {
                "commander_dependent": "MED",
            },
        }

    def _structural_payload(self) -> dict:
        return {
            "commander_dependency_signal_v1": 0.12345678,
            "primitive_concentration_index_v1": 0.6666666,
            "dead_slot_ids_v1": ["S2", "S1", "S2"],
            "structural_health_summary_v1": {
                "missing_required_count": 1,
            },
        }

    def _coherence_payload(self) -> dict:
        return {
            "version": "engine_coherence_v1",
            "status": "WARN",
            "summary": {
                "dead_slot_ratio": 0.3333333,
            },
        }

    def test_schema_and_keys_present(self) -> None:
        payload = run_commander_dependency_v2(
            engine_requirement_detection_v1_payload=self._engine_requirements_payload(),
            structural_snapshot_v1_payload=self._structural_payload(),
            engine_coherence_v1_payload=self._coherence_payload(),
        )

        self.assertEqual(payload.get("version"), COMMANDER_DEPENDENCY_V2_VERSION)
        self.assertIn(payload.get("status"), {"OK", "WARN", "SKIP"})
        self.assertEqual(
            list(payload.keys()),
            [
                "version",
                "status",
                "commander_dependency_v2",
                "signals",
                "notes",
                "codes",
            ],
        )

        summary = payload.get("commander_dependency_v2") if isinstance(payload.get("commander_dependency_v2"), dict) else {}
        self.assertEqual(
            list(summary.keys()),
            [
                "access_required",
                "amplifier",
                "line_share_percent",
            ],
        )

        signals = payload.get("signals") if isinstance(payload.get("signals"), dict) else {}
        self.assertEqual(
            list(signals.keys()),
            sorted(signals.keys()),
        )

    def test_determinism_repeat_call_identical(self) -> None:
        kwargs = {
            "engine_requirement_detection_v1_payload": self._engine_requirements_payload(),
            "structural_snapshot_v1_payload": self._structural_payload(),
            "engine_coherence_v1_payload": self._coherence_payload(),
        }
        first = run_commander_dependency_v2(**kwargs)
        second = run_commander_dependency_v2(**kwargs)
        self.assertEqual(first, second)

    def test_rounding_and_proxy_warning_behavior(self) -> None:
        rounded = run_commander_dependency_v2(
            engine_requirement_detection_v1_payload=self._engine_requirements_payload(),
            structural_snapshot_v1_payload=self._structural_payload(),
            engine_coherence_v1_payload=self._coherence_payload(),
        )
        rounded_summary = rounded.get("commander_dependency_v2") if isinstance(rounded.get("commander_dependency_v2"), dict) else {}
        self.assertEqual(rounded_summary.get("line_share_percent"), 0.123457)

        proxied = run_commander_dependency_v2(
            engine_requirement_detection_v1_payload=self._engine_requirements_payload(),
            structural_snapshot_v1_payload={
                "dead_slot_ids_v1": [],
            },
            engine_coherence_v1_payload=self._coherence_payload(),
        )
        self.assertEqual(proxied.get("status"), "WARN")
        codes = proxied.get("codes") if isinstance(proxied.get("codes"), list) else []
        self.assertIn("COMMANDER_DEPENDENCY_V2_LINE_SHARE_PROXY_USED", codes)


if __name__ == "__main__":
    unittest.main()
