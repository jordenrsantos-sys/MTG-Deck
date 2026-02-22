from __future__ import annotations

import unittest

from api.engine.layers.engine_coherence_v2 import (
    ENGINE_COHERENCE_V2_VERSION,
    run_engine_coherence_v2,
)


class EngineCoherenceV2Tests(unittest.TestCase):
    def test_skip_when_primitive_index_unavailable(self) -> None:
        payload = run_engine_coherence_v2(
            primitive_index_by_slot=None,
            deck_slot_ids_playable=["S1"],
            structural_snapshot_v1_payload={},
        )
        self.assertEqual(payload.get("version"), ENGINE_COHERENCE_V2_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "PRIMITIVE_INDEX_UNAVAILABLE")

    def test_warn_when_commander_signal_unavailable(self) -> None:
        payload = run_engine_coherence_v2(
            primitive_index_by_slot={"S1": ["P_A"], "S2": []},
            deck_slot_ids_playable=["S1", "S2"],
            structural_snapshot_v1_payload={"dead_slot_ids_v1": ["S2"]},
        )
        self.assertEqual(payload.get("status"), "WARN")
        codes = payload.get("codes") if isinstance(payload.get("codes"), list) else []
        self.assertIn("ENGINE_COHERENCE_V2_COMMANDER_SIGNAL_UNAVAILABLE", codes)

    def test_rounding_and_repeat_determinism(self) -> None:
        kwargs = {
            "primitive_index_by_slot": {
                "S1": ["P_A", "P_B"],
                "S2": ["P_B", "P_C"],
                "S3": ["P_A", "P_C"],
            },
            "deck_slot_ids_playable": ["S3", "S2", "S1"],
            "structural_snapshot_v1_payload": {
                "commander_dependency_signal_v1": 0.3333339,
                "structural_health_summary_v1": {"missing_required_count": 2},
            },
        }

        first = run_engine_coherence_v2(**kwargs)
        second = run_engine_coherence_v2(**kwargs)

        self.assertEqual(first, second)
        self.assertEqual(first.get("version"), ENGINE_COHERENCE_V2_VERSION)
        summary = first.get("summary") if isinstance(first.get("summary"), dict) else {}
        signals = first.get("signals") if isinstance(first.get("signals"), dict) else {}

        self.assertEqual(signals.get("commander_dependency_signal_v1"), 0.333334)
        self.assertEqual(summary.get("overlap_score"), 0.333333)
        self.assertIn("bridge_amplification_proxy_v1", summary)
        self.assertIn("diversity_proxy_v1", summary)


if __name__ == "__main__":
    unittest.main()
