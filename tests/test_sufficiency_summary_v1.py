from __future__ import annotations

import unittest

from api.engine.layers.sufficiency_summary_v1 import (
    SUFFICIENCY_SUMMARY_V1_VERSION,
    run_sufficiency_summary_v1,
)


class SufficiencySummaryV1Tests(unittest.TestCase):
    def _thresholds_payload(self) -> dict:
        return {
            "profile_thresholds_version": "profile_thresholds_v1",
            "calibration_snapshot_version": "calibration_snapshot_v1",
            "format": "commander",
            "requested_profile_id": "focused",
            "selected_profile_id": "focused",
            "selection_source": "profile",
            "domains": {
                "required_effects": {
                    "max_missing": 0,
                    "max_unknowns": 0,
                },
                "baseline_prob": {
                    "cast_reliability_t3_min": 0.5,
                    "cast_reliability_t4_min": 0.6,
                    "cast_reliability_t6_min": 0.75,
                },
                "stress_prob": {
                    "engine_continuity_after_removal_min": 0.7,
                    "rebuild_after_wipe_min": 0.6,
                    "graveyard_fragility_delta_max": 0.2,
                },
                "coherence": {
                    "dead_slot_ratio_max": 0.35,
                    "overlap_score_min": 0.0,
                },
                "resilience": {
                    "commander_fragility_delta_max": 0.25,
                },
                "commander": {
                    "protection_coverage_proxy_min": 0.2,
                    "commander_fragility_delta_max": 0.25,
                },
            },
        }

    def _required_effects_ok(self) -> dict:
        return {
            "version": "required_effects_coverage_v1",
            "status": "OK",
            "reason": None,
            "requirements_version": "required_effects_v1",
            "coverage": [],
            "missing": [],
            "unknowns": [],
        }

    def _required_effects_warn(self) -> dict:
        return {
            "version": "required_effects_coverage_v1",
            "status": "WARN",
            "reason": None,
            "requirements_version": "required_effects_v1",
            "coverage": [],
            "missing": [],
            "unknowns": [{"code": "REQUIRED_PRIMITIVE_UNSUPPORTED", "message": "unsupported"}],
        }

    def _upstream_ok_payloads(self) -> dict:
        return {
            "engine_requirement_detection_v1_payload": {
                "version": "engine_requirement_detection_v1",
                "status": "OK",
                "engine_requirements_v1": {"commander_dependent": "LOW"},
            },
            "engine_coherence_v1_payload": {
                "version": "engine_coherence_v1",
                "status": "OK",
                "summary": {
                    "dead_slot_ratio": 0.2,
                    "overlap_score": 0.1,
                },
            },
            "mulligan_model_v1_payload": {
                "version": "mulligan_model_v1",
                "status": "OK",
            },
            "substitution_engine_v1_payload": {
                "version": "substitution_engine_v1",
                "status": "OK",
            },
            "weight_multiplier_engine_v1_payload": {
                "version": "weight_multiplier_engine_v1",
                "status": "OK",
            },
            "probability_math_core_v1_payload": {
                "version": "probability_math_core_v1",
                "status": "OK",
            },
            "probability_checkpoint_layer_v1_payload": {
                "version": "probability_checkpoint_layer_v1",
                "status": "OK",
            },
            "stress_model_definition_v1_payload": {
                "version": "stress_model_definition_v1",
                "status": "OK",
            },
            "stress_transform_engine_v1_payload": {
                "version": "stress_transform_engine_v1",
                "status": "OK",
            },
            "resilience_math_engine_v1_payload": {
                "version": "resilience_math_engine_v1",
                "status": "OK",
                "metrics": {
                    "engine_continuity_after_removal": 0.8,
                    "rebuild_after_wipe": 0.7,
                    "graveyard_fragility_delta": 0.1,
                    "commander_fragility_delta": 0.1,
                },
            },
            "commander_reliability_model_v1_payload": {
                "version": "commander_reliability_model_v1",
                "status": "OK",
                "commander_dependent": "LOW",
                "metrics": {
                    "cast_reliability_t3": 0.6,
                    "cast_reliability_t4": 0.7,
                    "cast_reliability_t6": 0.85,
                    "protection_coverage_proxy": None,
                    "commander_fragility_delta": 0.1,
                },
            },
        }

    def _pipeline_versions(self) -> dict:
        return {
            "engine_coherence_version": "engine_coherence_v1",
            "mulligan_model_version": "mulligan_model_v1",
            "substitution_engine_version": "substitution_engine_v1",
            "weight_multiplier_engine_version": "weight_multiplier_engine_v1",
            "probability_model_version": "probability_math_core_v1",
            "probability_checkpoint_version": "probability_checkpoint_layer_v1",
            "stress_model_version": "stress_model_definition_v1",
            "stress_transform_version": "stress_transform_engine_v1",
            "resilience_math_engine_version": "resilience_math_engine_v1",
            "commander_reliability_model_version": "commander_reliability_model_v1",
            "required_effects_version": "required_effects_v1",
            "profile_thresholds_version": "profile_thresholds_v1",
            "calibration_snapshot_version": "calibration_snapshot_v1",
            "sufficiency_summary_version": "sufficiency_summary_v1",
        }

    def _base_kwargs(self) -> dict:
        kwargs = {
            "format": "commander",
            "profile_id": "focused",
            "profile_thresholds_v1_payload": self._thresholds_payload(),
            "required_effects_coverage_v1_payload": self._required_effects_ok(),
            "pipeline_versions": self._pipeline_versions(),
        }
        kwargs.update(self._upstream_ok_payloads())
        return kwargs

    def test_skip_when_upstream_payload_missing(self) -> None:
        kwargs = self._base_kwargs()
        kwargs["stress_transform_engine_v1_payload"] = None

        payload = run_sufficiency_summary_v1(**kwargs)

        self.assertEqual(payload.get("version"), SUFFICIENCY_SUMMARY_V1_VERSION)
        self.assertEqual(payload.get("status"), "SKIP")
        self.assertEqual(payload.get("reason_code"), "UPSTREAM_PHASE3_UNAVAILABLE")
        self.assertIn(
            "SUFFICIENCY_REQUIRED_UPSTREAM_UNAVAILABLE_STRESS_TRANSFORM_ENGINE_V1",
            payload.get("codes") or [],
        )

    def test_fail_when_required_domain_below_threshold(self) -> None:
        kwargs = self._base_kwargs()
        kwargs["engine_coherence_v1_payload"] = {
            "version": "engine_coherence_v1",
            "status": "OK",
            "summary": {
                "dead_slot_ratio": 0.5,
                "overlap_score": 0.1,
            },
        }

        payload = run_sufficiency_summary_v1(**kwargs)

        self.assertEqual(payload.get("status"), "FAIL")
        self.assertEqual(
            payload.get("failures"),
            ["SUFFICIENCY_COHERENCE_DEAD_SLOT_RATIO_ABOVE_MAX"],
        )

    def test_warn_when_only_warnings_present(self) -> None:
        kwargs = self._base_kwargs()
        kwargs["required_effects_coverage_v1_payload"] = self._required_effects_warn()
        kwargs["commander_reliability_model_v1_payload"] = {
            "version": "commander_reliability_model_v1",
            "status": "WARN",
            "commander_dependent": "HIGH",
            "metrics": {
                "cast_reliability_t3": 0.6,
                "cast_reliability_t4": 0.7,
                "cast_reliability_t6": 0.85,
                "protection_coverage_proxy": None,
                "commander_fragility_delta": 0.1,
            },
        }

        payload = run_sufficiency_summary_v1(**kwargs)

        self.assertEqual(payload.get("status"), "WARN")
        self.assertEqual(payload.get("failures"), [])
        self.assertIn("SUFFICIENCY_REQUIRED_EFFECTS_SOURCE_WARN", payload.get("warnings") or [])
        self.assertIn("SUFFICIENCY_COMMANDER_PROTECTION_PROXY_UNAVAILABLE", payload.get("warnings") or [])

    def test_determinism_repeat_call_identical(self) -> None:
        kwargs = self._base_kwargs()

        first = run_sufficiency_summary_v1(**kwargs)
        second = run_sufficiency_summary_v1(**kwargs)

        self.assertEqual(first, second)

    def test_deterministic_ordering_codes_failures_warnings(self) -> None:
        kwargs = self._base_kwargs()
        kwargs["required_effects_coverage_v1_payload"] = {
            "version": "required_effects_coverage_v1",
            "status": "WARN",
            "reason": None,
            "requirements_version": "required_effects_v1",
            "coverage": [],
            "missing": [{"primitive": "MANA_RAMP_ARTIFACT_ROCK", "min": 10, "count": 6}],
            "unknowns": [{"code": "REQUIRED_PRIMITIVE_UNSUPPORTED", "message": "unsupported"}],
        }
        kwargs["resilience_math_engine_v1_payload"] = {
            "version": "resilience_math_engine_v1",
            "status": "WARN",
            "metrics": {
                "engine_continuity_after_removal": 0.6,
                "rebuild_after_wipe": 0.7,
                "graveyard_fragility_delta": 0.1,
                "commander_fragility_delta": None,
            },
        }

        payload = run_sufficiency_summary_v1(**kwargs)

        codes = payload.get("codes") or []
        failures = payload.get("failures") or []
        warns = payload.get("warnings") or []

        self.assertEqual(codes, sorted(codes))
        self.assertEqual(failures, sorted(failures))
        self.assertEqual(warns, sorted(warns))


if __name__ == "__main__":
    unittest.main()
