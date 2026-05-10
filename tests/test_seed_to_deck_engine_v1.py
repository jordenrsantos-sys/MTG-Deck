"""Tests for seed_to_deck_engine_v1 (Phase 3 reduced-ambition v1)."""
from __future__ import annotations

import unittest
from typing import Any, Callable, Dict, List

from api.engine.layers.seed_to_deck_engine_v1 import (
    PHASE_3_1_DEFERRED,
    REASON_DECK_BUILD_DEFERRED,
    REASON_EMPTY_SEED_NO_COMMANDER,
    REASON_NO_COMMANDER_RECOMMENDABLE,
    REASON_SEED_OVERSIZED,
    SEED_TO_DECK_ENGINE_V1_VERSION,
    STATUS_FAIL_AFTER_TUNE,
    STATUS_INSUFFICIENT_SIGNAL,
    STATUS_INVALID_REQUEST,
    STATUS_OK,
    STATUS_OK_DECK_BUILD_DEFERRED,
    run_seed_to_deck_engine_v1,
)


def _stub_recommendation(
    *, oracle_id: str = "rec-cmdr-1", name: str = "Recommended Commander",
) -> Callable[..., Dict[str, Any]]:
    def runner(*, seed_primitive_ids, seed_color_identity, snapshot_id, **kwargs):
        return {
            "version": "commander_recommendation_v1",
            "status": "OK",
            "top_n": 10,
            "considered_count": 1,
            "candidates": [{
                "name": name, "oracle_id": oracle_id,
                "color_identity": [], "edge_score": 0.5, "primitives_overlap": 0.0,
            }],
        }
    return runner


def _stub_recommendation_insufficient() -> Callable[..., Dict[str, Any]]:
    def runner(**kwargs):
        return {
            "version": "commander_recommendation_v1",
            "status": "INSUFFICIENT_SIGNAL",
            "top_n": 10, "considered_count": 0, "candidates": [],
        }
    return runner


def _stub_deck_build(
    deck_99: List[str], sufficiency_status: str = "OK",
) -> Callable[..., Dict[str, Any]]:
    def runner(**kwargs):
        return {
            "deck_99": deck_99,
            "sufficiency_summary": {"status": sufficiency_status, "version": "sufficiency_summary_v1"},
            "swap_suggestions": [],
        }
    return runner


def _stub_deck_tune(swaps: List[Dict[str, Any]], post_status: str = "PASS") -> Callable[..., Dict[str, Any]]:
    def runner(**kwargs):
        return {
            "swap_suggestions": swaps,
            "sufficiency_summary": {"status": post_status, "version": "sufficiency_summary_v1"},
        }
    return runner


class SeedToDeckValidationTests(unittest.TestCase):
    def test_empty_seed_without_commander_returns_insufficient_signal(self) -> None:
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids=[], snapshot_id="s",
        )
        self.assertEqual(r["status"], STATUS_INSUFFICIENT_SIGNAL)
        self.assertEqual(r["reason_code"], REASON_EMPTY_SEED_NO_COMMANDER)
        self.assertEqual(r["version"], SEED_TO_DECK_ENGINE_V1_VERSION)

    def test_oversized_seed_returns_invalid_request(self) -> None:
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids=[f"oid-{i}" for i in range(100)],
            snapshot_id="s",
        )
        self.assertEqual(r["status"], STATUS_INVALID_REQUEST)
        self.assertEqual(r["reason_code"], REASON_SEED_OVERSIZED)

    def test_seed_99_is_valid(self) -> None:
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids=[f"oid-{i}" for i in range(99)],
            commander_oracle_id="cmdr-1",
            snapshot_id="s",
        )
        self.assertNotEqual(r["status"], STATUS_INVALID_REQUEST)

    def test_non_list_seed_returns_invalid_request(self) -> None:
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids="not-a-list",  # type: ignore[arg-type]
            snapshot_id="s",
        )
        self.assertEqual(r["status"], STATUS_INVALID_REQUEST)


class SeedToDeckCommanderResolutionTests(unittest.TestCase):
    def test_supplied_commander_used_directly(self) -> None:
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids=["a", "b"], commander_oracle_id="cmdr-pinned",
            snapshot_id="s",
        )
        self.assertEqual(r["commander"]["oracle_id"], "cmdr-pinned")

    def test_missing_commander_resolves_via_phase_2(self) -> None:
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids=["a", "b"], snapshot_id="s",
            commander_recommendation_runner=_stub_recommendation(
                oracle_id="phase2-cmdr", name="Phase2 Commander",
            ),
        )
        self.assertEqual(r["commander"]["oracle_id"], "phase2-cmdr")
        self.assertEqual(r["commander"]["name"], "Phase2 Commander")

    def test_phase_2_insufficient_returns_orchestrator_insufficient(self) -> None:
        # Safety #17: graceful fallback when Phase 2 returns INSUFFICIENT_SIGNAL.
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids=["a"], snapshot_id="s",
            commander_recommendation_runner=_stub_recommendation_insufficient(),
        )
        self.assertEqual(r["status"], STATUS_INSUFFICIENT_SIGNAL)
        self.assertEqual(r["reason_code"], REASON_NO_COMMANDER_RECOMMENDABLE)


class SeedToDeckBuildDeferredTests(unittest.TestCase):
    """Phase 3 v1: deck-build delegation deferred to Phase 3.1 unless injected."""

    def test_no_deck_build_runner_returns_deferred_status(self) -> None:
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids=["a", "b", "c"], commander_oracle_id="cmdr-1",
            snapshot_id="s",
        )
        self.assertEqual(r["status"], STATUS_OK_DECK_BUILD_DEFERRED)
        self.assertEqual(r["reason_code"], REASON_DECK_BUILD_DEFERRED)
        # deck_99 is populated with the seed (placeholder until Phase 3.1).
        self.assertEqual(r["deck_99"], ["a", "b", "c"])
        # phase_3_1_deferred enumerates what's left to build.
        self.assertGreater(len(r["phase_3_1_deferred"]), 0)
        for token in PHASE_3_1_DEFERRED:
            self.assertIn(token, r["phase_3_1_deferred"])


class SeedToDeckBuildInjectedTests(unittest.TestCase):
    def test_deck_build_runner_invoked_when_injected(self) -> None:
        deck = [f"oid-{i}" for i in range(99)]
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids=["seed-1"], commander_oracle_id="cmdr-1",
            snapshot_id="s",
            deck_build_runner=_stub_deck_build(deck, sufficiency_status="PASS"),
        )
        self.assertEqual(r["status"], STATUS_OK)
        self.assertEqual(r["deck_99"], deck)
        self.assertIsNotNone(r["sufficiency_summary"])

    def test_sufficiency_fail_triggers_single_tune_call(self) -> None:
        deck = ["d-1", "d-2"]
        swaps = [{"out": "d-1", "in": "better-card"}]
        # Sufficiency FAIL → tune called → tune returns PASS → status OK.
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids=["seed-1"], commander_oracle_id="cmdr-1",
            snapshot_id="s",
            deck_build_runner=_stub_deck_build(deck, sufficiency_status="FAIL"),
            deck_tune_runner=_stub_deck_tune(swaps, post_status="PASS"),
        )
        self.assertEqual(r["status"], STATUS_OK)
        self.assertEqual(r["swap_suggestions"], swaps)

    def test_sufficiency_fail_after_loop_cap_returns_fail_after_tune(self) -> None:
        # Phase 3.1: loop up to 3 iterations; if persistent FAIL, return
        # STATUS_FAIL_AFTER_TUNE with reason_code REASON_FAIL_AFTER_TUNE.
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids=["seed-1"], commander_oracle_id="cmdr-1",
            snapshot_id="s",
            deck_build_runner=_stub_deck_build(["d-1"], sufficiency_status="FAIL"),
            deck_tune_runner=_stub_deck_tune([{"out": "d-1", "in": "alt"}], post_status="FAIL"),
        )
        self.assertEqual(r["status"], STATUS_FAIL_AFTER_TUNE)
        # Phase 3.1 reason_code reflects loop-cap exhaustion (was "single_tune").
        self.assertEqual(r["reason_code"], "sufficiency_fail_after_loop_cap")
        # Phase 3.1: 3 iterations executed against the persistent FAIL.
        self.assertEqual(r["sufficiency_loop_iterations"], 3)
        # Swaps aggregated across all 3 iterations.
        self.assertEqual(len(r["swap_suggestions"]), 3)

    def test_no_tune_runner_when_sufficiency_fail_returns_fail_after_tune(self) -> None:
        # If sufficiency FAIL but no tune_runner injected: status reflects the
        # un-tunable failure; loop runs zero iterations.
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids=["seed-1"], commander_oracle_id="cmdr-1",
            snapshot_id="s",
            deck_build_runner=_stub_deck_build(["d-1"], sufficiency_status="FAIL"),
            # no deck_tune_runner
        )
        self.assertEqual(r["status"], STATUS_FAIL_AFTER_TUNE)
        self.assertEqual(r["sufficiency_loop_iterations"], 0)


class SeedToDeckEnvelopeShapeTests(unittest.TestCase):
    def test_envelope_keys_match_phase_3_1_spec(self) -> None:
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids=["a"], commander_oracle_id="cmdr-1",
            snapshot_id="s",
        )
        # Phase 3.1: envelope gained `repro_bundle` + `sufficiency_loop_iterations`.
        self.assertEqual(set(r.keys()), {
            "version", "status", "commander", "deck_99",
            "sufficiency_summary", "swap_suggestions", "reason_code",
            "phase_3_1_deferred", "repro_bundle", "sufficiency_loop_iterations",
        })

    def test_phase_3_1_deferred_field_documents_remaining_work(self) -> None:
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids=["a"], commander_oracle_id="cmdr-1",
            snapshot_id="s",
        )
        # Phase 3.1 SHRUNK the deferred list — sufficiency-loop + repro_bundle
        # + deck_complete invocation are now implemented; only commander pool
        # expansion (gated on Phase 2.1) and native pipeline_build integration
        # (recursion-guard work) remain.
        deferred_str = " ".join(r["phase_3_1_deferred"])
        self.assertIn("commander_pool", deferred_str)
        self.assertIn("pipeline_build", deferred_str)


class SeedToDeckPhase3_1Tests(unittest.TestCase):
    """Phase 3.1 tests: success criterion + sufficiency loop + repro_bundle."""

    def test_six_card_seed_produces_populated_99_card_deck_status_ok(self) -> None:
        """Phase 3.1 end-to-end success criterion."""
        seed = ["seed-1", "seed-2", "seed-3", "seed-4", "seed-5", "seed-6"]
        full_deck = seed + [f"filler-{i}" for i in range(93)]  # 99 total
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids=seed, commander_oracle_id="cmdr-1",
            snapshot_id="s",
            deck_build_runner=_stub_deck_build(full_deck, sufficiency_status="PASS"),
        )
        self.assertEqual(r["status"], STATUS_OK)
        self.assertEqual(len(r["deck_99"]), 99)
        # Sufficiency PASS or WARN per success criterion (here PASS).
        self.assertIn(r["sufficiency_summary"]["status"], ("PASS", "WARN"))
        # OK_DECK_BUILD_DEFERRED no longer fires under this fixture.
        self.assertNotEqual(r["status"], "OK_DECK_BUILD_DEFERRED")

    def test_repro_bundle_emitted_when_deck_build_completes(self) -> None:
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids=["a", "b"], commander_oracle_id="cmdr-1",
            snapshot_id="snap-x",
            deck_build_runner=_stub_deck_build(["d-1", "d-2"], sufficiency_status="PASS"),
        )
        self.assertIsNotNone(r["repro_bundle"])
        self.assertIn("version", r["repro_bundle"])
        self.assertEqual(r["repro_bundle"]["version"], "repro_bundle_manifest_v1")
        self.assertIn("hashes", r["repro_bundle"])

    def test_repro_bundle_absent_when_deck_build_deferred(self) -> None:
        # No deck_build_runner + no canonical_deck_input → deferred → no repro.
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids=["a"], commander_oracle_id="cmdr-1",
            snapshot_id="s",
        )
        self.assertEqual(r["status"], "OK_DECK_BUILD_DEFERRED")
        self.assertIsNone(r["repro_bundle"])

    def test_sufficiency_loop_runs_zero_iterations_on_pass(self) -> None:
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids=["a"], commander_oracle_id="cmdr-1",
            snapshot_id="s",
            deck_build_runner=_stub_deck_build(["d-1"], sufficiency_status="PASS"),
            deck_tune_runner=_stub_deck_tune([{"out": "x", "in": "y"}]),
        )
        self.assertEqual(r["sufficiency_loop_iterations"], 0)
        self.assertEqual(r["swap_suggestions"], [])

    def test_sufficiency_loop_runs_one_iteration_when_tune_recovers(self) -> None:
        # FAIL → tune → PASS: exactly 1 iteration; status OK.
        r = run_seed_to_deck_engine_v1(
            seed_oracle_ids=["a"], commander_oracle_id="cmdr-1",
            snapshot_id="s",
            deck_build_runner=_stub_deck_build(["d-1"], sufficiency_status="FAIL"),
            deck_tune_runner=_stub_deck_tune([{"out": "x", "in": "y"}], post_status="PASS"),
        )
        self.assertEqual(r["status"], STATUS_OK)
        self.assertEqual(r["sufficiency_loop_iterations"], 1)
        self.assertEqual(len(r["swap_suggestions"]), 1)

    def test_canonical_deck_input_path_uses_default_deck_build_runner(self) -> None:
        # When canonical_deck_input + baseline_build_result are passed (without
        # an explicit deck_build_runner), the orchestrator constructs a default
        # runner that wraps run_deck_complete_engine_v1. We patch the lazy
        # import to verify the call site without hitting the real DB.
        from unittest.mock import patch
        with patch(
            "api.engine.deck_complete_engine_v1.run_deck_complete_engine_v1",
            return_value={
                "deck_99": ["filler-" + str(i) for i in range(99)],
                "sufficiency_summary": {"status": "PASS", "version": "sufficiency_summary_v1"},
                "swap_suggestions": [],
            },
        ) as mock_dc:
            r = run_seed_to_deck_engine_v1(
                seed_oracle_ids=["a", "b"], commander_oracle_id="cmdr-1",
                snapshot_id="s",
                canonical_deck_input={"slots": []},
                baseline_build_result={"snapshot_id": "s"},
            )
        self.assertEqual(r["status"], STATUS_OK)
        self.assertEqual(len(r["deck_99"]), 99)
        mock_dc.assert_called_once()

    def test_repro_bundle_deterministic_for_same_inputs(self) -> None:
        kwargs = dict(
            seed_oracle_ids=["a"], commander_oracle_id="cmdr-1",
            snapshot_id="s",
            deck_build_runner=_stub_deck_build(["d-1"], sufficiency_status="PASS"),
        )
        r1 = run_seed_to_deck_engine_v1(**kwargs)
        r2 = run_seed_to_deck_engine_v1(**kwargs)
        self.assertEqual(r1["repro_bundle"], r2["repro_bundle"])


class SeedToDeckDeterminismTests(unittest.TestCase):
    def test_same_inputs_produce_identical_output(self) -> None:
        kwargs = dict(
            seed_oracle_ids=["a", "b"],
            commander_oracle_id="cmdr-1",
            snapshot_id="s",
        )
        r1 = run_seed_to_deck_engine_v1(**kwargs)
        r2 = run_seed_to_deck_engine_v1(**kwargs)
        self.assertEqual(r1, r2)

    def test_phase_2_runner_called_with_consistent_args(self) -> None:
        captured: List[Dict[str, Any]] = []
        def runner(**kw):
            captured.append(kw)
            return {
                "version": "commander_recommendation_v1", "status": "OK",
                "top_n": 10, "considered_count": 1,
                "candidates": [{"oracle_id": "c", "name": "C",
                                 "color_identity": [], "edge_score": 0.5,
                                 "primitives_overlap": 0.0}],
            }
        run_seed_to_deck_engine_v1(
            seed_oracle_ids=["a"], snapshot_id="snapshot-x",
            commander_recommendation_runner=runner,
        )
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0]["snapshot_id"], "snapshot-x")
        # v1: empty primitive + color filter (Phase 3.1 will derive these).
        self.assertEqual(captured[0]["seed_primitive_ids"], [])
        self.assertEqual(captured[0]["seed_color_identity"], [])


if __name__ == "__main__":
    unittest.main()
