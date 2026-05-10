"""Tests for commander_recommendation_v1 (Phase 2 PATH C reduced ambition)."""
from __future__ import annotations

import unittest
from typing import Any, Dict, List

from api.engine.layers import commander_recommendation_v1 as crv1
from api.engine.layers.commander_recommendation_v1 import (
    COMMANDER_RECOMMENDATION_V1_VERSION,
    STATUS_INSUFFICIENT_SIGNAL,
    STATUS_OK,
    run_commander_recommendation_v1,
)


def _candidate(*, name: str, oracle_id: str, color_identity: List[str],
               primitives: List[str]) -> Dict[str, Any]:
    return {
        "name": name,
        "oracle_id": oracle_id,
        "color_identity": color_identity,
        "primitives": primitives,
    }


class CommanderRecommendationParseTests(unittest.TestCase):
    def test_parse_color_identity_handles_json_string(self) -> None:
        self.assertEqual(crv1._parse_color_identity('["G"]'), ["G"])
        self.assertEqual(crv1._parse_color_identity('["B", "G"]'), ["B", "G"])
        self.assertEqual(crv1._parse_color_identity('[]'), [])

    def test_parse_color_identity_handles_invalid(self) -> None:
        self.assertEqual(crv1._parse_color_identity(None), [])
        self.assertEqual(crv1._parse_color_identity(""), [])
        self.assertEqual(crv1._parse_color_identity("not json"), [])

    def test_parse_primitives_handles_json_string(self) -> None:
        self.assertEqual(crv1._parse_primitives('["CARD_DRAW"]'), ["CARD_DRAW"])
        self.assertEqual(crv1._parse_primitives('[]'), [])

    def test_parse_primitives_handles_invalid(self) -> None:
        self.assertEqual(crv1._parse_primitives(None), [])
        self.assertEqual(crv1._parse_primitives("not json"), [])


class CommanderRecommendationFilterTests(unittest.TestCase):
    def test_color_compatible_when_seed_subset_of_candidate(self) -> None:
        self.assertTrue(crv1._color_identity_compatible(["G", "U"], ["G"]))
        self.assertTrue(crv1._color_identity_compatible(["G"], ["G"]))
        self.assertTrue(crv1._color_identity_compatible(["G", "U"], []))

    def test_color_incompatible_when_candidate_lacks_seed_color(self) -> None:
        self.assertFalse(crv1._color_identity_compatible(["G"], ["G", "U"]))
        self.assertFalse(crv1._color_identity_compatible(["W"], ["B"]))

    def test_color_efficiency_exact_match_returns_one(self) -> None:
        self.assertEqual(crv1._color_efficiency(["G"], ["G"]), 1.0)

    def test_color_efficiency_decays_per_extra_color(self) -> None:
        # G seed; G+U candidate -> 1 extra -> 0.9
        self.assertAlmostEqual(crv1._color_efficiency(["G", "U"], ["G"]), 0.9, places=5)
        # G seed; 5-color candidate -> 4 extra -> 0.6
        self.assertAlmostEqual(crv1._color_efficiency(["B", "G", "R", "U", "W"], ["G"]), 0.6, places=5)

    def test_color_efficiency_zero_when_incompatible(self) -> None:
        self.assertEqual(crv1._color_efficiency(["W"], ["B"]), 0.0)


class CommanderRecommendationScoringTests(unittest.TestCase):
    def test_primitives_overlap_jaccard(self) -> None:
        # |{a,b} ∩ {b,c}| / |{a,b,c}| = 1/3
        self.assertAlmostEqual(crv1._primitives_overlap(["a", "b"], ["b", "c"]), 1/3, places=5)
        # exact match
        self.assertEqual(crv1._primitives_overlap(["a", "b"], ["a", "b"]), 1.0)
        # disjoint
        self.assertEqual(crv1._primitives_overlap(["a"], ["b"]), 0.0)
        # both empty
        self.assertEqual(crv1._primitives_overlap([], []), 0.0)

    def test_power_marker_density_recognises_game_changers(self) -> None:
        # Sol Ring is the canonical game changer; assert non-zero.
        from api.engine.constants import GAME_CHANGERS_SET
        if "Sol Ring" in GAME_CHANGERS_SET:
            self.assertEqual(crv1._power_marker_density("Sol Ring"), 1.0)
        # Random non-changer name
        self.assertEqual(crv1._power_marker_density("Llanowar Elves"), 0.0)

    def test_edge_score_weighted_combiner(self) -> None:
        weights = {"primitives_overlap": 0.6, "color_efficiency": 0.25, "power_marker_density": 0.15}
        score = crv1._edge_score(
            primitives_overlap=1.0, color_efficiency=1.0, power_marker_density=1.0,
            weights=weights,
        )
        self.assertAlmostEqual(score, 1.0, places=5)
        score_zero = crv1._edge_score(
            primitives_overlap=0.0, color_efficiency=0.0, power_marker_density=0.0,
            weights=weights,
        )
        self.assertEqual(score_zero, 0.0)


class CommanderRecommendationEnvelopeTests(unittest.TestCase):
    def test_empty_seed_returns_insufficient_signal(self) -> None:
        result = run_commander_recommendation_v1(
            seed_primitive_ids=[], seed_color_identity=[], snapshot_id="x",
            candidates_override=[],
        )
        self.assertEqual(result["status"], STATUS_INSUFFICIENT_SIGNAL)
        self.assertEqual(result["candidates"], [])
        self.assertEqual(result["version"], COMMANDER_RECOMMENDATION_V1_VERSION)

    def test_top_n_default_is_ten(self) -> None:
        # Build 20 mock candidates with varying primitive overlap.
        candidates = [
            _candidate(
                name=f"Cmdr_{i:02d}", oracle_id=f"oid-{i:02d}",
                color_identity=["G"],
                primitives=["P1", "P2"] if i < 10 else ["P3"],
            )
            for i in range(20)
        ]
        result = run_commander_recommendation_v1(
            seed_primitive_ids=["P1", "P2"], seed_color_identity=["G"],
            snapshot_id="x", candidates_override=candidates,
        )
        self.assertEqual(result["status"], STATUS_OK)
        self.assertEqual(len(result["candidates"]), 10)
        self.assertEqual(result["top_n"], 10)
        self.assertEqual(result["considered_count"], 20)

    def test_color_filter_excludes_incompatible_commanders(self) -> None:
        # Seed = G; one candidate is W-only (incompatible) and should be filtered.
        candidates = [
            _candidate(name="GreenCmdr", oracle_id="g-1", color_identity=["G"], primitives=["P"]),
            _candidate(name="WhiteCmdr", oracle_id="w-1", color_identity=["W"], primitives=["P"]),
        ]
        result = run_commander_recommendation_v1(
            seed_primitive_ids=["P"], seed_color_identity=["G"],
            snapshot_id="x", candidates_override=candidates,
        )
        self.assertEqual(len(result["candidates"]), 1)
        self.assertEqual(result["candidates"][0]["name"], "GreenCmdr")

    def test_alphabetical_oracle_id_tie_break(self) -> None:
        # Two candidates with identical edge_score; alphabetical oracle_id wins.
        candidates = [
            _candidate(name="Z_Cmdr", oracle_id="z-uuid", color_identity=["G"], primitives=["P"]),
            _candidate(name="A_Cmdr", oracle_id="a-uuid", color_identity=["G"], primitives=["P"]),
        ]
        result = run_commander_recommendation_v1(
            seed_primitive_ids=["P"], seed_color_identity=["G"],
            snapshot_id="x", candidates_override=candidates,
        )
        # Both have edge_score = 0.6×1 + 0.25×1 + 0.15×0 = 0.85; tie-broken by oracle_id.
        self.assertEqual(result["candidates"][0]["oracle_id"], "a-uuid")
        self.assertEqual(result["candidates"][1]["oracle_id"], "z-uuid")

    def test_determinism_same_inputs_produce_same_output(self) -> None:
        candidates = [
            _candidate(name=f"C{i}", oracle_id=f"oid-{i}", color_identity=["G"],
                       primitives=["P1"] if i % 2 else ["P2"])
            for i in range(8)
        ]
        kwargs = dict(
            seed_primitive_ids=["P1", "P2"], seed_color_identity=["G"],
            snapshot_id="x", candidates_override=candidates,
        )
        r1 = run_commander_recommendation_v1(**kwargs)
        r2 = run_commander_recommendation_v1(**kwargs)
        self.assertEqual(r1, r2)

    def test_envelope_shape_matches_phase_2_path_c_spec(self) -> None:
        candidates = [_candidate(
            name="TestCmdr", oracle_id="oid-1", color_identity=["G"], primitives=["P"])]
        result = run_commander_recommendation_v1(
            seed_primitive_ids=["P"], seed_color_identity=["G"],
            snapshot_id="x", candidates_override=candidates,
        )
        self.assertEqual(set(result.keys()),
                         {"version", "status", "top_n", "considered_count", "candidates"})
        cand = result["candidates"][0]
        self.assertEqual(set(cand.keys()),
                         {"name", "color_identity", "edge_score", "primitives_overlap", "oracle_id"})


class CommanderRecommendationPerformanceTests(unittest.TestCase):
    """Performance check using mock candidates at production scale (3K+).

    Live-DB integration verified separately via standalone probe (Stage 2
    discovery): 3438 legendary creatures enumerated against snapshot
    `20260217_190902_tagpass_20260222` in 41ms; full layer run with
    seed_primitive_ids=['CARD_DRAW','RAMP'] + seed_color_identity=['G']
    completes in 41ms returning 10 G-compatible candidates. Hermetic tests
    here use mock candidates because tests/conftest.py's autouse fixture
    swaps MTG_ENGINE_DB_PATH to a tmp test DB without populated cards.
    """

    def test_full_run_at_production_scale_completes_under_safety_threshold(self) -> None:
        # Safety #15: layer must complete <2 sec on ~3K mock candidates
        # (proxy for the live-DB scale of 3438 legendary creatures).
        import time
        candidates = [
            _candidate(
                name=f"Cmdr_{i:04d}", oracle_id=f"oid-{i:04d}",
                color_identity=["G"] if i % 5 == 0 else ["G", "U"] if i % 3 == 0 else ["W"],
                primitives=["CARD_DRAW", "RAMP"][i % 2:i % 2 + 1] if i % 4 else [],
            )
            for i in range(3500)
        ]
        start = time.monotonic()
        result = run_commander_recommendation_v1(
            seed_primitive_ids=["CARD_DRAW", "RAMP"],
            seed_color_identity=["G"],
            snapshot_id="x",
            candidates_override=candidates,
        )
        elapsed = time.monotonic() - start
        self.assertEqual(result["status"], STATUS_OK)
        self.assertEqual(result["considered_count"], 3500)
        self.assertLessEqual(len(result["candidates"]), 10)
        # Safety #15: <2 sec target; 15 sec halt threshold.
        self.assertLess(elapsed, 2.0, f"full run took {elapsed:.2f}s; safety #15 target is 2 sec")


class CommanderRecommendationWeightsPackTests(unittest.TestCase):
    def test_default_weights_pack_loads_with_expected_values(self) -> None:
        weights = crv1._load_weights()
        self.assertAlmostEqual(weights["primitives_overlap"], 0.6, places=5)
        self.assertAlmostEqual(weights["color_efficiency"], 0.25, places=5)
        self.assertAlmostEqual(weights["power_marker_density"], 0.15, places=5)


if __name__ == "__main__":
    unittest.main()
