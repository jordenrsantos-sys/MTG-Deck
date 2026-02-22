from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from api.engine.graph_bounds_policy_v1 import load_graph_bounds_policy_v1


class GraphBoundsPolicyV1Tests(unittest.TestCase):
    def test_load_returns_expected_bounds(self) -> None:
        payload = load_graph_bounds_policy_v1()
        self.assertEqual(payload.get("version"), "graph_bounds_policy_v1")
        self.assertEqual(
            payload.get("bounds"),
            {
                "MAX_CARD_CARD_EDGES_TOTAL": 5000,
                "MAX_PRIMS_PER_SLOT": 24,
                "MAX_SLOTS_PER_PRIM": 80,
            },
        )

    def test_invalid_root_keys_fail_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            spec_path = Path(tmp_dir) / "graph_bounds_spec_v1.json"
            spec_path.write_text(
                '{"version":"graph_bounds_policy_v1","bounds":{"MAX_PRIMS_PER_SLOT":24,"MAX_SLOTS_PER_PRIM":80,"MAX_CARD_CARD_EDGES_TOTAL":5000},"unknown":1}',
                encoding="utf-8",
            )
            with patch("api.engine.graph_bounds_policy_v1._GRAPH_BOUNDS_SPEC_FILE", spec_path):
                with self.assertRaises(RuntimeError) as ctx:
                    load_graph_bounds_policy_v1()

        self.assertIn("GRAPH_BOUNDS_POLICY_V1_INVALID", str(ctx.exception))

    def test_falls_back_to_legacy_policy_path_when_new_spec_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            missing_spec_path = Path(tmp_dir) / "missing_graph_bounds_spec_v1.json"
            legacy_policy_path = Path(tmp_dir) / "graph_bounds_policy_v1.json"
            legacy_policy_path.write_text(
                '{"version":"graph_bounds_policy_v1","bounds":{"MAX_PRIMS_PER_SLOT":24,"MAX_SLOTS_PER_PRIM":80,"MAX_CARD_CARD_EDGES_TOTAL":5000}}',
                encoding="utf-8",
            )
            with patch("api.engine.graph_bounds_policy_v1._GRAPH_BOUNDS_SPEC_FILE", missing_spec_path):
                with patch("api.engine.graph_bounds_policy_v1._GRAPH_BOUNDS_POLICY_FILE", legacy_policy_path):
                    payload = load_graph_bounds_policy_v1()

        self.assertEqual(payload.get("version"), "graph_bounds_policy_v1")
        self.assertEqual(payload.get("bounds", {}).get("MAX_PRIMS_PER_SLOT"), 24)

    def test_prefers_new_graph_spec_path_over_legacy_policy_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            spec_path = Path(tmp_dir) / "graph_bounds_spec_v1.json"
            spec_path.write_text(
                '{"version":"graph_bounds_policy_v1_new","bounds":{"MAX_PRIMS_PER_SLOT":25,"MAX_SLOTS_PER_PRIM":81,"MAX_CARD_CARD_EDGES_TOTAL":5001}}',
                encoding="utf-8",
            )
            legacy_policy_path = Path(tmp_dir) / "graph_bounds_policy_v1.json"
            legacy_policy_path.write_text(
                '{"version":"graph_bounds_policy_v1_legacy","bounds":{"MAX_PRIMS_PER_SLOT":24,"MAX_SLOTS_PER_PRIM":80,"MAX_CARD_CARD_EDGES_TOTAL":5000}}',
                encoding="utf-8",
            )

            with patch("api.engine.graph_bounds_policy_v1._GRAPH_BOUNDS_SPEC_FILE", spec_path):
                with patch("api.engine.graph_bounds_policy_v1._GRAPH_BOUNDS_POLICY_FILE", legacy_policy_path):
                    payload = load_graph_bounds_policy_v1()

        self.assertEqual(payload.get("version"), "graph_bounds_policy_v1_new")
        self.assertEqual(payload.get("bounds", {}).get("MAX_PRIMS_PER_SLOT"), 25)


if __name__ == "__main__":
    unittest.main()
