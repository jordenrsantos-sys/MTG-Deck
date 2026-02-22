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
            policy_path = Path(tmp_dir) / "graph_bounds_policy_v1.json"
            policy_path.write_text(
                '{"version":"graph_bounds_policy_v1","bounds":{"MAX_PRIMS_PER_SLOT":24,"MAX_SLOTS_PER_PRIM":80,"MAX_CARD_CARD_EDGES_TOTAL":5000},"unknown":1}',
                encoding="utf-8",
            )
            with patch("api.engine.graph_bounds_policy_v1._GRAPH_BOUNDS_POLICY_FILE", policy_path):
                with self.assertRaises(RuntimeError) as ctx:
                    load_graph_bounds_policy_v1()

        self.assertIn("GRAPH_BOUNDS_POLICY_V1_INVALID", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
