"""Phase A2 integration tests — verifies that the iteration-2 LLM probe
wires in without breaking iteration-1 behavior.

Specifically: with ANTHROPIC_API_KEY unset, `compute_agent_build_deck_v1`
must
  - produce the same structural shape as iteration 1,
  - include `llm_metrics` in the summary with `available=False`,
  - emit a `LLM_LAYER_UNAVAILABLE` warning,
  - NOT touch the network (no anthropic.Anthropic() instantiation).

These tests run with a mocked Phase B/C/D so they're fast and don't need
a real db_snapshot.
"""
from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from api.engine.layers.agent_llm_client_v1 import reset_default_client_for_tests


class FallbackPathTests(unittest.TestCase):
    """Without an API key, build_deck must complete the iteration-1 path
    cleanly and surface a clear warning."""

    def setUp(self) -> None:
        reset_default_client_for_tests()
        self._saved_key = os.environ.pop("ANTHROPIC_API_KEY", None)

    def tearDown(self) -> None:
        if self._saved_key is not None:
            os.environ["ANTHROPIC_API_KEY"] = self._saved_key
        reset_default_client_for_tests()

    def _stub_pool(self, **_kwargs):
        return {
            "candidates": [],
            "color_identity": ["B"],
            "archetype_brief": {"staple_cards": []},
            "must_includes_resolved": [],
            "must_includes_dropped": [],
            "warnings": [],
            "endpoint_calls": 1,
        }

    def _stub_validate(self, **_kwargs):
        return {
            "issues": [],
            "themes_classified": [],
            "theme_coherence_score": 1.0,
            "bracket_estimate": None,
            "strength_check_summary": None,
            "endpoint_calls_made": 0,
        }

    def test_build_runs_without_api_key_and_marks_llm_unavailable(self) -> None:
        from api.engine.layers import agent_build_deck_v1 as mod

        with patch.object(mod, "_build_candidate_pool", side_effect=self._stub_pool), \
             patch.object(mod, "_validate_deck", side_effect=self._stub_validate):
            result = mod.compute_agent_build_deck_v1(
                db_snapshot_id="test-snap",
                commander="Edgar Markov",
                bracket="B3",
                theme_hints=["TYPAL_VAMPIRES"],
                must_include_cards=[],
                skip_strength_check=True,
            )

        self.assertEqual(result["status"], "OK")
        # Summary keeps the iteration-1 shape PLUS iteration-2 additions.
        summary = result["summary"]
        self.assertIn("llm_metrics", summary)
        self.assertFalse(summary["llm_metrics"]["available"])
        self.assertEqual(summary["llm_metrics"]["total_cost_usd"], 0.0)
        self.assertEqual(summary["llm_metrics"]["calls"], [])
        self.assertIn("summary_narrative", summary)
        self.assertIsNone(summary["summary_narrative"])
        self.assertIn("consider_adding", summary)
        self.assertEqual(summary["consider_adding"], [])
        self.assertIn("novel_combo_flags", summary)
        self.assertEqual(summary["novel_combo_flags"], [])

        # Warning exists and is specific.
        codes = [w["code"] for w in result["warnings"]]
        self.assertIn("LLM_LAYER_UNAVAILABLE", codes)

    def test_summary_for_invalid_bracket_also_has_llm_keys(self) -> None:
        # The early-return paths (bracket invalid, missing commander) must
        # also surface the iteration-2 shape so the UI doesn't crash on
        # missing keys.
        from api.engine.layers.agent_build_deck_v1 import compute_agent_build_deck_v1
        result = compute_agent_build_deck_v1(
            db_snapshot_id="test-snap",
            commander="Edgar Markov",
            bracket="B99",  # invalid
        )
        self.assertEqual(result["status"], "FAILED")
        summary = result["summary"]
        self.assertIn("llm_metrics", summary)
        self.assertFalse(summary["llm_metrics"]["available"])
        self.assertIn("summary_narrative", summary)
        self.assertIn("novel_combo_flags", summary)


class NoNetworkContactTests(unittest.TestCase):
    """A defense-in-depth check: with all auth paths UNSET, no SDK
    network call must occur during a build.

    v13 migration: assertion target changed from `anthropic.Anthropic`
    to `claude_agent_sdk.query` (the Agent SDK entry point the
    migrated wrapper invokes). Also patches `shutil.which` to force
    CLI absence so the wrapper's `is_available()` returns False.
    """

    def setUp(self) -> None:
        reset_default_client_for_tests()
        self._saved_key = os.environ.pop("ANTHROPIC_API_KEY", None)

    def tearDown(self) -> None:
        if self._saved_key is not None:
            os.environ["ANTHROPIC_API_KEY"] = self._saved_key
        reset_default_client_for_tests()

    def test_no_sdk_call_when_no_auth_available(self) -> None:
        from api.engine.layers import agent_build_deck_v1 as mod

        def _stub_pool(**_kwargs):
            return {
                "candidates": [], "color_identity": [],
                "archetype_brief": {"staple_cards": []},
                "must_includes_resolved": [], "must_includes_dropped": [],
                "warnings": [], "endpoint_calls": 1,
            }

        def _stub_validate(**_kwargs):
            return {
                "issues": [], "themes_classified": [],
                "theme_coherence_score": 1.0, "bracket_estimate": None,
                "strength_check_summary": None, "endpoint_calls_made": 0,
            }

        with patch.object(mod, "_build_candidate_pool", side_effect=_stub_pool), \
             patch.object(mod, "_validate_deck", side_effect=_stub_validate), \
             patch(
                "api.engine.layers.agent_llm_client_v1.shutil.which",
                return_value=None,
             ), \
             patch("claude_agent_sdk.query") as mock_query:
            mod.compute_agent_build_deck_v1(
                db_snapshot_id="test-snap",
                commander="Yuriko, the Tiger's Shadow",
                bracket="B3",
                skip_strength_check=True,
            )
            mock_query.assert_not_called()


if __name__ == "__main__":
    unittest.main()
