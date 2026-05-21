"""Phase B2 tests — LLM call #1 (intent interpreter) integration.

Covers:
  - The intent interpreter parses well-formed structured output.
  - Inferred themes augment theme_hints fed to _build_candidate_pool.
  - Suggested extensions get LLM_EXTENSION_BOOST in the candidate pool.
  - Conflict warnings surface as INTENT_CONFLICT_WARNING entries.
  - The interpreter is skipped entirely when the LLM client is
    unavailable (Phase A2 fallback path).
  - LLM call metrics (tokens, cost, latency) accrue to summary.llm_metrics.
"""
from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

from api.engine.layers.agent_llm_client_v1 import (
    AnthropicClient,
    CallResult,
    reset_default_client_for_tests,
)
from api.engine.layers.agent_build_deck_v1 import (
    LLM_EXTENSION_BOOST,
    USER_PICK_SCORE,
    _as_list_of_dicts,
    _as_list_of_strings,
    _build_intent_interpreter_user_prompt,
    _run_intent_interpreter,
    _build_candidate_pool,
)


# ============================================================
# Pure helpers.
# ============================================================


class NormalizationTests(unittest.TestCase):
    def test_as_list_of_strings_drops_non_strings(self) -> None:
        self.assertEqual(_as_list_of_strings(["a", 1, None, " b ", ""]), ["a", "b"])

    def test_as_list_of_strings_none(self) -> None:
        self.assertEqual(_as_list_of_strings(None), [])

    def test_as_list_of_dicts_drops_non_dicts(self) -> None:
        self.assertEqual(_as_list_of_dicts([{"a": 1}, "x", None]), [{"a": 1}])


class PromptBuilderTests(unittest.TestCase):
    def test_includes_commander_bracket_themes_must_includes(self) -> None:
        prompt = _build_intent_interpreter_user_prompt(
            commander="Edgar Markov",
            bracket="B3",
            theme_hints=["TYPAL_VAMPIRES"],
            must_include_cards=["Vito, Thorn of the Dusk Rose"],
        )
        self.assertIn("Edgar Markov", prompt)
        self.assertIn("B3", prompt)
        self.assertIn("TYPAL_VAMPIRES", prompt)
        self.assertIn("Vito, Thorn of the Dusk Rose", prompt)
        # The output-shape spec must be present so the model returns valid JSON.
        self.assertIn("suggested_extensions", prompt)
        self.assertIn("implicit_themes", prompt)
        self.assertIn("likely_win_condition", prompt)

    def test_handles_empty_inputs(self) -> None:
        prompt = _build_intent_interpreter_user_prompt(
            commander="Atraxa, Praetors' Voice",
            bracket="B2",
            theme_hints=[],
            must_include_cards=[],
        )
        self.assertIn("(none provided)", prompt)
        self.assertIn("Atraxa", prompt)


# ============================================================
# _run_intent_interpreter — mocked LLM.
# ============================================================


def _make_call_result(text: str, ok: bool = True,
                      input_tokens: int = 1000, output_tokens: int = 500,
                      cost_usd: float = 0.0105, latency_ms: int = 1234,
                      error_code: str = None) -> CallResult:
    """Build a CallResult with parsed_json populated from text via the
    real `_try_parse_json` so the tests exercise the actual JSON-parse
    logic rather than mocking past it."""
    from api.engine.layers.agent_llm_client_v1 import _try_parse_json
    return CallResult(
        ok=ok, text=text, parsed_json=_try_parse_json(text) if ok else None,
        input_tokens=input_tokens, output_tokens=output_tokens,
        cost_usd=cost_usd, latency_ms=latency_ms,
        model="claude-sonnet-4-6", error_code=error_code,
    )


class IntentInterpreterParseTests(unittest.TestCase):
    """The interpreter returns a normalized dict on a well-formed
    response, or None on failure."""

    def test_well_formed_response_parses(self) -> None:
        sample_json = """
        {
          "must_include_analysis": [
            {"card": "Vito, Thorn of the Dusk Rose", "type": "Creature - Vampire",
             "key_abilities": ["Lifegain", "Drain"], "signals_archetype": "Lifegain Vampires"}
          ],
          "implicit_themes": ["lifegain_payoffs", "blood_token_synergy"],
          "suggested_extensions": [
            {"card": "Sanguine Bond", "why": "Doubles up with Vito's drain."},
            {"card": "Crested Sunmare", "why": "Lifegain payoff that fits the theme."}
          ],
          "conflict_warnings": [],
          "likely_win_condition": "Drain opponents via lifegain triggers."
        }
        """
        client = MagicMock()
        client.call_with_budget.return_value = _make_call_result(sample_json)
        metrics = {"calls": []}
        warnings: list = []
        result = _run_intent_interpreter(
            llm_client=client,
            commander="Edgar Markov",
            bracket="B3",
            theme_hints=["TYPAL_VAMPIRES"],
            must_include_cards=["Vito, Thorn of the Dusk Rose"],
            llm_metrics=metrics,
            warnings=warnings,
        )
        self.assertIsNotNone(result)
        self.assertEqual(len(result["must_include_analysis"]), 1)
        self.assertEqual(result["implicit_themes"], ["lifegain_payoffs", "blood_token_synergy"])
        self.assertEqual(len(result["suggested_extensions"]), 2)
        self.assertEqual(result["conflict_warnings"], [])
        self.assertIn("Drain", result["likely_win_condition"])

    def test_metrics_recorded_on_success(self) -> None:
        client = MagicMock()
        client.call_with_budget.return_value = _make_call_result('{"implicit_themes": []}')
        metrics: dict = {"calls": []}
        _run_intent_interpreter(
            llm_client=client, commander="X", bracket="B3",
            theme_hints=[], must_include_cards=[],
            llm_metrics=metrics, warnings=[],
        )
        self.assertEqual(len(metrics["calls"]), 1)
        rec = metrics["calls"][0]
        self.assertEqual(rec["phase"], "B2_intent_interpreter")
        self.assertTrue(rec["ok"])
        self.assertEqual(rec["input_tokens"], 1000)
        self.assertEqual(rec["output_tokens"], 500)
        self.assertGreater(rec["cost_usd"], 0)
        self.assertGreater(rec["latency_ms"], 0)

    def test_invalid_json_emits_warning_and_returns_none(self) -> None:
        client = MagicMock()
        client.call_with_budget.return_value = _make_call_result("not json at all")
        warnings: list = []
        result = _run_intent_interpreter(
            llm_client=client, commander="X", bracket="B3",
            theme_hints=[], must_include_cards=[],
            llm_metrics={"calls": []}, warnings=warnings,
        )
        self.assertIsNone(result)
        codes = [w["code"] for w in warnings]
        self.assertIn("INTENT_INTERPRETER_INVALID_JSON", codes)

    def test_llm_call_failure_emits_warning(self) -> None:
        client = MagicMock()
        client.call_with_budget.return_value = _make_call_result(
            "", ok=False, error_code="LLM_RATE_LIMITED",
        )
        warnings: list = []
        metrics: dict = {"calls": []}
        result = _run_intent_interpreter(
            llm_client=client, commander="X", bracket="B3",
            theme_hints=[], must_include_cards=[],
            llm_metrics=metrics, warnings=warnings,
        )
        self.assertIsNone(result)
        codes = [w["code"] for w in warnings]
        self.assertIn("INTENT_INTERPRETER_FAILED", codes)
        # Metrics still record the attempt for the cost-accountability story.
        self.assertEqual(len(metrics["calls"]), 1)
        self.assertFalse(metrics["calls"][0]["ok"])


# ============================================================
# Candidate-pool integration — suggested_extension_names boost.
# ============================================================


def _stub_archetype_brief(staples: list, color_identity: list = None) -> dict:
    return {
        "color_identity": color_identity or ["B"],
        "staple_cards": staples,
        "common_archetypes": [],
        "theme_distribution": [],
        "warnings": [],
    }


class CandidatePoolBoostTests(unittest.TestCase):
    """Cards in suggested_extension_names get LLM_EXTENSION_BOOST added
    to their score and an 'llm_intent_extension' source marker."""

    def test_boost_applied_to_named_card(self) -> None:
        from api.engine.layers import agent_endpoints_v1 as ep
        # Stub archetype_brief and theme_top_cards so the pool builds
        # from synthetic data only.
        brief_stub = _stub_archetype_brief(staples=[
            {"name": "Sanguine Bond", "usage_pct": 0.10},
        ])

        def _theme_stub(**_kwargs):
            return {
                "results": [
                    {"name": "Sanguine Bond", "theme_signal_count": 2,
                     "primitives": ["LIFEGAIN_PAYOFF"], "type_line": "Enchantment", "cmc": 5},
                ],
                "warnings": [],
            }

        with patch.object(ep, "compute_archetype_brief_v1", return_value=brief_stub), \
             patch.object(ep, "compute_theme_top_cards_v1", side_effect=_theme_stub):
            pool_no_boost = _build_candidate_pool(
                db_snapshot_id="snap", commander="Edgar Markov",
                bracket="B3", theme_hints=["TYPAL_VAMPIRES"],
                must_include_cards=[], seed=42,
                call_counter={"calls": 0},
                suggested_extension_names=None,
            )
            pool_with_boost = _build_candidate_pool(
                db_snapshot_id="snap", commander="Edgar Markov",
                bracket="B3", theme_hints=["TYPAL_VAMPIRES"],
                must_include_cards=[], seed=42,
                call_counter={"calls": 0},
                suggested_extension_names=["Sanguine Bond"],
            )
        s_no = next(c for c in pool_no_boost["candidates"] if c["name"] == "Sanguine Bond")
        s_yes = next(c for c in pool_with_boost["candidates"] if c["name"] == "Sanguine Bond")
        self.assertAlmostEqual(s_yes["score"], s_no["score"] + LLM_EXTENSION_BOOST, places=3)
        self.assertIn("llm_intent_extension", s_yes["source"])

    def test_boost_does_not_upgrade_user_pick(self) -> None:
        # If a card is BOTH a user must_include AND named by the LLM as
        # an extension, it stays at INF — not INF+25 (which is still INF
        # but we shouldn't munge the rationale weirdly).
        from api.engine.layers import agent_endpoints_v1 as ep

        # Mock find_card_by_name so the must_include validates.
        from engine import db as eng_db

        def _fake_find(snap, name):
            return {
                "name": name, "color_identity": ["B"],
                "primitives": ["LIFEGAIN_PAYOFF"], "type_line": "Enchantment",
                "cmc": 5,
            }

        with patch.object(ep, "compute_archetype_brief_v1",
                          return_value=_stub_archetype_brief(staples=[])), \
             patch.object(ep, "compute_theme_top_cards_v1",
                          return_value={"results": [], "warnings": []}), \
             patch.object(eng_db, "find_card_by_name", side_effect=_fake_find):
            pool = _build_candidate_pool(
                db_snapshot_id="snap", commander="Edgar Markov",
                bracket="B3", theme_hints=[],
                must_include_cards=["Sanguine Bond"], seed=42,
                call_counter={"calls": 0},
                suggested_extension_names=["Sanguine Bond"],
            )
        s = next(c for c in pool["candidates"] if c["name"] == "Sanguine Bond")
        self.assertEqual(s["score"], USER_PICK_SCORE)

    def test_unrecognized_extension_is_skipped(self) -> None:
        # An LLM-named card that's NOT in the deterministic pool simply
        # gets ignored (no new candidate injection). This is the
        # iteration-2 contract — the LLM re-ranks; it doesn't introduce.
        from api.engine.layers import agent_endpoints_v1 as ep
        with patch.object(ep, "compute_archetype_brief_v1",
                          return_value=_stub_archetype_brief(staples=[
                              {"name": "Sol Ring", "usage_pct": 0.50},
                          ])), \
             patch.object(ep, "compute_theme_top_cards_v1",
                          return_value={"results": [], "warnings": []}):
            pool = _build_candidate_pool(
                db_snapshot_id="snap", commander="X",
                bracket="B3", theme_hints=["TYPAL_VAMPIRES"],
                must_include_cards=[], seed=42,
                call_counter={"calls": 0},
                suggested_extension_names=["NonexistentCard1234"],
            )
        names = [c["name"] for c in pool["candidates"]]
        self.assertNotIn("NonexistentCard1234", names)


# ============================================================
# End-to-end: build_deck_v1 with mocked LLM uses LLM augmentation.
# ============================================================


class BuildDeckWithLlmTests(unittest.TestCase):
    """The compute_agent_build_deck_v1 happy path with LLM available
    threads the intent_analysis result through to summary.intent_analysis
    AND surfaces conflict_warnings."""

    def setUp(self) -> None:
        reset_default_client_for_tests()
        # Conftest's autouse fixture sets the kill switch; clear it here.
        os.environ.pop("MTG_ENGINE_DISABLE_LLM", None)
        os.environ["ANTHROPIC_API_KEY"] = "sk-test"

    def tearDown(self) -> None:
        os.environ.pop("ANTHROPIC_API_KEY", None)
        # Restore kill switch for subsequent autouse runs.
        os.environ["MTG_ENGINE_DISABLE_LLM"] = "1"
        reset_default_client_for_tests()

    def test_intent_analysis_appears_in_summary(self) -> None:
        from api.engine.layers import agent_build_deck_v1 as mod

        intent_json = """
        {
          "must_include_analysis": [],
          "implicit_themes": ["combo_kill"],
          "suggested_extensions": [{"card": "Brainstorm", "why": "filters into combo"}],
          "conflict_warnings": ["B2 with Thoracle+Consult is a B5-class combo"],
          "likely_win_condition": "Thoracle + Consultation."
        }
        """

        # Patch the LLM client's call_with_budget to return our canned response.
        fake_result = _make_call_result(intent_json)
        # Build a real client but force is_available=True via env var,
        # and replace call_with_budget to return our canned response.
        from api.engine.layers.agent_llm_client_v1 import get_default_client
        client = get_default_client()
        with patch.object(client, "call_with_budget", return_value=fake_result):
            # Stub Phase B/C/D to keep the test fast and focused.
            def _pool_stub(**kwargs):
                # Verify the augmented themes made it in.
                self.assertIn("combo_kill", kwargs["theme_hints"])
                self.assertIn("TYPAL_NINJAS", kwargs["theme_hints"])
                # Suggested extensions threaded through.
                self.assertEqual(kwargs["suggested_extension_names"], ["Brainstorm"])
                return {
                    "candidates": [], "color_identity": ["U", "B"],
                    "archetype_brief": {"staple_cards": []},
                    "must_includes_resolved": [], "must_includes_dropped": [],
                    "warnings": [], "endpoint_calls": 1,
                }

            def _validate_stub(**_kwargs):
                return {
                    "issues": [], "themes_classified": [],
                    "theme_coherence_score": 1.0, "bracket_estimate": None,
                    "strength_check_summary": None, "endpoint_calls_made": 0,
                }

            # Stub out C2.1 / C2.2 / D2 LLM calls so this test only
            # exercises the B2 intent-interpreter path. Each phase
            # downstream of B2 has its own dedicated test file.
            def _critic_noop(*, deck, **_kwargs):
                return deck, []

            def _wild_noop(*, deck, **_kwargs):
                return deck, []

            def _final_noop(*, deck, **_kwargs):
                return deck, []

            with patch.object(mod, "_build_candidate_pool", side_effect=_pool_stub), \
                 patch.object(mod, "_validate_deck", side_effect=_validate_stub), \
                 patch.object(mod, "_run_candidate_critic", side_effect=_critic_noop), \
                 patch.object(mod, "_run_wild_combo_discovery", side_effect=_wild_noop), \
                 patch.object(mod, "_run_final_critic", side_effect=_final_noop):
                result = mod.compute_agent_build_deck_v1(
                    db_snapshot_id="snap",
                    commander="Yuriko, the Tiger's Shadow",
                    bracket="B2",
                    theme_hints=["TYPAL_NINJAS"],
                    must_include_cards=[],
                    skip_strength_check=True,
                )

        summary = result["summary"]
        ia = summary["intent_analysis"]
        self.assertIsNotNone(ia)
        self.assertEqual(ia["likely_win_condition"], "Thoracle + Consultation.")
        # Conflict warning surfaced in build warnings.
        codes = [w["code"] for w in result["warnings"]]
        self.assertIn("INTENT_CONFLICT_WARNING", codes)
        # LLM metrics accrued. We stubbed C2.1/C2.2/D2 so B2 is the
        # only LLM call we initiated; Pillar E (iter-3 Phase 10) may
        # add a mana_base_critique entry if the optimizer detects a
        # discrepancy in the stubbed deck. Accept 1-2 entries; the
        # first must be the B2 intent interpreter.
        calls = summary["llm_metrics"]["calls"]
        self.assertIn(len(calls), (1, 2))
        self.assertEqual(calls[0]["phase"], "B2_intent_interpreter")
        self.assertGreater(summary["llm_metrics"]["total_cost_usd"], 0)


if __name__ == "__main__":
    unittest.main()
