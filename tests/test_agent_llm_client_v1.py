"""Phase A2 tests for agent_llm_client_v1.

Covers the LLM client wrapper. Tests are pure-Python + mock-only --
no actual LLM network calls.

Updated in mega-task v13 (2026-05-24) for the migration from the
`anthropic` SDK to `claude-agent-sdk`. Mocks now target the wrapper's
internal `_invoke_agent_sdk` async helper + `shutil.which` (for CLI
auth detection) instead of the old `anthropic.Anthropic` class.

The end-to-end "agent runs without API key / without CLI" path is
verified in the phase-A2 integration test in
test_agent_build_deck_v1_phase_a2.py.
"""
from __future__ import annotations

import os
import unittest
from typing import Optional
from unittest.mock import MagicMock, patch

from api.engine.layers.agent_llm_client_v1 import (
    AnthropicClient,
    AgentSdkClient,  # v13: alias for AnthropicClient
    CallResult,
    DEFAULT_MODEL,
    PRICING_USD_PER_MTOK,
    _classify_error_code,
    _is_retriable,
    _classify_agent_sdk_error,
    _is_retriable_agent_sdk_error,
    _try_parse_json,
    get_default_client,
    reset_default_client_for_tests,
)


class PricingMathTests(unittest.TestCase):
    def test_sonnet_pricing_per_million_tokens(self) -> None:
        c = AnthropicClient(model="claude-sonnet-4-6", api_key="dummy")
        # 1M input + 1M output = $3 + $15 = $18.
        self.assertAlmostEqual(c.estimate_cost_usd(1_000_000, 1_000_000), 18.00, places=4)

    def test_partial_token_counts(self) -> None:
        c = AnthropicClient(model="claude-sonnet-4-6", api_key="dummy")
        # 10k input + 2k output. Input: 10000/1M * 3 = 0.03; Output: 2000/1M * 15 = 0.03.
        self.assertAlmostEqual(c.estimate_cost_usd(10_000, 2_000), 0.06, places=4)

    def test_unknown_model_returns_zero(self) -> None:
        c = AnthropicClient(model="claude-bogus-9-9", api_key="dummy")
        self.assertEqual(c.estimate_cost_usd(1_000_000, 1_000_000), 0.0)

    def test_pricing_table_includes_iteration_3_targets(self) -> None:
        # Iteration 3 will swap sonnet for opus; verify the table is
        # already populated so we don't drift later.
        self.assertIn("claude-opus-4-6", PRICING_USD_PER_MTOK)
        self.assertIn("claude-opus-4-7", PRICING_USD_PER_MTOK)


class TokenEstimationTests(unittest.TestCase):
    def test_empty_string_is_zero(self) -> None:
        self.assertEqual(AnthropicClient.estimate_input_tokens(""), 0)

    def test_short_string_is_at_least_one_token(self) -> None:
        self.assertGreaterEqual(AnthropicClient.estimate_input_tokens("a"), 1)

    def test_long_string_scales_roughly_linearly(self) -> None:
        s1 = "x" * 3500
        s2 = "x" * 7000
        t1 = AnthropicClient.estimate_input_tokens(s1)
        t2 = AnthropicClient.estimate_input_tokens(s2)
        # 7000 chars at ~3.5 chars/token should be ~2x of 3500 chars.
        self.assertGreater(t2, t1)
        ratio = t2 / max(1, t1)
        self.assertGreater(ratio, 1.5)
        self.assertLess(ratio, 2.5)


class JsonParserTests(unittest.TestCase):
    def test_raw_json_object(self) -> None:
        self.assertEqual(_try_parse_json('{"a": 1}'), {"a": 1})

    def test_raw_json_array(self) -> None:
        self.assertEqual(_try_parse_json("[1, 2, 3]"), [1, 2, 3])

    def test_inside_code_fence(self) -> None:
        text = 'Here it is:\n```json\n{"x": "y"}\n```\nDone.'
        self.assertEqual(_try_parse_json(text), {"x": "y"})

    def test_inside_unlabelled_code_fence(self) -> None:
        text = "```\n[\"a\", \"b\"]\n```"
        self.assertEqual(_try_parse_json(text), ["a", "b"])

    def test_balanced_bracket_extraction_with_prefix(self) -> None:
        text = "Here's the JSON: {\"k\": [1, 2, 3]} hope this helps."
        self.assertEqual(_try_parse_json(text), {"k": [1, 2, 3]})

    def test_returns_none_on_invalid_json(self) -> None:
        self.assertIsNone(_try_parse_json("not json at all"))

    def test_returns_none_on_empty(self) -> None:
        self.assertIsNone(_try_parse_json(""))


class IsAvailableTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_default_client_for_tests()
        # Conftest's autouse fixture sets MTG_ENGINE_DISABLE_LLM=1;
        # clear it here so these tests verify the live availability path.
        self._saved_kill = os.environ.pop("MTG_ENGINE_DISABLE_LLM", None)
        self._saved_key = os.environ.pop("ANTHROPIC_API_KEY", None)

    def tearDown(self) -> None:
        if self._saved_key is not None:
            os.environ["ANTHROPIC_API_KEY"] = self._saved_key
        else:
            os.environ.pop("ANTHROPIC_API_KEY", None)
        if self._saved_kill is not None:
            os.environ["MTG_ENGINE_DISABLE_LLM"] = self._saved_kill
        reset_default_client_for_tests()

    def test_unavailable_when_no_cli_and_no_api_key(self) -> None:
        """v13 migration: 'available' = (claude CLI in PATH) OR
        ANTHROPIC_API_KEY env. Without either, unavailable."""
        # Force CLI absent so the new auth path falls through.
        with patch(
            "api.engine.layers.agent_llm_client_v1.shutil.which",
            return_value=None,
        ):
            c = AnthropicClient()
            self.assertFalse(c.is_available())
            reason = c.unavailable_reason()
            self.assertIn("ANTHROPIC_API_KEY", reason)
            self.assertIn("Claude Code CLI", reason)

    def test_available_with_explicit_api_key_arg(self) -> None:
        # Even with CLI absent, an explicit API key is acceptable
        # auth (API-fallback path).
        with patch(
            "api.engine.layers.agent_llm_client_v1.shutil.which",
            return_value=None,
        ):
            c = AnthropicClient(api_key="sk-test-explicit")
            self.assertTrue(c.is_available())
            self.assertEqual(c.unavailable_reason(), "")

    def test_available_with_env_var_set(self) -> None:
        os.environ["ANTHROPIC_API_KEY"] = "sk-test-env"
        with patch(
            "api.engine.layers.agent_llm_client_v1.shutil.which",
            return_value=None,
        ):
            c = AnthropicClient()
            self.assertTrue(c.is_available())

    def test_available_via_claude_cli_without_api_key(self) -> None:
        """v13 NEW: with CLI present and NO API key, the wrapper
        reports available via subscription auth."""
        with patch(
            "api.engine.layers.agent_llm_client_v1.shutil.which",
            return_value="/usr/local/bin/claude",
        ):
            c = AnthropicClient()
            self.assertTrue(c.is_available())
            self.assertEqual(c.unavailable_reason(), "")
            self.assertEqual(c._resolve_auth_mode(), "subscription")

    def test_resolve_auth_mode_priorities(self) -> None:
        """CLI takes priority over API key in auth-mode reporting."""
        # Both available -> subscription wins.
        os.environ["ANTHROPIC_API_KEY"] = "sk-test"
        with patch(
            "api.engine.layers.agent_llm_client_v1.shutil.which",
            return_value="/usr/local/bin/claude",
        ):
            c = AnthropicClient()
            self.assertEqual(c._resolve_auth_mode(), "subscription")
        # No CLI but API key -> api_key.
        with patch(
            "api.engine.layers.agent_llm_client_v1.shutil.which",
            return_value=None,
        ):
            c = AnthropicClient()
            self.assertEqual(c._resolve_auth_mode(), "api_key")
        # Neither -> none.
        os.environ.pop("ANTHROPIC_API_KEY", None)
        with patch(
            "api.engine.layers.agent_llm_client_v1.shutil.which",
            return_value=None,
        ):
            c = AnthropicClient()
            self.assertEqual(c._resolve_auth_mode(), "none")

    def test_kill_switch_disables_even_with_key(self) -> None:
        # MTG_ENGINE_DISABLE_LLM=1 forces is_available() to False.
        os.environ["ANTHROPIC_API_KEY"] = "sk-real-key"
        os.environ["MTG_ENGINE_DISABLE_LLM"] = "1"
        try:
            c = AnthropicClient()
            self.assertFalse(c.is_available())
            self.assertIn("MTG_ENGINE_DISABLE_LLM", c.unavailable_reason())
        finally:
            os.environ.pop("MTG_ENGINE_DISABLE_LLM", None)


class BudgetGuardTests(unittest.TestCase):
    """Pre-call input-token budget guard. No SDK call should happen when
    the input estimate blows the budget."""

    def setUp(self) -> None:
        # Conftest sets MTG_ENGINE_DISABLE_LLM=1 by default; clear it
        # here so these tests can exercise the live availability path.
        os.environ.pop("MTG_ENGINE_DISABLE_LLM", None)
        os.environ["ANTHROPIC_API_KEY"] = "sk-test"

    def tearDown(self) -> None:
        os.environ.pop("ANTHROPIC_API_KEY", None)

    def test_input_budget_exceeded_short_circuits(self) -> None:
        c = AnthropicClient(api_key="sk-test")
        # Inflate user prompt past the budget.
        huge = "x" * 20_000  # ~5700 tokens at 3.5 chars/token
        # v13: assert no SDK invocation occurs by patching the async
        # helper at the wrapper layer.
        with patch(
            "api.engine.layers.agent_llm_client_v1._invoke_agent_sdk",
        ) as mock_invoke:
            result = c.call_with_budget(
                system="sys", user=huge,
                max_input_tokens=100, max_output_tokens=1000,
            )
            mock_invoke.assert_not_called()
        self.assertFalse(result.ok)
        self.assertTrue(result.budget_exceeded)
        self.assertEqual(result.error_code, "INPUT_TOKEN_BUDGET_EXCEEDED")

    def test_fallback_path_when_unavailable(self) -> None:
        # v13: BOTH auth paths must be absent for is_available()=False.
        os.environ.pop("ANTHROPIC_API_KEY", None)
        with patch(
            "api.engine.layers.agent_llm_client_v1.shutil.which",
            return_value=None,
        ):
            c = AnthropicClient()  # no api key, no CLI
            result = c.call_with_budget(
                system="s", user="u",
                max_input_tokens=1000, max_output_tokens=100,
            )
        self.assertFalse(result.ok)
        self.assertTrue(result.fallback_used)
        self.assertEqual(result.error_code, "LLM_UNAVAILABLE")


class SuccessfulCallTests(unittest.TestCase):
    """call_with_budget happy path — mocked SDK returns a Message-like
    object; we verify text/parsed_json/usage are returned and cost is
    computed."""

    def setUp(self) -> None:
        # Conftest sets MTG_ENGINE_DISABLE_LLM=1 by default; clear it
        # here so these tests can exercise the live availability path.
        os.environ.pop("MTG_ENGINE_DISABLE_LLM", None)
        os.environ["ANTHROPIC_API_KEY"] = "sk-test"

    def tearDown(self) -> None:
        os.environ.pop("ANTHROPIC_API_KEY", None)

    def _make_fake_invoke(
        self, text: str, input_tokens: int = 1000,
        output_tokens: int = 200,
        total_cost_usd: Optional[float] = None,
        error_category: Optional[str] = None,
    ):
        """Return an async function suitable for patching
        `_invoke_agent_sdk`. v13: replaces the old MagicMock SDK shape."""
        async def _fake(*, system, user, model, max_budget_usd):
            return {
                "text": text,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "total_cost_usd": total_cost_usd,
                "error_category": error_category,
                "error_message": None,
                "api_error_status": None,
                "stop_reason": "end_turn",
            }
        return _fake

    def test_returns_text_and_parsed_json(self) -> None:
        c = AnthropicClient(api_key="sk-test")
        fake = self._make_fake_invoke(
            '{"hello": "world"}', input_tokens=1500, output_tokens=300,
        )
        with patch(
            "api.engine.layers.agent_llm_client_v1._invoke_agent_sdk",
            new=fake,
        ):
            result = c.call_with_budget(
                system="sys", user="user prompt",
                max_input_tokens=10_000, max_output_tokens=2_000,
            )
        self.assertTrue(result.ok)
        self.assertEqual(result.text, '{"hello": "world"}')
        self.assertEqual(result.parsed_json, {"hello": "world"})
        self.assertEqual(result.input_tokens, 1500)
        self.assertEqual(result.output_tokens, 300)
        # Sonnet 4.6 pricing: 1500/1M * 3 + 300/1M * 15 = 0.0045 + 0.0045 = 0.009
        self.assertAlmostEqual(result.cost_usd, 0.009, places=4)
        # No total_cost_usd from SDK -> cost_basis falls back to estimate.
        self.assertEqual(result.cost_basis, "api_estimate")
        self.assertEqual(result.model, DEFAULT_MODEL)

    def test_invalid_json_text_still_returns_ok(self) -> None:
        c = AnthropicClient(api_key="sk-test")
        fake = self._make_fake_invoke("plain text response, no json")
        with patch(
            "api.engine.layers.agent_llm_client_v1._invoke_agent_sdk",
            new=fake,
        ):
            result = c.call_with_budget(
                system="s", user="u",
                max_input_tokens=10_000, max_output_tokens=2_000,
            )
        self.assertTrue(result.ok)
        self.assertIsNone(result.parsed_json)
        self.assertTrue(result.text.startswith("plain text"))

    def test_sdk_reported_cost_promotes_basis_to_subscription(self) -> None:
        """v13 NEW: when ResultMessage.total_cost_usd > 0, cost_basis
        flips to 'subscription_credit' so callers can render the
        right caption."""
        c = AnthropicClient(api_key="sk-test")
        fake = self._make_fake_invoke(
            '{"ok": 1}', input_tokens=500, output_tokens=100,
            total_cost_usd=0.0123,
        )
        with patch(
            "api.engine.layers.agent_llm_client_v1._invoke_agent_sdk",
            new=fake,
        ):
            result = c.call_with_budget(
                system="s", user="u",
                max_input_tokens=10_000, max_output_tokens=2_000,
            )
        self.assertTrue(result.ok)
        self.assertEqual(result.cost_basis, "subscription_credit")
        self.assertAlmostEqual(result.cost_usd, 0.0123, places=4)

    def test_api_error_category_classifies_correctly(self) -> None:
        """v13 NEW: AssistantMessage.error='invalid_request' should
        map to LLM_BAD_REQUEST. Non-retriable -> single attempt."""
        c = AnthropicClient(api_key="sk-test")
        fake = self._make_fake_invoke(
            "", input_tokens=10, output_tokens=0,
            error_category="invalid_request",
        )
        with patch(
            "api.engine.layers.agent_llm_client_v1._invoke_agent_sdk",
            new=fake,
        ):
            result = c.call_with_budget(
                system="s", user="u",
                max_input_tokens=10_000, max_output_tokens=2_000,
            )
        self.assertFalse(result.ok)
        self.assertEqual(result.error_code, "LLM_BAD_REQUEST")


class RetryTests(unittest.TestCase):
    """Retriable errors (RateLimitError, APIConnectionError) trigger
    backoff; permanent errors (AuthenticationError) do not."""

    def setUp(self) -> None:
        # Conftest sets MTG_ENGINE_DISABLE_LLM=1 by default; clear it
        # here so these tests can exercise the live availability path.
        os.environ.pop("MTG_ENGINE_DISABLE_LLM", None)
        os.environ["ANTHROPIC_API_KEY"] = "sk-test"

    def tearDown(self) -> None:
        os.environ.pop("ANTHROPIC_API_KEY", None)

    def test_classify_error_code_map(self) -> None:
        class RateLimitError(Exception):
            pass

        class AuthenticationError(Exception):
            pass

        self.assertEqual(_classify_error_code(RateLimitError("x")), "LLM_RATE_LIMITED")
        self.assertEqual(_classify_error_code(AuthenticationError("y")), "LLM_AUTH_FAILED")

    def test_is_retriable_on_rate_limit(self) -> None:
        class RateLimitError(Exception):
            pass
        self.assertTrue(_is_retriable(RateLimitError("x")))

    def test_is_retriable_false_on_auth(self) -> None:
        class AuthenticationError(Exception):
            pass
        self.assertFalse(_is_retriable(AuthenticationError("x")))


class AgentSdkErrorEnumTests(unittest.TestCase):
    """v13 NEW: Agent SDK reports errors via AssistantMessage.error
    enum, not raised exceptions. Separate helpers map those to
    LLM_* codes + retriability."""

    def test_classify_known_categories(self) -> None:
        self.assertEqual(
            _classify_agent_sdk_error("authentication_failed"),
            "LLM_AUTH_FAILED",
        )
        self.assertEqual(
            _classify_agent_sdk_error("rate_limit"),
            "LLM_RATE_LIMITED",
        )
        self.assertEqual(
            _classify_agent_sdk_error("server_error"),
            "LLM_SERVER_ERROR",
        )
        self.assertEqual(
            _classify_agent_sdk_error("billing_error"),
            "LLM_BILLING_ERROR",
        )

    def test_classify_unknown_falls_through(self) -> None:
        self.assertEqual(
            _classify_agent_sdk_error("some_new_category"),
            "LLM_UNEXPECTED_ERROR",
        )

    def test_retriable_rate_limit_and_server_error(self) -> None:
        self.assertTrue(_is_retriable_agent_sdk_error("rate_limit"))
        self.assertTrue(_is_retriable_agent_sdk_error("server_error"))

    def test_not_retriable_auth_or_billing(self) -> None:
        self.assertFalse(_is_retriable_agent_sdk_error("authentication_failed"))
        self.assertFalse(_is_retriable_agent_sdk_error("billing_error"))
        self.assertFalse(_is_retriable_agent_sdk_error("invalid_request"))


class AgentSdkClientAliasTests(unittest.TestCase):
    """v13: AgentSdkClient is an alias for AnthropicClient (class
    renamed conceptually; old name kept for backwards compat)."""

    def test_alias_is_anthropic_client(self) -> None:
        self.assertIs(AgentSdkClient, AnthropicClient)


class DefaultClientSingletonTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_default_client_for_tests()

    def tearDown(self) -> None:
        reset_default_client_for_tests()

    def test_same_instance_returned(self) -> None:
        a = get_default_client()
        b = get_default_client()
        self.assertIs(a, b)


if __name__ == "__main__":
    unittest.main()
