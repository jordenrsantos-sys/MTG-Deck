"""Phase A2 tests for agent_llm_client_v1.

Covers the LLM client wrapper that iteration 2 stacks four reasoning
phases on top of (intent / candidate critic / wild combo / final
rationale). Tests are pure-Python + mock-only — no actual Anthropic
network calls.

Three groups:
  - Pricing + token estimation — pure math.
  - JSON parsing helper — string handling.
  - is_available() / call_with_budget() — mocked SDK paths covering
    success, retry, budget overflow, missing-key fallback.

The end-to-end "agent runs without API key" path is verified in the
phase-A2 integration test in test_agent_build_deck_v1_phase_a2.py.
"""
from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

from api.engine.layers.agent_llm_client_v1 import (
    AnthropicClient,
    CallResult,
    DEFAULT_MODEL,
    PRICING_USD_PER_MTOK,
    _classify_error_code,
    _is_retriable,
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
        self._saved_key = os.environ.pop("ANTHROPIC_API_KEY", None)

    def tearDown(self) -> None:
        if self._saved_key is not None:
            os.environ["ANTHROPIC_API_KEY"] = self._saved_key
        else:
            os.environ.pop("ANTHROPIC_API_KEY", None)
        reset_default_client_for_tests()

    def test_unavailable_when_env_var_missing_and_no_explicit_key(self) -> None:
        # No env var; no constructor key.
        c = AnthropicClient()
        self.assertFalse(c.is_available())
        reason = c.unavailable_reason()
        self.assertIn("ANTHROPIC_API_KEY", reason)

    def test_available_with_explicit_api_key_arg(self) -> None:
        c = AnthropicClient(api_key="sk-test-explicit")
        self.assertTrue(c.is_available())
        self.assertEqual(c.unavailable_reason(), "")

    def test_available_with_env_var_set(self) -> None:
        os.environ["ANTHROPIC_API_KEY"] = "sk-test-env"
        c = AnthropicClient()
        self.assertTrue(c.is_available())


class BudgetGuardTests(unittest.TestCase):
    """Pre-call input-token budget guard. No SDK call should happen when
    the input estimate blows the budget."""

    def setUp(self) -> None:
        os.environ["ANTHROPIC_API_KEY"] = "sk-test"

    def tearDown(self) -> None:
        os.environ.pop("ANTHROPIC_API_KEY", None)

    def test_input_budget_exceeded_short_circuits(self) -> None:
        c = AnthropicClient(api_key="sk-test")
        # Inflate user prompt past the budget.
        huge = "x" * 20_000  # ~5700 tokens at 3.5 chars/token
        # No network call should occur — assert by patching sdk import.
        with patch("anthropic.Anthropic") as mock_class:
            result = c.call_with_budget(
                system="sys",
                user=huge,
                max_input_tokens=100,
                max_output_tokens=1000,
            )
            mock_class.assert_not_called()
        self.assertFalse(result.ok)
        self.assertTrue(result.budget_exceeded)
        self.assertEqual(result.error_code, "INPUT_TOKEN_BUDGET_EXCEEDED")

    def test_fallback_path_when_unavailable(self) -> None:
        os.environ.pop("ANTHROPIC_API_KEY", None)
        c = AnthropicClient()  # no api key, env var unset
        result = c.call_with_budget(
            system="s",
            user="u",
            max_input_tokens=1000,
            max_output_tokens=100,
        )
        self.assertFalse(result.ok)
        self.assertTrue(result.fallback_used)
        self.assertEqual(result.error_code, "LLM_UNAVAILABLE")


class SuccessfulCallTests(unittest.TestCase):
    """call_with_budget happy path — mocked SDK returns a Message-like
    object; we verify text/parsed_json/usage are returned and cost is
    computed."""

    def setUp(self) -> None:
        os.environ["ANTHROPIC_API_KEY"] = "sk-test"

    def tearDown(self) -> None:
        os.environ.pop("ANTHROPIC_API_KEY", None)

    def _make_response(self, text: str, input_tokens: int = 1000,
                       output_tokens: int = 200) -> MagicMock:
        block = MagicMock()
        block.type = "text"
        block.text = text
        usage = MagicMock()
        usage.input_tokens = input_tokens
        usage.output_tokens = output_tokens
        response = MagicMock()
        response.content = [block]
        response.usage = usage
        return response

    def test_returns_text_and_parsed_json(self) -> None:
        c = AnthropicClient(api_key="sk-test")
        fake_sdk_client = MagicMock()
        fake_sdk_client.messages.create.return_value = self._make_response(
            '{"hello": "world"}', input_tokens=1500, output_tokens=300,
        )
        with patch("anthropic.Anthropic", return_value=fake_sdk_client):
            result = c.call_with_budget(
                system="sys",
                user="user prompt",
                max_input_tokens=10_000,
                max_output_tokens=2_000,
            )
        self.assertTrue(result.ok)
        self.assertEqual(result.text, '{"hello": "world"}')
        self.assertEqual(result.parsed_json, {"hello": "world"})
        self.assertEqual(result.input_tokens, 1500)
        self.assertEqual(result.output_tokens, 300)
        # Sonnet 4.6 pricing: 1500/1M * 3 + 300/1M * 15 = 0.0045 + 0.0045 = 0.009
        self.assertAlmostEqual(result.cost_usd, 0.009, places=4)
        self.assertEqual(result.model, DEFAULT_MODEL)

    def test_invalid_json_text_still_returns_ok(self) -> None:
        # Text that isn't JSON — parsed_json should be None but ok=True.
        c = AnthropicClient(api_key="sk-test")
        fake_sdk_client = MagicMock()
        fake_sdk_client.messages.create.return_value = self._make_response(
            "plain text response, no json",
        )
        with patch("anthropic.Anthropic", return_value=fake_sdk_client):
            result = c.call_with_budget(
                system="s",
                user="u",
                max_input_tokens=10_000,
                max_output_tokens=2_000,
            )
        self.assertTrue(result.ok)
        self.assertIsNone(result.parsed_json)
        self.assertTrue(result.text.startswith("plain text"))


class RetryTests(unittest.TestCase):
    """Retriable errors (RateLimitError, APIConnectionError) trigger
    backoff; permanent errors (AuthenticationError) do not."""

    def setUp(self) -> None:
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
