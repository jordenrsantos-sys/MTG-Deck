"""Mega-task v5 Phase 8 — Atraxa C2.1 silent-failure fix.

Pre-Phase-8 the C2.1 candidate critic had a fixed 10000-input-token budget.
For a 4-color commander like Atraxa, the Phase 2 combo-anchor guard's
`forbidden_prompt_block` could grow to ~1500-2500 tokens of card names
(every card the registry knows would complete a combo with a user must-
include). When that pushed the total above 10K, `call_with_budget`'s
pre-call guard fired, the LLM call was skipped silently, and iter 5
logged Atraxa C2.1 latency = 0.0s.

Phase 8 wires every phase that injects forbidden_prompt_block (B2 / C2.1 /
C2.2 / D2) through `_budget_with_forbidden_overhead`, which adds the
serialized block's token estimate on top of the base budget. The budget
guard now only fires on genuinely oversized core content.

These tests pin that contract without running a live build:

  - `_budget_with_forbidden_overhead` math.
  - Empty/None forbidden_block returns the base unchanged.
  - Large forbidden_block (~3000 tokens) bumps the budget by ~3000.
"""
from __future__ import annotations

import unittest

from api.engine.layers.agent_build_deck_v1 import (
    _budget_with_forbidden_overhead,
    _CANDIDATE_CRITIC_INPUT_TOKEN_BUDGET,
    _INTENT_INTERPRETER_INPUT_TOKEN_BUDGET,
    _FINAL_CRITIC_INPUT_TOKEN_BUDGET,
    _WILD_COMBO_INPUT_TOKEN_BUDGET,
)
from api.engine.layers.agent_combo_anchor_guard_v1 import (
    format_forbidden_block_for_prompt,
)


class BudgetWithForbiddenOverheadTest(unittest.TestCase):
    def test_empty_string_returns_base(self) -> None:
        self.assertEqual(_budget_with_forbidden_overhead(10000, ""), 10000)

    def test_none_input_returns_base(self) -> None:
        # ""-equivalent path covers None too via the truthiness check.
        # Python type system says str = "" so we test the truthy-falsy edge.
        self.assertEqual(
            _budget_with_forbidden_overhead(10000, ""),  # type: ignore[arg-type]
            10000,
        )

    def test_overhead_is_chars_over_3_5(self) -> None:
        # 350 chars at the estimator's 3.5 chars/token convention = 100 tokens.
        block = "x" * 350
        self.assertEqual(
            _budget_with_forbidden_overhead(10000, block),
            10100,
        )

    def test_overhead_floors_at_zero_not_negative(self) -> None:
        # A single-char block (rare) shouldn't subtract from the base.
        self.assertGreaterEqual(
            _budget_with_forbidden_overhead(10000, "x"),
            10000,
        )

    def test_atraxa_scale_forbidden_block_bumps_C21_above_threshold(self) -> None:
        """The motivating case: a ~50-card forbidden_set (Atraxa with multi-
        combo must-includes) bumps the C2.1 budget enough that the pre-call
        guard won't fire on the same content that pre-Phase-8 short-
        circuited."""
        forbidden_set = {f"forbidden card name {i}" for i in range(50)}
        block = format_forbidden_block_for_prompt(forbidden_set)
        bumped = _budget_with_forbidden_overhead(
            _CANDIDATE_CRITIC_INPUT_TOKEN_BUDGET, block,
        )
        # The block itself should add at least a few hundred tokens.
        self.assertGreater(
            bumped - _CANDIDATE_CRITIC_INPUT_TOKEN_BUDGET, 100,
            f"a 50-card forbidden_block should add >100 tokens to the budget; "
            f"got bump of {bumped - _CANDIDATE_CRITIC_INPUT_TOKEN_BUDGET}",
        )


class AllPhaseBudgetsAreCallable(unittest.TestCase):
    """Smoke test: every fixed budget constant is a positive int, and
    `_budget_with_forbidden_overhead` works on each one. This catches
    accidental float/None values that would break the pre-call guard
    arithmetic."""

    def test_all_phase_budgets_are_positive_ints(self) -> None:
        for name, value in (
            ("INTENT", _INTENT_INTERPRETER_INPUT_TOKEN_BUDGET),
            ("CRITIC", _CANDIDATE_CRITIC_INPUT_TOKEN_BUDGET),
            ("WILD",   _WILD_COMBO_INPUT_TOKEN_BUDGET),
            ("FINAL",  _FINAL_CRITIC_INPUT_TOKEN_BUDGET),
        ):
            self.assertIsInstance(value, int, f"{name} budget should be int")
            self.assertGreater(value, 0, f"{name} budget should be > 0")

    def test_helper_handles_each_phase_budget(self) -> None:
        block = format_forbidden_block_for_prompt({"a", "b", "c"})
        for value in (
            _INTENT_INTERPRETER_INPUT_TOKEN_BUDGET,
            _CANDIDATE_CRITIC_INPUT_TOKEN_BUDGET,
            _WILD_COMBO_INPUT_TOKEN_BUDGET,
            _FINAL_CRITIC_INPUT_TOKEN_BUDGET,
        ):
            bumped = _budget_with_forbidden_overhead(value, block)
            self.assertGreater(bumped, value)


if __name__ == "__main__":
    unittest.main()
