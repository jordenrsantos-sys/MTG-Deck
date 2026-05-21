"""Iter 5 Phase 2 — C2.1 prompt trim for wallclock reduction.

Verifies:
  - Pool size reduced 100 → 70
  - Oracle text cap reduced 180 → 150 chars
  - User-prompt no longer contains the verbose POSITIONAL CONTEXT
    explainer (moved to system prompt)
  - System prompt contains the POSITIONAL CONTEXT explainer
  - C2.1 user prompt for a typical pool fits in 8000 input tokens
    (approximate char-count heuristic since we don't have a tokenizer
    in the test env)
"""
from __future__ import annotations

import unittest

from api.engine.layers.agent_build_deck_v1 import (
    _CANDIDATE_CRITIC_INPUT_TOKEN_BUDGET,
    _CANDIDATE_CRITIC_POOL_SIZE,
    _CANDIDATE_CRITIC_SYSTEM_PROMPT,
    _build_candidate_critic_user_prompt,
)


def _cand(name="Card", oracle_len=400):
    return {
        "name": name, "type_line": "Creature - Test", "cmc": 3,
        "primitives": ["sac-outlet", "death-trigger"],
        "color_identity": ["B"],
        "oracle_text": "A" * oracle_len,
        "rationale_components": ["theme:tribal"],
        "score": 10.0,
    }


class C21ConstantsTests(unittest.TestCase):
    def test_pool_size_in_kickoff_band(self) -> None:
        self.assertGreaterEqual(_CANDIDATE_CRITIC_POOL_SIZE, 60)
        self.assertLessEqual(_CANDIDATE_CRITIC_POOL_SIZE, 80)

    def test_input_budget_reduced(self) -> None:
        # Iter 5 Phase 2: was 16000, target ≤10000.
        self.assertLessEqual(_CANDIDATE_CRITIC_INPUT_TOKEN_BUDGET, 10000)


class C21SystemPromptTests(unittest.TestCase):
    def test_system_prompt_contains_positional_context_explainer(self) -> None:
        # The verbose explainer moved from user prompt to system prompt.
        self.assertIn("POSITIONAL CONTEXT", _CANDIDATE_CRITIC_SYSTEM_PROMPT)
        self.assertIn("interacts_with", _CANDIDATE_CRITIC_SYSTEM_PROMPT)
        self.assertIn("pairs_with", _CANDIDATE_CRITIC_SYSTEM_PROMPT)


class C21UserPromptTests(unittest.TestCase):
    def _build(self, n_pool=70, oracle_len=400, with_index=False):
        pool = [_cand(f"Card{i}", oracle_len=oracle_len) for i in range(n_pool)]
        index = (
            [{"card_name": "DeckCardA", "primitives": ["sac-outlet"]}]
            if with_index else None
        )
        return _build_candidate_critic_user_prompt(
            commander="Test Commander", bracket="B3",
            theme_hints=["tribal"], intent_analysis=None,
            current_deck_summary=[{"card_name": "DeckCardA", "source": "user_intent"}],
            swappable_slots=[{"card_name": "Filler", "source": "agent"}],
            candidate_pool=pool,
            bracket_policy_summary="B3 allows late combos.",
            deck_primitive_index=index,
        )

    def test_oracle_text_trimmed_to_150_chars(self) -> None:
        prompt = self._build(n_pool=1, oracle_len=300)
        # Find the "text: ..." line for the candidate; ensure the
        # rendered text is ≤150 chars (with a `...` suffix on truncation).
        text_lines = [l for l in prompt.splitlines() if l.strip().startswith("text:")]
        self.assertGreaterEqual(len(text_lines), 1)
        for line in text_lines:
            body = line.split("text:", 1)[1].strip()
            self.assertLessEqual(len(body), 150 + 5)
            if body.endswith("..."):
                self.assertEqual(len(body), 150)

    def test_user_prompt_no_longer_has_verbose_explainer(self) -> None:
        prompt = self._build(n_pool=5, with_index=True)
        # The verbose iter-3 explainer block is gone from the user prompt.
        self.assertNotIn(
            "POSITIONAL CONTEXT (iter 3 Phase 8)",
            prompt,
        )

    def test_user_prompt_fits_in_8k_chars_for_pool_70(self) -> None:
        # Rough approximation: 1 token ≈ 4 chars for English text. An
        # 8000-token budget = ~32k chars. We aim for ≤32k chars on a
        # 70-card pool with full annotations.
        prompt = self._build(n_pool=70, oracle_len=400, with_index=True)
        self.assertLessEqual(
            len(prompt), 32_000,
            msg=f"User prompt is {len(prompt)} chars; target ≤32k chars "
                f"(~8k tokens at 4 chars/token).",
        )


if __name__ == "__main__":
    unittest.main()
