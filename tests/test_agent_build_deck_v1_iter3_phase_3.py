"""Iter 3 Phase 3 tests — D2 batched rewrites (parallel calls).

Covers:
  - Priority-30 cards split into 3 batches of 10.
  - Each batch fires its own LLM call.
  - Batch 0 produces summary_narrative + consider_adding; batches 1+2
    only produce card_rationales.
  - Per-batch metrics are recorded under `D2_final_critic_batch_N` phase
    keys.
  - Partial failure: 1 of 3 batches fails — successful batches still
    apply their rewrites; warning surfaces the partial failure.
  - Total failure: all batches fail — deck unchanged.
  - Parallel execution actually parallelises (smoke check on wall-clock).
"""
from __future__ import annotations

import os
import time
import unittest
from unittest.mock import MagicMock

from api.engine.layers.agent_build_deck_v1 import (
    _FINAL_CRITIC_BATCH_PARALLEL,
    _FINAL_CRITIC_BATCH_SIZE,
    _run_final_critic,
)
from api.engine.layers.agent_llm_client_v1 import CallResult


def _make_100_card_deck_with_30_priorities():
    """Build a deck whose priority selector returns 30 cards (commander
    + must-includes + creative outliers + combo flags + corpus-delta
    fillers)."""
    deck = [{"card_name": "Edgar Markov", "source": "user_intent", "reason": "Cmdr."}]
    deck.append({"card_name": "Vito", "source": "user_intent", "reason": "x"})
    deck.append({"card_name": "Bloodthirsty Conqueror", "source": "user_intent", "reason": "x"})
    deck.append({"card_name": "Cult of Skaab",
                 "source": "llm_candidate_critic|creative_outlier", "reason": "x"})
    for n in ["Mirkwood Bats", "Sanguine Bond", "Exquisite Blood"]:
        deck.append({"card_name": n, "source": "llm_wild_combo_discovery", "reason": "x"})
    for i in range(60):
        deck.append({"card_name": f"Picks {i:02d}", "source": "theme:vampires", "reason": "x"})
    for _ in range(33):
        deck.append({"card_name": "Swamp", "source": "mana_base", "reason": "Mana."})
    return deck


class BatchingTests(unittest.TestCase):
    def setUp(self) -> None:
        os.environ.pop("MTG_ENGINE_DISABLE_LLM", None)
        os.environ["ANTHROPIC_API_KEY"] = "sk-test"

    def tearDown(self) -> None:
        os.environ["MTG_ENGINE_DISABLE_LLM"] = "1"

    def _ok_result(self, text: str) -> CallResult:
        from api.engine.layers.agent_llm_client_v1 import _try_parse_json
        return CallResult(
            ok=True, text=text, parsed_json=_try_parse_json(text),
            input_tokens=4000, output_tokens=1200, cost_usd=0.030,
            latency_ms=15000, model="claude-sonnet-4-6",
        )

    def _failed_result(self) -> CallResult:
        return CallResult(
            ok=False, text="", parsed_json=None,
            input_tokens=4000, output_tokens=0, cost_usd=0.012,
            latency_ms=1000, error_code="LLM_RATE_LIMITED",
            error_message="429 from upstream", model="claude-sonnet-4-6",
        )

    def test_three_parallel_batches_for_30_priority_cards(self) -> None:
        deck = _make_100_card_deck_with_30_priorities()
        # Each batch gets a different response — let's make each one
        # rewrite ONE distinguishable card so we can verify aggregation.
        responses = [
            self._ok_result('{"card_rationales": [{"card": "Edgar Markov", "reason": "Cmdr rewrite from batch 0"}],'
                            ' "summary_narrative": "Narrative.", "consider_adding": [{"card": "Reconnaissance", "why": "..."}]}'),
            self._ok_result('{"card_rationales": [{"card": "Mirkwood Bats", "reason": "Bats rewrite from batch 1"}]}'),
            self._ok_result('{"card_rationales": [{"card": "Cult of Skaab", "reason": "Skaab rewrite from batch 2"}]}'),
        ]
        client = MagicMock()
        client.call_with_budget.side_effect = responses
        last_findings: dict = {"themes_classified": [], "strength_check_summary": None}
        metrics: dict = {"calls": []}
        new_deck, warnings = _run_final_critic(
            llm_client=client, deck=deck,
            commander="Edgar Markov", bracket="B3",
            theme_hints=[], intent_analysis=None,
            last_findings=last_findings, llm_metrics=metrics,
            must_include_cards=["Vito", "Bloodthirsty Conqueror"],
            novel_combo_flags=[
                {"cards": ["Mirkwood Bats", "Vito"], "applied_swap": True},
                {"cards": ["Sanguine Bond", "Exquisite Blood"], "applied_swap": False},
            ],
            archetype_brief={"staple_cards": [{"name": "Sol Ring", "usage_pct": 0.9}]},
        )
        # Three batches fired.
        self.assertEqual(len(metrics["calls"]), 3)
        phases = {c["phase"] for c in metrics["calls"]}
        self.assertEqual(phases, {
            "D2_final_critic_batch_0",
            "D2_final_critic_batch_1",
            "D2_final_critic_batch_2",
        })
        # Each batch's rewrite was applied.
        by_name = {c["card_name"]: c for c in new_deck}
        self.assertIn("Cmdr rewrite from batch 0", by_name["Edgar Markov"]["reason"])
        self.assertIn("Bats rewrite from batch 1", by_name["Mirkwood Bats"]["reason"])
        self.assertIn("Skaab rewrite from batch 2", by_name["Cult of Skaab"]["reason"])
        # Narrative came from batch 0.
        self.assertEqual(last_findings.get("summary_narrative"), "Narrative.")
        self.assertEqual(len(last_findings.get("consider_adding") or []), 1)

    def test_partial_batch_failure_applies_successful_results(self) -> None:
        deck = _make_100_card_deck_with_30_priorities()
        # Batch 1 fails; batches 0 and 2 succeed.
        responses = [
            self._ok_result('{"card_rationales": [{"card": "Edgar Markov", "reason": "Batch 0 rewrite"}],'
                            ' "summary_narrative": "ok", "consider_adding": []}'),
            self._failed_result(),
            self._ok_result('{"card_rationales": [{"card": "Cult of Skaab", "reason": "Batch 2 rewrite"}]}'),
        ]
        client = MagicMock()
        client.call_with_budget.side_effect = responses
        last_findings: dict = {"themes_classified": [], "strength_check_summary": None}
        metrics: dict = {"calls": []}
        new_deck, warnings = _run_final_critic(
            llm_client=client, deck=deck,
            commander="Edgar Markov", bracket="B3",
            theme_hints=[], intent_analysis=None,
            last_findings=last_findings, llm_metrics=metrics,
            must_include_cards=["Vito", "Bloodthirsty Conqueror"],
            novel_combo_flags=[],
            archetype_brief={},
        )
        codes = [w["code"] for w in warnings]
        self.assertIn("FINAL_CRITIC_PARTIAL_BATCH_FAILURE", codes)
        # Batch 0 + 2 rewrites applied.
        by_name = {c["card_name"]: c for c in new_deck}
        self.assertEqual(by_name["Edgar Markov"]["reason"], "Batch 0 rewrite")
        self.assertEqual(by_name["Cult of Skaab"]["reason"], "Batch 2 rewrite")

    def test_total_failure_keeps_deck_unchanged(self) -> None:
        deck = _make_100_card_deck_with_30_priorities()
        responses = [self._failed_result(), self._failed_result(), self._failed_result()]
        client = MagicMock()
        client.call_with_budget.side_effect = responses
        last_findings: dict = {"themes_classified": [], "strength_check_summary": None}
        new_deck, warnings = _run_final_critic(
            llm_client=client, deck=deck,
            commander="Edgar Markov", bracket="B3",
            theme_hints=[], intent_analysis=None,
            last_findings=last_findings, llm_metrics={"calls": []},
            must_include_cards=["Vito"], novel_combo_flags=[],
            archetype_brief={},
        )
        codes = [w["code"] for w in warnings]
        self.assertIn("FINAL_CRITIC_ALL_BATCHES_FAILED", codes)
        for orig, new in zip(deck, new_deck):
            self.assertEqual(orig["reason"], new["reason"])

    def test_batch_constants_match_spec(self) -> None:
        # The kickoff spec says 3 batches of 10. If anyone changes the
        # constants, this test surfaces it for review.
        self.assertEqual(_FINAL_CRITIC_BATCH_SIZE, 10)
        self.assertEqual(_FINAL_CRITIC_BATCH_PARALLEL, 3)

    def test_parallel_execution_finishes_in_max_not_sum(self) -> None:
        """A loose smoke check that ThreadPoolExecutor actually parallel-
        izes: each batch's mocked LLM call sleeps for 0.3s. Serial would
        take 0.9s+; parallel should land near 0.3s with overhead.
        """
        deck = _make_100_card_deck_with_30_priorities()

        from api.engine.layers.agent_llm_client_v1 import _try_parse_json

        def _slow_call(*args, **kwargs):
            time.sleep(0.3)
            text = ('{"card_rationales": [{"card": "Edgar Markov", "reason": "x"}],'
                    ' "summary_narrative": "ok", "consider_adding": []}')
            return CallResult(
                ok=True, text=text, parsed_json=_try_parse_json(text),
                input_tokens=4000, output_tokens=500, cost_usd=0.02,
                latency_ms=300, model="claude-sonnet-4-6",
            )

        client = MagicMock()
        client.call_with_budget.side_effect = _slow_call

        t0 = time.perf_counter()
        _run_final_critic(
            llm_client=client, deck=deck,
            commander="Edgar Markov", bracket="B3",
            theme_hints=[], intent_analysis=None,
            last_findings={"themes_classified": [], "strength_check_summary": None},
            llm_metrics={"calls": []},
            must_include_cards=["Vito", "Bloodthirsty Conqueror"],
            novel_combo_flags=[],
            archetype_brief={},
        )
        elapsed = time.perf_counter() - t0
        # 3 sequential calls @ 0.3s would be ≥0.9s; parallel should be
        # well under 0.7s (allowing for thread overhead). The test would
        # need to be very generous if it actually parallelized poorly.
        self.assertLess(elapsed, 0.8, f"D2 batches did not parallelize: took {elapsed:.2f}s")


if __name__ == "__main__":
    unittest.main()
