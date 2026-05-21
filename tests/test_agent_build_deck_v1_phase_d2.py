"""Phase D2 tests — final critic + rationale rewrite.

Covers:
  - Per-card rationale rewrites apply to matching cards only.
  - summary_narrative + consider_adding flow into last_findings (and
    from there into the response summary).
  - consider_adding entries that turn out to be in the deck are dropped.
  - At most 3 consider_adding entries are kept.
  - LLM failure leaves card reasons + last_findings unchanged.
  - Source string is bumped with `llm_rationale_rewrite`.
"""
from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock

from api.engine.layers.agent_build_deck_v1 import (
    _build_final_critic_user_prompt,
    _run_final_critic,
)
from api.engine.layers.agent_llm_client_v1 import CallResult


def _mk_call_result(text, ok=True):
    from api.engine.layers.agent_llm_client_v1 import _try_parse_json
    return CallResult(
        ok=ok, text=text,
        parsed_json=_try_parse_json(text) if ok else None,
        input_tokens=8000, output_tokens=3000,
        cost_usd=0.069, latency_ms=3210, model="claude-sonnet-4-6",
    )


def _make_simple_deck():
    return [
        {"card_name": "Edgar Markov", "reason": "Cmdr.", "source": "user_intent"},
        {"card_name": "Sol Ring", "reason": "Iteration-1 staple text.",
         "source": "archetype_staple"},
        {"card_name": "Vito, Thorn of the Dusk Rose",
         "reason": "Theme 'TYPAL_VAMPIRES' signal_count=2 (freq_in_corpus=0.05) [slot=creature]",
         "source": "theme:TYPAL_VAMPIRES"},
        {"card_name": "Swamp", "reason": "Mana base.", "source": "mana_base"},
    ]


class FinalCriticTests(unittest.TestCase):
    def setUp(self) -> None:
        os.environ.pop("MTG_ENGINE_DISABLE_LLM", None)
        os.environ["ANTHROPIC_API_KEY"] = "sk-test"

    def tearDown(self) -> None:
        os.environ["MTG_ENGINE_DISABLE_LLM"] = "1"

    def _llm(self, text, ok=True):
        c = MagicMock()
        c.is_available.return_value = True
        c.call_with_budget.return_value = _mk_call_result(text, ok=ok)
        return c

    def test_rationale_rewrites_apply_to_matching_cards(self) -> None:
        deck = _make_simple_deck()
        llm_json = """
        {
          "card_rationales": [
            {"card": "Sol Ring",
             "reason": "Powers out Edgar a turn early; pairs with Vito's drain via faster lifegain."},
            {"card": "Vito, Thorn of the Dusk Rose",
             "reason": "Drain payoff that punishes every lifegain trigger Edgar's tokens generate."}
          ],
          "summary_narrative": "A B/R/W vampire tribal deck that wins by stacking lifegain triggers behind Vito.",
          "consider_adding": [
            {"card": "Sanguine Bond", "why": "Doubles up the drain plan."}
          ]
        }
        """
        client = self._llm(llm_json)
        last_findings: dict = {"themes_classified": [], "strength_check_summary": None}
        new_deck, warnings = _run_final_critic(
            llm_client=client, deck=deck,
            commander="Edgar Markov", bracket="B3",
            theme_hints=["TYPAL_VAMPIRES"], intent_analysis=None,
            last_findings=last_findings,
            llm_metrics={"calls": []},
        )
        # Sol Ring + Vito got rewrites; Swamp + commander did not.
        sol = next(c for c in new_deck if c["card_name"] == "Sol Ring")
        self.assertIn("Edgar", sol["reason"])
        self.assertIn("llm_rationale_rewrite", sol["source"])
        vito = next(c for c in new_deck if c["card_name"] == "Vito, Thorn of the Dusk Rose")
        self.assertIn("drain", vito["reason"].lower())
        # Untouched.
        swamp = next(c for c in new_deck if c["card_name"] == "Swamp")
        self.assertEqual(swamp["reason"], "Mana base.")
        # Summary narrative + consider_adding flow to last_findings.
        self.assertIn("vampire", last_findings["summary_narrative"].lower())
        self.assertEqual(len(last_findings["consider_adding"]), 1)
        self.assertEqual(last_findings["consider_adding"][0]["card"], "Sanguine Bond")

    def test_consider_adding_already_in_deck_is_dropped(self) -> None:
        deck = _make_simple_deck()
        llm_json = """
        {
          "card_rationales": [],
          "summary_narrative": "...",
          "consider_adding": [
            {"card": "Sol Ring", "why": "Should be in every deck."},
            {"card": "Sanguine Bond", "why": "Drain payoff."}
          ]
        }
        """
        client = self._llm(llm_json)
        last_findings: dict = {"themes_classified": [], "strength_check_summary": None}
        _run_final_critic(
            llm_client=client, deck=deck,
            commander="Edgar Markov", bracket="B3",
            theme_hints=[], intent_analysis=None,
            last_findings=last_findings,
            llm_metrics={"calls": []},
        )
        # Sol Ring is in the deck — dropped from consider_adding.
        names = [e["card"] for e in last_findings["consider_adding"]]
        self.assertNotIn("Sol Ring", names)
        self.assertIn("Sanguine Bond", names)

    def test_consider_adding_capped_at_three(self) -> None:
        deck = _make_simple_deck()
        llm_json = """
        {
          "card_rationales": [],
          "summary_narrative": "...",
          "consider_adding": [
            {"card": "Card 1", "why": "..."},
            {"card": "Card 2", "why": "..."},
            {"card": "Card 3", "why": "..."},
            {"card": "Card 4", "why": "..."},
            {"card": "Card 5", "why": "..."}
          ]
        }
        """
        client = self._llm(llm_json)
        last_findings: dict = {"themes_classified": [], "strength_check_summary": None}
        _run_final_critic(
            llm_client=client, deck=deck,
            commander="Edgar Markov", bracket="B3",
            theme_hints=[], intent_analysis=None,
            last_findings=last_findings,
            llm_metrics={"calls": []},
        )
        self.assertEqual(len(last_findings["consider_adding"]), 3)

    def test_failure_keeps_reasons_unchanged(self) -> None:
        deck = _make_simple_deck()
        client = self._llm("", ok=False)
        last_findings: dict = {"themes_classified": [], "strength_check_summary": None}
        new_deck, warnings = _run_final_critic(
            llm_client=client, deck=deck,
            commander="Edgar Markov", bracket="B3",
            theme_hints=[], intent_analysis=None,
            last_findings=last_findings,
            llm_metrics={"calls": []},
        )
        # All reasons unchanged.
        for orig, new in zip(deck, new_deck):
            self.assertEqual(orig["reason"], new["reason"])
        # last_findings doesn't gain summary_narrative.
        self.assertNotIn("summary_narrative", last_findings)
        codes = [w["code"] for w in warnings]
        self.assertIn("FINAL_CRITIC_FAILED", codes)

    def test_invalid_json_keeps_reasons_unchanged(self) -> None:
        deck = _make_simple_deck()
        client = self._llm("not parseable json at all")
        last_findings: dict = {"themes_classified": [], "strength_check_summary": None}
        new_deck, warnings = _run_final_critic(
            llm_client=client, deck=deck,
            commander="Edgar Markov", bracket="B3",
            theme_hints=[], intent_analysis=None,
            last_findings=last_findings,
            llm_metrics={"calls": []},
        )
        for orig, new in zip(deck, new_deck):
            self.assertEqual(orig["reason"], new["reason"])
        codes = [w["code"] for w in warnings]
        self.assertIn("FINAL_CRITIC_INVALID_JSON", codes)

    def test_no_rewrites_emits_warning(self) -> None:
        deck = _make_simple_deck()
        # LLM returns valid JSON but no card_rationales — perhaps it
        # decided all reasons were good enough.
        llm_json = """
        {"card_rationales": [], "summary_narrative": "...", "consider_adding": []}
        """
        client = self._llm(llm_json)
        last_findings: dict = {"themes_classified": [], "strength_check_summary": None}
        new_deck, warnings = _run_final_critic(
            llm_client=client, deck=deck,
            commander="Edgar Markov", bracket="B3",
            theme_hints=[], intent_analysis=None,
            last_findings=last_findings,
            llm_metrics={"calls": []},
        )
        codes = [w["code"] for w in warnings]
        self.assertIn("FINAL_CRITIC_NO_REWRITES", codes)

    def test_metrics_recorded(self) -> None:
        deck = _make_simple_deck()
        client = self._llm('{"card_rationales": [], "summary_narrative": "x", "consider_adding": []}')
        metrics: dict = {"calls": []}
        _run_final_critic(
            llm_client=client, deck=deck,
            commander="Edgar Markov", bracket="B3",
            theme_hints=[], intent_analysis=None,
            last_findings={"themes_classified": [], "strength_check_summary": None},
            llm_metrics=metrics,
        )
        self.assertEqual(len(metrics["calls"]), 1)
        rec = metrics["calls"][0]
        self.assertEqual(rec["phase"], "D2_final_critic")
        self.assertTrue(rec["ok"])


class PromptShapeTests(unittest.TestCase):
    def test_prompt_includes_deck_themes_and_intent(self) -> None:
        deck = [{"card_name": "Vito", "source": "user_intent", "reason": "x"}]
        prompt = _build_final_critic_user_prompt(
            commander="Edgar Markov", bracket="B3",
            theme_hints=["TYPAL_VAMPIRES"],
            intent_analysis={"likely_win_condition": "Drain via lifegain",
                              "implicit_themes": ["lifegain_payoffs"]},
            deck=deck,
            priority_cards=deck,
            classified_themes=[{"theme_id": "TYPAL_VAMPIRES"}],
            strength_check_summary={"bracket_signal": "B3", "mean_similarity": 0.6},
        )
        self.assertIn("Edgar Markov", prompt)
        self.assertIn("FINAL 100-CARD DECK", prompt)
        self.assertIn("TYPAL_VAMPIRES", prompt)
        self.assertIn("Drain via lifegain", prompt)
        self.assertIn("bracket_signal", prompt)
        self.assertIn("card_rationales", prompt)
        # Iter 3 Phase 1 — priority rewrite list explicit in prompt.
        self.assertIn("PRIORITY REWRITE LIST", prompt)
        self.assertIn("[PRIORITY]", prompt)
        self.assertIn("summary_narrative", prompt)


if __name__ == "__main__":
    unittest.main()
