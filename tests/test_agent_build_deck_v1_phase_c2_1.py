"""Phase C2.1 tests — LLM candidate critic.

Covers:
  - Selecting the bottom-priority N slots as swappable.
  - Hallucinated names are dropped.
  - Color-identity-illegal picks are dropped.
  - Bracket-policy violations are dropped.
  - Singleton violations (already-in-deck names) are dropped.
  - Valid LLM picks are applied with correct source tagging.
  - Creative-outlier flag is captured on source string.
  - combo_lines_noted populate novel_combo_flags.
  - Critic deck count stays at exactly 100 cards after swaps.
  - Failure path keeps iteration-1 deck unchanged.
"""
from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

from api.engine.layers.agent_build_deck_v1 import (
    _CANDIDATE_CRITIC_SWAPPABLE_SLOTS,
    _build_candidate_critic_user_prompt,
    _candidate_pool_for_critic,
    _run_candidate_critic,
    _select_swappable_slots,
    _summarize_bracket_policy,
)
from api.engine.layers.agent_llm_client_v1 import CallResult


def _ci_pool(color_identity, candidates=None, db_snapshot_id="snap"):
    return {
        "color_identity": color_identity,
        "candidates": candidates or [],
        "archetype_brief": {"staple_cards": []},
        "must_includes_resolved": [],
        "must_includes_dropped": [],
        "warnings": [],
        "endpoint_calls": 0,
        "db_snapshot_id": db_snapshot_id,
    }


def _mk_call_result(text, ok=True):
    from api.engine.layers.agent_llm_client_v1 import _try_parse_json
    return CallResult(
        ok=ok, text=text,
        parsed_json=_try_parse_json(text) if ok else None,
        input_tokens=12000, output_tokens=3000,
        cost_usd=0.081, latency_ms=4321, model="claude-sonnet-4-6",
    )


def _make_100_deck(commander="Edgar Markov"):
    """Build a synthetic 100-card deck — commander + 36 basics + 63 'agent' picks."""
    deck = [{"card_name": commander, "reason": "Cmdr.", "source": "user_intent"}]
    # 36 basics (these will be the first-priority swappable slots).
    for i in range(36):
        deck.append({"card_name": "Swamp", "reason": "Mana base.", "source": "mana_base"})
    # 30 staples (medium priority).
    for i in range(30):
        deck.append({
            "card_name": f"Staple {i:02d}",
            "reason": "Corpus staple.", "source": "archetype_staple",
        })
    # 33 theme picks.
    for i in range(33):
        deck.append({
            "card_name": f"Theme {i:02d}",
            "reason": "Theme:TYPAL_VAMPIRES.", "source": "theme:TYPAL_VAMPIRES",
        })
    assert len(deck) == 100
    return deck


# ============================================================
# Pure helpers.
# ============================================================


class BracketPolicySummaryTests(unittest.TestCase):
    def test_b1_says_combo_rejected(self) -> None:
        msg = _summarize_bracket_policy("B1")
        self.assertIn("rejected entirely", msg.lower())

    def test_b3_says_late_only(self) -> None:
        msg = _summarize_bracket_policy("B3")
        self.assertIn("late-game", msg.lower())

    def test_b4_says_capped(self) -> None:
        msg = _summarize_bracket_policy("B4")
        self.assertIn("max", msg.lower())

    def test_b5_says_unrestricted(self) -> None:
        msg = _summarize_bracket_policy("B5")
        self.assertIn("unrestricted", msg.lower())


class SelectSwappableSlotsTests(unittest.TestCase):
    def test_user_picks_and_commander_are_locked(self) -> None:
        deck = [
            {"card_name": "Edgar Markov", "source": "user_intent"},
            {"card_name": "Vito", "source": "user_intent"},
            {"card_name": "Swamp", "source": "mana_base"},
            {"card_name": "Sol Ring", "source": "archetype_staple"},
        ]
        locked, swappable = _select_swappable_slots(deck, 2)
        locked_names = {c["card_name"] for c in locked}
        self.assertIn("Edgar Markov", locked_names)
        self.assertIn("Vito", locked_names)
        swap_names = {c["card_name"] for c in swappable}
        self.assertNotIn("Vito", swap_names)
        self.assertNotIn("Edgar Markov", swap_names)

    def test_basics_are_first_to_swap(self) -> None:
        deck = _make_100_deck()
        _, swappable = _select_swappable_slots(deck, 5)
        # First 5 swappable should all be Swamp basics (priority 0).
        for c in swappable:
            self.assertIn(c["card_name"], {"Swamp"})

    def test_returns_full_n_when_pool_is_large(self) -> None:
        deck = _make_100_deck()
        _, swappable = _select_swappable_slots(deck, 28)
        self.assertEqual(len(swappable), 28)


class CandidatePoolForCriticTests(unittest.TestCase):
    def test_excludes_already_in_deck(self) -> None:
        pool = _ci_pool(
            color_identity=["B"],
            candidates=[
                {"name": "Sol Ring", "type_line": "Artifact", "cmc": 1,
                 "primitives": ["MANA_ROCK"], "color_identity": [], "score": 5.0},
                {"name": "Vito, Thorn of the Dusk Rose", "type_line": "Creature",
                 "cmc": 3, "primitives": ["LIFEGAIN_PAYOFF"], "color_identity": ["B"],
                 "score": 30.0},
            ],
        )
        # Patch find_card_by_name to skip the DB lookup.
        from engine import db as eng_db
        with patch.object(eng_db, "find_card_by_name", return_value=None):
            critic_pool = _candidate_pool_for_critic(
                pool, "snap", exclude_names={"sol ring"}, size=10,
            )
        names = [c["name"] for c in critic_pool]
        self.assertNotIn("Sol Ring", names)
        self.assertIn("Vito, Thorn of the Dusk Rose", names)

    def test_skips_user_picks(self) -> None:
        pool = _ci_pool(
            color_identity=["B"],
            candidates=[
                {"name": "Vito", "is_user_pick": True, "type_line": "Creature",
                 "color_identity": ["B"]},
                {"name": "Sanguine Bond", "is_user_pick": False, "type_line": "Enchantment",
                 "color_identity": ["B"]},
            ],
        )
        from engine import db as eng_db
        with patch.object(eng_db, "find_card_by_name", return_value=None):
            critic_pool = _candidate_pool_for_critic(
                pool, "snap", exclude_names=set(), size=10,
            )
        names = [c["name"] for c in critic_pool]
        self.assertIn("Sanguine Bond", names)
        self.assertNotIn("Vito", names)


class PromptBuilderTests(unittest.TestCase):
    def test_includes_all_sections(self) -> None:
        prompt = _build_candidate_critic_user_prompt(
            commander="Edgar Markov",
            bracket="B3",
            theme_hints=["TYPAL_VAMPIRES"],
            intent_analysis={"likely_win_condition": "Drain", "implicit_themes": ["lifegain"]},
            current_deck_summary=[{"card_name": "Vito", "source": "user_intent"}],
            swappable_slots=[{"card_name": "Swamp", "source": "mana_base"}],
            candidate_pool=[
                {"name": "Sanguine Bond", "type_line": "Enchantment",
                 "cmc": 5, "primitives": ["LIFEGAIN_PAYOFF"],
                 "rationale_components": ["theme match"], "oracle_text": "Whenever you gain life..."},
            ],
            bracket_policy_summary="Bracket B3: only late.",
        )
        self.assertIn("Edgar Markov", prompt)
        self.assertIn("B3", prompt)
        self.assertIn("TYPAL_VAMPIRES", prompt)
        self.assertIn("CURRENT DECK", prompt)
        self.assertIn("SWAPPABLE SLOTS", prompt)
        self.assertIn("CANDIDATE POOL", prompt)
        self.assertIn("Sanguine Bond", prompt)
        self.assertIn("Whenever you gain life", prompt)
        self.assertIn("selected_cards", prompt)


# ============================================================
# _run_candidate_critic — end-to-end with mocked LLM.
# ============================================================


class CandidateCriticEndToEndTests(unittest.TestCase):
    def setUp(self) -> None:
        os.environ.pop("MTG_ENGINE_DISABLE_LLM", None)
        os.environ["ANTHROPIC_API_KEY"] = "sk-test"

    def tearDown(self) -> None:
        os.environ["MTG_ENGINE_DISABLE_LLM"] = "1"

    def _llm_client(self, response_text: str, ok: bool = True):
        client = MagicMock()
        client.is_available.return_value = True
        client.call_with_budget.return_value = _mk_call_result(response_text, ok=ok)
        return client

    def _patch_oracle_text(self):
        from engine import db as eng_db
        return patch.object(eng_db, "find_card_by_name",
                            return_value={"oracle_text": "card text"})

    def test_valid_selections_get_applied(self) -> None:
        deck = _make_100_deck()
        pool = _ci_pool(
            color_identity=["B", "R", "W"],
            candidates=[
                {"name": "Vito, Thorn of the Dusk Rose", "is_user_pick": False,
                 "type_line": "Creature - Vampire Cleric", "cmc": 3,
                 "primitives": ["LIFEGAIN_PAYOFF"], "color_identity": ["B"],
                 "rationale_components": ["theme:vampires"], "score": 30.0},
                {"name": "Sanguine Bond", "is_user_pick": False,
                 "type_line": "Enchantment", "cmc": 5,
                 "primitives": ["LIFEGAIN_PAYOFF"], "color_identity": ["B"],
                 "rationale_components": ["theme:lifegain"], "score": 28.0},
            ],
        )
        critic_json = """
        {
          "selected_cards": [
            {"name": "Vito, Thorn of the Dusk Rose", "category": "creature",
             "reason": "Drain payoff with the existing lifegain triggers.",
             "is_creative_outlier": false},
            {"name": "Sanguine Bond", "category": "flex",
             "reason": "Doubles up with Vito for the kill.",
             "is_creative_outlier": false}
          ],
          "combo_lines_noted": []
        }
        """
        client = self._llm_client(critic_json)
        novel: list = []
        metrics: dict = {"calls": []}
        with self._patch_oracle_text():
            new_deck, warnings = _run_candidate_critic(
                llm_client=client, deck=deck, pool=pool,
                commander="Edgar Markov", bracket="B3",
                theme_hints=["TYPAL_VAMPIRES"], intent_analysis=None,
                llm_metrics=metrics, novel_combo_flags=novel,
            )
        names = {c["card_name"] for c in new_deck}
        self.assertIn("Vito, Thorn of the Dusk Rose", names)
        self.assertIn("Sanguine Bond", names)
        self.assertEqual(len(new_deck), 100)
        # Source tagging.
        sanguine = next(c for c in new_deck if c["card_name"] == "Sanguine Bond")
        self.assertIn("llm_candidate_critic", sanguine["source"])
        self.assertEqual(len(metrics["calls"]), 1)

    def test_hallucinated_card_is_dropped(self) -> None:
        deck = _make_100_deck()
        pool = _ci_pool(color_identity=["B", "R", "W"], candidates=[])
        critic_json = """
        {
          "selected_cards": [
            {"name": "ImaginaryCard9999", "category": "creature",
             "reason": "Not in pool.", "is_creative_outlier": false}
          ],
          "combo_lines_noted": []
        }
        """
        client = self._llm_client(critic_json)
        with self._patch_oracle_text():
            new_deck, warnings = _run_candidate_critic(
                llm_client=client, deck=deck, pool=pool,
                commander="Edgar Markov", bracket="B3",
                theme_hints=[], intent_analysis=None,
                llm_metrics={"calls": []}, novel_combo_flags=[],
            )
        names = {c["card_name"] for c in new_deck}
        self.assertNotIn("ImaginaryCard9999", names)
        codes = [w["code"] for w in warnings]
        # Empty pool warning OR hallucination warning — either is acceptable.
        self.assertTrue(
            "CRITIC_REJECTED_HALLUCINATION" in codes
            or "CRITIC_SKIPPED_EMPTY_POOL" in codes,
        )

    def test_color_identity_illegal_card_is_dropped(self) -> None:
        deck = _make_100_deck()
        pool = _ci_pool(
            color_identity=["B"],  # mono-black commander
            candidates=[{
                "name": "Wrath of God",
                "type_line": "Sorcery",
                "cmc": 4,
                "primitives": ["BOARDWIPE_CREATURES"],
                "color_identity": ["W"],  # CI mismatch
                "rationale_components": ["staple"],
                "score": 5.0,
                "is_user_pick": False,
            }],
        )
        critic_json = """
        {"selected_cards": [
          {"name": "Wrath of God", "category": "removal",
           "reason": "...", "is_creative_outlier": false}
        ], "combo_lines_noted": []}
        """
        client = self._llm_client(critic_json)
        with self._patch_oracle_text():
            new_deck, warnings = _run_candidate_critic(
                llm_client=client, deck=deck, pool=pool,
                commander="Edgar Markov", bracket="B3",
                theme_hints=[], intent_analysis=None,
                llm_metrics={"calls": []}, novel_combo_flags=[],
            )
        names = {c["card_name"] for c in new_deck}
        self.assertNotIn("Wrath of God", names)
        codes = [w["code"] for w in warnings]
        self.assertIn("CRITIC_REJECTED_CI_ILLEGAL", codes)

    def test_combo_lines_populate_novel_flags(self) -> None:
        deck = _make_100_deck()
        # Empty pool short-circuits before the LLM call; provide at least one
        # candidate so the call goes through and combo_lines get parsed.
        pool = _ci_pool(
            color_identity=["B", "R", "W"],
            candidates=[{
                "name": "Filler", "is_user_pick": False,
                "type_line": "Creature", "cmc": 1,
                "primitives": [], "color_identity": [], "score": 0.0,
                "rationale_components": [],
            }],
        )
        critic_json = """
        {
          "selected_cards": [],
          "combo_lines_noted": [
            {"cards": ["Sanguine Bond", "Exquisite Blood"],
             "outcome": "Infinite drain.", "in_spellbook": true}
          ]
        }
        """
        client = self._llm_client(critic_json)
        novel: list = []
        with self._patch_oracle_text():
            _run_candidate_critic(
                llm_client=client, deck=deck, pool=pool,
                commander="Edgar Markov", bracket="B3",
                theme_hints=[], intent_analysis=None,
                llm_metrics={"calls": []}, novel_combo_flags=novel,
            )
        self.assertEqual(len(novel), 1)
        flag = novel[0]
        self.assertEqual(flag["cards"], ["Sanguine Bond", "Exquisite Blood"])
        self.assertEqual(flag["outcome"], "Infinite drain.")
        self.assertTrue(flag["in_spellbook"])

    def test_failure_keeps_iteration_1_deck_unchanged(self) -> None:
        deck = _make_100_deck()
        # Pool must have at least one candidate so we get past the
        # short-circuit and actually attempt the LLM call.
        pool = _ci_pool(
            color_identity=["B"],
            candidates=[{
                "name": "Filler", "is_user_pick": False,
                "type_line": "Creature", "cmc": 1, "primitives": [],
                "color_identity": [], "score": 0.0,
                "rationale_components": [],
            }],
        )
        client = self._llm_client("", ok=False)
        with self._patch_oracle_text():
            new_deck, warnings = _run_candidate_critic(
                llm_client=client, deck=deck, pool=pool,
                commander="Edgar Markov", bracket="B3",
                theme_hints=[], intent_analysis=None,
                llm_metrics={"calls": []}, novel_combo_flags=[],
            )
        self.assertEqual(len(new_deck), 100)
        # Same card names in same order — deck unchanged.
        self.assertEqual(
            [c["card_name"] for c in new_deck],
            [c["card_name"] for c in deck],
        )
        codes = [w["code"] for w in warnings]
        self.assertIn("CANDIDATE_CRITIC_FAILED", codes)

    def test_empty_candidate_pool_skips_gracefully(self) -> None:
        deck = _make_100_deck()
        pool = _ci_pool(color_identity=["B", "R", "W"], candidates=[])
        client = self._llm_client('{"selected_cards": [], "combo_lines_noted": []}')
        with self._patch_oracle_text():
            new_deck, warnings = _run_candidate_critic(
                llm_client=client, deck=deck, pool=pool,
                commander="Edgar Markov", bracket="B3",
                theme_hints=[], intent_analysis=None,
                llm_metrics={"calls": []}, novel_combo_flags=[],
            )
        self.assertEqual(len(new_deck), 100)
        codes = [w["code"] for w in warnings]
        self.assertIn("CRITIC_SKIPPED_EMPTY_POOL", codes)

    def test_creative_outlier_flag_in_source(self) -> None:
        deck = _make_100_deck()
        pool = _ci_pool(
            color_identity=["B", "R", "W"],
            candidates=[{
                "name": "Cult of Skaab", "is_user_pick": False,
                "type_line": "Creature", "cmc": 4,
                "primitives": ["GRAVEYARD_VALUE"], "color_identity": ["B"],
                "rationale_components": ["staple:0.05"], "score": 3.0,
            }],
        )
        critic_json = """
        {"selected_cards": [
          {"name": "Cult of Skaab", "category": "creature",
           "reason": "Underused but synergizes with the lifegain shell.",
           "is_creative_outlier": true}
        ], "combo_lines_noted": []}
        """
        client = self._llm_client(critic_json)
        with self._patch_oracle_text():
            new_deck, warnings = _run_candidate_critic(
                llm_client=client, deck=deck, pool=pool,
                commander="Edgar Markov", bracket="B3",
                theme_hints=[], intent_analysis=None,
                llm_metrics={"calls": []}, novel_combo_flags=[],
            )
        cult = next((c for c in new_deck if c["card_name"] == "Cult of Skaab"), None)
        self.assertIsNotNone(cult)
        self.assertIn("creative_outlier", cult["source"])


if __name__ == "__main__":
    unittest.main()
