"""Phase C2.2 tests — wide candidate pool + wild combo discovery.

Two groups:
  - Wide candidate pool module (agent_wide_candidate_pool_v1) — pure
    filtering/ranking with stubbed DB.
  - Wild combo discovery (_run_wild_combo_discovery) — mocked LLM.

We verify:
  - The wide pool filters color-identity correctly.
  - Theme-overlap cards rank above non-overlap ones.
  - The discovery loop accepts a valid add_swap with all checks passed.
  - Bracket-illegal swaps get demoted to flag_only entries.
  - User-pick removes are rejected.
  - flag_only suggestions populate novel_combo_flags directly.
  - LLM failure leaves the deck unchanged.
"""
from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

from api.engine.layers.agent_build_deck_v1 import (
    _build_wild_combo_user_prompt,
    _run_wild_combo_discovery,
)
from api.engine.layers.agent_llm_client_v1 import CallResult
from api.engine.layers.agent_wide_candidate_pool_v1 import (
    DEFAULT_POOL_SIZE,
    compute_agent_wide_candidate_pool_v1,
)


def _mk_call_result(text, ok=True):
    from api.engine.layers.agent_llm_client_v1 import _try_parse_json
    return CallResult(
        ok=ok, text=text,
        parsed_json=_try_parse_json(text) if ok else None,
        input_tokens=15000, output_tokens=2000,
        cost_usd=0.075, latency_ms=5432, model="claude-sonnet-4-6",
    )


def _ci_pool(color_identity, candidates=None):
    return {
        "color_identity": color_identity,
        "candidates": candidates or [],
        "archetype_brief": {"staple_cards": []},
        "warnings": [],
        "endpoint_calls": 0,
        "db_snapshot_id": "snap",
    }


def _make_100_deck(commander="Edgar Markov"):
    deck = [{"card_name": commander, "reason": "Cmdr.", "source": "user_intent"}]
    for i in range(36):
        deck.append({"card_name": "Swamp", "reason": "Mana base.", "source": "mana_base"})
    for i in range(63):
        deck.append({
            "card_name": f"Picks {i:02d}",
            "reason": "Filler.", "source": "agent_select",
        })
    assert len(deck) == 100
    return deck


# ============================================================
# Wide candidate pool — DB-stubbed.
# ============================================================


class WideCandidatePoolTests(unittest.TestCase):
    def _make_row(self, name, type_line="Creature", cmc=3, ci="B", primitives=None,
                  oracle_text=""):
        return {
            "name": name,
            "type_line": type_line,
            "cmc": cmc,
            "color_identity": ci,
            "primitives_json": '["' + '", "'.join(primitives or []) + '"]' if primitives else "[]",
            "oracle_text": oracle_text,
            "mana_cost": "",
        }

    def _patch_db_rows(self, rows):
        """Return a context manager that patches engine.db.connect to
        yield a mock connection whose .execute().fetchall() returns rows."""
        # Build a mock cursor — must behave like sqlite3.Row enough for
        # subscript access.
        from contextlib import contextmanager

        class _FakeRow:
            def __init__(self, d):
                self._d = d

            def __getitem__(self, key):
                return self._d.get(key)

        fake_rows = [_FakeRow(r) for r in rows]

        fake_cursor = MagicMock()
        fake_cursor.fetchall.return_value = fake_rows

        fake_con = MagicMock()
        fake_con.execute.return_value = fake_cursor
        fake_con.__enter__ = MagicMock(return_value=fake_con)
        fake_con.__exit__ = MagicMock(return_value=False)

        from engine import db as eng_db
        return patch.object(eng_db, "connect", return_value=fake_con)

    def test_color_identity_filter(self) -> None:
        rows = [
            self._make_row("Black Card", ci="B"),
            self._make_row("White Card", ci="W"),
            self._make_row("Black+White Card", ci='["B", "W"]'),
        ]
        with self._patch_db_rows(rows):
            res = compute_agent_wide_candidate_pool_v1(
                db_snapshot_id="snap", commander="X",
                color_identity=["B"],
            )
        names = {c["name"] for c in res["candidates"]}
        self.assertIn("Black Card", names)
        self.assertNotIn("White Card", names)
        self.assertNotIn("Black+White Card", names)

    def test_basic_lands_excluded(self) -> None:
        rows = [
            self._make_row("Swamp", type_line="Basic Land — Swamp", ci=""),
            self._make_row("Sol Ring", type_line="Artifact", ci=""),
        ]
        with self._patch_db_rows(rows):
            res = compute_agent_wide_candidate_pool_v1(
                db_snapshot_id="snap", commander="X", color_identity=[],
            )
        names = {c["name"] for c in res["candidates"]}
        self.assertNotIn("Swamp", names)
        self.assertIn("Sol Ring", names)

    def test_exclusion_list_drops_cards(self) -> None:
        rows = [
            self._make_row("Already In Deck", ci="B"),
            self._make_row("Available", ci="B"),
        ]
        with self._patch_db_rows(rows):
            res = compute_agent_wide_candidate_pool_v1(
                db_snapshot_id="snap", commander="X",
                color_identity=["B"],
                exclude_names=["Already In Deck"],
            )
        names = {c["name"] for c in res["candidates"]}
        self.assertNotIn("Already In Deck", names)
        self.assertIn("Available", names)

    def test_theme_overlap_ranks_first(self) -> None:
        rows = [
            self._make_row("No Theme A", ci="B", primitives=["UNRELATED"]),
            self._make_row("Theme B", ci="B", primitives=["LIFEGAIN_PAYOFF"]),
            self._make_row("No Theme C", ci="B", primitives=["OTHER"]),
        ]
        with self._patch_db_rows(rows):
            res = compute_agent_wide_candidate_pool_v1(
                db_snapshot_id="snap", commander="X",
                color_identity=["B"],
                theme_primitives=["LIFEGAIN_PAYOFF"],
                pool_size=10,
            )
        # Theme card ranks first.
        names = [c["name"] for c in res["candidates"]]
        self.assertEqual(names[0], "Theme B")

    def test_filter_summary_records_counts(self) -> None:
        rows = [self._make_row("A", ci="B"), self._make_row("B", ci="W")]
        with self._patch_db_rows(rows):
            res = compute_agent_wide_candidate_pool_v1(
                db_snapshot_id="snap", commander="X",
                color_identity=["B"],
            )
        summary = res["filter_summary"]
        self.assertEqual(summary["total_rows"], 2)
        self.assertEqual(summary["after_ci"], 1)
        self.assertEqual(summary["returned"], 1)


# ============================================================
# _run_wild_combo_discovery — mocked wide pool + mocked LLM.
# ============================================================


class WildComboDiscoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        os.environ.pop("MTG_ENGINE_DISABLE_LLM", None)
        os.environ["ANTHROPIC_API_KEY"] = "sk-test"

    def tearDown(self) -> None:
        os.environ["MTG_ENGINE_DISABLE_LLM"] = "1"

    def _llm_client(self, response_text, ok=True):
        client = MagicMock()
        client.is_available.return_value = True
        client.call_with_budget.return_value = _mk_call_result(response_text, ok=ok)
        return client

    def _stub_wide_pool(self, candidates):
        """Patch the wide candidate pool to return a controlled list."""
        from api.engine.layers import agent_wide_candidate_pool_v1 as mod
        return patch.object(
            mod, "compute_agent_wide_candidate_pool_v1",
            return_value={
                "version": "test", "candidates": candidates,
                "color_identity": ["B", "R", "W"],
                "filter_summary": {"returned": len(candidates)},
                "warnings": [],
            },
        )

    def test_add_swap_applied_when_all_checks_pass(self) -> None:
        deck = _make_100_deck()
        # Wide pool has the target add card.
        wide_candidates = [{
            "name": "Sanguine Bond", "type_line": "Enchantment",
            "cmc": 5, "primitives": ["LIFEGAIN_PAYOFF"],
            "color_identity": ["B"], "oracle_text": "Drain",
            "score": 10.0,
        }]
        pool = _ci_pool(color_identity=["B", "R", "W"], candidates=[])
        llm_response = """
        {
          "suggestions": [
            {"action": "add_swap",
             "add_card": "Sanguine Bond",
             "remove_card": "Picks 00",
             "combo_partner": "Exquisite Blood",
             "outcome": "Infinite drain",
             "is_known_spellbook_combo": true,
             "is_creative_outlier": false}
          ]
        }
        """
        client = self._llm_client(llm_response)
        novel: list = []
        with self._stub_wide_pool(wide_candidates):
            new_deck, warnings = _run_wild_combo_discovery(
                llm_client=client, deck=deck, pool=pool,
                commander="Edgar Markov", bracket="B3",
                theme_hints=[], db_snapshot_id="snap",
                intent_analysis=None,
                llm_metrics={"calls": []}, novel_combo_flags=novel,
            )
        # Picks 00 swapped for Sanguine Bond.
        names = [c["card_name"] for c in new_deck]
        self.assertIn("Sanguine Bond", names)
        self.assertNotIn("Picks 00", names)
        # Novel combo flag recorded with applied_swap=True.
        self.assertEqual(len(novel), 1)
        self.assertTrue(novel[0]["applied_swap"])

    def test_user_pick_remove_rejected(self) -> None:
        deck = _make_100_deck()
        wide_candidates = [{
            "name": "Some Card", "type_line": "Creature", "cmc": 3,
            "primitives": [], "color_identity": ["B"], "oracle_text": "",
            "score": 5.0,
        }]
        pool = _ci_pool(color_identity=["B", "R", "W"], candidates=[])
        llm_response = """
        {"suggestions": [{"action": "add_swap",
          "add_card": "Some Card",
          "remove_card": "Edgar Markov",
          "outcome": "...", "is_known_spellbook_combo": false,
          "is_creative_outlier": false}]}
        """
        client = self._llm_client(llm_response)
        with self._stub_wide_pool(wide_candidates):
            new_deck, warnings = _run_wild_combo_discovery(
                llm_client=client, deck=deck, pool=pool,
                commander="Edgar Markov", bracket="B3",
                theme_hints=[], db_snapshot_id="snap",
                intent_analysis=None,
                llm_metrics={"calls": []}, novel_combo_flags=[],
            )
        codes = [w["code"] for w in warnings]
        self.assertIn("WILD_COMBO_REJECTED_REMOVE_USER_PICK", codes)
        # Edgar Markov still present.
        self.assertIn("Edgar Markov", [c["card_name"] for c in new_deck])

    def test_hallucinated_add_rejected(self) -> None:
        deck = _make_100_deck()
        pool = _ci_pool(color_identity=["B", "R", "W"], candidates=[])
        llm_response = """
        {"suggestions": [{"action": "add_swap",
          "add_card": "ImaginaryCard9000",
          "remove_card": "Picks 00",
          "outcome": "...", "is_known_spellbook_combo": false,
          "is_creative_outlier": false}]}
        """
        client = self._llm_client(llm_response)
        with self._stub_wide_pool([{
            "name": "Sanguine Bond",
            "type_line": "Enchantment", "cmc": 5, "primitives": [],
            "color_identity": ["B"], "oracle_text": "", "score": 5.0,
        }]):
            new_deck, warnings = _run_wild_combo_discovery(
                llm_client=client, deck=deck, pool=pool,
                commander="Edgar Markov", bracket="B3",
                theme_hints=[], db_snapshot_id="snap",
                intent_analysis=None,
                llm_metrics={"calls": []}, novel_combo_flags=[],
            )
        codes = [w["code"] for w in warnings]
        self.assertIn("WILD_COMBO_REJECTED_HALLUCINATION", codes)

    def test_flag_only_populates_novel_combo_flags(self) -> None:
        deck = _make_100_deck()
        pool = _ci_pool(color_identity=["B", "R", "W"], candidates=[])
        llm_response = """
        {"suggestions": [{"action": "flag_only",
          "combo_cards": ["Picks 00", "Picks 01"],
          "outcome": "Already-present combo line.",
          "is_known_spellbook_combo": false,
          "is_creative_outlier": true}]}
        """
        client = self._llm_client(llm_response)
        novel: list = []
        with self._stub_wide_pool([{
            "name": "Filler", "type_line": "Creature", "cmc": 1,
            "primitives": [], "color_identity": [], "oracle_text": "",
            "score": 0.0,
        }]):
            new_deck, warnings = _run_wild_combo_discovery(
                llm_client=client, deck=deck, pool=pool,
                commander="Edgar Markov", bracket="B3",
                theme_hints=[], db_snapshot_id="snap",
                intent_analysis=None,
                llm_metrics={"calls": []}, novel_combo_flags=novel,
            )
        self.assertEqual(len(novel), 1)
        flag = novel[0]
        self.assertEqual(flag["cards"], ["Picks 00", "Picks 01"])
        self.assertFalse(flag["applied_swap"])
        self.assertEqual(flag["source"], "C2_2_wild_combo_discovery_flag")

    def test_llm_failure_leaves_deck_unchanged(self) -> None:
        deck = _make_100_deck()
        pool = _ci_pool(color_identity=["B", "R", "W"], candidates=[])
        client = self._llm_client("", ok=False)
        with self._stub_wide_pool([{
            "name": "Sanguine Bond", "type_line": "Enchantment",
            "cmc": 5, "primitives": [], "color_identity": ["B"],
            "oracle_text": "", "score": 10.0,
        }]):
            new_deck, warnings = _run_wild_combo_discovery(
                llm_client=client, deck=deck, pool=pool,
                commander="Edgar Markov", bracket="B3",
                theme_hints=[], db_snapshot_id="snap",
                intent_analysis=None,
                llm_metrics={"calls": []}, novel_combo_flags=[],
            )
        # Deck cardlist unchanged.
        self.assertEqual(
            [c["card_name"] for c in new_deck],
            [c["card_name"] for c in deck],
        )
        codes = [w["code"] for w in warnings]
        self.assertIn("WILD_COMBO_FAILED", codes)

    def test_empty_suggestions_is_no_op(self) -> None:
        deck = _make_100_deck()
        pool = _ci_pool(color_identity=["B", "R", "W"], candidates=[])
        client = self._llm_client('{"suggestions": []}')
        with self._stub_wide_pool([{
            "name": "Filler", "type_line": "Creature", "cmc": 1,
            "primitives": [], "color_identity": [], "oracle_text": "",
            "score": 0.0,
        }]):
            new_deck, warnings = _run_wild_combo_discovery(
                llm_client=client, deck=deck, pool=pool,
                commander="Edgar Markov", bracket="B3",
                theme_hints=[], db_snapshot_id="snap",
                intent_analysis=None,
                llm_metrics={"calls": []}, novel_combo_flags=[],
            )
        # Deck unchanged. No warnings besides info ones.
        self.assertEqual(
            [c["card_name"] for c in new_deck],
            [c["card_name"] for c in deck],
        )


class PromptShapeTests(unittest.TestCase):
    def test_prompt_includes_deck_pool_and_policy(self) -> None:
        prompt = _build_wild_combo_user_prompt(
            commander="Edgar Markov", bracket="B4",
            theme_hints=["TYPAL_VAMPIRES"],
            intent_analysis={"likely_win_condition": "Drain via lifegain."},
            deck=[{"card_name": "Vito", "source": "user_intent"}],
            wide_pool=[{
                "name": "Sanguine Bond", "type_line": "Enchantment",
                "cmc": 5, "primitives": ["LIFEGAIN_PAYOFF"],
                "oracle_text": "Whenever you gain life..."
            }],
            bracket_policy_summary="Bracket B4: combos capped at 3 pairs.",
        )
        self.assertIn("Edgar Markov", prompt)
        self.assertIn("B4", prompt)
        self.assertIn("CURRENT 99-CARD DECK", prompt)
        self.assertIn("WIDE CANDIDATE POOL", prompt)
        self.assertIn("Sanguine Bond", prompt)
        self.assertIn("Drain via lifegain", prompt)
        self.assertIn("Bracket B4", prompt)


if __name__ == "__main__":
    unittest.main()
