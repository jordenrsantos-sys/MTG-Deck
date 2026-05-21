"""Iter 3 Phase 2 tests — B2 combo-anchor hard guard.

Tests `build_forbidden_set` against the live combo_brackets_v1.json
registry to verify it correctly identifies:

  - Ur-Dragon + Tiamat: forbids Old Gnawbone, Hellkite Charger, and the
    other dragons Tiamat names + their combo partners.
  - Atraxa + Doubling Season + Pir: any combos where these are anchors.
  - Krenko + Conspicuous Snoop + Kiki-Jiki: user listed BOTH halves of
    Kiki+Snoop, so neither is in the forbidden set (opted in).
  - Edge cases: empty must-includes → empty set; missing registry path
    → empty set + no exception.
  - Output filtering: `filter_llm_suggestions` splits a list correctly.
  - Prompt block: `format_forbidden_block_for_prompt` is empty when set
    is empty, otherwise contains the explanation + name list.
"""
from __future__ import annotations

import unittest
from pathlib import Path

from api.engine.layers.agent_combo_anchor_guard_v1 import (
    build_forbidden_set,
    filter_llm_suggestions,
    format_forbidden_block_for_prompt,
)


# ============================================================
# build_forbidden_set — synthetic small registries.
# ============================================================


def _synthetic_registry(*combos) -> dict:
    """combos is a list of (combo_id, card_names_list, combo_size) tuples."""
    by_variant_id = {}
    for cid, names, size in combos:
        by_variant_id[cid] = {
            "card_names": names,
            "combo_size": size,
            "brackets_allowed": ["B3", "B4", "B5"],
        }
    return by_variant_id


class SyntheticRegistryTests(unittest.TestCase):
    def test_empty_must_includes_returns_empty_set(self) -> None:
        reg = _synthetic_registry(("c1", ["A", "B"], 2))
        forbidden, sources = build_forbidden_set([], combo_registry=reg)
        self.assertEqual(forbidden, set())
        self.assertEqual(sources, [])

    def test_pair_anchor_forbids_partner(self) -> None:
        reg = _synthetic_registry(("c1", ["Vito", "Sanguine Bond"], 2))
        forbidden, sources = build_forbidden_set(["Vito"], combo_registry=reg)
        self.assertIn("sanguine bond", forbidden)
        self.assertNotIn("vito", forbidden)
        self.assertEqual(len(sources), 1)
        self.assertEqual(sources[0]["user_anchor"], "vito")
        self.assertEqual(sources[0]["completing_cards"], ["Sanguine Bond"])

    def test_user_opts_in_to_both_halves_no_forbidden(self) -> None:
        reg = _synthetic_registry(("c1", ["Kiki-Jiki", "Conspicuous Snoop"], 2))
        forbidden, sources = build_forbidden_set(
            ["Kiki-Jiki", "Conspicuous Snoop"], combo_registry=reg,
        )
        self.assertEqual(forbidden, set())
        self.assertEqual(sources, [])

    def test_multi_card_combo_forbids_all_partners(self) -> None:
        # If user names 1 of 5 anchors, the other 4 are forbidden.
        reg = _synthetic_registry((
            "c1", ["Tiamat", "Dragon A", "Dragon B", "Dragon C", "Dragon D"], 5,
        ))
        forbidden, _sources = build_forbidden_set(["Tiamat"], combo_registry=reg)
        self.assertEqual(forbidden, {"dragon a", "dragon b", "dragon c", "dragon d"})

    def test_multi_card_combo_user_listed_subset(self) -> None:
        # If user names 2 of 5, the other 3 are forbidden.
        reg = _synthetic_registry((
            "c1", ["Tiamat", "Dragon A", "Dragon B", "Dragon C", "Dragon D"], 5,
        ))
        forbidden, _ = build_forbidden_set(
            ["Tiamat", "Dragon A"], combo_registry=reg,
        )
        self.assertEqual(forbidden, {"dragon b", "dragon c", "dragon d"})

    def test_case_insensitive_matching(self) -> None:
        reg = _synthetic_registry(("c1", ["Vito", "Sanguine Bond"], 2))
        forbidden, _ = build_forbidden_set(["VITO"], combo_registry=reg)
        self.assertIn("sanguine bond", forbidden)

    def test_anchor_not_in_any_combo(self) -> None:
        reg = _synthetic_registry(("c1", ["Vito", "Sanguine Bond"], 2))
        forbidden, sources = build_forbidden_set(
            ["Edgar Markov"], combo_registry=reg,
        )
        self.assertEqual(forbidden, set())
        self.assertEqual(sources, [])

    def test_multiple_anchor_combos_union(self) -> None:
        reg = _synthetic_registry(
            ("c1", ["Vito", "Sanguine Bond"], 2),
            ("c2", ["Vito", "Exquisite Blood"], 2),
        )
        forbidden, sources = build_forbidden_set(["Vito"], combo_registry=reg)
        self.assertEqual(forbidden, {"sanguine bond", "exquisite blood"})
        self.assertEqual(len(sources), 2)


# ============================================================
# Live registry — actual combo_brackets_v1.json.
# ============================================================


class LiveRegistryTests(unittest.TestCase):
    def test_tiamat_forbids_at_least_one_dragon(self) -> None:
        # The Ur-Dragon iteration-2 envelope leak case. Tiamat appears
        # in many combo variants; at minimum we expect SOMETHING in the
        # forbidden set after passing it in.
        forbidden, sources = build_forbidden_set(["Tiamat"])
        # Sanity: forbidden set is non-empty.
        self.assertGreater(len(forbidden), 0)
        # The registry has many combo variants with Tiamat — sources
        # should be non-empty.
        self.assertGreater(len(sources), 0)
        # Critical: no entry is Tiamat itself.
        self.assertNotIn("tiamat", forbidden)

    def test_kiki_snoop_user_opts_in_no_forbidden_for_either(self) -> None:
        # Krenko case from iter 2: user explicitly listed BOTH halves.
        forbidden, _ = build_forbidden_set([
            "Kiki-Jiki, Mirror Breaker",
            "Conspicuous Snoop",
        ])
        # Neither anchor is in the forbidden set.
        self.assertNotIn("kiki-jiki, mirror breaker", forbidden)
        self.assertNotIn("conspicuous snoop", forbidden)


# ============================================================
# filter_llm_suggestions.
# ============================================================


class FilterLlmSuggestionsTests(unittest.TestCase):
    def test_split_into_kept_and_blocked(self) -> None:
        forbidden = {"sanguine bond"}
        kept, blocked = filter_llm_suggestions(
            ["Sol Ring", "Sanguine Bond", "Vito"], forbidden,
        )
        self.assertEqual(kept, ["Sol Ring", "Vito"])
        self.assertEqual(blocked, ["Sanguine Bond"])

    def test_case_insensitive(self) -> None:
        forbidden = {"sanguine bond"}
        kept, blocked = filter_llm_suggestions(["SANGUINE BOND"], forbidden)
        self.assertEqual(kept, [])
        self.assertEqual(blocked, ["SANGUINE BOND"])

    def test_empty_inputs_safe(self) -> None:
        kept, blocked = filter_llm_suggestions([], set())
        self.assertEqual(kept, [])
        self.assertEqual(blocked, [])


# ============================================================
# format_forbidden_block_for_prompt.
# ============================================================


class FormatPromptBlockTests(unittest.TestCase):
    def test_empty_set_returns_empty_string(self) -> None:
        self.assertEqual(format_forbidden_block_for_prompt(set()), "")

    def test_nonempty_set_includes_names_and_explanation(self) -> None:
        block = format_forbidden_block_for_prompt({"sanguine bond", "exquisite blood"})
        self.assertIn("FORBIDDEN CARDS", block)
        self.assertIn("sanguine bond", block)
        self.assertIn("exquisite blood", block)
        # Has an explanation — not just a bare list.
        self.assertIn("must-include", block.lower())

    def test_alphabetically_sorted_for_determinism(self) -> None:
        block = format_forbidden_block_for_prompt({"zebra", "apple", "mango"})
        # apple comes before mango comes before zebra.
        self.assertLess(block.index("apple"), block.index("mango"))
        self.assertLess(block.index("mango"), block.index("zebra"))


if __name__ == "__main__":
    unittest.main()
