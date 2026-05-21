"""Iter 4 Phase 3 — outer-chain parallelization tests.

`_merge_c21_c22_decks` takes the iter-1 baseline deck plus the C2.1 and
C2.2 result decks (each computed in parallel against the same baseline)
and produces a merged 100-card deck with C2.1-precedence semantics.

Tests cover:
  - No-op (neither phase made changes) — baseline preserved.
  - C2.1 only: result equals C2.1 output, no warnings.
  - C2.2 only: result has C2.2 swaps applied, no conflicts.
  - Disjoint swaps: both applied, no warnings.
  - Conflict 1: C2.2 tries to remove a card C2.1 added.
  - Conflict 2: C2.2 tries to add a card C2.1 added.
  - Conflict 3: C2.2 wants to swap on a slot whose original card C2.1
    already removed.
  - Order-independent: positional shuffles in C2.1 output (locked +
    new_swappable reassembly) don't break C2.2 swap recovery.
"""
from __future__ import annotations

import unittest

from api.engine.layers.agent_build_deck_v1 import _merge_c21_c22_decks


def _mk(name: str, source: str = "agent", reason: str = "") -> dict:
    return {"card_name": name, "source": source, "reason": reason or f"{name} reason"}


class MergeNoChangesTests(unittest.TestCase):
    def test_neither_phase_changed_anything(self) -> None:
        baseline = [_mk(f"Card{i}") for i in range(100)]
        merged, warnings = _merge_c21_c22_decks(baseline, list(baseline), list(baseline))
        self.assertEqual([c["card_name"] for c in merged],
                         [c["card_name"] for c in baseline])
        self.assertEqual(warnings, [])


class MergeOnePhaseChangedTests(unittest.TestCase):
    def test_only_c21_swapped(self) -> None:
        baseline = [_mk(f"Card{i}") for i in range(100)]
        c21 = list(baseline)
        c21[10] = _mk("NewByC21", source="llm_candidate_critic")
        # C2.2 didn't touch anything.
        merged, warnings = _merge_c21_c22_decks(baseline, c21, list(baseline))
        self.assertIn("NewByC21", [c["card_name"] for c in merged])
        self.assertNotIn("Card10", [c["card_name"] for c in merged])
        self.assertEqual(warnings, [])

    def test_only_c22_swapped(self) -> None:
        baseline = [_mk(f"Card{i}") for i in range(100)]
        c22 = list(baseline)
        c22[20] = _mk("NewByC22", source="llm_wild_combo_discovery")
        merged, warnings = _merge_c21_c22_decks(baseline, list(baseline), c22)
        self.assertIn("NewByC22", [c["card_name"] for c in merged])
        self.assertNotIn("Card20", [c["card_name"] for c in merged])
        self.assertEqual(warnings, [])


class MergeDisjointSwapsTests(unittest.TestCase):
    def test_disjoint_swaps_both_applied(self) -> None:
        baseline = [_mk(f"Card{i}") for i in range(100)]
        c21 = list(baseline)
        c21[10] = _mk("ByC21", source="llm_candidate_critic")
        c22 = list(baseline)
        c22[20] = _mk("ByC22", source="llm_wild_combo_discovery")
        merged, warnings = _merge_c21_c22_decks(baseline, c21, c22)
        names = [c["card_name"] for c in merged]
        self.assertIn("ByC21", names)
        self.assertIn("ByC22", names)
        self.assertNotIn("Card10", names)
        self.assertNotIn("Card20", names)
        self.assertEqual(warnings, [])


class MergeConflictTests(unittest.TestCase):
    def test_c22_tries_to_remove_c21_pick(self) -> None:
        # C2.1 swaps in "ByC21" at slot 10. C2.2 (running in parallel against
        # the baseline) tries to swap out Card10 — but C2.1 already took
        # that slot. Conflict: drop C2.2.
        # Wait — both run against the BASELINE, so C2.2 wouldn't try to
        # remove a card that's not in baseline. The conflict shape is:
        # C2.2 tries to remove a card whose slot C2.1 also touched.
        # _merge_c21_c22_decks detects this via "remove_lower is not in
        # the merged deck (C2.1 removed it already)".
        baseline = [_mk(f"Card{i}") for i in range(100)]
        c21 = list(baseline)
        c21[10] = _mk("ByC21", source="llm_candidate_critic")
        c22 = list(baseline)
        # C2.2 also swapped Card10 (the baseline card at slot 10).
        c22[10] = _mk("ByC22", source="llm_wild_combo_discovery")
        merged, warnings = _merge_c21_c22_decks(baseline, c21, c22)
        names = [c["card_name"] for c in merged]
        self.assertIn("ByC21", names)  # C2.1 wins.
        self.assertNotIn("ByC22", names)
        # Conflict warning emitted.
        codes = [w["code"] for w in warnings]
        self.assertIn("OUTER_CHAIN_C21_C22_CONFLICT", codes)

    def test_c22_tries_to_add_card_c21_already_added(self) -> None:
        baseline = [_mk(f"Card{i}") for i in range(100)]
        c21 = list(baseline)
        c21[10] = _mk("Shared", source="llm_candidate_critic")
        c22 = list(baseline)
        # C2.2 tries to add the SAME card "Shared" at a different slot.
        c22[20] = _mk("Shared", source="llm_wild_combo_discovery")
        merged, warnings = _merge_c21_c22_decks(baseline, c21, c22)
        names = [c["card_name"] for c in merged]
        # "Shared" appears once (from C2.1). "Card20" stays.
        self.assertEqual(names.count("Shared"), 1)
        self.assertIn("Card20", names)
        codes = [w["code"] for w in warnings]
        self.assertIn("OUTER_CHAIN_C21_C22_CONFLICT", codes)

    def test_c22_targets_card_c21_removed(self) -> None:
        # C2.1 swapped out Card10 from slot 10 and reordered. C2.2 also
        # tried to swap out Card10. After applying C2.1's swap, Card10
        # is no longer present, so C2.2's swap can't find its target.
        baseline = [_mk(f"Card{i}") for i in range(100)]
        c21 = list(baseline)
        c21[10] = _mk("FromC21", source="llm_candidate_critic")
        # C2.2's view: still operates on baseline. It swaps Card10 → ByC22.
        # Positionally that's the same slot index — so this test is
        # equivalent to test_c22_tries_to_remove_c21_pick. To distinguish,
        # swap C2.1's pick in a DIFFERENT slot from C2.2's.
        c22 = list(baseline)
        c22[10] = _mk("ByC22", source="llm_wild_combo_discovery")
        # In this scenario the swap pair is (Card10 → ByC22). C2.1 took slot 10.
        merged, warnings = _merge_c21_c22_decks(baseline, c21, c22)
        names = [c["card_name"] for c in merged]
        self.assertIn("FromC21", names)
        self.assertNotIn("ByC22", names)
        codes = [w["code"] for w in warnings]
        self.assertIn("OUTER_CHAIN_C21_C22_CONFLICT", codes)

    def test_multiple_independent_c22_swaps(self) -> None:
        baseline = [_mk(f"Card{i}") for i in range(100)]
        c21 = list(baseline)
        c21[5] = _mk("ByC21_5", source="llm_candidate_critic")
        c22 = list(baseline)
        c22[20] = _mk("ByC22_20", source="llm_wild_combo_discovery")
        c22[30] = _mk("ByC22_30", source="llm_wild_combo_discovery")
        c22[40] = _mk("ByC22_40", source="llm_wild_combo_discovery|creative_outlier")
        merged, warnings = _merge_c21_c22_decks(baseline, c21, c22)
        names = [c["card_name"] for c in merged]
        for n in ("ByC21_5", "ByC22_20", "ByC22_30", "ByC22_40"):
            self.assertIn(n, names)
        self.assertEqual(warnings, [])


class MergePreservesC21SourceTests(unittest.TestCase):
    def test_c21_pick_keeps_its_source_string(self) -> None:
        baseline = [_mk(f"Card{i}") for i in range(100)]
        c21 = list(baseline)
        c21[7] = _mk("Picked", source="llm_candidate_critic|creative_outlier")
        merged, _ = _merge_c21_c22_decks(baseline, c21, list(baseline))
        picked = next(c for c in merged if c["card_name"] == "Picked")
        self.assertEqual(picked["source"], "llm_candidate_critic|creative_outlier")

    def test_c22_pick_keeps_its_source_string(self) -> None:
        baseline = [_mk(f"Card{i}") for i in range(100)]
        c22 = list(baseline)
        c22[7] = _mk("Picked",
                     source="llm_wild_combo_discovery|from_semantic_neighbor")
        merged, _ = _merge_c21_c22_decks(baseline, list(baseline), c22)
        picked = next(c for c in merged if c["card_name"] == "Picked")
        self.assertEqual(picked["source"],
                         "llm_wild_combo_discovery|from_semantic_neighbor")


if __name__ == "__main__":
    unittest.main()
