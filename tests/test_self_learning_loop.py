"""Tests for tools/playtest/self_learning_loop.py + playtest_changelog_v1
manifest registration (Phase 5b stage 5b.9)."""
from __future__ import annotations

import unittest

from tools.playtest.self_learning_loop import (
    PER_BATCH_CAP_DEFAULT,
    CUMULATIVE_CAP_DEFAULT,
    SELF_LEARNING_LOOP_V1_VERSION,
    TIER_AUTO_ADJUST,
    TIER_HALT,
    TIER_QUEUE_FOR_REVIEW,
    DriftEvaluation,
    evaluate_batch,
    evaluate_drift,
    make_changelog_entry,
)


class SelfLearningTierClassificationTests(unittest.TestCase):
    def test_drift_within_per_batch_cap_is_auto_adjust(self) -> None:
        e = evaluate_drift(
            deck_id="A", metric="win_rate", old_value=0.50, new_value=0.52,
        )
        self.assertEqual(e.tier, TIER_AUTO_ADJUST)
        self.assertAlmostEqual(e.drift, 0.02, places=6)

    def test_drift_exceeding_per_batch_cap_is_queue_for_review(self) -> None:
        e = evaluate_drift(
            deck_id="A", metric="win_rate", old_value=0.50, new_value=0.60,
        )
        self.assertEqual(e.tier, TIER_QUEUE_FOR_REVIEW)
        self.assertAlmostEqual(e.drift, 0.10, places=6)

    def test_architectural_shift_forces_halt_regardless_of_drift(self) -> None:
        # Even a tiny drift halts when architectural_shift is set.
        e = evaluate_drift(
            deck_id="A", metric="win_rate", old_value=0.50, new_value=0.501,
            architectural_shift="new_bracket_id",
        )
        self.assertEqual(e.tier, TIER_HALT)
        self.assertEqual(e.reason, "new_bracket_id")

    def test_cumulative_drift_breach_pushes_to_queue_for_review(self) -> None:
        # Per-batch drift within cap, but cumulative would exceed -> queue.
        e = evaluate_drift(
            deck_id="A", metric="win_rate", old_value=0.50, new_value=0.52,
            cumulative_drift=0.14,  # +0.02 -> 0.16 > 0.15 cap
        )
        self.assertEqual(e.tier, TIER_QUEUE_FOR_REVIEW)


class SelfLearningChangelogEntryTests(unittest.TestCase):
    def test_make_changelog_entry_has_required_fields(self) -> None:
        eval = evaluate_drift(deck_id="A", metric="win_rate", old_value=0.5, new_value=0.51)
        entry = make_changelog_entry(eval, timestamp_utc="2026-05-09T07:00:00Z",
                                      game_count=100, run_id="r1")
        for field in ("timestamp_utc", "deck_id", "metric", "old_value", "new_value",
                      "drift", "tier", "game_count", "run_id", "reason"):
            self.assertIn(field, entry)
        self.assertEqual(entry["tier"], TIER_AUTO_ADJUST)


class SelfLearningBatchEvalTests(unittest.TestCase):
    def test_batch_with_all_within_cap_no_halt(self) -> None:
        drifts = [
            {"deck_id": "A", "metric": "win_rate", "old_value": 0.5, "new_value": 0.51},
            {"deck_id": "B", "metric": "win_rate", "old_value": 0.4, "new_value": 0.42},
        ]
        evals, halt = evaluate_batch(drifts)
        self.assertFalse(halt)
        self.assertTrue(all(e.tier == TIER_AUTO_ADJUST for e in evals))

    def test_batch_with_architectural_shift_triggers_halt(self) -> None:
        drifts = [
            {"deck_id": "A", "metric": "win_rate", "old_value": 0.5, "new_value": 0.51},
            {"deck_id": "B", "metric": "win_rate", "old_value": 0.4, "new_value": 0.42,
             "architectural_shift": "new_bracket_id"},
        ]
        evals, halt = evaluate_batch(drifts)
        self.assertTrue(halt)

    def test_batch_with_excessive_cumulative_drift_triggers_safety_9_halt(self) -> None:
        # Safety #9: cumulative drift > 5x per-batch-cap = halt.
        # 5 drifts each at 0.10 (way over per-batch cap of 0.03).
        drifts = [
            {"deck_id": f"D{i}", "metric": "win_rate", "old_value": 0.5, "new_value": 0.6}
            for i in range(5)
        ]
        evals, halt = evaluate_batch(drifts)
        # Total |drift| = 5 * 0.10 = 0.50. Threshold = 0.03 * 5 * 5 = 0.75.
        # Total < threshold so no SAFETY #9 halt; queue-for-review tier instead.
        self.assertFalse(halt)
        self.assertTrue(all(e.tier == TIER_QUEUE_FOR_REVIEW for e in evals))


class PlaytestChangelogManifestRegistrationTests(unittest.TestCase):
    """Calibration-only manifest registration check (safety #1)."""

    def test_playtest_changelog_pack_is_calibration_only_in_manifest(self) -> None:
        from api.engine.curated_pack_manifest_v1 import (
            iter_runtime_pack_entries,
            load_curated_pack_manifest_v1,
        )
        manifest = load_curated_pack_manifest_v1()
        all_pack_ids = {e["pack_id"] for e in manifest["packs"]}
        self.assertIn("playtest_changelog_v1", all_pack_ids)
        runtime_pack_ids = {e["pack_id"] for e in iter_runtime_pack_entries()}
        self.assertNotIn("playtest_changelog_v1", runtime_pack_ids)


if __name__ == "__main__":
    unittest.main()
