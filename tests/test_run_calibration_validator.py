from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import tools.run_calibration_validator as validator


def _fake_resolver(card_data_by_oid):
    """Build a resolver that returns canned card data for the given oracle_ids.

    `card_data_by_oid` is a dict mapping oracle_id -> {"name": str, "primitives": set[str]}.
    Mirrors the output shape of `_load_oracle_id_to_card_map` so the validator
    can run fully offline without hitting mtg.sqlite.
    """

    def resolver(*, snapshot_id, oracle_ids):
        return {oid.lower(): card_data_by_oid.get(oid.lower(), {"name": "", "primitives": set()}) for oid in oracle_ids}

    return resolver


def _fake_combo_detector(matches=0):
    """Return a callable that mimics `detect_two_card_combos` returning a fixed
    count of combo matches regardless of input."""

    def detector(combo_input):
        return {"supported": True, "count": matches, "matches": []}

    return detector


def _build_corpus_pack(decks):
    return {
        "decks": decks,
        "generated_from": "test-fixture",
        "pack_id": "external_decks_v1",
        "source": "archidekt",
        "version": "external_decks_v1",
    }


def _make_deck(*, deck_id, self_reported, commander_oids, deck_oids):
    return {
        "commander_oracle_ids": list(commander_oids),
        "deck_id": deck_id,
        "deck_oracle_ids": list(deck_oids),
        "dedup_hash": f"hash-{deck_id}",
        "engine_assigned_bracket": None,
        "fetched_at_utc": "2026-05-08T00:00:00Z",
        "self_reported_bracket": self_reported,
        "source": "archidekt",
        "source_metadata": {"name": f"Deck {deck_id}", "engine_version_at_ingest": "post-2D"},
        "source_url": f"https://archidekt.com/decks/{deck_id}/",
    }


def _write_test_manifest(manifest_path, *, external_decks_sha):
    payload = {
        "packs": [
            {
                "load_order": 10,
                "pack_id": "sentinel_other_pack",
                "pack_version": "sentinel_v1",
                "path": "api/engine/data/sentinel/sentinel.json",
                "sha256": "0" * 64,
            },
            {
                "calibration_only": True,
                "load_order": 900,
                "pack_id": "external_decks_v1",
                "pack_version": "external_decks_v1",
                "path": "api/engine/data/calibration/external_decks_v1.json",
                "sha256": external_decks_sha,
            },
        ],
        "version": "curated_pack_manifest_v1",
    }
    text = json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
    manifest_path.write_text(text, encoding="utf-8")
    return text


class CalibrationValidatorBracketAssignmentTests(unittest.TestCase):
    """Pure unit tests for `_assign_engine_bracket_v1`. No DB / file I/O."""

    def test_assign_b2_for_minimal_composition(self) -> None:
        # 0 GC + 0 MLD + 0 ET + 0 combos → B2 (the "Reasonably casual"
        # catch-all). Pre-5a.6.2 returned B1; the 5a.6.2 tie-break flip
        # makes B2 the default for compositionally-empty decks because
        # WotC's framework names B2 as the precon + lightly-upgraded band.
        self.assertEqual(
            validator._assign_engine_bracket_v1(gc=0, mld=0, et=0, combos=0),
            "B2",
        )

    def test_assign_b2_for_low_signal_via_b1_b2_tie_break_flip(self) -> None:
        # B2 is policy-identical to B1 in BRACKET_RULES.md. Pre-5a.6.2 the
        # walk-up algorithm returned B1 first (B2 unreachable). Post-5a.6.2
        # the tie-break flip detects the tie and returns B2 — aligning
        # engine output with WotC's "Reasonably casual" catch-all convention.
        self.assertEqual(
            validator._assign_engine_bracket_v1(gc=0, mld=0, et=0, combos=0),
            "B2",
        )

    def test_assign_b3_for_moderate_signal_with_gc_in_range(self) -> None:
        # 2 GC + no MLD/ET/combos → B3 (B1/B2 cap gc at 0; B3 has gc_min=1).
        self.assertEqual(
            validator._assign_engine_bracket_v1(gc=2, mld=0, et=0, combos=0),
            "B3",
        )

    def test_assign_b4_for_high_signal_with_combos(self) -> None:
        # 0 GC + 0 MLD + 0 ET + 2 combos → B4 (combos DISALLOWED at B1/B2;
        # B3 has gc_min=1 so 0-GC composition can't fit; B4 ALLOW everything).
        self.assertEqual(
            validator._assign_engine_bracket_v1(gc=0, mld=0, et=0, combos=2),
            "B4",
        )

    def test_assign_b5_for_max_signal_via_b5_gc_min_floor(self) -> None:
        # 8 GC + 2 MLD + 1 ET + 3 combos → B5. Pre-5a.5.2 this returned B4
        # because B4 and B5 had identical caps (both gc=null..null + all
        # ALLOW). 5a.5.2 added explicit B4 gc_max=5 + B5 gc_min=6, so 8 GCs
        # now fails B4's gc_max and qualifies for B5's gc_min — first valid
        # bracket in walk order is B5. (Earlier docstring claimed "B5
        # unreachable in current rules"; that claim was specific to pre-
        # 5a.5.2 data and no longer holds.)
        self.assertEqual(
            validator._assign_engine_bracket_v1(gc=8, mld=2, et=1, combos=3),
            "B5",
        )


class CalibrationValidatorPowerStapleDiagnosticTests(unittest.TestCase):
    """5a.6 (Path C) — power-staple axis demoted to TRACK_ONLY analytics-only diagnostic.

    Pre-5a.6 the per-axis power-staple caps drove bracket assignment via the
    `power_staple_counts` + `power_staple_caps` kwargs on `_assign_engine_bracket_v1`.
    Sub-task 5a.6 demoted that gating: the classifier now reads ONLY canonical
    4-axis signals (GC count + MLD/ET/combo policy). The power-staple data is
    preserved (curated 34-entry list + per-bracket caps both still on disk;
    `power_staple_caps_v1.json` gained `informational_only: true` flag) and the
    counts are STILL computed by `_count_composition_signals`, but they ride on
    the per-deck pack output as a separate `power_staple_diagnostic` field, not
    consulted in the bracket walk.

    The 3 tests in this class verify the diagnostic computation path still
    works (`_count_composition_signals` populates per-axis counts; the curated
    pack still loads with the expected shape). The 3 tests from the prior
    `CalibrationValidatorPowerStapleDensityTests` class that asserted bracket
    assignment via per-axis caps were REMOVED in 5a.6 because their assertions
    are now wrong under canonical realignment (replaced by the new
    `CalibrationValidatorCanonicalRealignmentTests` class below).
    """

    def test_count_composition_signals_includes_power_staple_per_axis_breakdown(self) -> None:
        # Two power-staples (1 fast_mana + 1 cheap_tutor) wired through
        # `power_staple_axis_by_oid` produce per-axis counts in the result.
        oid_to_card = {
            "uuid-cmdr": {"name": "Cmdr", "primitives": set()},
            "uuid-fast-1": {"name": "Mana Crypt", "primitives": set()},
            "uuid-tutor-1": {"name": "Eladamri's Call", "primitives": set()},
            "uuid-other": {"name": "Generic Card", "primitives": set()},
        }
        power_staple_axis_by_oid = {
            "uuid-fast-1": "fast_mana",
            "uuid-tutor-1": "cheap_tutor",
        }
        signals = validator._count_composition_signals(
            commander_oracle_ids=["uuid-cmdr"],
            deck_oracle_ids=["uuid-fast-1", "uuid-tutor-1", "uuid-other"],
            oid_to_card=oid_to_card,
            combo_detector=lambda input_: {"supported": True, "count": 0, "matches": []},
            power_staple_axis_by_oid=power_staple_axis_by_oid,
        )
        self.assertEqual(signals["power_staple_counts"]["fast_mana"], 1)
        self.assertEqual(signals["power_staple_counts"]["cheap_tutor"], 1)
        self.assertEqual(signals["power_staple_counts"]["value_staple"], 0)
        self.assertEqual(signals["power_staple_counts"]["interaction_staple"], 0)
        self.assertEqual(signals["power_staples_total"], 2)
        # tracked_features_count rolls up all 5 axes (gc + mld + et + combos + power_staples)
        self.assertEqual(signals["tracked_features_count"], 0 + 0 + 0 + 0 + 2)

    def test_power_staples_v1_pack_loads_with_expected_axes_and_entry_count(self) -> None:
        # Live-pack sanity: the curated power_staples_v1 pack on disk must
        # have entries across all 4 axes, all entries must have valid
        # oracle_id+name+axis fields. Specific count is the curated 34
        # entries shipped in 5a.5 (data preserved by 5a.6 even after the
        # demotion of the per-axis caps from gating to diagnostic).
        axis_by_oid = validator._load_power_staples_v1()
        self.assertEqual(len(axis_by_oid), 34)
        self.assertEqual(set(axis_by_oid.values()), set(validator.POWER_STAPLE_AXES))


class CalibrationValidatorCanonicalRealignmentTests(unittest.TestCase):
    """5a.6 (Path C) — canonical realignment: bracket walk reads ONLY canonical
    4-axis signals (GC count + MLD/ET/combo policy); power-staple counts are
    diagnostic-only (TRACK_ONLY).

    5a.6.2 — B1/B2 tie-break flipped from "prefer lower (B1)" to "prefer higher
    (B2)" to align engine output with WotC's "B2 = Reasonably casual" catch-all
    convention for precons + lightly-upgraded decks.
    """

    def test_canonical_realignment_assigns_b2_for_zero_canonical_signal(self) -> None:
        # 0 GCs, 0 combos, 0 MLD, 0 ET → engine_assigned == "B2" (was B1
        # pre-5a.6.2). The walk hits B1 first (policy-satisfied), then tests
        # B2; both pass per BRACKET_RULES.md policy-identical structure, so
        # the tie-break flip returns B2 — WotC's "Reasonably casual" catch-all.
        self.assertEqual(
            validator._assign_engine_bracket_v1(gc=0, mld=0, et=0, combos=0),
            "B2",
        )

    def test_canonical_realignment_b1_b2_collapsed_default_b2(self) -> None:
        # B1 and B2 are policy-identical in BRACKET_RULES.md: both DISALLOW
        # MLD/ET/combos AND share gc_max=0. Pre-5a.6.2 the walk returned B1
        # (canonically lower). Post-5a.6.2 the tie-break flips to B2 because
        # B2 ("Reasonably casual") is WotC's catch-all band for precons +
        # lightly-upgraded decks; B1 ("Casual") is the narrower exhibition tier.
        self.assertEqual(
            validator._assign_engine_bracket_v1(gc=0, mld=0, et=0, combos=0),
            "B2",
        )

    def test_b1_b2_tie_break_b2_default_preserves_b4_b5_tie_break_b4_default(self) -> None:
        # 5a.6.2 — exercise both tie-break pairs in a single test:
        # (1) B1/B2 tie (zero canonical signal): walk hits B1 + B2 both
        #     satisfied → tie-break returns B2.
        # (2) B4/B5 tie (5 GCs): B4 gc_max=5 satisfied, B5 gc_min=6 NOT
        #     satisfied → only B4 matches; walk returns B4. The B4/B5 tie-
        #     break stays "prefer lower" because gc_limits already splits
        #     them; the new tie-break logic only applies to B1/B2.
        self.assertEqual(
            validator._assign_engine_bracket_v1(gc=0, mld=0, et=0, combos=0),
            "B2",
        )
        self.assertEqual(
            validator._assign_engine_bracket_v1(gc=5, mld=0, et=0, combos=0),
            "B4",
        )
        # Sanity: 6 GCs lifts B4 → B5 via gc_limits, NOT via any tie-break logic.
        self.assertEqual(
            validator._assign_engine_bracket_v1(gc=6, mld=0, et=0, combos=0),
            "B5",
        )

    def test_power_staple_diagnostic_populated_separately_from_bracket_assignment(self) -> None:
        # Deck with 8 power-staples but 0 canonical signal (0 GCs, 0 MLD, 0
        # ET, 0 combos). Pre-5a.6 this would have crossed B1's per-axis caps
        # and lifted to B2; post-5a.6 the bracket walk ignores power-staples
        # entirely → engine_assigned == "B1". The per-deck output STILL has
        # the per-axis power_staple_diagnostic populated for analytics.
        oid_to_card = {
            "uuid-cmdr": {"name": "Cmdr", "primitives": set()},
            "uuid-fast-1": {"name": "Mana Crypt", "primitives": set()},
            "uuid-fast-2": {"name": "Jeweled Lotus", "primitives": set()},
            "uuid-tutor-1": {"name": "Eladamri's Call", "primitives": set()},
            "uuid-tutor-2": {"name": "Personal Tutor", "primitives": set()},
            "uuid-value-1": {"name": "Mystic Remora", "primitives": set()},
            "uuid-value-2": {"name": "Sylvan Library", "primitives": set()},
            "uuid-int-1": {"name": "Toxic Deluge", "primitives": set()},
            "uuid-int-2": {"name": "Counterspell", "primitives": set()},
        }
        power_staple_axis_by_oid = {
            "uuid-fast-1": "fast_mana",
            "uuid-fast-2": "fast_mana",
            "uuid-tutor-1": "cheap_tutor",
            "uuid-tutor-2": "cheap_tutor",
            "uuid-value-1": "value_staple",
            "uuid-value-2": "value_staple",
            "uuid-int-1": "interaction_staple",
            "uuid-int-2": "interaction_staple",
        }
        signals = validator._count_composition_signals(
            commander_oracle_ids=["uuid-cmdr"],
            deck_oracle_ids=[
                "uuid-fast-1", "uuid-fast-2",
                "uuid-tutor-1", "uuid-tutor-2",
                "uuid-value-1", "uuid-value-2",
                "uuid-int-1", "uuid-int-2",
            ],
            oid_to_card=oid_to_card,
            combo_detector=lambda input_: {"supported": True, "count": 0, "matches": []},
            power_staple_axis_by_oid=power_staple_axis_by_oid,
        )
        # Canonical 4-axis: zero across the board → B1.
        engine = validator._assign_engine_bracket_v1(
            gc=signals["gc"], mld=signals["mld"],
            et=signals["et"], combos=signals["combos"],
        )
        # Post-5a.6.2: zero canonical signal → B1 + B2 both satisfy → tie-
        # break returns B2 (the "Reasonably casual" catch-all). Power-staples
        # still don't drive the assignment; the diagnostic counts are
        # populated separately for analytics consumers.
        self.assertEqual(engine, "B2")
        # But the diagnostic counts ARE populated — analytics still see all
        # 8 power-staples broken down per axis (2 per axis).
        self.assertEqual(signals["power_staple_counts"]["fast_mana"], 2)
        self.assertEqual(signals["power_staple_counts"]["cheap_tutor"], 2)
        self.assertEqual(signals["power_staple_counts"]["value_staple"], 2)
        self.assertEqual(signals["power_staple_counts"]["interaction_staple"], 2)
        self.assertEqual(signals["power_staples_total"], 8)


class CalibrationValidatorB5MinGcTests(unittest.TestCase):
    """5a.5.2 — B5 minimum-GC threshold + complementary B4 gc_max.

    `gc_limits_v1.json` now has explicit B4 ({min: null, max: 5}) and B5
    ({min: 6, max: null}). High-GC decks (>=6 GCs) fail B4's gc_max and
    qualify for B5; mid-GC decks (4-5) qualify for B4 only; low-GC decks
    (0-3) qualify for B1/B2/B3 normally. The `_assign_engine_bracket_v1`
    walk already honors both `gc_min` and `gc_max` via the existing checks
    -- no algorithm change needed for this sub-task; data-only update to
    the gc_limits_v1.json file.
    """

    def test_assigns_b5_for_high_gc_count_with_gc_min_satisfied(self) -> None:
        # 8 GCs: fails B1/B2 (gc_max=0), B3 (gc_max=3), B4 (gc_max=5);
        # passes B5 (gc_min=6 met). Was unreachable B5 pre-5a.5.2.
        self.assertEqual(
            validator._assign_engine_bracket_v1(gc=8, mld=0, et=0, combos=0),
            "B5",
        )

    def test_assigns_b4_when_gc_count_below_b5_floor(self) -> None:
        # 5 GCs: just below B5's gc_min=6. B4 still qualifies (gc<=5).
        # Engine assigns B4, NOT B5 (the gc_min floor blocks the upgrade).
        self.assertEqual(
            validator._assign_engine_bracket_v1(gc=5, mld=0, et=0, combos=0),
            "B4",
        )

    def test_gc_min_null_brackets_unchanged(self) -> None:
        # 0 GCs + 0 of every tracked axis: B1 STILL QUALIFIES (gc_min=null;
        # 5a.5.2's B5 gc_min=6 doesn't reach down to B1). Verifies 5a.5.2's
        # data-only change preserves B1-B3 qualification byte-identical.
        # Post-5a.6.2: the return value is B2 because the new B1/B2 tie-break
        # flip kicks in once both B1 and B2 are satisfied — but that's
        # orthogonal to gc_min; the underlying B1 qualification check is
        # unchanged from 5a.5.2.
        self.assertEqual(
            validator._assign_engine_bracket_v1(gc=0, mld=0, et=0, combos=0),
            "B2",
        )


class CalibrationValidatorDriftCategorizationTests(unittest.TestCase):
    """Pure unit tests for `_categorize_drift`."""

    def test_categorize_drift_handles_all_four_categories(self) -> None:
        # agree: self == engine.
        self.assertEqual(
            validator._categorize_drift(
                self_reported="B1", engine_assigned="B1", tracked_features_count=0
            ),
            validator.CATEGORY_AGREE,
        )
        # null-signal-collapse: disagree AND zero tracked features.
        self.assertEqual(
            validator._categorize_drift(
                self_reported="B3", engine_assigned="B1", tracked_features_count=0
            ),
            validator.CATEGORY_NULL_SIGNAL_COLLAPSE,
        )
        # user-mislabel-candidate: disagree AND >=1 tracked feature.
        self.assertEqual(
            validator._categorize_drift(
                self_reported="B1", engine_assigned="B3", tracked_features_count=1
            ),
            validator.CATEGORY_USER_MISLABEL_CANDIDATE,
        )
        # precon-no-self-report: self is None (regardless of engine output).
        self.assertEqual(
            validator._categorize_drift(
                self_reported=None, engine_assigned="B1", tracked_features_count=0
            ),
            validator.CATEGORY_PRECON_NO_SELF_REPORT,
        )


class CalibrationValidatorMainTests(unittest.TestCase):
    """Integration tests for `main()` using tmp corpus + manifest fixtures.

    All DB lookups + combo detection are mocked via `oid_resolver` and
    `combo_detector` injection points so tests run fully offline.
    """

    def _setup_tmp_corpus_and_manifest(self, tmp_path: Path):
        corpus_path = tmp_path / "external_decks_v1.json"
        manifest_path = tmp_path / "curated_pack_manifest_v1.json"
        decks = [
            _make_deck(
                deck_id="ARCHIDEKT_TEST_001",
                self_reported="B1",
                commander_oids=["uuid-cmdr-1"],
                deck_oids=["uuid-card-a", "uuid-card-b"],
            ),
            _make_deck(
                deck_id="ARCHIDEKT_TEST_002",
                self_reported="B3",
                commander_oids=["uuid-cmdr-2"],
                deck_oids=["uuid-gc-1", "uuid-card-c"],
            ),
        ]
        corpus = _build_corpus_pack(decks)
        text = json.dumps(corpus, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
        corpus_path.write_text(text, encoding="utf-8")
        sha_before = validator._sha256_text(text)
        _write_test_manifest(manifest_path, external_decks_sha=sha_before)
        return corpus_path, manifest_path, sha_before

    def test_dry_run_guard_skips_both_pack_and_manifest_writes(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            corpus_path, manifest_path, sha_before = self._setup_tmp_corpus_and_manifest(
                tmp_path
            )
            initial_corpus_text = corpus_path.read_text(encoding="utf-8")
            initial_manifest_text = manifest_path.read_text(encoding="utf-8")
            initial_corpus_mtime = corpus_path.stat().st_mtime_ns
            initial_manifest_mtime = manifest_path.stat().st_mtime_ns

            with (
                patch.object(
                    validator,
                    "_load_oracle_id_to_card_map",
                    side_effect=_fake_resolver({}),
                ),
                patch.object(
                    validator,
                    "detect_two_card_combos",
                    side_effect=_fake_combo_detector(matches=0),
                ),
            ):
                exit_code = validator.main(
                    [
                        "--corpus-path",
                        str(corpus_path),
                        "--manifest-path",
                        str(manifest_path),
                        "--dry-run",
                    ]
                )
            self.assertEqual(exit_code, validator.EXIT_SUCCESS)
            # Pack file content + mtime byte-identical pre/post.
            self.assertEqual(corpus_path.read_text(encoding="utf-8"), initial_corpus_text)
            self.assertEqual(corpus_path.stat().st_mtime_ns, initial_corpus_mtime)
            # Manifest file content + mtime byte-identical pre/post.
            self.assertEqual(manifest_path.read_text(encoding="utf-8"), initial_manifest_text)
            self.assertEqual(manifest_path.stat().st_mtime_ns, initial_manifest_mtime)

    def test_manifest_auto_refresh_fires_on_pack_change(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            corpus_path, manifest_path, sha_before = self._setup_tmp_corpus_and_manifest(
                tmp_path
            )
            # Force a stale manifest pin so we can verify the auto-refresh
            # actually rewrites the manifest's external_decks_v1 sha256 field.
            stale_sha = "8" * 64
            _write_test_manifest(manifest_path, external_decks_sha=stale_sha)

            with (
                patch.object(
                    validator,
                    "_load_oracle_id_to_card_map",
                    side_effect=_fake_resolver({}),
                ),
                patch.object(
                    validator,
                    "detect_two_card_combos",
                    side_effect=_fake_combo_detector(matches=0),
                ),
            ):
                exit_code = validator.main(
                    [
                        "--corpus-path",
                        str(corpus_path),
                        "--manifest-path",
                        str(manifest_path),
                    ]
                )
            self.assertEqual(exit_code, validator.EXIT_SUCCESS)
            new_pack_sha = validator._sha256_text(corpus_path.read_text(encoding="utf-8"))
            updated_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            external_entry = next(
                e for e in updated_manifest["packs"] if e["pack_id"] == "external_decks_v1"
            )
            self.assertEqual(external_entry["sha256"], new_pack_sha)
            # Sentinel entry preserved byte-identical (sha256 + load_order + path).
            sentinel_entry = next(
                e for e in updated_manifest["packs"] if e["pack_id"] == "sentinel_other_pack"
            )
            self.assertEqual(sentinel_entry["sha256"], "0" * 64)
            self.assertEqual(sentinel_entry["load_order"], 10)
            # Other external_decks_v1 fields preserved byte-identical.
            self.assertEqual(external_entry["calibration_only"], True)
            self.assertEqual(external_entry["load_order"], 900)
            self.assertEqual(
                external_entry["path"], "api/engine/data/calibration/external_decks_v1.json"
            )

    def test_full_happy_path_writes_engine_assigned_per_deck_preserving_other_fields(
        self,
    ) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            corpus_path, manifest_path, sha_before = self._setup_tmp_corpus_and_manifest(
                tmp_path
            )
            # Resolver returns name == "Ad Nauseam" for one card so deck #2's
            # gc count is 1 (in GAME_CHANGERS_SET → satisfies B3's gc_min).
            card_data = {
                "uuid-cmdr-1": {"name": "Cmdr One", "primitives": set()},
                "uuid-card-a": {"name": "Card A", "primitives": set()},
                "uuid-card-b": {"name": "Card B", "primitives": set()},
                "uuid-cmdr-2": {"name": "Cmdr Two", "primitives": set()},
                "uuid-gc-1": {"name": "Ad Nauseam", "primitives": set()},
                "uuid-card-c": {"name": "Card C", "primitives": set()},
            }
            with (
                patch.object(
                    validator,
                    "_load_oracle_id_to_card_map",
                    side_effect=_fake_resolver(card_data),
                ),
                patch.object(
                    validator,
                    "detect_two_card_combos",
                    side_effect=_fake_combo_detector(matches=0),
                ),
            ):
                exit_code = validator.main(
                    [
                        "--corpus-path",
                        str(corpus_path),
                        "--manifest-path",
                        str(manifest_path),
                    ]
                )
            self.assertEqual(exit_code, validator.EXIT_SUCCESS)

            updated = json.loads(corpus_path.read_text(encoding="utf-8"))
            decks = updated["decks"]
            self.assertEqual(len(decks), 2)

            # Deck 1: 0 GC + 0 of everything → engine_assigned=B2 post-5a.6.2
            # (B1/B2 tie-break flip); self=B1 → user-mislabel-candidate (-ish;
            # this fixture is synthetic and doesn't represent a real-world
            # mislabel). This assertion was B1 pre-5a.6.2 — updated for the
            # tie-break flip.
            d1 = decks[0]
            self.assertEqual(d1["deck_id"], "ARCHIDEKT_TEST_001")
            self.assertEqual(d1["engine_assigned_bracket"], "B2")
            # Deck 2: 1 GC ("Ad Nauseam") + 0 others → engine_assigned=B3.
            d2 = decks[1]
            self.assertEqual(d2["deck_id"], "ARCHIDEKT_TEST_002")
            self.assertEqual(d2["engine_assigned_bracket"], "B3")

            # Other fields preserved byte-identical for both decks.
            for new_d, original_id in (
                (d1, "ARCHIDEKT_TEST_001"),
                (d2, "ARCHIDEKT_TEST_002"),
            ):
                self.assertEqual(new_d["self_reported_bracket"] in {"B1", "B3"}, True)
                self.assertEqual(new_d["source"], "archidekt")
                self.assertEqual(new_d["fetched_at_utc"], "2026-05-08T00:00:00Z")
                self.assertEqual(new_d["dedup_hash"], f"hash-{original_id}")
                self.assertEqual(
                    new_d["source_url"],
                    f"https://archidekt.com/decks/{original_id}/",
                )


if __name__ == "__main__":
    unittest.main()
