"""First calibration validator (Phase 5a.4 — Path C).

Iterates the live external deck corpus, computes ``engine_assigned_bracket``
per deck via static composition analysis (game-changers / MLD / extra-turn-
chains / two-card-combos — closed-form, no simulation), populates the field,
atomic-writes the rewritten pack, refreshes the manifest in lockstep via
sub-task 5a.2.2's helper, and emits a drift summary.

Per the calibration boundary architecture in
``05_VALIDATION/CALIBRATION_BOUNDARY.md``, the validator runs OFFLINE; the
runtime engine never reads ``engine_assigned_bracket``.

Discovery from sub-task 5a.4's halt run (now resolved by Path C re-spec):
the engine's current 4-axis rule data treats B1/B2 as policy-identical
(both ``gc=0..0``, all ``DISALLOW``) and B4/B5 as policy-identical (both
``gc=null..null``, all ``ALLOW``). Under the walk-B1-to-B5 algorithm, the
classifier will never return B2 or B5 for any composition. Path C accepts
this gap honestly; Path A (sub-task 5a.5, queued) addresses it with
additional primitive signals (tutor density, fast-mana density, infinite-
mana enablers, turn-N-win estimates) ranked by the drift report's
"would re-classify if axis added" recommendation section.

Single source of truth: this validator reuses the production utility
helpers ``resolve_gc_limits`` (from ``api/engine/bracket_gc_limits.py``)
and ``resolve_bracket_rules_v2`` (from ``api/engine/bracket_rules_v2.py``)
plus the pack-loader/SHA/atomic-write helpers from
``tools/update_external_deck_corpus.py``. No rule duplication; no new DB
access patterns invented.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

from api.engine.bracket_gc_limits import resolve_gc_limits
from api.engine.bracket_rules_v2 import resolve_bracket_rules_v2
from api.engine.combos.two_card_combos_v2 import detect_two_card_combos
from api.engine.constants import GAME_CHANGERS_SET
from api.engine.db_cards import lookup_cards_by_oracle_ids

from tools.update_external_deck_corpus import (
    CURATED_PACK_MANIFEST_V1_DEFAULT_PATH,
    EXTERNAL_DECKS_PACK_ID,
    _refresh_manifest_sha256_v1,
    _sha256_text,
    _stable_json_dumps,
    _write_atomic,
)


CALIBRATION_VALIDATOR_V1_VERSION = "calibration_validator_v1"
ENGINE_VERSION_PIN_DEFAULT = "post-2D"
DEFAULT_SNAPSHOT_ID = "20260217_190902_tagpass_20260222"
EXTERNAL_DECKS_PACK_DEFAULT = (
    Path(__file__).resolve().parents[1]
    / "api"
    / "engine"
    / "data"
    / "calibration"
    / "external_decks_v1.json"
)
MLD_PRIMITIVE_TAG = "mass_land_denial"
EXTRA_TURN_PRIMITIVE_TAG = "extra_turn"

POWER_STAPLES_PACK_PATH = (
    Path(__file__).resolve().parents[1]
    / "api"
    / "engine"
    / "data"
    / "primitives"
    / "power_staples_v1.json"
)
POWER_STAPLE_CAPS_PACK_PATH = (
    Path(__file__).resolve().parents[1]
    / "api"
    / "engine"
    / "data"
    / "brackets"
    / "power_staple_caps_v1.json"
)
POWER_STAPLE_AXES = ("cheap_tutor", "fast_mana", "interaction_staple", "value_staple")

EXIT_SUCCESS = 0
EXIT_PARTIAL_FAILURE = 1
EXIT_TOTAL_FAILURE = 2

CATEGORY_AGREE = "agree"
CATEGORY_NULL_SIGNAL_COLLAPSE = "null-signal-collapse"
CATEGORY_USER_MISLABEL_CANDIDATE = "user-mislabel-candidate"
CATEGORY_PRECON_NO_SELF_REPORT = "precon-no-self-report"


def _load_power_staples_v1() -> Dict[str, str]:
    """Return ``{oracle_id: axis}`` mapping from the curated power-staples pack.

    Sub-task 5a.5 added this curated list of ~34 entries across 4 axes
    (cheap_tutor / fast_mana / value_staple / interaction_staple) — non-
    GAME_CHANGERS staples that mark a deck above precon-tier. Reused by the
    classifier to count per-axis matches and apply per-bracket caps from
    ``power_staple_caps_v1.json``.
    """
    raw = json.loads(POWER_STAPLES_PACK_PATH.read_text(encoding="utf-8"))
    out: Dict[str, str] = {}
    for entry in raw.get("entries", []) or []:
        if not isinstance(entry, dict):
            continue
        oracle_id = entry.get("oracle_id")
        axis = entry.get("axis")
        if isinstance(oracle_id, str) and isinstance(axis, str):
            out[oracle_id.lower()] = axis
    return out


def _load_power_staple_caps_v1() -> Dict[str, Dict[str, int | None]]:
    """Return ``{bracket_id: {axis: cap_or_None}}`` per-bracket per-axis caps."""
    raw = json.loads(POWER_STAPLE_CAPS_PACK_PATH.read_text(encoding="utf-8"))
    return raw.get("caps_per_bracket") or {}


def _bracket_satisfies_v1(
    bracket_id: str,
    *,
    gc: int,
    mld: int,
    et: int,
    combos: int,
) -> bool:
    """True iff the deck's signal triple passes ``bracket_id``'s gc + policy gates."""
    gc_min, gc_max, _, _ = resolve_gc_limits(bracket_id)
    bracket_rules, _, _ = resolve_bracket_rules_v2(bracket_id)
    if gc_min is not None and gc < gc_min:
        return False
    if gc_max is not None and gc > gc_max:
        return False
    for category, count in (
        ("mass_land_denial", mld),
        ("extra_turn_chains", et),
        ("two_card_combos", combos),
    ):
        policy = bracket_rules.get(category)
        if policy == "DISALLOW" and count > 0:
            return False
    return True


def _assign_engine_bracket_v1(
    *,
    gc: int,
    mld: int,
    et: int,
    combos: int,
) -> str:
    """Walk B1->B5; return the bracket the deck satisfies, with tie-break.

    Canonical realignment (sub-task 5a.6, Path C): reads ONLY canonical 4-axis
    signals — GC count via ``resolve_gc_limits`` + MLD/ET/combo policy via
    ``resolve_bracket_rules_v2``. Power-staple counts are NOT consulted in the
    walk (5a.5's per-axis caps demoted to TRACK_ONLY analytics-only diagnostic;
    counts STILL get computed by ``_count_composition_signals`` and surfaced
    via the ``power_staple_diagnostic`` field on per-deck pack output, but do
    not influence engine_assigned_bracket).

    Engineering thresholds preserved per Path C: ``gc_limits_v1.json`` has
    explicit B4 ``{min:null, max:5}`` + B5 ``{min:6, max:null}`` from sub-task
    5a.5.2. These are non-canonical engineering choices that capture WotC's
    cEDH-density distinction (without them, B5 is structurally unreachable
    because B4/B5 are policy-identical in BRACKET_RULES.md). They remain
    ACTIVE in the walk via ``resolve_gc_limits`` and are documented in
    gc_limits_v1.json's ``non_canonical_engineering`` annotation block.

    **B1/B2 tie-break flip (sub-task 5a.6.2):** B1 and B2 are policy-identical
    in BRACKET_RULES.md (both DISALLOW MLD/ET/combos, both gc=0..0). When the
    walk's first match is B1, the function ALSO tests B2's policy; if BOTH
    pass (the structurally-expected case), it returns B2 — WotC's framework
    names B2 "Reasonably casual" as the catch-all band for precons +
    lightly-upgraded decks; B1 ("Casual") is the narrower exhibition/theme
    tier. Returning B2 on tie aligns the engine output with WotC's intent.

    B4/B5 tie-break stays "prefer lower (B4)" via the natural walk: 5a.5.2's
    gc_min=6 floor on B5 + gc_max=5 ceiling on B4 already split them at the
    GC level; truly policy-identical-and-gc-equivalent decks (which the
    engineering thresholds prevent) would not be ambiguous in practice.

    See ``01_RULES_SOURCE/BRACKET_RULES.md`` "Tie-break rule (engine
    convention)" annotation for the canonical source-of-truth statement.
    """
    for bracket_id in ("B1", "B2", "B3", "B4", "B5"):
        if _bracket_satisfies_v1(bracket_id, gc=gc, mld=mld, et=et, combos=combos):
            if bracket_id == "B1" and _bracket_satisfies_v1(
                "B2", gc=gc, mld=mld, et=et, combos=combos
            ):
                return "B2"
            return bracket_id
    return "B5"


def _categorize_drift(
    *,
    self_reported: str | None,
    engine_assigned: str,
    tracked_features_count: int,
) -> str:
    """Categorize a deck's self vs engine bracket disagreement (or agreement).

    - ``precon-no-self-report``: corpus deck without a user-supplied bracket
      (e.g. WotC precons; ``self_reported`` is None).
    - ``agree``: ``self_reported == engine_assigned``.
    - ``null-signal-collapse``: disagree, but the engine sees ZERO of its 4
      tracked axes (GC/MLD/ET/combos). The engine's signal set is too narrow;
      Path A (5a.5) adds the missing axes.
    - ``user-mislabel-candidate``: disagree, and the engine sees AT LEAST ONE
      tracked feature. The engine's classification has a basis; the divergence
      is either a user mislabel or an engine over-read worth user review.
    """
    if self_reported is None:
        return CATEGORY_PRECON_NO_SELF_REPORT
    if self_reported == engine_assigned:
        return CATEGORY_AGREE
    if tracked_features_count == 0:
        return CATEGORY_NULL_SIGNAL_COLLAPSE
    return CATEGORY_USER_MISLABEL_CANDIDATE


def _load_oracle_id_to_card_map(
    *,
    snapshot_id: str,
    oracle_ids: set,
) -> Dict[str, Dict[str, Any]]:
    """Resolve a set of oracle_ids to ``{name, primitives}`` via mtg.sqlite.

    Reuses the existing offline-friendly helper ``lookup_cards_by_oracle_ids``
    (from ``api/engine/db_cards.py``); no new DB access pattern invented.
    Caller passes a single batch; helper opens its own connection.
    """
    rows = lookup_cards_by_oracle_ids(
        None,
        snapshot_id,
        set(oracle_ids),
        requested_fields=["oracle_id", "name", "primitives_json"],
    )
    out: Dict[str, Dict[str, Any]] = {}
    for oid, card in rows.items():
        primitives_json_raw = card.get("primitives_json") or "[]"
        try:
            primitives = (
                set(json.loads(primitives_json_raw))
                if isinstance(primitives_json_raw, str)
                else set()
            )
        except Exception:
            primitives = set()
        out[oid.lower()] = {
            "name": card.get("name") or "",
            "primitives": primitives,
        }
    return out


def _count_composition_signals(
    *,
    commander_oracle_ids: List[str],
    deck_oracle_ids: List[str],
    oid_to_card: Dict[str, Dict[str, Any]],
    combo_detector: Callable[[List[Any]], Dict[str, Any]] | None = None,
    power_staple_axis_by_oid: Dict[str, str] | None = None,
) -> Dict[str, Any]:
    detector = combo_detector if combo_detector is not None else detect_two_card_combos
    all_oids = list(commander_oracle_ids) + list(deck_oracle_ids)
    gc_count = sum(
        1 for oid in all_oids if oid_to_card.get(oid, {}).get("name") in GAME_CHANGERS_SET
    )
    mld_count = sum(
        1
        for oid in all_oids
        if MLD_PRIMITIVE_TAG in oid_to_card.get(oid, {}).get("primitives", set())
    )
    et_count = sum(
        1
        for oid in all_oids
        if EXTRA_TURN_PRIMITIVE_TAG in oid_to_card.get(oid, {}).get("primitives", set())
    )
    combo_input = [
        {"oracle_id": oid, "name": oid_to_card.get(oid, {}).get("name", "")}
        for oid in all_oids
    ]
    try:
        combo_result = detector(combo_input)
        combo_count = (
            combo_result.get("count", 0)
            if isinstance(combo_result, dict) and combo_result.get("supported")
            else 0
        )
        if not isinstance(combo_count, int) or isinstance(combo_count, bool):
            combo_count = 0
    except Exception:
        combo_count = 0

    power_staple_counts: Dict[str, int] = {axis: 0 for axis in POWER_STAPLE_AXES}
    if power_staple_axis_by_oid is not None:
        for oid in all_oids:
            axis = power_staple_axis_by_oid.get(oid)
            if axis is not None and axis in power_staple_counts:
                power_staple_counts[axis] += 1
    power_staples_total = sum(power_staple_counts.values())

    return {
        "gc": gc_count,
        "mld": mld_count,
        "et": et_count,
        "combos": combo_count,
        "power_staple_counts": power_staple_counts,
        "power_staples_total": power_staples_total,
        "tracked_features_count": gc_count + mld_count + et_count + combo_count + power_staples_total,
    }


def _classify_corpus(
    corpus: Dict[str, Any],
    *,
    snapshot_id: str,
    oid_resolver: Callable[..., Dict[str, Dict[str, Any]]] | None = None,
    combo_detector: Callable[[List[Any]], Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    decks = corpus.get("decks") if isinstance(corpus, dict) else None
    if not isinstance(decks, list):
        return []
    all_oids: set = set()
    for deck in decks:
        if not isinstance(deck, dict):
            continue
        all_oids.update(deck.get("commander_oracle_ids", []) or [])
        all_oids.update(deck.get("deck_oracle_ids", []) or [])
    resolver = oid_resolver if oid_resolver is not None else _load_oracle_id_to_card_map
    oid_to_card = resolver(snapshot_id=snapshot_id, oracle_ids=all_oids)

    # 5a.6 Path C: load power-staple curated list once per corpus for the
    # diagnostic field. The per-bracket caps from power_staple_caps_v1.json
    # are NO LONGER loaded — the bracket walk reads only canonical 4-axis
    # signals. Power-staple counts get computed for every deck and surfaced
    # via the power_staple_diagnostic field on the per-deck pack output.
    power_staple_axis_by_oid = _load_power_staples_v1()

    results: List[Dict[str, Any]] = []
    for deck in decks:
        if not isinstance(deck, dict):
            continue
        signals = _count_composition_signals(
            commander_oracle_ids=deck.get("commander_oracle_ids", []) or [],
            deck_oracle_ids=deck.get("deck_oracle_ids", []) or [],
            oid_to_card=oid_to_card,
            combo_detector=combo_detector,
            power_staple_axis_by_oid=power_staple_axis_by_oid,
        )
        engine = _assign_engine_bracket_v1(
            gc=signals["gc"],
            mld=signals["mld"],
            et=signals["et"],
            combos=signals["combos"],
        )
        category = _categorize_drift(
            self_reported=deck.get("self_reported_bracket"),
            engine_assigned=engine,
            tracked_features_count=signals["tracked_features_count"],
        )
        results.append(
            {
                "deck_id": deck.get("deck_id"),
                "self_reported": deck.get("self_reported_bracket"),
                "engine_assigned": engine,
                "category": category,
                "tracked_features_count": signals["tracked_features_count"],
                "gc": signals["gc"],
                "mld": signals["mld"],
                "et": signals["et"],
                "combos": signals["combos"],
                "power_staple_diagnostic": signals["power_staple_counts"],
                "name": (deck.get("source_metadata") or {}).get("name"),
                "commander_oracle_ids": list(deck.get("commander_oracle_ids", []) or []),
            }
        )
    return results


def _build_updated_pack(
    corpus: Dict[str, Any],
    results_by_deck_id: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Return a new pack dict with engine_assigned_bracket + power_staple_diagnostic
    populated per deck.

    Other fields preserved byte-identical. Order of decks preserved.
    Per Path C / sub-task 5a.6: power_staple_diagnostic is the analytics-only
    per-axis count of curated power-staples (cheap_tutor / fast_mana /
    value_staple / interaction_staple). NOT consulted by the bracket walk;
    surfaced for consumer-side diagnostic richness.
    """
    if not isinstance(corpus, dict):
        return corpus
    new_corpus = dict(corpus)
    new_decks: List[Any] = []
    for deck in corpus.get("decks", []) or []:
        if not isinstance(deck, dict):
            new_decks.append(deck)
            continue
        deck_id = deck.get("deck_id")
        result = results_by_deck_id.get(deck_id) or {}
        new_deck = dict(deck)
        new_deck["engine_assigned_bracket"] = result.get("engine_assigned")
        new_deck["power_staple_diagnostic"] = result.get("power_staple_diagnostic")
        new_decks.append(new_deck)
    new_corpus["decks"] = new_decks
    return new_corpus


def _build_summary(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_category = Counter(row["category"] for row in results)
    self_dist = Counter(row["self_reported"] for row in results)
    engine_dist = Counter(row["engine_assigned"] for row in results)
    return {
        "total_decks": len(results),
        "categories": dict(by_category),
        "self_reported_distribution": {str(k): v for k, v in self_dist.items()},
        "engine_assigned_distribution": dict(engine_dist),
    }


def _parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Offline calibration validator: classify external_decks_v1 corpus "
            "by static composition analysis and populate engine_assigned_bracket."
        )
    )
    parser.add_argument("--corpus-path", default=str(EXTERNAL_DECKS_PACK_DEFAULT))
    parser.add_argument(
        "--manifest-path", default=str(CURATED_PACK_MANIFEST_V1_DEFAULT_PATH)
    )
    parser.add_argument("--snapshot-id", default=DEFAULT_SNAPSHOT_ID)
    parser.add_argument("--engine-version-pin", default=ENGINE_VERSION_PIN_DEFAULT)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-update-manifest", action="store_true")
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = _parse_args(argv)
    try:
        corpus_path = Path(args.corpus_path)
        if not corpus_path.is_file():
            print(f"ERROR: corpus path not found: {corpus_path}", file=sys.stderr)
            return EXIT_TOTAL_FAILURE
        previous_text = corpus_path.read_text(encoding="utf-8")
        corpus = json.loads(previous_text)

        results = _classify_corpus(corpus, snapshot_id=args.snapshot_id)
        if len(results) == 0:
            print(
                f"WARNING: no decks classified in {corpus_path}", file=sys.stderr
            )
            return EXIT_PARTIAL_FAILURE

        results_by_deck_id = {
            row["deck_id"]: {
                "engine_assigned": row["engine_assigned"],
                "power_staple_diagnostic": row["power_staple_diagnostic"],
            }
            for row in results
        }
        updated_corpus = _build_updated_pack(corpus, results_by_deck_id)
        text = _stable_json_dumps(updated_corpus) + "\n"
        sha_before = _sha256_text(previous_text)
        sha_after = _sha256_text(text)
        changed = sha_before != sha_after

        manifest_old_sha: str | None = None
        manifest_changed = False
        manifest_skipped_reason: str | None = None

        if args.dry_run:
            manifest_skipped_reason = "dry_run"
        elif not changed:
            manifest_skipped_reason = "byte_identical_pack"
        elif args.no_update_manifest:
            manifest_skipped_reason = "no_update_manifest_flag"

        if not args.dry_run and changed:
            _write_atomic(corpus_path, text)
            if manifest_skipped_reason is None:
                manifest_old_sha, manifest_changed = _refresh_manifest_sha256_v1(
                    manifest_path=Path(args.manifest_path),
                    pack_id=EXTERNAL_DECKS_PACK_ID,
                    new_sha256=sha_after,
                )

        summary = _build_summary(results)
        print(f"=== CALIBRATION VALIDATOR DRIFT SUMMARY ({CALIBRATION_VALIDATOR_V1_VERSION}) ===")
        print(f"deck_count={summary['total_decks']}")
        print(f"categories={summary['categories']}")
        print(f"self_reported_dist={summary['self_reported_distribution']}")
        print(f"engine_assigned_dist={summary['engine_assigned_distribution']}")
        print(
            f"old_sha256={sha_before} new_sha256={sha_after} changed={changed} "
            f"dry_run={args.dry_run}"
        )
        print(
            f"manifest_old_sha={manifest_old_sha if manifest_old_sha is not None else 'none'} "
            f"manifest_new_sha={sha_after} "
            f"manifest_changed={manifest_changed} "
            f"manifest_skipped_reason={manifest_skipped_reason if manifest_skipped_reason is not None else 'none'}"
        )
        for row in results:
            psd = row.get("power_staple_diagnostic") or {}
            ps_str = " ".join(
                f"{k[0]}{psd.get(k, 0)}"
                for k in ("cheap_tutor", "fast_mana", "interaction_staple", "value_staple")
                if psd.get(k)
            )
            print(
                f"  {row['deck_id']:30} self={row['self_reported'] or 'None':5} "
                f"engine={row['engine_assigned']:3} cat={row['category']:30} "
                f"gc={row['gc']:2} mld={row['mld']:2} et={row['et']:2} combos={row['combos']:2} "
                f"diag[{ps_str}]"
            )
        return EXIT_SUCCESS
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return EXIT_TOTAL_FAILURE


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
