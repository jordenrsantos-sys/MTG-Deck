"""Seed synergy detection v1 — Phase 1 layer.

Sub-task 2C.2 landed the theme-match algorithm body: ``run_seed_synergy_detection_v1``
now evaluates every theme's ``score_formula`` against the seed's primitive counts via
the ``score_formula_v1`` evaluator and bins matched themes via ``_bin_confidence``
against the ``themes_confidence_bands_v1`` pack. ``matched_themes`` is populated;
``matched_signatures`` / ``missing_roles`` / ``bracket_compatible_flag`` remain at
sub-task-1 defaults pending the future sub-tasks (signature match via
``engine_requirement_detection_v1``; missing-role calc against bracket-aware
``structural_quotas_v1_10``; bracket filter against ``bracket_rules_v2`` +
``gc_limits_v1``). Pack-loading helpers (themes / bands / signal_vocabulary /
primitives / primitive_index_map / adjacent_packs) and union-allowlist builder
landed in sub-task 2C.1; all delegate to the shared
``api.engine.curated_pack_manifest_v1`` infrastructure. See
``04_BUILD_PIPELINE/LAYERS/SEED_SYNERGY_DETECTION_V1.md`` in the vault for the
full design, dependency list, algorithm sketch, and sub-task plan.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Set

from api.engine.curated_pack_manifest_v1 import (
    resolve_pack_entry,
    resolve_pack_file_path,
    sha256_file,
)
from api.engine.scoring.score_formula_v1 import (
    ScoreFormulaError,
    evaluate as _evaluate_score_formula,
)


SEED_SYNERGY_DETECTION_V1_VERSION = "seed_synergy_detection_v1"

CANONICAL_MISSING_ROLE_KEYS: tuple[str, ...] = (
    "draw",
    "interaction",
    "protection",
    "ramp",
    "recursion",
)


# ---------------------------------------------------------------------------
# Pack-loading infrastructure (sub-task 2C.1)
# ---------------------------------------------------------------------------


# Manifest pack_ids the layer loads. Discovered at task start from the
# real manifest (do not guess; the spec's earlier names were wrong on two).
_PACK_ID_THEMES = "taxonomy_themes"
_PACK_ID_BANDS = "themes_confidence_bands_v1"
_PACK_ID_SIGNAL_VOCABULARY = "themes_signal_vocabulary_v1"
_PACK_ID_PRIMITIVES = "taxonomy_primitives"
_PACK_ID_PRIMITIVE_INDEX_MAP = "taxonomy_primitive_mappings"
_ADJACENT_PACK_IDS: tuple[str, ...] = (
    "taxonomy_qualifier_facets",
    "taxonomy_structural_roles",
    "taxonomy_equivalence_classes",
    "taxonomy_temporal_motifs",
    "taxonomy_commander_contracts",
    "taxonomy_need_buckets",
)

# Synthesized variables in the score_formula DSL — bound by the consuming
# layer at runtime, not pack identifiers.
_SYNTHESIZED_VARS: frozenset[str] = frozenset({"score", "token_kind"})

# Reserved keywords in the score_formula DSL family (sub-task 2A.1's
# evaluator handles only AND/OR-free arithmetic, but other field DSLs in the
# same pack family use these — exclude them from identifier extraction).
_DSL_KEYWORDS: frozenset[str] = frozenset(
    {"AND", "OR", "NOT", "TRUE", "FALSE", "NULL", "IF", "THEN", "ELSE", "WHEN"}
)

_IDENT_RX = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class SeedSynergyDetectionPackShaMismatch(RuntimeError):
    """Pack file's on-disk SHA-256 does not match the manifest pin.

    Closed-world rule defense in depth: every pack the layer reads is
    SHA-verified before parsing. Mismatch means the pack drifted since the
    manifest was last updated; halt rather than load possibly-stale data.
    """

    def __init__(
        self,
        *,
        pack_id: str,
        expected: str,
        actual: str,
        path: str,
    ) -> None:
        self.pack_id = pack_id
        self.expected_sha256 = expected
        self.actual_sha256 = actual
        self.path = path
        super().__init__(
            f"pack {pack_id!r} sha256 mismatch: expected={expected} "
            f"actual={actual} path={path}"
        )


def _load_pack_with_sha_check(
    pack_id: str,
    *,
    manifest_path: Optional[Path] = None,
) -> Any:
    """Resolve pack via manifest, verify SHA, parse JSON, return.

    Delegates manifest resolution to the shared
    ``api.engine.curated_pack_manifest_v1`` module. The shared helpers raise
    ``RuntimeError`` with stable error codes on missing pack / missing file /
    invalid manifest; this loader adds SHA verification on top and raises
    :class:`SeedSynergyDetectionPackShaMismatch` on mismatch.
    """

    entry = resolve_pack_entry(pack_id=pack_id, manifest_path=manifest_path)
    abs_path = resolve_pack_file_path(pack_id=pack_id, manifest_path=manifest_path)
    expected_sha = str(entry["sha256"])
    actual_sha = sha256_file(abs_path)
    if actual_sha != expected_sha:
        raise SeedSynergyDetectionPackShaMismatch(
            pack_id=pack_id,
            expected=expected_sha,
            actual=actual_sha,
            path=str(abs_path),
        )
    return json.loads(abs_path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Specialized loaders — each wraps _load_pack_with_sha_check and returns the
# pack in the shape the future algorithm wants (O(1) lookup keyed by id where
# applicable).
# ---------------------------------------------------------------------------


def _load_themes(*, manifest_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Return themes_v1_5's 41 entries as a list (preserves source order)."""
    data = _load_pack_with_sha_check(_PACK_ID_THEMES, manifest_path=manifest_path)
    if not isinstance(data, list):
        raise RuntimeError(
            f"{_PACK_ID_THEMES} expected list-of-dicts, got {type(data).__name__}"
        )
    return data


def _load_bands(*, manifest_path: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """Return themes_confidence_bands_v1 keyed by ``theme_id`` for O(1) lookup."""
    data = _load_pack_with_sha_check(_PACK_ID_BANDS, manifest_path=manifest_path)
    if not isinstance(data, list):
        raise RuntimeError(
            f"{_PACK_ID_BANDS} expected list-of-dicts, got {type(data).__name__}"
        )
    return {entry["theme_id"]: entry for entry in data}


def _load_signal_vocabulary(
    *, manifest_path: Optional[Path] = None
) -> Dict[str, Dict[str, Any]]:
    """Return themes_signal_vocabulary_v1 keyed by ``id`` for O(1) lookup."""
    data = _load_pack_with_sha_check(
        _PACK_ID_SIGNAL_VOCABULARY, manifest_path=manifest_path
    )
    if not isinstance(data, list):
        raise RuntimeError(
            f"{_PACK_ID_SIGNAL_VOCABULARY} expected list-of-dicts, got "
            f"{type(data).__name__}"
        )
    return {entry["id"]: entry for entry in data}


def _load_primitives(
    *, manifest_path: Optional[Path] = None
) -> Dict[str, Dict[str, Any]]:
    """Return primitives.json keyed by ``primitive_id`` for O(1) lookup."""
    data = _load_pack_with_sha_check(_PACK_ID_PRIMITIVES, manifest_path=manifest_path)
    if not isinstance(data, list):
        raise RuntimeError(
            f"{_PACK_ID_PRIMITIVES} expected list-of-dicts, got {type(data).__name__}"
        )
    return {entry["primitive_id"]: entry for entry in data}


def _load_primitive_index_map(*, manifest_path: Optional[Path] = None) -> Any:
    """Return primitive_index_map_v1_14 as-is (shape varies; consumer normalizes)."""
    return _load_pack_with_sha_check(
        _PACK_ID_PRIMITIVE_INDEX_MAP, manifest_path=manifest_path
    )


def _extract_uppercase_idents(text: str) -> Set[str]:
    """Extract identifier-shaped tokens from a JSON-encoded pack text.

    Matches sub-task 2A.3e's regex strategy: any sequence matching
    ``[A-Za-z_][A-Za-z0-9_]*``, minus DSL keywords. Used by adjacent-pack
    inventory + union allowlist construction.
    """
    return set(_IDENT_RX.findall(text)) - _DSL_KEYWORDS


def _load_adjacent_packs(
    *, manifest_path: Optional[Path] = None
) -> Dict[str, Set[str]]:
    """Return ``{pack_id: set_of_identifiers}`` for the 6 adjacent canonical packs.

    Each pack's full JSON-encoded text is regex-walked for identifier-shaped
    tokens (matches sub-task 2A.3e). Dedupes within each pack; preserves the
    per-pack split so downstream debugging can attribute identifiers to source.
    """

    result: Dict[str, Set[str]] = {}
    for pack_id in _ADJACENT_PACK_IDS:
        abs_path = resolve_pack_file_path(pack_id=pack_id, manifest_path=manifest_path)
        entry = resolve_pack_entry(pack_id=pack_id, manifest_path=manifest_path)
        actual_sha = sha256_file(abs_path)
        expected_sha = str(entry["sha256"])
        if actual_sha != expected_sha:
            raise SeedSynergyDetectionPackShaMismatch(
                pack_id=pack_id,
                expected=expected_sha,
                actual=actual_sha,
                path=str(abs_path),
            )
        text = abs_path.read_text(encoding="utf-8")
        result[pack_id] = _extract_uppercase_idents(text)
    return result


def _build_union_allowlist(
    *, manifest_path: Optional[Path] = None
) -> Set[str]:
    """Construct the closed-world identifier allowlist for the score-formula evaluator.

    Union of: ``primitives.json`` keys, ``themes_signal_vocabulary_v1`` keys,
    flatten of all adjacent-pack identifier sets, and the synthesized
    ``score`` / ``token_kind`` sentinels. Cross-references sub-task 2A.3e's
    1026-identifier projection.
    """

    primitives = _load_primitives(manifest_path=manifest_path)
    signal_vocabulary = _load_signal_vocabulary(manifest_path=manifest_path)
    adjacent = _load_adjacent_packs(manifest_path=manifest_path)

    allowlist: Set[str] = set()
    allowlist.update(primitives.keys())
    allowlist.update(signal_vocabulary.keys())
    for pack_idents in adjacent.values():
        allowlist.update(pack_idents)
    allowlist.update(_SYNTHESIZED_VARS)
    return allowlist


def _empty_missing_roles() -> Dict[str, int]:
    return {role: 0 for role in CANONICAL_MISSING_ROLE_KEYS}


def _normalized_bracket_id(value: Any) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        if token != "":
            return token
    return None


def _seed_size(primitive_index_by_slot: Any) -> int:
    if isinstance(primitive_index_by_slot, dict):
        return len(primitive_index_by_slot)
    return 0


def _empty_payload(
    *,
    seed_size: int,
    bracket_id: str | None,
) -> Dict[str, Any]:
    return {
        "seed_size": seed_size,
        "bracket_id": bracket_id,
        "matched_themes": [],
        "matched_signatures": [],
        "missing_roles": _empty_missing_roles(),
        "bracket_compatible_flag": True,
    }


# ---------------------------------------------------------------------------
# Sub-task 2C.2: theme-match algorithm
# ---------------------------------------------------------------------------


def _bin_confidence(
    score: float,
    theme_id: str,
    bands_by_theme_id: Mapping[str, Mapping[str, Any]],
) -> Optional[str]:
    """Bin a numeric ``score`` for ``theme_id`` against the bands pack.

    Returns ``"HIGH"`` / ``"MED"`` / ``"LOW"`` per inclusive-lower-bound
    semantics, or ``None`` if ``score`` falls below the LOW boundary (theme does
    not match). Raises ``KeyError`` if ``theme_id`` is missing from the bands
    pack — that's a pack-drift signal the caller should surface.
    """

    bands = bands_by_theme_id[theme_id]
    if score >= bands["high"]:
        return "HIGH"
    if score >= bands["med"]:
        return "MED"
    if score >= bands["low"]:
        return "LOW"
    return None


def _build_primitive_counts(slot_ids_by_primitive: Any) -> Dict[str, int]:
    """Convert sub-task-1's ``slot_ids_by_primitive`` input into ``{primitive_id: count}``.

    Tolerant of unusable shapes (returns empty dict) so the layer can degrade
    gracefully rather than raise on malformed orchestrator input. The empty-input
    case (sub-task-1's structural tests) flows through cleanly: empty mapping
    means every primitive count defaults to 0 in the evaluator, every formula
    score evaluates to 0, no theme matches its threshold.
    """

    counts: Dict[str, int] = {}
    if not isinstance(slot_ids_by_primitive, dict):
        return counts
    for prim_id, slot_ids in slot_ids_by_primitive.items():
        if not isinstance(prim_id, str):
            continue
        if isinstance(slot_ids, (list, tuple)):
            counts[prim_id] = len(slot_ids)
    return counts


def _evaluate_theme(
    theme: Mapping[str, Any],
    primitive_counts: Mapping[str, int],
    allowlist: Set[str],
    bands_by_theme_id: Mapping[str, Mapping[str, Any]],
    formula_bindings: Dict[str, float],
) -> Optional[Dict[str, Any]]:
    """Evaluate one theme; return a match dict or ``None`` if threshold unmet.

    The caller threads ``formula_bindings`` (a per-call dict pre-populated with
    every allowlist identifier defaulting to 0 plus current primitive_counts)
    so the per-theme cost is just two ``score_formula_v1.evaluate`` calls, not
    a fresh dict construction per theme. ``formula_bindings["score"]`` is
    overwritten in place for the threshold evaluation.
    """

    theme_id = theme["theme_id"]
    formula = theme["score_formula"]
    threshold = theme["classify_threshold"]

    formula_bindings["score"] = 0.0
    score = _evaluate_score_formula(formula, formula_bindings, allowlist)
    if not isinstance(score, (int, float)) or isinstance(score, bool):
        # Score formula returned non-numeric (would mean evaluator misuse);
        # surface as a non-match so the layer continues processing other themes.
        return None

    formula_bindings["score"] = float(score)
    matched = _evaluate_score_formula(threshold, formula_bindings, allowlist)
    if not isinstance(matched, bool) or not matched:
        return None

    confidence = _bin_confidence(float(score), theme_id, bands_by_theme_id)
    if confidence is None:
        # Threshold matched but bin returned None — bands.low must equal the
        # classify_threshold N for monotonicity (verified in 2B.2's pack
        # creation). If this fires, the bands pack drifted from the threshold.
        raise RuntimeError(
            f"_bin_confidence returned None for {theme_id} despite threshold "
            f"match (score={score}, bands={dict(bands_by_theme_id[theme_id])})"
        )

    return {
        "theme_id": theme_id,
        "score": float(score),
        "confidence": confidence,
    }


def run_seed_synergy_detection_v1(
    primitive_index_by_slot: Any,
    slot_ids_by_primitive: Any,
    bracket_id: Any = None,
    commander_slot_id: Any = None,
) -> dict:
    """Compute matched themes for the input seed under the active bracket.

    Sub-task 2C.2 implementation. Inputs mirror the sibling
    ``engine_requirement_detection_v1`` layer so the future Phase 3 orchestrator
    can feed both layers from one primitive-index pass. ``bracket_id`` is the
    new parameter for bracket-aware filtering; ``commander_slot_id`` is optional,
    matching the sibling.

    Algorithm:
      - Compute ``seed_size`` from ``primitive_index_by_slot``.
      - Build ``primitive_counts`` from ``slot_ids_by_primitive`` (count = number
        of slots carrying each primitive).
      - Load themes / bands / union allowlist via the 2C.1 helpers.
      - Surface any input primitive_id not in the union allowlist via the
        ``unknowns`` envelope field (status flips to ``WARN``).
      - For each of the 41 themes: evaluate ``score_formula`` against
        ``primitive_counts``; if ``classify_threshold`` matches, bin via
        ``_bin_confidence`` against the bands pack.
      - Sort matches by ``score`` desc, then ``theme_id`` asc (deterministic
        ordering per the design doc).

    Out of scope this sub-task (still at sub-task-1 defaults):
      - ``matched_signatures`` (engine-requirement-detection integration is
        a future task).
      - ``missing_roles`` (bracket-aware role-quota calc is a future task).
      - ``bracket_compatible_flag`` (bracket filter is a future task; defaults
        to ``True`` per sub-task-1's safe-default rationale for empty seeds).
    """

    seed_size = _seed_size(primitive_index_by_slot)
    bracket = _normalized_bracket_id(bracket_id)
    primitive_counts = _build_primitive_counts(slot_ids_by_primitive)

    themes = _load_themes()
    bands_by_theme_id = _load_bands()
    allowlist = _build_union_allowlist()

    # Surface input primitives not in the closed-world allowlist.
    unknown_primitive_ids = sorted(
        prim_id for prim_id in primitive_counts if prim_id not in allowlist
    )
    unknowns: List[Dict[str, Any]] = []
    if unknown_primitive_ids:
        unknowns.append(
            {
                "code": "UNKNOWN_PRIMITIVE_IN_SEED",
                "primitive_ids": unknown_primitive_ids,
            }
        )

    # Pre-populate evaluator bindings once per call: every allowlist identifier
    # defaults to 0; positive primitive counts overwrite the defaults. The
    # ``score`` sentinel is updated in place inside ``_evaluate_theme`` as it
    # transitions from formula-eval to threshold-eval.
    formula_bindings: Dict[str, float] = {ident: 0.0 for ident in allowlist}
    for prim_id, count in primitive_counts.items():
        if prim_id in allowlist:
            formula_bindings[prim_id] = float(count)

    matches: List[Dict[str, Any]] = []
    for theme in themes:
        match = _evaluate_theme(
            theme,
            primitive_counts,
            allowlist,
            bands_by_theme_id,
            formula_bindings,
        )
        if match is not None:
            matches.append(match)

    # Deterministic ordering: score desc, then theme_id asc.
    matches.sort(key=lambda m: (-m["score"], m["theme_id"]))

    payload = {
        "seed_size": seed_size,
        "bracket_id": bracket,
        "matched_themes": matches,
        "matched_signatures": [],
        "missing_roles": _empty_missing_roles(),
        "bracket_compatible_flag": True,
    }

    return {
        "version": SEED_SYNERGY_DETECTION_V1_VERSION,
        "status": "WARN" if unknowns else "OK",
        "reason_code": None,
        "codes": [],
        "unknowns": unknowns,
        "seed_synergy_detection_v1": payload,
    }
