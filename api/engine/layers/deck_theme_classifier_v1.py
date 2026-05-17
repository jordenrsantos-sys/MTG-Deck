"""
deck_theme_classifier_v1 — Phase 2.1a engine layer.

Classifies a completed deck against the brain's 41 themes + 78 concrete typal themes.
Emits `deck_themes_v1: List[ClassifiedTheme]` ordered by score descending.

Algorithm (from PHASE_2_1_SYNERGY_ENRICHMENT_DESIGN.md):
  1. Aggregate primitive counts across slots (consume primitive_index_v1 output).
  2. Compute composite signal counts via themes_signal_vocabulary_v1.
  3. For each main theme: evaluate required_signals → if true, compute score_formula →
     if score >= classify_threshold AND anti_signals false → emit ACTIVE.
  4. For each subtype in deck: evaluate each typal theme similarly, with TYPE_DENSITY
     bound to the per-subtype card count.
  5. Bind confidence band per themes_confidence_bands_v1 (low/med/high thresholds).
  6. Sort active themes by score descending; cap at top 10.

Reads (BYTE-IDENTICAL — calibration boundary):
  - taxonomy/packs/taxonomy_v1_23/themes_v1_5.json (41 themes)
  - taxonomy/packs/taxonomy_v1_23/typal_themes_v1_6.json (79 typal themes, 78 concrete)
  - api/engine/data/themes/themes_signal_vocabulary_v1.json (31 composite signals)
  - api/engine/data/themes/themes_confidence_bands_v1.json (per-theme confidence thresholds)

Pure function. No DB writes, no network. Sub-millisecond runtime per deck after
data files are loaded (cached at module import).

Phase 2.1a scope note: typal themes' TRIBAL_PAYOFFS / TRIBAL_LORD_EFFECT / TRIBAL_TUTORS
are treated as GLOBAL primitive counts (not subtype-restriction-filtered). This will
slightly over-classify for decks with lots of tribal effects spread across multiple
types. The limitation is documented and tractable as Phase 2.1.5 work — the
restriction-filtered primitive index is a separate enrichment. Initial classification
is calibration-honest: false positives surface as ACTIVE themes the user can verify,
not silent omissions.
"""
from __future__ import annotations

import json
import os
import traceback
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from api.engine.layers._theme_expression_parser import (
    evaluate_predicate,
    evaluate_score,
    make_lenient_lookup,
    ThemeExpressionError,
)


# ====== Diagnostic trace (gated by env var; writes last-call trace for debug) ======

_TRACE_PATH = Path(__file__).resolve().parents[2] / "engine" / "data" / "themes" / "_last_classify_trace.json"


def _write_trace(trace: Dict[str, Any]) -> None:
    """Best-effort diagnostic write. Never raises."""
    try:
        _TRACE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(_TRACE_PATH, "w", encoding="utf-8") as f:
            json.dump(trace, f, indent=2, default=str)
    except Exception:
        pass


# ====== Data file paths (BYTE-IDENTICAL frozen) ======

_REPO_ROOT = Path(__file__).resolve().parents[3]  # api/engine/layers/THIS -> repo/

_THEMES_PATH = _REPO_ROOT / "taxonomy" / "packs" / "taxonomy_v1_23" / "themes_v1_5.json"
_TYPAL_THEMES_PATH = _REPO_ROOT / "taxonomy" / "packs" / "taxonomy_v1_23" / "typal_themes_v1_6.json"
_SIGNAL_VOCAB_PATH = _REPO_ROOT / "api" / "engine" / "data" / "themes" / "themes_signal_vocabulary_v1.json"
_SIGNAL_VOCAB_BRIDGE_PATH = _REPO_ROOT / "api" / "engine" / "data" / "themes" / "themes_signal_vocabulary_v1_5_tribal_bridge.json"
_SIGNAL_VOCAB_MAIN_BRIDGE_PATH = _REPO_ROOT / "api" / "engine" / "data" / "themes" / "themes_signal_vocabulary_v1_6_main_themes_bridge.json"
_CONFIDENCE_BANDS_PATH = _REPO_ROOT / "api" / "engine" / "data" / "themes" / "themes_confidence_bands_v1.json"


# ====== Module-cached data ======

def _load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


_THEMES: List[Dict[str, Any]] = _load_json(_THEMES_PATH)
_TYPAL_THEMES: List[Dict[str, Any]] = _load_json(_TYPAL_THEMES_PATH)
_SIGNAL_VOCAB_BASE: List[Dict[str, Any]] = _load_json(_SIGNAL_VOCAB_PATH)
# v1.5 tribal bridge: additive extension to v1 vocabulary. Aliases existing
# card_tags primitives into the abstract signal names typal themes reference.
# Documented in DESIGN_DECISIONS.md as the vocabulary-gap calibration patch.
try:
    _SIGNAL_VOCAB_BRIDGE: List[Dict[str, Any]] = _load_json(_SIGNAL_VOCAB_BRIDGE_PATH)
except Exception:
    _SIGNAL_VOCAB_BRIDGE = []
# v1.6 main-themes bridge: additive extension covering the broader vocabulary
# the 41 main themes reference. Placeholder entries (composite_of_primitives=[])
# evaluate to 0 — calibration-honest where no analog exists in card_tags.
try:
    _SIGNAL_VOCAB_MAIN_BRIDGE: List[Dict[str, Any]] = _load_json(_SIGNAL_VOCAB_MAIN_BRIDGE_PATH)
except Exception:
    _SIGNAL_VOCAB_MAIN_BRIDGE = []
_SIGNAL_VOCAB: List[Dict[str, Any]] = (
    list(_SIGNAL_VOCAB_BASE)
    + list(_SIGNAL_VOCAB_BRIDGE)
    + list(_SIGNAL_VOCAB_MAIN_BRIDGE)
)
_CONFIDENCE_BANDS: List[Dict[str, Any]] = _load_json(_CONFIDENCE_BANDS_PATH)

# Index: signal_id → list of composite primitive names
_SIGNAL_COMPOSITES: Dict[str, List[str]] = {
    s["id"]: list(s.get("composite_of_primitives") or [])
    for s in _SIGNAL_VOCAB
    if isinstance(s, dict) and isinstance(s.get("id"), str)
}

# Index: theme_id → {low, med, high} thresholds
_CONFIDENCE_INDEX: Dict[str, Dict[str, float]] = {
    b["theme_id"]: {
        "low": float(b.get("low", 0)),
        "med": float(b.get("med", 0)),
        "high": float(b.get("high", 0)),
    }
    for b in _CONFIDENCE_BANDS
    if isinstance(b, dict) and isinstance(b.get("theme_id"), str)
}

# Filter typal themes to concrete entries only (skip the <Subtype> template).
_CONCRETE_TYPAL_THEMES: List[Dict[str, Any]] = [
    t for t in _TYPAL_THEMES
    if isinstance(t.get("subtype"), str) and not t["subtype"].startswith("<")
]


# ====== Public types ======

ClassifiedTheme = Dict[str, Any]
# Shape:
# {
#   "theme_id": "TRIBAL_GOBLIN" | "THEME_CONTROL" | ...,
#   "theme_type": "MECHANIC" | "ROLE" | "TYPAL" | ...,
#   "subtype": "Goblin" | None,
#   "score": 47.0,
#   "confidence_band": "HIGH" | "MED" | "LOW" | "BELOW_THRESHOLD",
#   "classify_threshold": "score>=22",
#   "passed_classify_threshold": True,
#   "anti_signal_hit": False,
#   "contributing_primitives": ["TRIBAL_LORD_EFFECT", "TRIBAL_TOKEN_PRODUCTION", ...],
# }


# ====== Aggregation helpers ======

def _aggregate_primitive_counts(
    primitive_index_by_slot: Optional[Dict[str, Iterable[str]]],
) -> Dict[str, int]:
    """Sum primitive occurrences across all slots in the deck."""
    counts: Counter[str] = Counter()
    if not isinstance(primitive_index_by_slot, dict):
        return {}
    for primitives in primitive_index_by_slot.values():
        if isinstance(primitives, (list, tuple, set)):
            for p in primitives:
                if isinstance(p, str) and p:
                    counts[p] += 1
    return dict(counts)


def _compute_signal_counts(primitive_counts: Dict[str, int]) -> Dict[str, int]:
    """Aggregate composite signals from primitive counts.

    Signals like AGGRO_CONVERSION = sum of (COMBAT_DAMAGE_PAYOFF + HAS_EVASION_KEYWORDS
    + EXTRA_COMBAT). The signal's count is the sum of its composite primitives' counts.
    """
    signal_counts: Dict[str, int] = {}
    for signal_id, composites in _SIGNAL_COMPOSITES.items():
        total = 0
        for prim in composites:
            total += int(primitive_counts.get(prim, 0))
        signal_counts[signal_id] = total
    return signal_counts


def _build_main_lookup(
    primitive_counts: Dict[str, int],
    signal_counts: Dict[str, int],
) -> Dict[str, int]:
    """Merged lookup namespace for main themes — primitives + signals.

    Signal counts override primitive counts on collision (signals are aggregates;
    no real primitive should share its name with an aggregate signal ID, but if
    one does the signal wins).
    """
    merged: Dict[str, int] = {}
    merged.update(primitive_counts)
    merged.update(signal_counts)
    return merged


def _build_typal_lookup(
    primitive_counts: Dict[str, int],
    signal_counts: Dict[str, int],
    subtype: str,
    subtype_density: int,
) -> Dict[str, int]:
    """Lookup namespace for evaluating a typal theme against a specific subtype.

    Phase 2.1a scope: TYPE_DENSITY is per-subtype; other identifiers fall through to
    global primitive/signal counts. The brain doc acknowledges this limitation —
    restriction-filtered tribal counts are Phase 2.1.5 work.
    """
    merged: Dict[str, int] = {}
    merged.update(primitive_counts)
    merged.update(signal_counts)
    # Bind subtype-specific identifiers
    merged["TYPE_DENSITY"] = subtype_density
    # Function-call-form lookups synthesized by the parser as "IDENT__OF__ARG":
    # TYPE_DENSITY(subtype) → TYPE_DENSITY__OF__subtype → density of `subtype`.
    # This handles the typal-theme TEMPLATE entry's literal `TYPE_DENSITY(subtype)`
    # if anyone evaluates it directly (concrete entries don't use this form).
    merged[f"TYPE_DENSITY__OF__{subtype}"] = subtype_density
    return merged


def _band_for_score(theme_id: str, score: float, classify_threshold_value: Optional[float]) -> str:
    """Return 'HIGH' / 'MED' / 'LOW' / 'BELOW_THRESHOLD' confidence band for a score."""
    bands = _CONFIDENCE_INDEX.get(theme_id)
    if bands is None:
        # No confidence-band entry — fall back to classify_threshold pass/fail
        if classify_threshold_value is not None and score >= classify_threshold_value:
            return "MED"
        return "BELOW_THRESHOLD"
    if score >= bands["high"]:
        return "HIGH"
    if score >= bands["med"]:
        return "MED"
    if score >= bands["low"]:
        return "LOW"
    return "BELOW_THRESHOLD"


def _extract_classify_threshold_value(theme: Dict[str, Any]) -> Optional[float]:
    """Pull the numeric threshold out of `score>=N`. Returns None if unparseable."""
    text = theme.get("classify_threshold", "")
    if not isinstance(text, str) or not text.strip():
        return None
    # Match patterns like "score>=22" or "score >= 22.5"
    import re
    m = re.search(r"score\s*>=\s*(\d+(?:\.\d+)?)", text)
    if not m:
        return None
    try:
        return float(m.group(1))
    except (TypeError, ValueError):
        return None


def _contributing_primitives(
    theme: Dict[str, Any],
    primitive_counts: Dict[str, int],
) -> List[str]:
    """Identify which primitives actually contributed positively to this theme's score.

    Heuristic: any UPPERCASE_IDENTIFIER appearing in score_formula whose primitive
    count is > 0. Used by Stage 2.1b synergy reasons to attribute chips correctly.
    """
    text = theme.get("score_formula", "")
    if not isinstance(text, str):
        return []
    import re
    tokens = set(re.findall(r"\b[A-Z][A-Z0-9_]+\b", text))
    # Strip 'AND', 'OR', 'score' if they slip in
    tokens.discard("AND")
    tokens.discard("OR")
    tokens.discard("SCORE")
    contributing = []
    for tok in sorted(tokens):
        # Check primitive_counts only (we want the actual deck primitives,
        # not aggregate signals)
        if primitive_counts.get(tok, 0) > 0:
            contributing.append(tok)
    return contributing


# ====== Per-theme evaluation ======

def _classify_main_theme(
    theme: Dict[str, Any],
    lookup_namespace: Dict[str, int],
    primitive_counts: Dict[str, int],
) -> Optional[ClassifiedTheme]:
    """Evaluate a single main theme. Returns None if it doesn't pass required_signals."""
    theme_id = theme.get("theme_id", "")
    if not theme_id:
        return None
    lookup = make_lenient_lookup(lookup_namespace)

    # Required signals gate
    required_text = theme.get("required_signals", "")
    try:
        if not evaluate_predicate(required_text, lookup):
            return None
    except ThemeExpressionError:
        return None

    # Score formula
    score_text = theme.get("score_formula", "")
    try:
        raw_score = evaluate_score(score_text, lookup)
    except ThemeExpressionError:
        return None

    # Anti-signals: if hit, exclude from active list
    anti_text = theme.get("anti_signals", "")
    anti_hit = False
    try:
        if anti_text.strip():
            anti_hit = evaluate_predicate(anti_text, lookup)
    except ThemeExpressionError:
        anti_hit = False

    threshold_value = _extract_classify_threshold_value(theme)
    passed_classify = (threshold_value is None) or (raw_score >= threshold_value)

    if anti_hit or not passed_classify:
        # Don't return this theme as ACTIVE; calibration-honest
        return None

    return {
        "theme_id": theme_id,
        "theme_type": theme.get("theme_type", "UNKNOWN"),
        "subtype": None,
        "score": float(raw_score),
        "confidence_band": _band_for_score(theme_id, raw_score, threshold_value),
        "classify_threshold": theme.get("classify_threshold", ""),
        "passed_classify_threshold": True,
        "anti_signal_hit": False,
        "contributing_primitives": _contributing_primitives(theme, primitive_counts),
    }


def _classify_typal_theme(
    theme: Dict[str, Any],
    subtype: str,
    subtype_density: int,
    primitive_counts: Dict[str, int],
    signal_counts: Dict[str, int],
) -> Optional[ClassifiedTheme]:
    """Evaluate a typal theme against a deck's specific subtype population."""
    typal_id = theme.get("typal_id", "")
    if not typal_id:
        return None

    namespace = _build_typal_lookup(primitive_counts, signal_counts, subtype, subtype_density)
    lookup = make_lenient_lookup(namespace)

    required_text = theme.get("required_signals", "")
    try:
        if not evaluate_predicate(required_text, lookup):
            return None
    except ThemeExpressionError:
        return None

    score_text = theme.get("score_formula", "")
    try:
        raw_score = evaluate_score(score_text, lookup)
    except ThemeExpressionError:
        return None

    anti_text = theme.get("anti_signals", "")
    anti_hit = False
    try:
        if anti_text.strip():
            anti_hit = evaluate_predicate(anti_text, lookup)
    except ThemeExpressionError:
        anti_hit = False

    # Typal themes encode threshold in `thresholds` field, e.g.
    # "Classify if score>=24; MED if >=24 and payoffs>=2; HIGH if >=30 ..."
    import re
    threshold_value: Optional[float] = None
    thresholds_text = theme.get("thresholds", "")
    if isinstance(thresholds_text, str):
        m = re.search(r"score\s*>=\s*(\d+(?:\.\d+)?)", thresholds_text)
        if m:
            try:
                threshold_value = float(m.group(1))
            except (TypeError, ValueError):
                pass

    passed_classify = (threshold_value is None) or (raw_score >= threshold_value)

    if anti_hit or not passed_classify:
        return None

    # Build a per-typal theme_id for output: "TYPAL_GOBLINS:Goblin" etc.
    output_theme_id = f"{typal_id}:{subtype}"

    return {
        "theme_id": output_theme_id,
        "theme_type": "TYPAL",
        "subtype": subtype,
        "score": float(raw_score),
        "confidence_band": _band_for_score(typal_id, raw_score, threshold_value),
        "classify_threshold": f"score>={threshold_value}" if threshold_value is not None else "",
        "passed_classify_threshold": True,
        "anti_signal_hit": False,
        "contributing_primitives": _contributing_primitives(theme, primitive_counts),
    }


# ====== Subtype counting helper ======

def compute_primitive_index_from_card_names(
    db_snapshot_id: str,
    card_names: Iterable[str],
) -> Dict[str, List[str]]:
    """Build a primitive_index_by_slot from a list of card names.

    Each slot_id is `card_<index>_<name>` for determinism. Unresolved names are
    skipped silently — the classifier handles missing primitives as count 0.

    Phase 2.1a wire-in convenience: the Complete engine doesn't have
    primitive_index_by_slot pre-computed (that's a build-pipeline product). For
    the classifier to fire on Complete responses, we resolve card names through
    db_cards + card_tags table directly inline.

    IMPORTANT: cards.primitives_json is mostly empty for the production snapshot.
    The REAL granular primitive vocabulary (TRIBAL_LORD_EFFECT, BOARDWIPE_CREATURES,
    CARD_DRAW_REPEATABLE, etc.) lives in the card_tags table's primitive_ids_json
    column, keyed by (snapshot_id, taxonomy_version, oracle_id). The build
    pipeline reads from card_tags via bulk_get_card_tags (pipeline_build.py:1407);
    we mirror that pattern here.
    """
    from api.engine.db_cards import find_card_by_name, cards_db_connect  # local import
    from engine.db_tags import bulk_get_card_tags, ensure_tag_tables, TagSnapshotMissingError
    from api.engine.version_resolve_v1 import resolve_runtime_taxonomy_version

    trace: Dict[str, Any] = {
        "fn": "compute_primitive_index_from_card_names",
        "db_snapshot_id": db_snapshot_id,
        "input_count": 0,
        "resolved_oracle_count": 0,
        "unresolved_samples": [],
        "taxonomy_version": None,
        "tags_row_count": 0,
        "non_empty_primitive_rows": 0,
        "primitive_total": 0,
        "first_primitive_samples": [],
        "failure_at": None,
        "exception": None,
    }

    # First pass: resolve names → oracle_ids.
    # Speed budget rule (DESIGN_DECISIONS 1.2): use a single bulk SQL query
    # rather than per-name find_card_by_name calls (which open/close connection
    # each invocation and add ~30ms × N). The bulk query brings 100-card deck
    # resolution from ~1s to ~50ms.
    name_list = [n.strip() for n in card_names if isinstance(n, str) and n.strip()]
    trace["input_count"] = len(name_list)
    if not name_list:
        trace["failure_at"] = "empty_input"
        _write_trace(trace)
        return {}

    # Build unique-lower-name → oracle_id map via bulk query
    unique_lowered: List[str] = []
    seen_lower: set = set()
    for n in name_list:
        nl = n.lower()
        if nl not in seen_lower:
            seen_lower.add(nl)
            unique_lowered.append(nl)

    lower_to_oracle: Dict[str, str] = {}
    try:
        from engine.db import connect as _bulk_connect
        placeholders = ",".join("?" for _ in unique_lowered)
        query = (
            "SELECT name, oracle_id FROM cards "
            "WHERE snapshot_id = ? AND LOWER(name) IN (" + placeholders + ")"
        )
        params: List[Any] = [db_snapshot_id] + unique_lowered
        with _bulk_connect() as con:
            rows = con.execute(query, params).fetchall()
        for row in rows:
            rd = dict(row) if hasattr(row, "keys") else dict(row)
            rname = rd.get("name")
            oid = rd.get("oracle_id")
            if isinstance(rname, str) and isinstance(oid, str) and oid:
                lower_to_oracle[rname.lower()] = oid
    except Exception as exc:
        # Bulk path failed; fall back to per-name (preserves original behavior)
        trace["bulk_resolve_warning"] = f"{exc.__class__.__name__}: {exc}"
        for n in name_list:
            try:
                card = find_card_by_name(db_snapshot_id, n)
                if isinstance(card, dict):
                    oid = card.get("oracle_id")
                    if isinstance(oid, str) and oid:
                        lower_to_oracle[n.lower()] = oid
            except Exception:
                continue

    slot_to_oracle: Dict[str, str] = {}
    unresolved: List[str] = []
    for i, name in enumerate(name_list):
        oid = lower_to_oracle.get(name.lower())
        if oid:
            slot_to_oracle[f"slot_{i}_{name}"] = oid
        else:
            unresolved.append(name)
    trace["resolved_oracle_count"] = len(slot_to_oracle)
    trace["unresolved_samples"] = unresolved[:10]

    if not slot_to_oracle:
        trace["failure_at"] = "no_names_resolved"
        _write_trace(trace)
        return {}

    # Second pass: bulk-fetch card_tags for all oracle_ids → granular primitives
    try:
        conn = cards_db_connect()
    except Exception as exc:
        trace["failure_at"] = "cards_db_connect"
        trace["exception"] = f"{exc.__class__.__name__}: {exc}"
        _write_trace(trace)
        return {}

    try:
        try:
            ensure_tag_tables(conn)
        except Exception as exc:
            trace["ensure_tag_tables_warning"] = f"{exc.__class__.__name__}: {exc}"
        try:
            taxonomy_version = resolve_runtime_taxonomy_version(
                snapshot_id=db_snapshot_id,
                requested=None,
                db=conn,
            )
        except Exception as exc:
            taxonomy_version = None
            trace["resolve_taxonomy_exception"] = f"{exc.__class__.__name__}: {exc}"
        trace["taxonomy_version"] = taxonomy_version
        if not isinstance(taxonomy_version, str) or not taxonomy_version:
            trace["failure_at"] = "taxonomy_version_unresolved"
            _write_trace(trace)
            return {}

        oracle_ids = list({oid for oid in slot_to_oracle.values()})
        try:
            tags_by_oracle = bulk_get_card_tags(
                conn=conn,
                oracle_ids=oracle_ids,
                snapshot_id=db_snapshot_id,
                taxonomy_version=taxonomy_version,
            )
        except TagSnapshotMissingError as exc:
            trace["failure_at"] = "tag_snapshot_missing"
            trace["exception"] = f"TagSnapshotMissingError: {exc}"
            _write_trace(trace)
            return {}
        except Exception as exc:
            trace["failure_at"] = "bulk_get_card_tags"
            trace["exception"] = f"{exc.__class__.__name__}: {exc}"
            trace["traceback"] = traceback.format_exc().splitlines()[-6:]
            _write_trace(trace)
            return {}

        trace["tags_row_count"] = len(tags_by_oracle) if isinstance(tags_by_oracle, dict) else -1

        index: Dict[str, List[str]] = {}
        non_empty = 0
        total_prims = 0
        first_samples: List[str] = []
        for slot_id, oracle_id in slot_to_oracle.items():
            tag_row = tags_by_oracle.get(oracle_id)
            if not isinstance(tag_row, dict):
                index[slot_id] = []
                continue
            # _decode_row in db_tags converts primitive_ids_json → primitive_ids list
            primitives = tag_row.get("primitive_ids", []) or tag_row.get("primitive_ids_json", [])
            if not isinstance(primitives, list):
                index[slot_id] = []
                continue
            clean = [p for p in primitives if isinstance(p, str) and p]
            index[slot_id] = clean
            if clean:
                non_empty += 1
                total_prims += len(clean)
                if len(first_samples) < 12:
                    first_samples.append(f"{slot_id} -> {clean[:5]}")
        trace["non_empty_primitive_rows"] = non_empty
        trace["primitive_total"] = total_prims
        trace["first_primitive_samples"] = first_samples
        trace["failure_at"] = None if non_empty > 0 else "all_rows_empty_primitives"
        _write_trace(trace)
        return index
    finally:
        try:
            conn.close()
        except Exception:
            pass


def compute_subtype_counts_from_card_names(
    db_snapshot_id: str,
    card_names: Iterable[str],
) -> Dict[str, int]:
    """Compute per-subtype card counts by resolving card names through db_cards.

    Used by deck_complete_engine_v1's classifier wire-in to derive the
    `deck_subtype_counts` argument from working_cards + commander.

    type_line shape: "Legendary Creature — Goblin Warrior" → subtypes ["Goblin", "Warrior"]
    Cards without a subtype clause (e.g., "Instant") contribute zero.

    Lookup failures are silently skipped (returns 0 for unresolved names). The
    classifier downstream handles {} input by skipping typal themes — calibration-honest.
    """
    from api.engine.db_cards import find_card_by_name  # local import — avoids module-load db cost

    trace: Dict[str, Any] = {
        "fn": "compute_subtype_counts_from_card_names",
        "input_count": 0,
        "resolved_count": 0,
        "subtypes_found": {},
        "unresolved_samples": [],
    }
    counts: Counter[str] = Counter()
    resolved = 0
    unresolved: List[str] = []
    input_count = 0
    for name in card_names:
        if not isinstance(name, str) or not name.strip():
            continue
        input_count += 1
        try:
            card = find_card_by_name(db_snapshot_id, name.strip())
        except Exception as exc:
            unresolved.append(f"{name} [exception: {exc.__class__.__name__}]")
            continue
        if not isinstance(card, dict):
            unresolved.append(f"{name} [not-dict]")
            continue
        type_line = card.get("type_line", "")
        if not isinstance(type_line, str):
            unresolved.append(f"{name} [no-type_line]")
            continue
        resolved += 1
        # Both em-dash variants seen in Scryfall data: "—" (U+2014) and "-".
        for separator in ("—", "–", " - "):
            if separator in type_line:
                after = type_line.split(separator, 1)[1].strip()
                for subtype in after.split():
                    # Subtypes are capitalized single tokens; filter obvious non-subtypes
                    if subtype and subtype[0].isupper() and "," not in subtype:
                        counts[subtype] += 1
                break
    trace["input_count"] = input_count
    trace["resolved_count"] = resolved
    trace["subtypes_found"] = dict(counts.most_common(20))
    trace["unresolved_samples"] = unresolved[:10]
    # Append to existing trace file (additive); don't overwrite primitive trace
    try:
        existing = {}
        if _TRACE_PATH.exists():
            with open(_TRACE_PATH, "r", encoding="utf-8") as f:
                existing = json.load(f)
        if not isinstance(existing, dict):
            existing = {}
        existing["subtype_trace"] = trace
        with open(_TRACE_PATH, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2, default=str)
    except Exception:
        pass
    return dict(counts)


# ====== Public entrypoint ======

def classify_deck_themes_v1(
    *,
    primitive_index_by_slot: Optional[Dict[str, Iterable[str]]],
    deck_subtype_counts: Optional[Dict[str, int]] = None,
    max_themes: int = 10,
) -> List[ClassifiedTheme]:
    """Classify the deck against all 41 main themes + applicable typal themes.

    Args:
        primitive_index_by_slot: Output of primitive_index_v1 layer. Maps slot_id
            (deck position) to list of primitives that slot's card has.
        deck_subtype_counts: For each subtype present in the deck, how many cards
            have that subtype. e.g., {"Goblin": 25, "Wizard": 3}. If None or empty,
            typal themes are skipped (no subtype context).
        max_themes: Cap the returned list at this many entries (top-N by score).

    Returns:
        List of ClassifiedTheme dicts, ordered by score descending. Empty list if
        no theme passes required_signals + classify_threshold + anti_signals.
    """
    primitive_counts = _aggregate_primitive_counts(primitive_index_by_slot)
    signal_counts = _compute_signal_counts(primitive_counts)

    main_namespace = _build_main_lookup(primitive_counts, signal_counts)

    active: List[ClassifiedTheme] = []
    main_evaluated = 0
    main_passed = 0
    typal_evaluated = 0
    typal_passed = 0

    for theme in _THEMES:
        main_evaluated += 1
        result = _classify_main_theme(theme, main_namespace, primitive_counts)
        if result is not None:
            main_passed += 1
            active.append(result)

    typal_attempts: List[Dict[str, Any]] = []
    if isinstance(deck_subtype_counts, dict):
        for subtype, density in deck_subtype_counts.items():
            if not isinstance(subtype, str) or not subtype:
                continue
            try:
                density_int = int(density)
            except (TypeError, ValueError):
                continue
            if density_int <= 0:
                continue
            for theme in _CONCRETE_TYPAL_THEMES:
                if theme.get("subtype") != subtype:
                    continue
                typal_evaluated += 1
                result = _classify_typal_theme(
                    theme, subtype, density_int, primitive_counts, signal_counts
                )
                typal_attempts.append({
                    "subtype": subtype,
                    "density": density_int,
                    "typal_id": theme.get("typal_id"),
                    "required_signals": theme.get("required_signals", ""),
                    "thresholds": theme.get("thresholds", ""),
                    "result": "active" if result is not None else "rejected",
                    "score": result.get("score") if result else None,
                })
                if result is not None:
                    typal_passed += 1
                    active.append(result)

    # Sort by score descending, stable on (theme_id, subtype) for determinism
    active.sort(key=lambda t: (-float(t.get("score", 0)), t.get("theme_id", "")))

    # Append classifier trace to the diagnostic file (best-effort)
    try:
        existing = {}
        if _TRACE_PATH.exists():
            with open(_TRACE_PATH, "r", encoding="utf-8") as f:
                existing = json.load(f)
        if not isinstance(existing, dict):
            existing = {}
        existing["classify_trace"] = {
            "primitive_counts_total": sum(primitive_counts.values()),
            "primitive_distinct": len(primitive_counts),
            "primitive_top10": dict(sorted(primitive_counts.items(), key=lambda kv: -kv[1])[:10]),
            "signal_counts_total": sum(signal_counts.values()),
            "signal_distinct": len([k for k, v in signal_counts.items() if v > 0]),
            "signal_top10": dict(sorted(signal_counts.items(), key=lambda kv: -kv[1])[:10]),
            "deck_subtype_counts_top10": dict(sorted((deck_subtype_counts or {}).items(), key=lambda kv: -kv[1])[:10]),
            "main_evaluated": main_evaluated,
            "main_passed": main_passed,
            "typal_evaluated": typal_evaluated,
            "typal_passed": typal_passed,
            "active_count": len(active),
            "typal_attempts_sample": typal_attempts[:8],
        }
        with open(_TRACE_PATH, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2, default=str)
    except Exception:
        pass

    return active[: max(0, int(max_themes))]
