"""v1.7.2 Stage 1 — Deck-combo insight engine layer.

Scans the FINAL completed deck (commander + initial deckText + engine
adds) against the Spellbook-derived combo dataset and surfaces TWO
insight surfaces consumed by the UI's `DeckCombosPanel`:

  - `detected_combos_v1`  — pairs where BOTH halves are present.
  - `missing_partners_v1` — pairs where EXACTLY ONE half is present
    AND the partner is NOT being added (and therefore would be a
    productive add the user could make).

Pillar A.7 alignment (combo dataset divergence fix):
  Previously this layer loaded `two_card_combos_v2.json` (oracle_id-
  keyed; 4423 pairs filtered to those with outcome labels) while the
  bracket verifier in `corpus_batch_ingest_v1._compute_min_legal_bracket`
  loaded the Spellbook-derived `combo_brackets_v1.json` (name-keyed;
  3679 unique pairs after the 2-card + no-extra-prerequisite filter).
  Same upstream data — different shapes, different detection rules,
  different coverage. Repro: Old Gnawbone + Hellkite Charger
  (variant_id 1800-3398, a known Spellbook O/core_plus combo) showed
  up in the bracket verifier's auto-bump path but NOT in the UI's
  `detected_combos_v1`. Root cause: the v2 oracle_id resolution path
  was less robust than the bracket verifier's lowercase-name match
  (different printings can produce different oracle_ids for the same
  card name; the snapshot DB's `find_card_by_name` doesn't always
  return the same oracle_id that v2's `a`/`b` fields carry).

  Fix: load `combo_brackets_v1.json` here with the SAME filter the
  bracket verifier uses (`combo_size == 2`, `has_extra_prerequisite ==
  False`, non-empty `brackets_allowed`). Detect by lowercase NAME so
  the UI matches the verifier semantics exactly. Outcome label is
  taken from the existing outcomes pack when present (variant_id
  keyed), falling back to the `results` array on the bracket entry
  (joined with "; ") when the outcomes pack lacks a record.

Engine-side contract (UNCHANGED, additive to combo_enabler /
bracket_aware):

    {
      "detected_combos_v1": [
        {
          "variant_id": "3940-5195",
          "card_a_name": "Storm-Kiln Artist",
          "card_a_oracle_id": "a145ff8c-...",
          "card_b_name": "Haze of Rage",
          "card_b_oracle_id": "f17d0fb8-...",
          "combo_outcome_label": "Infinite colored mana; …"
        }, ...
      ],
      "missing_partners_v1": [
        {
          "variant_id": "3940-5195",
          "present_card_name": "Storm-Kiln Artist",
          "present_card_oracle_id": "a145ff8c-...",
          "partner_card_name": "Haze of Rage",
          "partner_card_oracle_id": "f17d0fb8-...",
          "combo_outcome_label": "Infinite colored mana; …"
        }, ...
      ]
    }

Both arrays sort deterministically by variant_id and cap at 25 entries
(see DECK_COMBO_INSIGHTS_MAX_ENTRIES) so the response payload size is
bounded for high-combo decks.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple

from api.engine.layers._combo_data_loader import (
    load_outcomes,
    normalize_card_name,
    resolve_name_to_oracle_id,
)
# v1.7.4: direct lookup for color_identity in addition to name. The
# shared _combo_data_loader.resolve_oracle_ids_to_names is SHA-locked,
# so we go through db_cards directly for the extended field set rather
# than extending the loader signature.
from api.engine.db_cards import lookup_cards_by_oracle_ids


DECK_COMBO_INSIGHTS_V1_VERSION = "deck_combo_insights_v1"
DECK_COMBO_INSIGHTS_MAX_ENTRIES = 25


# Pillar A.7 — switched data source from `two_card_combos_v2.json`
# (oracle_id-keyed) to `combo_brackets_v1.json` (name-keyed) so the
# insights layer matches the corpus_batch_ingest bracket verifier
# pair-for-pair. The file lives alongside the v2 file under
# api/engine/data/combos/.
_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "combos"
COMBO_BRACKETS_V1_PATH = _DATA_DIR / "combo_brackets_v1.json"


def _normalize_name_lower(value: Any) -> Optional[str]:
    """Lowercase + strip a card-name value (mirrors the bracket verifier's
    `str(...).strip().lower()` invariant). Returns None on bad input."""
    if not isinstance(value, str):
        return None
    token = value.strip().lower()
    return token if token else None


def _fallback_label_from_results(results: Any) -> Optional[str]:
    """Join the `results` array on a combo_brackets entry into a single
    display label. Returns None when the array is missing/empty so the
    caller can chain to a category fallback."""
    if not isinstance(results, list):
        return None
    cleaned: List[str] = []
    for item in results:
        if isinstance(item, str):
            token = item.strip()
            if token:
                cleaned.append(token)
    if not cleaned:
        return None
    return "; ".join(cleaned)


def _load_combo_brackets_pair_index(
    path: Path = COMBO_BRACKETS_V1_PATH,
    outcomes: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[FrozenSet[str], Dict[str, Any]]:
    """Load `combo_brackets_v1.json` into a NAME-keyed pair index that
    matches the bracket verifier's filter (2-card combos with no extra
    non-card prerequisite and a non-empty `brackets_allowed`).

    The returned mapping is keyed by `frozenset({name_a_lower, name_b_lower})`.
    Each value carries everything the insights layer needs to surface a
    pair: variant_id, the canonical Spellbook-cased names, and a display
    label (sourced from the outcomes pack when the variant_id has an
    outcome record; otherwise joined from the bracket entry's `results`
    array; otherwise a generic "Combo (category)" placeholder).

    Mirrors `corpus_batch_ingest_v1._load_combo_brackets()` in semantics
    so the two layers see the SAME 3679-pair filtered subset.
    """
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    by_variant_id = parsed.get("by_variant_id") if isinstance(parsed, dict) else None
    if not isinstance(by_variant_id, dict):
        return {}

    outcomes_pack = outcomes or {}
    index: Dict[FrozenSet[str], Dict[str, Any]] = {}
    for variant_id, info in by_variant_id.items():
        if not isinstance(variant_id, str) or not isinstance(info, dict):
            continue
        if info.get("combo_size") != 2:
            continue
        if info.get("has_extra_prerequisite"):
            continue
        brackets_allowed = info.get("brackets_allowed") or []
        if not isinstance(brackets_allowed, list) or not brackets_allowed:
            continue
        names = info.get("card_names") or []
        if not isinstance(names, list) or len(names) != 2:
            continue
        name_a, name_b = names[0], names[1]
        if not isinstance(name_a, str) or not isinstance(name_b, str):
            continue
        a_lower = _normalize_name_lower(name_a)
        b_lower = _normalize_name_lower(name_b)
        if a_lower is None or b_lower is None or a_lower == b_lower:
            continue
        key = frozenset({a_lower, b_lower})

        # Display label resolution — outcomes pack → results join → generic.
        label: Optional[str] = None
        outcome_rec = outcomes_pack.get(variant_id) if isinstance(outcomes_pack, dict) else None
        if isinstance(outcome_rec, dict):
            candidate = outcome_rec.get("label")
            if isinstance(candidate, str) and candidate.strip():
                label = candidate.strip()
        if label is None:
            label = _fallback_label_from_results(info.get("results"))
        if label is None:
            category = info.get("category") or "unclassified"
            label = f"Combo ({category})"

        # Dedup: if the same name-pair appears under multiple variant_ids
        # (rare — confirmed zero collisions in the current dataset),
        # prefer the lexicographically smallest variant_id for stability.
        if key in index and variant_id >= index[key]["variant_id"]:
            continue

        index[key] = {
            "variant_id": variant_id,
            "name_a": name_a.strip(),
            "name_b": name_b.strip(),
            "name_a_lower": a_lower,
            "name_b_lower": b_lower,
            "label": label,
            "category": info.get("category") or "",
            "brackets_allowed": list(brackets_allowed),
        }
    return index


# Module-import-time load (frozen for the process lifetime). The
# outcomes pack is loaded once and threaded into the bracket loader
# so labels are sourced from Spellbook's authoritative outcome text
# when available.
_OUTCOMES: Dict[str, Dict[str, Any]] = load_outcomes()
_BRACKET_PAIR_INDEX: Dict[FrozenSet[str], Dict[str, Any]] = (
    _load_combo_brackets_pair_index(outcomes=_OUTCOMES)
)


_COLOR_LETTERS: Set[str] = {"W", "U", "B", "R", "G"}


def _normalize_color_identity_field(value: Any) -> Set[str]:
    """v1.7.4 — parse a card's `color_identity` field (DB row value)
    into a normalized upper-case set. Mirrors
    `proactive_combo_completion_v1._normalize_color_identity_field`
    (duplicated rather than imported because that helper is
    `_private`; future refactor can lift the helper into a shared
    module). Accepts JSON-array strings (`'["W","U"]'`), Python lists/
    sets, raw color-letter strings (`"WU"`), or None."""
    if value is None:
        return set()
    if isinstance(value, (list, tuple, set)):
        out: Set[str] = set()
        for c in value:
            if isinstance(c, str):
                t = c.strip().upper()
                if t in _COLOR_LETTERS:
                    out.add(t)
        return out
    if isinstance(value, str):
        token = value.strip()
        if token.startswith("[") and token.endswith("]"):
            try:
                parsed = json.loads(token)
                if isinstance(parsed, list):
                    return _normalize_color_identity_field(parsed)
            except (ValueError, TypeError):
                pass
        out = set()
        for c in token:
            if c.upper() in _COLOR_LETTERS:
                out.add(c.upper())
        return out
    return set()


def _resolve_commander_color_identity(
    snapshot_id: str, commander_oracle_ids: Set[str]
) -> Set[str]:
    """Compute the UNION of color identities across all commanders
    (partner/companion decks have multiple commanders). Empty set →
    colorless deck → only colorless partners legal."""
    if not commander_oracle_ids:
        return set()
    raw = lookup_cards_by_oracle_ids(
        conn=None,
        snapshot_id=snapshot_id,
        oracle_ids=commander_oracle_ids,
        requested_fields=["oracle_id", "color_identity"],
    )
    union_ci: Set[str] = set()
    for _, row in raw.items():
        if not isinstance(row, dict):
            continue
        union_ci |= _normalize_color_identity_field(row.get("color_identity"))
    return union_ci


def compute_deck_combo_insights_v1(
    *,
    db_snapshot_id: str,
    commander_names: List[str],
    deck_cards_after_completion: List[str],
) -> Dict[str, List[Dict[str, Any]]]:
    """Compute detected + missing-partner combo insights for the deck.

    Returns `{detected_combos_v1: [...], missing_partners_v1: [...]}`
    with each list deterministically sorted by variant_id and capped at
    `DECK_COMBO_INSIGHTS_MAX_ENTRIES`. Returns empty arrays for either
    field when preconditions fail (empty snapshot id, no pair index,
    no resolvable deck cards) — never raises.

    Pillar A.7: detection runs against the SAME 3679-pair filtered
    `combo_brackets_v1.json` subset that
    `corpus_batch_ingest_v1._compute_min_legal_bracket` consumes, so a
    pair that auto-bumps a bracket in ingest also shows up in the UI's
    `detected_combos_v1` — no more divergence.
    """
    empty: Dict[str, List[Dict[str, Any]]] = {
        "detected_combos_v1": [],
        "missing_partners_v1": [],
    }

    snapshot_id = db_snapshot_id if isinstance(db_snapshot_id, str) and db_snapshot_id.strip() else ""
    if snapshot_id == "" or not _BRACKET_PAIR_INDEX:
        return empty

    # Build the present-set as lowercase canonical names (commander +
    # deck cards). Matches the bracket verifier's `deck_lower` set so
    # detection runs over the same surface as auto-bump.
    commander_name_set: Set[str] = set()
    if isinstance(commander_names, list):
        for n in commander_names:
            norm = normalize_card_name(n)
            if norm is not None:
                lowered = norm.lower()
                if lowered:
                    commander_name_set.add(lowered)
    present_name_set: Set[str] = set(commander_name_set)
    if isinstance(deck_cards_after_completion, list):
        for n in deck_cards_after_completion:
            norm = normalize_card_name(n)
            if norm is not None:
                lowered = norm.lower()
                if lowered:
                    present_name_set.add(lowered)
    if not present_name_set:
        return empty

    # First pass — match pairs by name. Cheap pure-Python set lookups.
    detected_raw: List[Tuple[str, str, str, str]] = []  # (vid, name_a_lower, name_b_lower, label)
    missing_raw: List[Tuple[str, str, str, str]] = []   # (vid, present_lower, partner_lower, label)

    for pair_key, pair_info in _BRACKET_PAIR_INDEX.items():
        a_lower = pair_info["name_a_lower"]
        b_lower = pair_info["name_b_lower"]
        a_present = a_lower in present_name_set
        b_present = b_lower in present_name_set
        if not (a_present or b_present):
            continue

        vid = pair_info["variant_id"]
        label = pair_info["label"]

        if a_present and b_present:
            detected_raw.append((vid, a_lower, b_lower, label))
        elif a_present:
            missing_raw.append((vid, a_lower, b_lower, label))
        else:
            missing_raw.append((vid, b_lower, a_lower, label))

    if not detected_raw and not missing_raw:
        return empty

    # Second pass — resolve every name (lowercase) we'll surface in the
    # response to its canonical-cased name + oracle_id from the
    # snapshot. The bracket index already carries Spellbook-canonical
    # casing, so we lean on that for `*_name` fields; oracle_id has to
    # come from the snapshot.
    name_pool_lower: Set[str] = set()
    name_to_display: Dict[str, str] = {}
    for pair_key, pair_info in _BRACKET_PAIR_INDEX.items():
        if pair_info["name_a_lower"] in present_name_set or pair_info["name_b_lower"] in present_name_set:
            name_to_display.setdefault(pair_info["name_a_lower"], pair_info["name_a"])
            name_to_display.setdefault(pair_info["name_b_lower"], pair_info["name_b"])
    # Commander names always go through resolution so the v1.7.4
    # color-identity filter has its commander_ci even when the commander
    # doesn't itself appear in any combo pair (most decks).
    for cname in commander_name_set:
        name_pool_lower.add(cname)
        name_to_display.setdefault(cname, cname)
    for _, a_lower, b_lower, _ in detected_raw:
        name_pool_lower.add(a_lower)
        name_pool_lower.add(b_lower)
    for _, present_lower, partner_lower, _ in missing_raw:
        name_pool_lower.add(present_lower)
        name_pool_lower.add(partner_lower)

    # Resolve each name → oracle_id (single-card lookups against the
    # snapshot DB — same path the v1.7.2 layer used). Using the
    # canonical-cased name from the bracket index gives us the best
    # chance of hitting a row even when the user's deckText casing
    # diverges slightly. find_card_by_name is case-insensitive on
    # `name`, so either casing works.
    name_lower_to_oracle: Dict[str, str] = {}
    for name_lower in name_pool_lower:
        display = name_to_display.get(name_lower) or name_lower
        oid = resolve_name_to_oracle_id(snapshot_id, display)
        if oid is None and display != name_lower:
            # Fall back to the literal lowercase form (case-insensitive
            # match still works) — covers names not in the bracket
            # index's canonical casing.
            oid = resolve_name_to_oracle_id(snapshot_id, name_lower)
        if oid is not None:
            name_lower_to_oracle[name_lower] = oid

    # v1.7.4 — apply color-identity filter to missing_partners ONLY.
    # The commander color identity gates which partners are CR 903.4-
    # legal for this deck. detected_combos is by definition already in
    # the deck → presumed legal by being there.
    commander_oid_set: Set[str] = set()
    for name_lower in commander_name_set:
        oid = name_lower_to_oracle.get(name_lower)
        if oid is not None:
            commander_oid_set.add(oid)
    commander_ci = _resolve_commander_color_identity(snapshot_id, commander_oid_set)

    # Resolve partner color_identity in one batch DB lookup for the
    # color-filter step. Partner = any unresolved missing-side oracle_id
    # plus the partner half of each missing_raw entry.
    partner_oid_pool: Set[str] = set()
    for _, _, partner_lower, _ in missing_raw:
        oid = name_lower_to_oracle.get(partner_lower)
        if oid is not None:
            partner_oid_pool.add(oid)
    partner_rows = lookup_cards_by_oracle_ids(
        conn=None,
        snapshot_id=snapshot_id,
        oracle_ids=partner_oid_pool,
        requested_fields=["oracle_id", "name", "color_identity"],
    )
    partner_ci: Dict[str, Set[str]] = {}
    for oracle_id, row in partner_rows.items():
        if not isinstance(row, dict):
            continue
        key = oracle_id.lower() if isinstance(oracle_id, str) else oracle_id
        partner_ci[key] = _normalize_color_identity_field(row.get("color_identity"))

    if commander_ci:
        missing_raw = [
            entry
            for entry in missing_raw
            if (
                (oid := name_lower_to_oracle.get(entry[2])) is not None
                and partner_ci.get(oid, set()).issubset(commander_ci)
            )
        ]

    # Build the response entries. Entries are dropped when oracle_id
    # resolution fails for either half — preserves the v1.7.2 contract
    # of skipping un-resolvable rows rather than emitting partial data.
    detected_entries: List[Dict[str, Any]] = []
    for vid, a_lower, b_lower, label in detected_raw:
        oid_a = name_lower_to_oracle.get(a_lower)
        oid_b = name_lower_to_oracle.get(b_lower)
        if oid_a is None or oid_b is None:
            continue
        name_a = name_to_display.get(a_lower, a_lower)
        name_b = name_to_display.get(b_lower, b_lower)
        # Stable ordering — sort the pair alphabetically by name so the
        # UI surface doesn't flip card_a/card_b across runs.
        first, second = sorted(
            [(name_a, oid_a), (name_b, oid_b)], key=lambda t: t[0]
        )
        detected_entries.append({
            "variant_id": vid,
            "card_a_name": first[0],
            "card_a_oracle_id": first[1],
            "card_b_name": second[0],
            "card_b_oracle_id": second[1],
            "combo_outcome_label": label,
        })

    missing_entries: List[Dict[str, Any]] = []
    for vid, present_lower, partner_lower, label in missing_raw:
        present_oid = name_lower_to_oracle.get(present_lower)
        partner_oid = name_lower_to_oracle.get(partner_lower)
        if present_oid is None or partner_oid is None:
            continue
        missing_entries.append({
            "variant_id": vid,
            "present_card_name": name_to_display.get(present_lower, present_lower),
            "present_card_oracle_id": present_oid,
            "partner_card_name": name_to_display.get(partner_lower, partner_lower),
            "partner_card_oracle_id": partner_oid,
            "combo_outcome_label": label,
        })

    detected_entries.sort(key=lambda e: (e["variant_id"], e["card_a_name"], e["card_b_name"]))
    missing_entries.sort(key=lambda e: (e["variant_id"], e["partner_card_name"]))

    return {
        "detected_combos_v1": detected_entries[:DECK_COMBO_INSIGHTS_MAX_ENTRIES],
        "missing_partners_v1": missing_entries[:DECK_COMBO_INSIGHTS_MAX_ENTRIES],
    }
