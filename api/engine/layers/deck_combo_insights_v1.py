"""v1.7.2 Stage 1 — Deck-combo insight engine layer.

Scans the FINAL completed deck (commander + initial deckText + engine
adds) against the v2 pair index + outcome pack and surfaces TWO
insight surfaces consumed by the UI's `DeckCombosPanel`:

  - `detected_combos_v1`  — pairs where BOTH halves are present.
  - `missing_partners_v1` — pairs where EXACTLY ONE half is present
    AND the partner is NOT being added (and therefore would be a
    productive add the user could make).

Pre-v1.7.2, the Stage 2 `combo_enabler_reasons_v1` layer covered
the narrow "engine added a partner, flag it on that row" case. The
v1.7 Cowork browser-walk (2026-05-16) confirmed near-zero production
coverage from that layer — the engine's primitive-coverage-driven
completion rarely picks combo partners on its own. v1.7.2's insights
panel surfaces combos regardless of how cards entered the deck.

Engine-side contract (NEW, additive — does not modify v1.7
combo_enabler / bracket_aware contracts):

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

from typing import Any, Dict, List, Optional, Set, Tuple

from api.engine.layers._combo_data_loader import (
    load_outcomes,
    load_two_card_pair_index,
    normalize_card_name,
    resolve_name_to_oracle_id,
    resolve_oracle_ids_to_names,
)
# v1.7.4: direct lookup for color_identity in addition to name. The
# shared _combo_data_loader.resolve_oracle_ids_to_names is SHA-locked,
# so we go through db_cards directly for the extended field set rather
# than extending the loader signature.
from api.engine.db_cards import lookup_cards_by_oracle_ids


DECK_COMBO_INSIGHTS_V1_VERSION = "deck_combo_insights_v1"
DECK_COMBO_INSIGHTS_MAX_ENTRIES = 25


# Module-import-time load (frozen for the process lifetime), same
# pattern as combo_enabler_reasons_v1 — keeps per-request work to
# pure-Python index lookups.
_PAIR_INDEX = load_two_card_pair_index()
_OUTCOMES = load_outcomes()


def _select_variant_with_outcome(variant_ids: List[str]) -> Optional[str]:
    """Take the first variant_id in `variant_ids` that has an outcome
    record in the pack. Returns None if no variant has an outcome
    (i.e. the pair predates the Stage 1.5 dump or was removed)."""
    for vid in variant_ids:
        if vid in _OUTCOMES:
            return vid
    return None


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
                import json as _json
                parsed = _json.loads(token)
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
    no outcomes, no resolvable deck cards) — never raises.
    """
    empty: Dict[str, List[Dict[str, Any]]] = {
        "detected_combos_v1": [],
        "missing_partners_v1": [],
    }

    snapshot_id = db_snapshot_id if isinstance(db_snapshot_id, str) and db_snapshot_id.strip() else ""
    if snapshot_id == "" or not _PAIR_INDEX or not _OUTCOMES:
        return empty

    # Build the present-set as oracle_ids (commander + deck cards).
    name_inputs: List[str] = []
    if isinstance(commander_names, list):
        for n in commander_names:
            norm = normalize_card_name(n)
            if norm is not None:
                name_inputs.append(norm)
    if isinstance(deck_cards_after_completion, list):
        for n in deck_cards_after_completion:
            norm = normalize_card_name(n)
            if norm is not None:
                name_inputs.append(norm)
    if not name_inputs:
        return empty

    present_oracle_to_name: Dict[str, str] = {}
    for name in name_inputs:
        oid = resolve_name_to_oracle_id(snapshot_id, name)
        if oid is None or oid in present_oracle_to_name:
            continue
        present_oracle_to_name[oid] = name

    if not present_oracle_to_name:
        return empty

    present_set: Set[str] = set(present_oracle_to_name.keys())

    detected_raw: List[Tuple[str, str, str, str]] = []  # (variant_id, oid_lo, oid_hi, label)
    missing_raw: List[Tuple[str, str, str, str]] = []   # (variant_id, present_oid, partner_oid, label)

    for pair_key, variant_ids in _PAIR_INDEX.items():
        oid_a, oid_b = sorted(pair_key)
        a_present = oid_a in present_set
        b_present = oid_b in present_set
        if not (a_present or b_present):
            continue

        vid = _select_variant_with_outcome(variant_ids)
        if vid is None:
            continue
        outcome = _OUTCOMES.get(vid) or {}
        label = outcome.get("label")
        if not isinstance(label, str) or label.strip() == "":
            continue

        if a_present and b_present:
            detected_raw.append((vid, oid_a, oid_b, label))
        elif a_present:
            missing_raw.append((vid, oid_a, oid_b, label))
        else:
            missing_raw.append((vid, oid_b, oid_a, label))

    if not detected_raw and not missing_raw:
        return empty

    # v1.7.4 — apply color-identity filter to missing_partners ONLY.
    # detected_combos is by definition already in the deck → presumed
    # legal by being there. Filter: a candidate partner_oid is legal
    # iff its color_identity ⊆ commander_color_identity (CR 903.4).
    # The filter is SUBTRACTIVE — it can only REDUCE the missing
    # count, never add (calibration sanity invariant from v1.7.4 spec).
    commander_oid_set: Set[str] = set()
    for oid, name in present_oracle_to_name.items():
        if isinstance(commander_names, list) and name in [
            normalize_card_name(n) for n in commander_names if isinstance(n, str)
        ]:
            commander_oid_set.add(oid)
    commander_ci = _resolve_commander_color_identity(snapshot_id, commander_oid_set)

    # Resolve partner oracle_ids → {name, color_identity} in one batch
    # DB lookup. Color identity is the v1.7.4 addition; name resolution
    # is unchanged from v1.7.2.
    partner_oid_pool: Set[str] = set()
    for _, oid_a, oid_b, _ in detected_raw:
        if oid_a not in present_oracle_to_name:
            partner_oid_pool.add(oid_a)
        if oid_b not in present_oracle_to_name:
            partner_oid_pool.add(oid_b)
    for _, _, partner_oid, _ in missing_raw:
        partner_oid_pool.add(partner_oid)
    partner_rows = lookup_cards_by_oracle_ids(
        conn=None,
        snapshot_id=snapshot_id,
        oracle_ids=partner_oid_pool,
        requested_fields=["oracle_id", "name", "color_identity"],
    )
    extra_names: Dict[str, str] = {}
    partner_ci: Dict[str, Set[str]] = {}
    for oracle_id, row in partner_rows.items():
        if not isinstance(row, dict):
            continue
        name = row.get("name")
        if isinstance(name, str) and name.strip() != "":
            extra_names[oracle_id.lower() if isinstance(oracle_id, str) else oracle_id] = name.strip()
        partner_ci[oracle_id.lower() if isinstance(oracle_id, str) else oracle_id] = (
            _normalize_color_identity_field(row.get("color_identity"))
        )

    # v1.7.4 — apply the filter to missing_raw ONLY. If the commander
    # CI is empty (unresolvable / colorless), skip filtering rather
    # than over-restricting (preserves v1.7.2 backward-compat for
    # snapshots without commander color metadata).
    if commander_ci:
        missing_raw = [
            entry for entry in missing_raw
            if partner_ci.get(entry[2], set()).issubset(commander_ci)
        ]

    # Merge — present names take precedence (they came from the user's
    # actual deck input, preserving casing).
    oracle_to_name: Dict[str, str] = dict(extra_names)
    oracle_to_name.update(present_oracle_to_name)

    detected_entries: List[Dict[str, Any]] = []
    for vid, oid_a, oid_b, label in detected_raw:
        name_a = oracle_to_name.get(oid_a)
        name_b = oracle_to_name.get(oid_b)
        if name_a is None or name_b is None:
            continue
        # Stable ordering — sort the pair alphabetically by name so the
        # UI surface doesn't flip card_a/card_b across runs.
        first, second = sorted([(name_a, oid_a), (name_b, oid_b)], key=lambda t: t[0])
        detected_entries.append({
            "variant_id": vid,
            "card_a_name": first[0],
            "card_a_oracle_id": first[1],
            "card_b_name": second[0],
            "card_b_oracle_id": second[1],
            "combo_outcome_label": label,
        })

    missing_entries: List[Dict[str, Any]] = []
    for vid, present_oid, partner_oid, label in missing_raw:
        present_name = oracle_to_name.get(present_oid)
        partner_name = oracle_to_name.get(partner_oid)
        if present_name is None or partner_name is None:
            continue
        missing_entries.append({
            "variant_id": vid,
            "present_card_name": present_name,
            "present_card_oracle_id": present_oid,
            "partner_card_name": partner_name,
            "partner_card_oracle_id": partner_oid,
            "combo_outcome_label": label,
        })

    detected_entries.sort(key=lambda e: (e["variant_id"], e["card_a_name"], e["card_b_name"]))
    missing_entries.sort(key=lambda e: (e["variant_id"], e["partner_card_name"]))

    return {
        "detected_combos_v1": detected_entries[:DECK_COMBO_INSIGHTS_MAX_ENTRIES],
        "missing_partners_v1": missing_entries[:DECK_COMBO_INSIGHTS_MAX_ENTRIES],
    }
