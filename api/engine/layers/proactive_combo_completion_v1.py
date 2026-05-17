"""v1.7.3 Stage 1 — Proactive combo completion engine layer.

When the user's deck contains exactly one half of a known v2 combo
pair AND the missing partner is color-identity-legal under the
commander AND the bracket allows combos, this layer proposes the
partner as a proactive add. The pipeline integration (Stage 2) then
injects each proposal into `added_cards_v1` so:

  - The existing `combo_enabler_reasons_v1` annotator naturally
    attaches a COMBO_ENABLER chip to the proactive row.
  - The existing `compute_deck_combo_insights_v1` re-classifies the
    pair as detected_combos_v1 (no longer missing).

Three v1.7 layers cascade cleanly with zero contract changes.

Bracket gating mirrors v1.7 Stage 4's bracket-aware GC pattern:

    B1, B2 → 0  (combos DISALLOW per bracket_rules_v2.json)
    B3     → 1
    B4     → 2
    B5     → 3
    other  → 0

Calibration boundary intact — no bracket policy / primitive-coverage
thresholds / GC pool / combo data / commander pool modifications.

Sort order for ties: the outcome pack carries no `popularity` field
(v1.7.2 Stage 0 audit), so we fall back to lex `variant_id asc` as
the spec's documented secondary key. Future v1.7.4 may regenerate the
pack with popularity weights.
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
from api.engine.db_cards import lookup_cards_by_oracle_ids


PROACTIVE_COMBO_COMPLETION_V1_VERSION = "proactive_combo_completion_v1"
PROACTIVE_COMBO_REASON_CODE = "PROACTIVE_COMBO_TARGET"

# Bracket-scaled cap: number of proactive partners the layer will
# propose. Matches the v1.7 Stage 4 bracket-aware GC scaling pattern
# (B1/B2 disallow combos per bracket_rules_v2.json policy).
_PER_BRACKET_TARGET: Dict[str, int] = {
    "B1": 0,
    "B2": 0,
    "B3": 1,
    "B4": 2,
    "B5": 3,
}


# Module-import-time load (frozen for process lifetime); same pattern
# as combo_enabler_reasons_v1 + deck_combo_insights_v1.
_PAIR_INDEX = load_two_card_pair_index()
_OUTCOMES = load_outcomes()


def _select_variant_with_outcome(variant_ids: List[str]) -> Optional[str]:
    for vid in variant_ids:
        if vid in _OUTCOMES:
            return vid
    return None


def _normalize_commander_color_identity(value: Any) -> Set[str]:
    """Accept either a `set` of color letters or a fallback type
    (None / non-set means "skip color filter"); returns a normalized
    upper-case set. Empty set → colorless commander → only colorless
    candidates allowed."""
    if isinstance(value, set):
        out: Set[str] = set()
        for c in value:
            if isinstance(c, str):
                t = c.strip().upper()
                if t in {"W", "U", "B", "R", "G"}:
                    out.add(t)
        return out
    return set()


def _candidate_color_identity_legal(card_ci: Set[str], commander_ci: Set[str]) -> bool:
    """A candidate is color-legal iff its color identity is a SUBSET of
    the commander's color identity (per CR 903.4 — Commander color
    identity rules). Colorless candidates (empty set) are legal under
    any commander."""
    return card_ci.issubset(commander_ci)


def _normalize_color_identity_field(value: Any) -> Set[str]:
    """Parse a card's color_identity stored as JSON-array string,
    list, or set into a normalized upper-case `set`."""
    if value is None:
        return set()
    if isinstance(value, (list, tuple, set)):
        out: Set[str] = set()
        for c in value:
            if isinstance(c, str):
                t = c.strip().upper()
                if t in {"W", "U", "B", "R", "G"}:
                    out.add(t)
        return out
    if isinstance(value, str):
        token = value.strip()
        if token.startswith("[") and token.endswith("]"):
            try:
                import json
                parsed = json.loads(token)
                if isinstance(parsed, list):
                    return _normalize_color_identity_field(parsed)
            except (ValueError, TypeError):
                pass
        out = set()
        for c in token:
            if c.upper() in {"W", "U", "B", "R", "G"}:
                out.add(c.upper())
        return out
    return set()


def propose_proactive_combo_partners_v1(
    *,
    db_snapshot_id: str,
    commander_names: List[str],
    deck_cards: List[str],
    current_added_cards_v1: List[Dict[str, Any]],
    bracket_id: str,
    commander_color_identity: Any = None,
) -> List[Dict[str, Any]]:
    """Compute proactive combo-partner proposals for the deck.

    Returns a list of `ProposedComboAdd` dicts:
        {
          "partner_card_name": str,
          "partner_card_oracle_id": str,
          "variant_id": str,
          "present_card_name": str,
          "present_card_oracle_id": str,
          "combo_outcome_label": str,
        }

    SIDE-EFFECT-FREE — caller is responsible for injecting proposals
    into added_cards_v1 / working_cards. Returns [] (never raises) when
    any precondition fails (empty snapshot id, missing data, B1/B2
    bracket cap, no legal candidates, no commander color identity).
    """
    snapshot_id = db_snapshot_id if isinstance(db_snapshot_id, str) and db_snapshot_id.strip() else ""
    if snapshot_id == "" or not _PAIR_INDEX or not _OUTCOMES:
        return []

    bracket_token = bracket_id.strip() if isinstance(bracket_id, str) else ""
    target = _PER_BRACKET_TARGET.get(bracket_token, 0)
    if target <= 0:
        return []

    commander_ci = _normalize_commander_color_identity(commander_color_identity)
    if not commander_ci:
        # Unknown commander color identity — skip rather than guess. The
        # main pipeline already surfaces UNKNOWN_COLOR_IDENTITY upstream.
        return []

    # Build present_oracle_ids from commander_names + deck_cards +
    # current_added_cards_v1's `name` field. Each name → oracle_id via
    # the DB; un-resolvable names are skipped (the layer is best-effort,
    # not authoritative on deck legality).
    name_inputs: List[str] = []
    name_to_origin: Dict[str, str] = {}  # name → "commander" / "deck" / "added"
    if isinstance(commander_names, list):
        for n in commander_names:
            norm = normalize_card_name(n)
            if norm is not None and norm not in name_to_origin:
                name_inputs.append(norm)
                name_to_origin[norm] = "commander"
    if isinstance(deck_cards, list):
        for n in deck_cards:
            norm = normalize_card_name(n)
            if norm is not None and norm not in name_to_origin:
                name_inputs.append(norm)
                name_to_origin[norm] = "deck"
    if isinstance(current_added_cards_v1, list):
        for row in current_added_cards_v1:
            if not isinstance(row, dict):
                continue
            norm = normalize_card_name(row.get("name"))
            if norm is not None and norm not in name_to_origin:
                name_inputs.append(norm)
                name_to_origin[norm] = "added"
    if not name_inputs:
        return []

    present_oracle_to_name: Dict[str, str] = {}
    for name in name_inputs:
        oid = resolve_name_to_oracle_id(snapshot_id, name)
        if oid is None or oid in present_oracle_to_name:
            continue
        present_oracle_to_name[oid] = name
    if not present_oracle_to_name:
        return []

    present_set: Set[str] = set(present_oracle_to_name.keys())

    # Find pairs where EXACTLY ONE side is present and the OTHER is a
    # candidate the layer might add. Same scan pattern as
    # compute_deck_combo_insights_v1's missing-partner branch.
    candidate_proposals: List[Tuple[str, str, str, str]] = []  # (vid, present_oid, candidate_oid, label)
    candidate_oid_pool: Set[str] = set()

    for pair_key, variant_ids in _PAIR_INDEX.items():
        oid_a, oid_b = sorted(pair_key)
        a_present = oid_a in present_set
        b_present = oid_b in present_set
        if a_present == b_present:
            # Both present → already a detected combo; not our concern.
            # Both absent → nothing in the deck for this pair; skip.
            continue
        present_oid = oid_a if a_present else oid_b
        candidate_oid = oid_b if a_present else oid_a

        vid = _select_variant_with_outcome(variant_ids)
        if vid is None:
            continue
        outcome = _OUTCOMES.get(vid) or {}
        label = outcome.get("label")
        if not isinstance(label, str) or label.strip() == "":
            continue

        candidate_proposals.append((vid, present_oid, candidate_oid, label.strip()))
        candidate_oid_pool.add(candidate_oid)

    if not candidate_proposals:
        return []

    # Look up candidate name + color identity in one batched DB call.
    raw_candidate_rows = lookup_cards_by_oracle_ids(
        conn=None,
        snapshot_id=snapshot_id,
        oracle_ids=candidate_oid_pool,
        requested_fields=["oracle_id", "name", "color_identity"],
    )

    # Filter by color identity + dedupe by candidate_oid (multiple v2
    # pairs may suggest the same partner — keep the first variant_id
    # the candidate appeared with, which is also the lex-min vid since
    # we'll sort later).
    deduped: Dict[str, Tuple[str, str, str, str]] = {}  # candidate_oid → (vid, present_oid, candidate_oid, label)
    candidate_names: Dict[str, str] = {}
    for vid, present_oid, candidate_oid, label in candidate_proposals:
        if candidate_oid in present_set:
            # Already in the deck (shouldn't happen given the gate
            # above, but defensive — never recommend cards already
            # present).
            continue
        card_row = raw_candidate_rows.get(candidate_oid)
        if not isinstance(card_row, dict):
            continue
        card_name_raw = card_row.get("name")
        if not isinstance(card_name_raw, str) or card_name_raw.strip() == "":
            continue
        candidate_ci = _normalize_color_identity_field(card_row.get("color_identity"))
        if not _candidate_color_identity_legal(candidate_ci, commander_ci):
            continue
        candidate_names[candidate_oid] = card_name_raw.strip()

        existing = deduped.get(candidate_oid)
        if existing is None or vid < existing[0]:
            deduped[candidate_oid] = (vid, present_oid, candidate_oid, label)

    if not deduped:
        return []

    # Resolve present-side names (commander/deck/added) for proposal
    # payloads. present_oracle_to_name already covers commander + deck
    # names; for any present_oid not in that map (shouldn't happen but
    # defensive), batch-resolve.
    missing_present_oids = {
        present_oid for _, present_oid, _, _ in deduped.values()
        if present_oid not in present_oracle_to_name
    }
    if missing_present_oids:
        extra_names = resolve_oracle_ids_to_names(snapshot_id, missing_present_oids)
        present_oracle_to_name = {**extra_names, **present_oracle_to_name}

    # Sort by variant_id asc (no popularity field in outcome pack —
    # v1.7.2 Stage 0 audit); take top `target`. Spec's documented
    # fallback ordering.
    sorted_proposals = sorted(deduped.values(), key=lambda t: t[0])
    chosen = sorted_proposals[:target]

    return [
        {
            "partner_card_name": candidate_names[candidate_oid],
            "partner_card_oracle_id": candidate_oid,
            "variant_id": vid,
            "present_card_name": present_oracle_to_name.get(present_oid, ""),
            "present_card_oracle_id": present_oid,
            "combo_outcome_label": label,
        }
        for vid, present_oid, candidate_oid, label in chosen
    ]
