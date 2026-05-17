"""
complete_bracket_violations_v1 — bracket-combo compliance check for Complete responses.

Background:
  v1.7 added three combo layers (proactive_combo_completion_v1 + combo_enabler_reasons_v1
  + deck_combo_insights_v1). v1.7.3 proactively completes combos at B3+ and gates B1/B2
  to zero proactive adds. BUT — if the user's INPUT deck already contains a 2-card combo
  (Storm-Kiln Artist + Haze of Rage) and they submit at bracket B1 or B2, the Complete
  pipeline previously returned status:OK with zero violations. The deck was effectively
  certified as bracket-legal when it wasn't.

  profile_bracket_enforcement_v1.py has the relevant detection (TWO_CARD_COMBOS_DISALLOWED
  constant + detect_two_card_combos import) but runs only in pipeline_build.py — the
  Complete pipeline never invokes it. This layer closes that gap WITHOUT modifying the
  Engine-4A frozen file: it consumes the existing detected_combos_v1 output (which is
  populated by v1.7.2's deck_combo_insights_v1) and emits violations_v1 entries when the
  request's bracket disallows the combos.

Bracket policy (per Wizards' Commander Brackets system):
  B1 (Exhibition): no 2-card infinite combos at all
  B2 (Core): no 2-card combos that win the game / are infinite
  B3 (Upgraded): combos allowed but limited
  B4 (Optimized): combos allowed
  B5 (cEDH): combos allowed

This layer is calibration-honest about scope: it flags ALL detected combos at B1/B2
(the strictest interpretation), letting the user decide whether their specific combo
is "game-winning" or "infinite". Future refinement can scope to specific outcome
labels (e.g., only flag combos whose outcome_label contains "Infinite" or "Win the game").

Side-effect-free pure function. No data file modifications. No network.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


# Brackets that disallow 2-card combos. Used as the canonical gate set.
_COMBO_DISALLOWING_BRACKETS = frozenset({"B1", "B2"})

# Violation code prefix per bracket — matches the established Engine-4A pattern
# (TWO_CARD_COMBOS_DISALLOWED) but with bracket-specific suffix for granular triage.
_VIOLATION_CODE_BY_BRACKET = {
    "B1": "TWO_CARD_COMBOS_DISALLOWED_B1",
    "B2": "TWO_CARD_COMBOS_DISALLOWED_B2",
}


def _normalize_bracket(value: Any) -> str:
    """Canonicalize bracket_id to the engine's standard B1-B5 form."""
    if not isinstance(value, str):
        return ""
    token = value.strip().upper()
    if token in {"B1", "B2", "B3", "B4", "B5"}:
        return token
    return ""


def _is_combo_disallowed_bracket(bracket_id: str) -> bool:
    return bracket_id in _COMBO_DISALLOWING_BRACKETS


def _build_violation(
    bracket_id: str,
    combo_entry: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Construct a single violation_v1 entry from a detected_combo entry."""
    if not isinstance(combo_entry, dict):
        return None
    code = _VIOLATION_CODE_BY_BRACKET.get(bracket_id)
    if code is None:
        return None
    card_a = combo_entry.get("card_a_name") or ""
    card_b = combo_entry.get("card_b_name") or ""
    outcome = combo_entry.get("combo_outcome_label") or ""
    variant_id = combo_entry.get("variant_id") or ""
    if not isinstance(card_a, str) or not isinstance(card_b, str):
        return None
    message_parts = [
        f"Bracket {bracket_id} disallows 2-card combos.",
        f"Detected: {card_a} + {card_b}",
    ]
    if isinstance(outcome, str) and outcome.strip() != "":
        message_parts.append(f"→ {outcome}")
    return {
        "code": code,
        "message": " ".join(message_parts),
        "category": "two_card_combos",
        "card_a_name": card_a,
        "card_b_name": card_b,
        "combo_outcome_label": outcome,
        "variant_id": variant_id,
        "bracket_id": bracket_id,
    }


def compute_complete_bracket_violations_v1(
    *,
    bracket_id: Any,
    detected_combos_v1: Any,
) -> Dict[str, Any]:
    """
    Public entrypoint. Returns:
      {
        "violations_v1": List[ViolationDict],
        "deck_status_override": Optional[str],   # "BRACKET_VIOLATION" when violations exist
      }

    deck_status_override is set when ANY violation is emitted. The caller (Complete
    pipeline) should downgrade the response's top-level `status` field from "OK" to
    this value so the UI knows the deck is not bracket-legal.
    """
    bracket_norm = _normalize_bracket(bracket_id)
    if not _is_combo_disallowed_bracket(bracket_norm):
        return {
            "violations_v1": [],
            "deck_status_override": None,
        }
    if not isinstance(detected_combos_v1, list) or len(detected_combos_v1) == 0:
        return {
            "violations_v1": [],
            "deck_status_override": None,
        }

    violations: List[Dict[str, Any]] = []
    for entry in detected_combos_v1:
        v = _build_violation(bracket_norm, entry)
        if v is not None:
            violations.append(v)

    if not violations:
        return {
            "violations_v1": [],
            "deck_status_override": None,
        }

    return {
        "violations_v1": violations,
        "deck_status_override": "BRACKET_VIOLATION",
    }
