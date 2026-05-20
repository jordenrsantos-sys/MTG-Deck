"""
agent_build_deck_v1 — Pillar D: AI deck-building agent.

Takes user intent (commander + bracket + theme hints + must-include cards) and
returns a 99-card deck with per-card reasoning, respecting the creativity envelope
(user picks dominate, no forced staples that don't match user intent).

Architectural rules served:
  - 1.1 Creativity envelope: user `must_include_cards` are locked (score=∞).
    Common-corpus staples get a frequency penalty unless they synergize with
    user picks or theme_hints. Agent does NOT auto-expand combo chains from
    a single user-pick anchor.
  - 1.2 Speed budget: target <15s end-to-end per build (Phase F target),
    bounded by an endpoint-call budget of <=30 inter-layer calls.
  - 1.3 Audit: each card carries a non-empty `reason` string.

Phase A (this commit): stub implementation. Returns commander + 99 Wastes
(colorless basic, always color-identity-legal) so the request/response contract
is testable end-to-end before the selection algorithm exists in Phases B-D.
"""
from __future__ import annotations

from time import perf_counter
from typing import Any, Dict, List, Optional


AGENT_BUILD_DECK_VERSION = "agent_build_deck_v1.0"

VALID_BRACKETS = ("B1", "B2", "B3", "B4", "B5")

# Per-bracket combo policy (Fix 1 from kickoff patch).
# Used by Phase C selection; encoded here so the policy lives next to the agent.
#   B1/B2: reject all 2-card combo halves entirely.
#   B3   : allow ONLY late combos (Spellbook tags S, P); reject early (R).
#   B4   : allow early + late combos, cap at 3 distinct 2-card pairs.
#   B5   : no restriction.
# User `must_include_cards` always override these caps — the policy applies
# only to AGENT-CHOSEN cards.
BRACKET_COMBO_POLICY: Dict[str, Dict[str, Any]] = {
    "B1": {"allow_early": False, "allow_late": False, "pair_cap": 0},
    "B2": {"allow_early": False, "allow_late": False, "pair_cap": 0},
    "B3": {"allow_early": False, "allow_late": True, "pair_cap": None},
    "B4": {"allow_early": True, "allow_late": True, "pair_cap": 3},
    "B5": {"allow_early": True, "allow_late": True, "pair_cap": None},
}

# Phase D endpoint-call budget (Fix 5 from kickoff patch).
ENDPOINT_CALL_BUDGET = 30
# Phase D inner swap-iteration cap (Fix 4 from kickoff patch).
MAX_SWAP_ITERATIONS = 12


def compute_agent_build_deck_v1(
    *,
    db_snapshot_id: str,
    commander: str,
    bracket: str,
    theme_hints: Optional[List[str]] = None,
    must_include_cards: Optional[List[str]] = None,
    max_iterations: int = 5,
    seed: Optional[int] = None,
) -> Dict[str, Any]:
    """Build a 99-card deck for `commander` honoring user intent.

    Args:
        db_snapshot_id: Required snapshot ID for card / corpus / taxonomy reads.
        commander: Commander card name (exact match).
        bracket: One of B1..B5.
        theme_hints: Theme IDs or human-readable theme labels to bias toward.
        must_include_cards: Card names that MUST appear in the output deck.
        max_iterations: Hint for outer build retries (rarely exceeded; Phase D's
            internal swap loop is bounded separately at MAX_SWAP_ITERATIONS=12).
        seed: Optional deterministic tie-break seed for candidate ordering.

    Returns:
        Response dict matching the AgentBuildDeckV1Response Pydantic shape.

    Phase A: stub — commander + 99 Wastes. The candidate-pool / selection /
    validation logic lands in Phases B / C / D respectively.
    """
    t_start = perf_counter()
    theme_hints = list(theme_hints or [])
    must_include_cards = list(must_include_cards or [])
    warnings: List[Dict[str, str]] = []

    if bracket not in VALID_BRACKETS:
        return {
            "version": AGENT_BUILD_DECK_VERSION,
            "status": "FAILED",
            "deck": [],
            "summary": _empty_summary(bracket, must_include_cards),
            "warnings": [{
                "code": "INVALID_BRACKET",
                "message": f"bracket must be one of {list(VALID_BRACKETS)}, got {bracket!r}",
            }],
            "elapsed_ms": int((perf_counter() - t_start) * 1000),
        }

    if not isinstance(commander, str) or not commander.strip():
        return {
            "version": AGENT_BUILD_DECK_VERSION,
            "status": "FAILED",
            "deck": [],
            "summary": _empty_summary(bracket, must_include_cards),
            "warnings": [{
                "code": "MISSING_COMMANDER",
                "message": "commander is required and must be a non-empty string",
            }],
            "elapsed_ms": int((perf_counter() - t_start) * 1000),
        }

    deck: List[Dict[str, str]] = [{
        "card_name": commander.strip(),
        "reason": "Commander (locked by user intent).",
        "source": "user_intent",
    }]
    for _ in range(99):
        deck.append({
            "card_name": "Wastes",
            "reason": "Phase A stub filler (basic land, always color-identity-legal).",
            "source": "phase_a_stub",
        })

    summary = _empty_summary(bracket, must_include_cards)
    warnings.append({
        "code": "PHASE_A_STUB",
        "message": (
            "Phase A scaffold returns commander + 99 Wastes. Real candidate-pool / "
            "selection / validation logic lands in Phases B-D."
        ),
    })

    return {
        "version": AGENT_BUILD_DECK_VERSION,
        "status": "OK",
        "deck": deck,
        "summary": summary,
        "warnings": warnings,
        "elapsed_ms": int((perf_counter() - t_start) * 1000),
    }


def _empty_summary(bracket: str, must_include_cards: List[str]) -> Dict[str, Any]:
    return {
        "themes_classified": [],
        "bracket_placement": bracket,
        "color_identity": [],
        "strength_check": None,
        "creativity_envelope_metrics": {
            "user_picks_present": 0,
            "user_picks_total": len(must_include_cards),
            "staples_avoided_count": 0,
            "theme_coherence_score": 0.0,
        },
        "endpoint_call_count": 0,
        "phase_timings_ms": {"pool": 0, "select": 0, "validate": 0},
    }
