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
from typing import Any, Dict, List, Optional, Tuple


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


# ============================================================
# Phase B — Candidate pool with user-intent anchoring.
# ============================================================
#
# Builds a ranked candidate pool from three signals:
#   1. User `must_include_cards` (score = INF, locked).
#   2. `theme_top_cards_v1` results for each theme hint (theme-match score).
#   3. `commander_archetype_brief_v1.staple_cards` (descriptive baseline,
#      penalized by corpus frequency to avoid forcing staples that don't
#      match user intent — DESIGN_DECISIONS rule 1.1, creativity envelope).
#
# Combo expansion is explicitly NOT performed here: agent does not infer
# combo intent from a single user-pick anchor (Fix 2 from kickoff patch).
# Cards that complete a combo with a user pick are scored on their own
# theme/synergy merit, not boosted via co-occurrence with user picks.

USER_PICK_SCORE = float("inf")
THEME_MATCH_WEIGHT = 10.0          # per primitive overlapping with a theme
ARCHETYPE_STAPLE_BASELINE = 5.0    # small boost for "fits commander archetype"
FREQUENCY_PENALTY_THRESHOLD = 0.30  # corpus freq above this gets penalized
FREQUENCY_PENALTY_WEIGHT = 30.0    # how strongly to penalize common-corpus cards


def _normalize_color_identity(raw: Any) -> List[str]:
    """Normalize the various shapes color_identity can take in our DB
    (JSON-string-of-list, list, comma string) into a sorted uppercase list."""
    import json as _j
    if raw is None:
        return []
    if isinstance(raw, list):
        return sorted({c.upper() for c in raw if isinstance(c, str) and c})
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        # JSON list?
        if s.startswith("["):
            try:
                parsed = _j.loads(s)
                if isinstance(parsed, list):
                    return sorted({c.upper() for c in parsed if isinstance(c, str) and c})
            except Exception:
                pass
        # Comma-separated?
        return sorted({c.strip().upper() for c in s.split(",") if c.strip()})
    return []


def _validate_must_includes(
    must_include_cards: List[str],
    commander_color_identity: List[str],
    db_snapshot_id: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    """Validate each must-include for (a) existence in snapshot and (b) color-identity
    legality. Returns (resolved_cards, warnings). Warn-and-skip per Fix 4."""
    from engine.db import find_card_by_name
    commander_ci = set(commander_color_identity)
    resolved: List[Dict[str, Any]] = []
    warnings: List[Dict[str, str]] = []
    for raw_name in must_include_cards:
        name = (raw_name or "").strip()
        if not name:
            continue
        try:
            card = find_card_by_name(db_snapshot_id, name)
        except Exception as exc:
            warnings.append({
                "code": "MUST_INCLUDE_LOOKUP_FAILED",
                "message": f"{name!r}: {exc.__class__.__name__}: {exc}",
            })
            continue
        if card is None:
            warnings.append({
                "code": "MUST_INCLUDE_NOT_FOUND",
                "message": f"{name!r} not in snapshot {db_snapshot_id}; skipping.",
            })
            continue
        ci = _normalize_color_identity(card.get("color_identity"))
        if not set(ci).issubset(commander_ci):
            warnings.append({
                "code": "MUST_INCLUDE_COLOR_ILLEGAL",
                "message": (
                    f"{name!r} CI={ci} not subset of commander CI={sorted(commander_ci)}; "
                    f"skipping."
                ),
            })
            continue
        resolved.append({
            "name": card.get("name") or name,
            "card": card,
            "color_identity": ci,
        })
    return resolved, warnings


def _score_theme_candidate(theme_signal_count: int, frequency_in_corpus: float) -> float:
    """Score a card surfaced by theme_top_cards_v1.

    Higher theme_signal_count → higher score. Frequency penalty only kicks in
    above the threshold, and is dampened (50%) when the card has theme signal —
    a theme-matched staple is still useful, just not as exciting as a non-obvious
    theme match.
    """
    theme_bonus = float(theme_signal_count) * THEME_MATCH_WEIGHT
    freq_penalty = max(0.0, frequency_in_corpus - FREQUENCY_PENALTY_THRESHOLD) * FREQUENCY_PENALTY_WEIGHT
    # Theme-matched cards have their frequency penalty halved.
    if theme_signal_count > 0:
        freq_penalty *= 0.5
    return theme_bonus - freq_penalty


def _score_archetype_staple(frequency_in_corpus: float) -> float:
    """Score a card surfaced only by archetype staple-list (no theme match).

    Pure staples take the full frequency penalty — common-corpus cards with
    no theme synergy should NOT crowd out theme-aligned picks. This is the
    creativity-envelope rule made concrete: Sol Ring at 70% corpus frequency
    with zero theme overlap scores deeply negative.
    """
    base = ARCHETYPE_STAPLE_BASELINE
    freq_penalty = max(0.0, frequency_in_corpus - FREQUENCY_PENALTY_THRESHOLD) * FREQUENCY_PENALTY_WEIGHT
    return base - freq_penalty


def _build_candidate_pool(
    *,
    db_snapshot_id: str,
    commander: str,
    bracket: str,
    theme_hints: List[str],
    must_include_cards: List[str],
    seed: Optional[int],
    call_counter: Dict[str, int],
) -> Dict[str, Any]:
    """Compose archetype_brief + theme_top_cards into a ranked candidate pool.

    `call_counter` is a single-key mutable dict ({"calls": int}) so callers
    can enforce the ENDPOINT_CALL_BUDGET across the whole build. Each upstream
    layer call increments this counter.
    """
    from api.engine.layers.agent_endpoints_v1 import (
        compute_archetype_brief_v1,
        compute_theme_top_cards_v1,
    )

    warnings: List[Dict[str, str]] = []

    # ---- archetype brief: gives commander color_identity + corpus baseline ----
    brief = compute_archetype_brief_v1(db_snapshot_id=db_snapshot_id, commander=commander)
    call_counter["calls"] += 1
    color_identity = brief.get("color_identity") or []
    for w in brief.get("warnings", []) or []:
        warnings.append({"code": f"BRIEF_{w.get('code', 'WARNING')}", "message": w.get("message", "")})

    # Build a name → corpus-frequency map for the staples (used for penalty).
    staple_freq: Dict[str, float] = {}
    for s in brief.get("staple_cards", []) or []:
        n = s.get("name")
        if isinstance(n, str):
            staple_freq[n] = float(s.get("usage_pct") or 0.0)

    # ---- user must-includes: locked at INF, color-identity validated ----
    resolved_picks, mi_warnings = _validate_must_includes(
        must_include_cards, color_identity, db_snapshot_id
    )
    warnings.extend(mi_warnings)
    resolved_pick_names = {p["name"] for p in resolved_picks}

    # ---- theme_top_cards: one call per theme hint ----
    by_name: Dict[str, Dict[str, Any]] = {}

    def _upsert(name: str, *, score: float, source: str, rationale: str,
                primitives: List[str] = None, type_line: str = None, cmc: Optional[float] = None,
                color_identity_: List[str] = None, is_user_pick: bool = False) -> None:
        existing = by_name.get(name)
        rationale_components = list(existing["rationale_components"]) if existing else []
        if rationale and rationale not in rationale_components:
            rationale_components.append(rationale)
        new_score = max(existing["score"], score) if existing else score
        merged_sources = (existing["source"] + "|" + source) if existing and source not in existing["source"].split("|") else (existing["source"] if existing else source)
        by_name[name] = {
            "name": name,
            "score": new_score,
            "source": merged_sources,
            "rationale_components": rationale_components,
            "primitives": primitives or (existing.get("primitives") if existing else []) or [],
            "type_line": type_line or (existing.get("type_line") if existing else None),
            "cmc": cmc if cmc is not None else (existing.get("cmc") if existing else None),
            "color_identity": color_identity_ or (existing.get("color_identity") if existing else []) or [],
            "is_user_pick": existing["is_user_pick"] if existing else is_user_pick,
            "is_combo_half": existing.get("is_combo_half", False) if existing else False,
        }

    # Insert user picks first (score=INF).
    for pick in resolved_picks:
        card = pick["card"]
        _upsert(
            pick["name"],
            score=USER_PICK_SCORE,
            source="user_intent",
            rationale="User must_include_cards (locked, score=INF).",
            primitives=list(card.get("primitives") or []),
            type_line=card.get("type_line"),
            cmc=card.get("cmc"),
            color_identity_=pick["color_identity"],
            is_user_pick=True,
        )

    # Insert theme candidates.
    for hint in theme_hints:
        try:
            theme_result = compute_theme_top_cards_v1(
                db_snapshot_id=db_snapshot_id,
                theme_id=hint,
                color_identity=color_identity or None,
                limit=80,
            )
        except Exception as exc:
            warnings.append({
                "code": "THEME_TOP_CARDS_FAILED",
                "message": f"theme_hint={hint!r}: {exc.__class__.__name__}: {exc}",
            })
            continue
        call_counter["calls"] += 1
        for w in theme_result.get("warnings", []) or []:
            warnings.append({"code": f"THEME_{w.get('code', 'WARNING')}", "message": f"hint={hint}: {w.get('message', '')}"})
        for r in theme_result.get("results", []) or []:
            name = r.get("name")
            if not isinstance(name, str):
                continue
            tsc = int(r.get("theme_signal_count") or 0)
            freq = staple_freq.get(name, 0.0)
            score = _score_theme_candidate(tsc, freq)
            _upsert(
                name,
                score=score,
                source=f"theme:{hint}",
                rationale=f"Theme '{hint}' signal_count={tsc} (freq_in_corpus={freq:.2f}).",
                primitives=list(r.get("primitives") or []),
                type_line=r.get("type_line"),
                cmc=r.get("cmc"),
            )

    # Insert archetype staples (descriptive baseline, frequency-penalized).
    for s in brief.get("staple_cards", []) or []:
        name = s.get("name")
        if not isinstance(name, str):
            continue
        if name in by_name:
            # Already scored via theme or user-pick; the theme path uses freq too.
            continue
        freq = float(s.get("usage_pct") or 0.0)
        score = _score_archetype_staple(freq)
        _upsert(
            name,
            score=score,
            source="archetype_staple",
            rationale=f"Corpus staple for {commander} (usage_pct={freq:.2f}).",
        )

    # Deterministic tie-break. When seed is provided, hash(name, seed) gives a
    # stable seed-dependent ordering for equal-score candidates without
    # depending on the non-deterministic `random` module (which is banned in
    # engine runtime modules by test_no_random_imports).
    candidates = list(by_name.values())

    def _sort_key(c: Dict[str, Any]) -> Tuple[float, int, str]:
        score = c["score"]
        # User picks (score=INF) sort to the top regardless of tie-break.
        primary = float("-inf") if score == USER_PICK_SCORE else -score
        if seed is None:
            tie = 0
        else:
            # hashlib gives a stable digest across processes; built-in hash() is
            # randomized per-interpreter by PYTHONHASHSEED.
            import hashlib as _hl
            digest = _hl.sha1(f"{c['name']}|{seed}".encode("utf-8")).digest()
            tie = int.from_bytes(digest[:8], "big")
        return (primary, tie, c["name"])

    candidates.sort(key=_sort_key)

    return {
        "candidates": candidates,
        "color_identity": color_identity,
        "archetype_brief": brief,
        "must_includes_resolved": [p["name"] for p in resolved_picks],
        "must_includes_dropped": [w["message"].split("'")[1] for w in mi_warnings if "'" in w.get("message", "")],
        "warnings": warnings,
        "endpoint_calls": call_counter["calls"],
    }
