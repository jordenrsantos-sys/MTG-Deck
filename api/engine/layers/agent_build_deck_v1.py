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
    skip_strength_check: bool = False,
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

    call_counter: Dict[str, int] = {"calls": 0}
    phase_timings_ms: Dict[str, int] = {"pool": 0, "select": 0, "validate": 0}

    # ---- Pillar D iteration 2: probe LLM client availability ----
    # If the SDK isn't installed or ANTHROPIC_API_KEY isn't set, every
    # iteration-2 LLM augmentation phase (B2 / C2.1 / C2.2 / D2) is a
    # no-op and we fall back to iteration-1 deterministic behavior with
    # a clear warning. Iteration 1's success criteria (5/5 test cases)
    # are preserved end-to-end without the API key, so this is a clean
    # development-mode path.
    from api.engine.layers.agent_llm_client_v1 import get_default_client as _get_llm_client
    llm_client = _get_llm_client()
    llm_metrics: Dict[str, Any] = {
        "available": llm_client.is_available(),
        "model": llm_client.model,
        "calls": [],   # one entry per LLM call: phase, ok, input_tokens, output_tokens, cost, latency_ms
    }
    if not llm_client.is_available():
        warnings.append({
            "code": "LLM_LAYER_UNAVAILABLE",
            "message": (
                "Pillar D iteration 2 LLM reasoning layer is unavailable; "
                "falling back to iteration 1 deterministic behavior. "
                + llm_client.unavailable_reason()
            ),
        })

    # ---- Phase B: build the candidate pool ----
    t_pool = perf_counter()
    try:
        pool = _build_candidate_pool(
            db_snapshot_id=db_snapshot_id,
            commander=commander.strip(),
            bracket=bracket,
            theme_hints=theme_hints,
            must_include_cards=must_include_cards,
            seed=seed,
            call_counter=call_counter,
        )
    except Exception as exc:
        return {
            "version": AGENT_BUILD_DECK_VERSION,
            "status": "FAILED",
            "deck": [],
            "summary": _empty_summary(bracket, must_include_cards),
            "warnings": warnings + [{
                "code": "POOL_BUILD_FAILED",
                "message": f"{exc.__class__.__name__}: {exc}",
            }],
            "elapsed_ms": int((perf_counter() - t_start) * 1000),
        }
    phase_timings_ms["pool"] = int((perf_counter() - t_pool) * 1000)
    warnings.extend(pool.get("warnings", []))

    if call_counter["calls"] >= ENDPOINT_CALL_BUDGET:
        warnings.append({
            "code": "ENDPOINT_BUDGET_EXCEEDED_DURING_POOL",
            "message": f"Pool build consumed {call_counter['calls']} of {ENDPOINT_CALL_BUDGET} calls; selection may be impaired.",
        })

    # ---- Phase C: greedy slot-filling selection ----
    t_select = perf_counter()
    body, select_warnings = _select_deck(
        pool=pool, bracket=bracket, commander=commander.strip(),
    )
    phase_timings_ms["select"] = int((perf_counter() - t_select) * 1000)
    warnings.extend(select_warnings)

    deck: List[Dict[str, str]] = [{
        "card_name": commander.strip(),
        "reason": "Commander (locked by user intent).",
        "source": "user_intent",
    }] + body

    # ---- Phase D: validation + swap iteration (≤12 iters, total ≤30 calls) ----
    t_validate = perf_counter()
    deck, last_findings, validate_warnings = _validate_and_iterate(
        deck=deck, pool=pool,
        commander=commander.strip(), bracket=bracket,
        theme_hints=theme_hints, db_snapshot_id=db_snapshot_id,
        call_counter=call_counter,
        skip_strength_check=skip_strength_check,
    )
    phase_timings_ms["validate"] = int((perf_counter() - t_validate) * 1000)
    warnings.extend(validate_warnings)

    # ---- Summary ----
    body = deck[1:]  # may have been swapped during Phase D
    user_picks_present = sum(1 for c in body if c.get("source") == "user_intent")
    # Staples avoided: high-frequency staples (>=30% corpus) NOT in deck.
    deck_names_lower = {c["card_name"].strip().lower() for c in deck}
    archetype_brief = pool.get("archetype_brief", {}) or {}
    staples_avoided = 0
    for s in archetype_brief.get("staple_cards", []) or []:
        n = (s.get("name") or "")
        if not n:
            continue
        if float(s.get("usage_pct") or 0.0) < FREQUENCY_PENALTY_THRESHOLD:
            continue
        if n.strip().lower() not in deck_names_lower:
            staples_avoided += 1

    # Iteration 2: roll up LLM-layer metrics for the response.
    llm_total_cost = sum(c.get("cost_usd", 0.0) for c in llm_metrics["calls"])
    llm_total_input_tokens = sum(c.get("input_tokens", 0) for c in llm_metrics["calls"])
    llm_total_output_tokens = sum(c.get("output_tokens", 0) for c in llm_metrics["calls"])
    llm_total_latency_ms = sum(c.get("latency_ms", 0) for c in llm_metrics["calls"])
    summary = {
        "themes_classified": last_findings.get("themes_classified") or [],
        "bracket_placement": bracket,
        "bracket_estimate": last_findings.get("bracket_estimate"),
        "color_identity": pool.get("color_identity") or [],
        "strength_check": last_findings.get("strength_check_summary"),
        "creativity_envelope_metrics": {
            "user_picks_present": user_picks_present,
            "user_picks_total": len(must_include_cards),
            "must_includes_resolved": pool.get("must_includes_resolved", []),
            "must_includes_dropped": pool.get("must_includes_dropped", []),
            "staples_avoided_count": staples_avoided,
            "theme_coherence_score": last_findings.get("theme_coherence_score", 0.0),
            "creativity_delta_count": last_findings.get("creativity_delta_count", 0),
        },
        "endpoint_call_count": call_counter["calls"],
        "phase_timings_ms": phase_timings_ms,
        "validation_issues": last_findings.get("issues") or [],
        # Iteration 2 additions — empty/zero when the LLM layer is unavailable.
        "llm_metrics": {
            "available": llm_metrics["available"],
            "model": llm_metrics["model"],
            "calls": llm_metrics["calls"],
            "total_cost_usd": round(llm_total_cost, 4),
            "total_input_tokens": llm_total_input_tokens,
            "total_output_tokens": llm_total_output_tokens,
            "total_latency_ms": llm_total_latency_ms,
        },
        "summary_narrative": last_findings.get("summary_narrative"),
        "consider_adding": last_findings.get("consider_adding") or [],
        "novel_combo_flags": last_findings.get("novel_combo_flags") or [],
        "intent_analysis": last_findings.get("intent_analysis"),
    }

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
            "creativity_delta_count": 0,
        },
        "endpoint_call_count": 0,
        "phase_timings_ms": {"pool": 0, "select": 0, "validate": 0},
        # Iteration 2 keys, always present for shape stability.
        "llm_metrics": {
            "available": False,
            "model": None,
            "calls": [],
            "total_cost_usd": 0.0,
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "total_latency_ms": 0,
        },
        "summary_narrative": None,
        "consider_adding": [],
        "novel_combo_flags": [],
        "intent_analysis": None,
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


# ============================================================
# Phase C — Selection with slot balancing + per-bracket combo policy.
# ============================================================

# Color → its basic land name (Wastes is the colorless basic).
_COLOR_TO_BASIC: Dict[str, str] = {
    "W": "Plains", "U": "Island", "B": "Swamp", "R": "Mountain", "G": "Forest",
}
_BASIC_LAND_NAMES: set = {"Plains", "Island", "Swamp", "Mountain", "Forest", "Wastes"}

# Slot category → (default target count). Adjusted per archetype downstream.
# Targets sum to ~99: 28 + 36 + 10 + 10 + 7 + 3 + 5 = 99.
_DEFAULT_SLOT_TARGETS: Dict[str, int] = {
    "creature": 28,
    "land": 36,
    "ramp": 10,
    "card_draw": 10,
    "removal": 7,
    "win_condition": 3,
    "flex": 5,
}

# Primitive markers per category. Type-line wins over primitives ("Land" beats
# "MANA_RAMP_LAND_SEARCH"), and primitives are checked in priority order.
_RAMP_PRIMITIVES: set = {"MANA_ROCK", "MANA_RAMP_LAND_SEARCH", "MANA_RAMP_CREATURE_DORK", "MANA_RAMP_SPELL"}
_DRAW_PRIMITIVES: set = {"CARD_DRAW_BURST", "CARD_DRAW_REPEATABLE", "DRAW_REPLACEMENT", "CARD_DRAW"}
_REMOVAL_PRIMITIVES: set = {"TARGETED_REMOVAL_CREATURE", "TARGETED_REMOVAL_ARTIFACT",
                            "TARGETED_REMOVAL_ENCHANTMENT", "TARGETED_REMOVAL_PLANESWALKER",
                            "BOARDWIPE_CREATURES", "COUNTERSPELL_GENERIC", "COUNTERSPELL_CREATURE"}
_WIN_CONDITION_PRIMITIVES: set = {"WINCON_COMBAT", "WINCON_COMBO", "WINCON_ALT", "INFINITE_COMBO"}


def _classify_card(*, name: str, type_line: Optional[str], primitives: Optional[List[str]]) -> str:
    """Map a candidate to a slot category. Type-line takes priority (land /
    creature are unambiguous from the type); other categories are derived
    from primitives."""
    type_line = (type_line or "").lower()
    primitives_set: set = set(primitives or [])

    if "land" in type_line:
        return "land"
    if any(p in primitives_set for p in _RAMP_PRIMITIVES):
        return "ramp"
    if any(p in primitives_set for p in _DRAW_PRIMITIVES):
        return "card_draw"
    if any(p in primitives_set for p in _REMOVAL_PRIMITIVES):
        return "removal"
    if any(p in primitives_set for p in _WIN_CONDITION_PRIMITIVES):
        return "win_condition"
    if "creature" in type_line:
        return "creature"
    # Default bucket — instants, sorceries, enchantments, artifacts that
    # don't ramp/draw/remove. Used for utility / flex slots.
    return "flex"


def _adjust_slot_targets(archetype_brief: Dict[str, Any]) -> Dict[str, int]:
    """Adjust default slot targets based on archetype signal. Tribal archetypes
    increase creature count at the cost of flex slots."""
    targets = dict(_DEFAULT_SLOT_TARGETS)
    archetypes = archetype_brief.get("common_archetypes") or []
    if not archetypes:
        return targets
    top = (archetypes[0].get("name") or "").lower()
    if "tribal" in top or "typal" in top:
        # Tribal decks want more creatures; pull from flex.
        delta = 4
        targets["creature"] += delta
        targets["flex"] = max(0, targets["flex"] - delta)
    if "combo" in top:
        # Combo decks lean heavier on tutors / win conditions; pull from creature.
        targets["win_condition"] += 2
        targets["card_draw"] += 2
        targets["creature"] = max(0, targets["creature"] - 4)
    return targets


def _load_two_card_pair_index() -> Dict[frozenset, set]:
    """Build {frozenset({a_name_lower, b_name_lower}): set_of_brackets_allowed}
    from combo_brackets_v1.json. Returns empty dict on any load error — the
    agent then falls through to bracket-policy defaults from BRACKET_COMBO_POLICY.
    """
    import json as _j
    from pathlib import Path as _P
    path = _P(__file__).resolve().parents[1] / "data" / "combos" / "combo_brackets_v1.json"
    try:
        raw = _j.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    bv = raw.get("by_variant_id") if isinstance(raw, dict) else None
    if not isinstance(bv, dict):
        return {}
    index: Dict[frozenset, set] = {}
    for _vid, variant in bv.items():
        if not isinstance(variant, dict):
            continue
        if variant.get("combo_size") != 2:
            continue
        names = variant.get("card_names")
        if not isinstance(names, list) or len(names) != 2:
            continue
        a = (names[0] or "").strip().lower()
        b = (names[1] or "").strip().lower()
        if not a or not b or a == b:
            continue
        brackets = variant.get("brackets_allowed")
        if not isinstance(brackets, list):
            continue
        key = frozenset({a, b})
        existing = index.get(key, set())
        existing.update(brackets)
        index[key] = existing
    return index


def _combo_violates_bracket(
    *,
    candidate_name: str,
    selected_names_lower: set,
    user_pick_names_lower: set,
    bracket: str,
    pair_index: Dict[frozenset, set],
    current_pair_count: int,
) -> Tuple[bool, Optional[str]]:
    """Return (violates, reason). Used per-candidate during selection.

    Policy (Fix 1 from kickoff patch):
      - If both halves of a pair are user picks: ALWAYS allowed (user override).
      - Else if the pair's brackets_allowed includes the request bracket:
          * For B4: still check the per-build cap on distinct pairs.
          * Otherwise: allowed.
      - Else: rejected.
    """
    cand_lower = candidate_name.strip().lower()
    for other in selected_names_lower:
        if other == cand_lower:
            continue
        key = frozenset({cand_lower, other})
        brackets_allowed = pair_index.get(key)
        if brackets_allowed is None:
            continue
        # Both halves are user-locked → always allowed.
        if cand_lower in user_pick_names_lower and other in user_pick_names_lower:
            continue
        if bracket not in brackets_allowed:
            return True, (
                f"would form 2-card combo with {other!r}; pair allowed in "
                f"{sorted(brackets_allowed)}, requested bracket={bracket}"
            )
        # Bracket-allowed pair. For B4, check pair cap.
        if bracket == "B4":
            cap = BRACKET_COMBO_POLICY["B4"].get("pair_cap")
            if isinstance(cap, int) and current_pair_count >= cap:
                return True, (
                    f"would exceed B4 combo pair cap of {cap} (pair with {other!r})"
                )
    return False, None


def _count_existing_combo_pairs(
    *,
    selected_names_lower: set,
    pair_index: Dict[frozenset, set],
) -> int:
    """Count distinct 2-card combo pairs already present in the selected set.
    Used to enforce B4's pair cap during selection."""
    seen: set = set()
    names_list = list(selected_names_lower)
    for i, a in enumerate(names_list):
        for b in names_list[i + 1:]:
            key = frozenset({a, b})
            if key in pair_index:
                seen.add(key)
    return len(seen)


def _fill_mana_base(color_identity: List[str], count: int) -> List[Dict[str, str]]:
    """Generate `count` basic lands, evenly distributed across the commander's
    color identity. Wastes covers the colorless case.

    Singleton rule does not apply to basics, so a fully-W deck gets `count` Plains.
    """
    cards: List[Dict[str, str]] = []
    if not color_identity:
        for _ in range(count):
            cards.append({
                "card_name": "Wastes",
                "reason": "Mana base: colorless commander, filling with Wastes.",
                "source": "mana_base",
            })
        return cards
    basics = [_COLOR_TO_BASIC[c] for c in color_identity if c in _COLOR_TO_BASIC]
    if not basics:
        # Color identity contained no W/U/B/R/G — fall back to Wastes.
        for _ in range(count):
            cards.append({
                "card_name": "Wastes",
                "reason": "Mana base: non-WUBRG color identity, filling with Wastes.",
                "source": "mana_base",
            })
        return cards
    for i in range(count):
        basic = basics[i % len(basics)]
        cards.append({
            "card_name": basic,
            "reason": f"Mana base: basic land for color identity {sorted(color_identity)}.",
            "source": "mana_base",
        })
    return cards


def _format_reason(candidate: Dict[str, Any], slot: str) -> str:
    """Compose a human-readable per-card reason from rationale_components."""
    parts = list(candidate.get("rationale_components") or [])
    if not parts:
        parts.append(f"Slot fill: {slot}.")
    if slot:
        parts.append(f"[slot={slot}]")
    return " ".join(parts)


def _select_deck(
    *,
    pool: Dict[str, Any],
    bracket: str,
    commander: str,
    target_size: int = 99,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """Greedy slot-filling selection. Consumes Phase B's `pool` and returns
    a tuple (deck_99, warnings). User picks are placed first regardless of slot
    overflow (locked); remaining slots fill greedy-by-score per category."""
    warnings: List[Dict[str, str]] = []
    candidates = list(pool.get("candidates") or [])
    color_identity = pool.get("color_identity") or []
    archetype_brief = pool.get("archetype_brief") or {}

    slot_targets = _adjust_slot_targets(archetype_brief)
    pair_index = _load_two_card_pair_index()
    if not pair_index:
        warnings.append({
            "code": "COMBO_INDEX_EMPTY",
            "message": "combo_brackets_v1.json unavailable; combo policy will fall back to BRACKET_COMBO_POLICY defaults.",
        })

    # Buckets per slot category.
    slot_counts: Dict[str, int] = {k: 0 for k in slot_targets}
    selected: List[Dict[str, str]] = []
    selected_names_lower: set = set()
    user_pick_names_lower: set = {
        c["name"].strip().lower() for c in candidates if c.get("is_user_pick")
    }

    # ---- Pass 1: lock in user must-includes (score=INF) ----
    user_picks = [c for c in candidates if c.get("is_user_pick")]
    for c in user_picks:
        slot = _classify_card(
            name=c["name"], type_line=c.get("type_line"), primitives=c.get("primitives"),
        )
        selected.append({
            "card_name": c["name"],
            "reason": _format_reason(c, slot),
            "source": c.get("source", "user_intent"),
        })
        selected_names_lower.add(c["name"].strip().lower())
        # User picks bypass the slot cap — they always go in.
        if slot in slot_counts:
            slot_counts[slot] += 1

    # ---- Pass 2: greedy fill non-land slots from pool ----
    # Iterate pool top-to-bottom. For each non-land card, place it in its
    # slot if that slot still has capacity AND it doesn't violate bracket
    # combo policy. Lands are deferred to Pass 3.
    pair_count = _count_existing_combo_pairs(
        selected_names_lower=selected_names_lower, pair_index=pair_index,
    )
    non_land_target = sum(v for k, v in slot_targets.items() if k != "land")
    for c in candidates:
        if c.get("is_user_pick"):
            continue
        name = c["name"]
        name_lower = name.strip().lower()
        if name_lower in selected_names_lower:
            continue
        slot = _classify_card(
            name=name, type_line=c.get("type_line"), primitives=c.get("primitives"),
        )
        if slot == "land":
            continue  # Pass 3 handles lands.
        if slot_counts.get(slot, 0) >= slot_targets.get(slot, 0):
            continue
        violates, reason = _combo_violates_bracket(
            candidate_name=name,
            selected_names_lower=selected_names_lower,
            user_pick_names_lower=user_pick_names_lower,
            bracket=bracket,
            pair_index=pair_index,
            current_pair_count=pair_count,
        )
        if violates:
            # Don't warn-spam; only emit once per rejected name.
            warnings.append({
                "code": "COMBO_POLICY_REJECT",
                "message": f"Rejected {name!r}: {reason}",
            })
            continue
        selected.append({
            "card_name": name,
            "reason": _format_reason(c, slot),
            "source": c.get("source", "agent_select"),
        })
        selected_names_lower.add(name_lower)
        slot_counts[slot] += 1
        # Pair count may have ticked up if we just completed an allowed pair.
        if bracket == "B4":
            pair_count = _count_existing_combo_pairs(
                selected_names_lower=selected_names_lower, pair_index=pair_index,
            )

        # Stop once non-land slots are full to leave room for the land base.
        non_land_used = sum(slot_counts[k] for k in slot_counts if k != "land")
        if non_land_used >= non_land_target:
            break

    # ---- Pass 3: fill lands ----
    # First take any land candidates from the pool (dual lands surfaced by
    # theme/staple data), then top up with basics.
    land_cap = slot_targets["land"]
    for c in candidates:
        if slot_counts["land"] >= land_cap:
            break
        if c.get("is_user_pick"):
            continue
        name = c["name"]
        name_lower = name.strip().lower()
        if name_lower in selected_names_lower:
            continue
        slot = _classify_card(
            name=name, type_line=c.get("type_line"), primitives=c.get("primitives"),
        )
        if slot != "land":
            continue
        selected.append({
            "card_name": name,
            "reason": _format_reason(c, "land"),
            "source": c.get("source", "agent_select"),
        })
        selected_names_lower.add(name_lower)
        slot_counts["land"] += 1

    # Top up with basics to reach land target.
    needed_basics = max(0, slot_targets["land"] - slot_counts["land"])
    selected.extend(_fill_mana_base(color_identity, needed_basics))
    slot_counts["land"] += needed_basics

    # ---- Pass 4: pad up to target_size with basics if anything is short ----
    deficit = target_size - len(selected)
    if deficit > 0:
        selected.extend(_fill_mana_base(color_identity, deficit))
        warnings.append({
            "code": "POOL_UNDER_FILL_PADDED_WITH_BASICS",
            "message": f"Pool yielded fewer than {target_size} non-commander cards; padded {deficit} basics.",
        })

    # ---- Pass 5: truncate any overflow (shouldn't happen given caps) ----
    if len(selected) > target_size:
        # Drop from the tail (lowest-priority basics).
        selected = selected[:target_size]

    return selected, warnings


# ============================================================
# Phase D — Validation + swap-iteration loop.
# ============================================================

# Theme coherence threshold below which a re-pick is attempted. Coherence is
# the fraction of requested theme_hints that appear in the classified themes;
# 1.0 = every hint matched, 0.0 = none matched.
THEME_COHERENCE_TARGET = 0.5


def _deck_to_raw_text(commander: str, deck_body: List[Dict[str, str]]) -> str:
    """Serialize the agent's selected deck into the TappedOut-style raw text
    that deck_analyze_v1 and deck_strength_check_v1 expect."""
    lines = ["Commander", f"1 {commander}", "Deck"]
    for c in deck_body:
        lines.append(f"1 {c['card_name']}")
    return "\n".join(lines)


def _compute_theme_coherence(
    requested_hints: List[str],
    classified_themes: List[Dict[str, Any]],
) -> float:
    """Fraction of `requested_hints` that appear in `classified_themes`.

    Matches on theme_id substring (case-insensitive) — the classifier may
    return either `TYPAL_VAMPIRES` or `TYPAL_VAMPIRES:Vampire`, and the user
    may have passed either form.
    """
    if not requested_hints:
        return 1.0  # No themes requested → trivially coherent.
    classified_ids = []
    for t in classified_themes or []:
        tid = t.get("theme_id") or t.get("id") or t.get("name") or ""
        if isinstance(tid, str) and tid:
            classified_ids.append(tid.lower())
    if not classified_ids:
        return 0.0
    matched = 0
    for hint in requested_hints:
        h = (hint or "").lower()
        if not h:
            continue
        # Match if the hint is a substring of any classified ID or vice-versa.
        if any(h in cid or cid.startswith(h.split(":", 1)[0]) for cid in classified_ids):
            matched += 1
    return matched / max(1, len(requested_hints))


def _validate_deck(
    *,
    deck: List[Dict[str, str]],
    commander: str,
    bracket: str,
    theme_hints: List[str],
    db_snapshot_id: str,
    call_counter: Dict[str, int],
    skip_strength_check: bool = False,
) -> Dict[str, Any]:
    """Run the validation suite and return structured findings.

    `deck` is the full 100-card deck (commander first). Returns a dict with:
      - `issues`: list of validation problem dicts (empty = deck passes).
      - `themes_classified`, `theme_coherence_score`, `bracket_estimate`,
        `strength_check_summary` — populated when the relevant call succeeded.
      - `endpoint_calls_made`: how many calls this validate pass cost.
    """
    issues: List[Dict[str, Any]] = []
    findings: Dict[str, Any] = {
        "issues": issues,
        "themes_classified": None,
        "theme_coherence_score": 0.0,
        "bracket_estimate": None,
        "strength_check_summary": None,
        "endpoint_calls_made": 0,
    }

    # ---- Structural checks (no endpoint calls) ----
    if len(deck) != 100:
        issues.append({
            "code": "DECK_SIZE_WRONG",
            "message": f"Deck has {len(deck)} cards; need exactly 100 (commander + 99).",
        })

    # Singleton: every non-basic name appears once.
    from collections import Counter as _Counter
    body = deck[1:]
    counter: _Counter = _Counter(c["card_name"] for c in body)
    for name, count in counter.items():
        if count > 1 and name not in _BASIC_LAND_NAMES:
            issues.append({
                "code": "SINGLETON_VIOLATION",
                "message": f"{name!r} appears {count} times; non-basics must be singleton.",
                "offending_card": name,
            })

    # ---- Analyze (1 call): themes + bracket_estimate + color identity ----
    if call_counter["calls"] >= ENDPOINT_CALL_BUDGET:
        issues.append({
            "code": "BUDGET_EXCEEDED_BEFORE_ANALYZE",
            "message": f"Endpoint call budget ({ENDPOINT_CALL_BUDGET}) consumed before validate.",
        })
        return findings

    raw_text = _deck_to_raw_text(commander, body)
    try:
        from api.engine.layers.deck_analyze_v1 import compute_deck_analyze_v1
        analyze_result = compute_deck_analyze_v1(
            db_snapshot_id=db_snapshot_id,
            commander=commander,
            raw_decklist_text=raw_text,
            include_debug=False,
        )
        call_counter["calls"] += 1
        findings["endpoint_calls_made"] += 1
        themes_classified = analyze_result.get("deck_themes_v1") or []
        findings["themes_classified"] = themes_classified
        coherence = _compute_theme_coherence(theme_hints, themes_classified)
        findings["theme_coherence_score"] = coherence
        if theme_hints and coherence < THEME_COHERENCE_TARGET:
            issues.append({
                "code": "THEME_COHERENCE_LOW",
                "message": (
                    f"theme_coherence_score={coherence:.2f} below target {THEME_COHERENCE_TARGET}; "
                    f"requested_hints={theme_hints}; classified_top="
                    f"{[t.get('theme_id') for t in themes_classified[:3]]}"
                ),
            })
        # Bracket estimate from analyze (cheap; doesn't need strength_check).
        be = analyze_result.get("bracket_estimate")
        findings["bracket_estimate"] = be
        if isinstance(be, dict):
            estimated = be.get("bracket") or be.get("bracket_id")
            if estimated and estimated != bracket:
                issues.append({
                    "code": "BRACKET_MISMATCH",
                    "message": (
                        f"Requested bracket={bracket}, analyze estimated={estimated}. "
                        f"Deck composition may need swap-iteration."
                    ),
                    "estimated_bracket": estimated,
                    "requested_bracket": bracket,
                })
    except Exception as exc:
        issues.append({
            "code": "ANALYZE_FAILED",
            "message": f"{exc.__class__.__name__}: {exc}",
        })

    # ---- Strength check (1 call): corpus-similarity bracket placement ----
    if skip_strength_check:
        return findings
    if call_counter["calls"] >= ENDPOINT_CALL_BUDGET:
        # Budget exhausted; skip strength check, deck stands.
        return findings
    try:
        from api.engine.layers.deck_strength_check_v1 import compute_deck_strength_check_v1
        sc = compute_deck_strength_check_v1(
            db_snapshot_id=db_snapshot_id,
            commander=commander,
            raw_decklist_text=raw_text,
            k_nearest=3,
        )
        call_counter["calls"] += 1
        findings["endpoint_calls_made"] += 1
        # Trim down to the summary fields we want to expose in the response.
        ma = sc.get("measurement_a") or {}
        findings["strength_check_summary"] = {
            "bracket_signal": ma.get("bracket_signal"),
            "mean_similarity": ma.get("mean_similarity"),
            "nearest_neighbors_count": len(ma.get("nearest_neighbors") or []),
        }
    except Exception as exc:
        issues.append({
            "code": "STRENGTH_CHECK_FAILED",
            "message": f"{exc.__class__.__name__}: {exc}",
        })

    return findings


def _attempt_swap(
    *,
    deck: List[Dict[str, str]],
    pool_candidates: List[Dict[str, Any]],
    issue: Dict[str, Any],
    color_identity: List[str],
) -> Tuple[Optional[List[Dict[str, str]]], Optional[str]]:
    """Given a deck and a validation issue, attempt a single swap.

    Returns (new_deck, swap_description). Returns (None, None) if no swap was
    possible (validation issue is structural / cannot be patched).
    """
    code = issue.get("code")

    if code == "SINGLETON_VIOLATION":
        # Drop the second-occurrence of the offending non-basic; replace with
        # the next pool candidate not already in the deck.
        name = issue.get("offending_card")
        if not name:
            return None, None
        # Find the LAST occurrence (deck[0] is commander; keep first occurrence).
        for idx in range(len(deck) - 1, 0, -1):
            if deck[idx]["card_name"] == name:
                # Pick a replacement.
                deck_names_lower = {c["card_name"].strip().lower() for c in deck}
                replacement: Optional[Dict[str, Any]] = None
                for cand in pool_candidates:
                    cname = cand["name"]
                    if cname.strip().lower() in deck_names_lower:
                        continue
                    replacement = cand
                    break
                if replacement is None:
                    # Pad with a basic instead.
                    new_basic = _fill_mana_base(color_identity, 1)[0]
                    deck[idx] = new_basic
                    return deck, f"swap_singleton:{name}→basic"
                deck[idx] = {
                    "card_name": replacement["name"],
                    "reason": " ".join(replacement.get("rationale_components") or [f"Swap-in for duplicate {name}."]),
                    "source": replacement.get("source", "swap_iteration"),
                }
                return deck, f"swap_singleton:{name}→{replacement['name']}"

    if code == "THEME_COHERENCE_LOW":
        # Replace the lowest-priority non-land in the deck (flex/staple) with
        # the next theme-source candidate not already present.
        deck_names_lower = {c["card_name"].strip().lower() for c in deck}
        replacement = None
        for cand in pool_candidates:
            if cand.get("is_user_pick"):
                continue
            if not isinstance(cand.get("source"), str) or "theme" not in cand["source"]:
                continue
            if cand["name"].strip().lower() in deck_names_lower:
                continue
            replacement = cand
            break
        if replacement is None:
            return None, None
        # Find a flex/basic to evict (don't touch user picks).
        for idx in range(len(deck) - 1, 0, -1):
            entry = deck[idx]
            if entry.get("source") == "user_intent":
                continue
            if entry["card_name"] in _BASIC_LAND_NAMES:
                # Replace one basic with the theme card.
                deck[idx] = {
                    "card_name": replacement["name"],
                    "reason": " ".join(replacement.get("rationale_components") or ["Theme coherence swap."]) + " [slot=swap]",
                    "source": replacement.get("source", "theme_swap"),
                }
                return deck, f"swap_theme:basic→{replacement['name']}"
        return None, None

    if code == "BRACKET_MISMATCH":
        estimated = issue.get("estimated_bracket")
        requested = issue.get("requested_bracket")
        # We can't easily walk both directions; skip swap and leave a warning.
        # Phase F observes whether this is common enough to warrant a proper
        # power-up / power-down heuristic.
        return None, None

    return None, None


def _validate_and_iterate(
    *,
    deck: List[Dict[str, str]],
    pool: Dict[str, Any],
    commander: str,
    bracket: str,
    theme_hints: List[str],
    db_snapshot_id: str,
    call_counter: Dict[str, int],
    skip_strength_check: bool = False,
) -> Tuple[List[Dict[str, str]], Dict[str, Any], List[Dict[str, str]]]:
    """Run the validate→swap→revalidate loop, bounded by MAX_SWAP_ITERATIONS
    and ENDPOINT_CALL_BUDGET.

    Returns (final_deck, last_findings, warnings).
    """
    warnings: List[Dict[str, str]] = []
    pool_candidates = pool.get("candidates") or []
    color_identity = pool.get("color_identity") or []

    last_findings: Dict[str, Any] = {"issues": []}
    for iteration in range(MAX_SWAP_ITERATIONS):
        if call_counter["calls"] >= ENDPOINT_CALL_BUDGET:
            warnings.append({
                "code": "ENDPOINT_BUDGET_EXCEEDED",
                "message": (
                    f"Halted validate-iterate at iteration {iteration}; "
                    f"calls={call_counter['calls']}/{ENDPOINT_CALL_BUDGET}."
                ),
            })
            break
        last_findings = _validate_deck(
            deck=deck, commander=commander, bracket=bracket,
            theme_hints=theme_hints, db_snapshot_id=db_snapshot_id,
            call_counter=call_counter,
            skip_strength_check=skip_strength_check,
        )
        issues = last_findings["issues"]
        if not issues:
            break
        # Try to swap the first patchable issue.
        swapped = False
        for issue in issues:
            new_deck, desc = _attempt_swap(
                deck=deck, pool_candidates=pool_candidates,
                issue=issue, color_identity=color_identity,
            )
            if new_deck is not None:
                deck = new_deck
                warnings.append({
                    "code": "SWAP_ITERATION",
                    "message": f"iter={iteration}: {desc} (issue={issue.get('code')})",
                })
                swapped = True
                break
        if not swapped:
            # No actionable swap for the remaining issues; bail.
            for issue in issues:
                warnings.append({
                    "code": f"UNRESOLVED_{issue.get('code', 'UNKNOWN')}",
                    "message": issue.get("message", ""),
                })
            break

    return deck, last_findings, warnings
