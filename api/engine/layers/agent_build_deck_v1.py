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
from typing import Any, Dict, List, Optional, Set, Tuple


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

    # ---- Iter 3 Phase 2: combo-anchor hard guard ----
    # For each user must-include, scan the combo registry for combos that
    # name it; every OTHER card in those combos enters the forbidden set.
    # Every downstream LLM phase (B2/C2.1/C2.2/D2) sees the forbidden
    # list in its prompt AND has its output re-validated against the
    # set. Cards on the forbidden list that the LLM proposes get dropped
    # and logged as guard_fire events. Exception: a partner that's also
    # a must-include means the user opted in — no addition.
    from api.engine.layers.agent_combo_anchor_guard_v1 import (
        build_forbidden_set as _build_forbidden_set,
        format_forbidden_block_for_prompt as _format_forbidden_block,
    )
    forbidden_set, forbidden_sources = _build_forbidden_set(must_include_cards)
    guard_fire_events: List[Dict[str, Any]] = []
    forbidden_prompt_block = _format_forbidden_block(forbidden_set)
    if forbidden_set:
        warnings.append({
            "code": "COMBO_ANCHOR_GUARD_ACTIVE",
            "message": (
                f"Combo-anchor guard is active: {len(forbidden_set)} cards forbidden "
                f"based on {len(forbidden_sources)} combos where a user must-include "
                f"is an anchor. The forbidden list will be enforced across all LLM "
                f"phases. Sample forbidden cards: {sorted(forbidden_set)[:6]}"
            ),
        })

    # ---- Iteration 2 Phase B2: LLM call #1 — intent interpreter ----
    # Inspects (commander, bracket, theme_hints, must_include_cards) and
    # returns implicit_themes, suggested_extensions, conflict_warnings,
    # and a likely_win_condition. Augments theme_hints (without forcing)
    # and gives suggested_extensions a non-locked score boost in the
    # candidate pool. Skipped cleanly when LLM unavailable.
    intent_analysis: Optional[Dict[str, Any]] = None
    augmented_theme_hints = list(theme_hints)
    suggested_extension_names: List[str] = []
    if llm_client.is_available():
        intent_analysis = _run_intent_interpreter(
            llm_client=llm_client,
            commander=commander.strip(),
            bracket=bracket,
            theme_hints=theme_hints,
            must_include_cards=must_include_cards,
            llm_metrics=llm_metrics,
            warnings=warnings,
            forbidden_prompt_block=forbidden_prompt_block,
        )
        if intent_analysis:
            # Inferred themes added to hints; tracked separately via the
            # `intent_analysis.implicit_themes` field in the response so
            # the user can see what we inferred.
            for inferred in intent_analysis.get("implicit_themes") or []:
                if isinstance(inferred, str) and inferred and inferred not in augmented_theme_hints:
                    augmented_theme_hints.append(inferred)
            for ext in intent_analysis.get("suggested_extensions") or []:
                if isinstance(ext, dict):
                    name = ext.get("card")
                    if isinstance(name, str) and name.strip():
                        # Iter 3 Phase 2: drop forbidden cards before they
                        # enter the suggested-extensions list.
                        if name.strip().lower() in forbidden_set:
                            guard_fire_events.append({
                                "phase": "B2_intent_interpreter",
                                "field": "suggested_extensions",
                                "card": name.strip(),
                            })
                            continue
                        suggested_extension_names.append(name.strip())
            # Surface conflict_warnings to the user.
            for cw in intent_analysis.get("conflict_warnings") or []:
                if isinstance(cw, str) and cw:
                    warnings.append({
                        "code": "INTENT_CONFLICT_WARNING",
                        "message": cw,
                    })

    # ---- Phase B: build the candidate pool ----
    t_pool = perf_counter()
    try:
        pool = _build_candidate_pool(
            db_snapshot_id=db_snapshot_id,
            commander=commander.strip(),
            bracket=bracket,
            theme_hints=augmented_theme_hints,
            must_include_cards=must_include_cards,
            seed=seed,
            call_counter=call_counter,
            suggested_extension_names=suggested_extension_names,
            forbidden_set=forbidden_set,
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

    # ---- Iteration 2 Phase C2.1: LLM candidate critic (swap flex slots) ----
    # Iteration 1 produced a structurally-correct 99-card deck. Now the
    # LLM looks at the BOTTOM-priority N cards (basics first, then lowest-
    # scored non-essentials) and proposes swaps from the remaining pool.
    # Color identity + bracket policy are re-checked on every proposed
    # swap, and hallucinated card names (not in the pool) are dropped.
    novel_combo_flags: List[Dict[str, Any]] = []
    if llm_client.is_available():
        deck, critic_warnings = _run_candidate_critic(
            llm_client=llm_client,
            deck=deck,
            pool=pool,
            commander=commander.strip(),
            bracket=bracket,
            theme_hints=theme_hints,
            intent_analysis=intent_analysis,
            llm_metrics=llm_metrics,
            novel_combo_flags=novel_combo_flags,
            forbidden_set=forbidden_set,
            forbidden_prompt_block=forbidden_prompt_block,
            guard_fire_events=guard_fire_events,
        )
        warnings.extend(critic_warnings)

    # ---- Iteration 2 Phase C2.2: wild combo discovery ----
    # With the near-final 99-card deck assembled, build a WIDER pool
    # (300-500 cards, color-legal + theme-adjacent, full oracle text)
    # and ask the LLM to find wild synergies / novel combos the
    # iteration-1 corpus-driven pool wouldn't surface. Suggestions can
    # be either a SWAP (drop X, add Y because Y forms combo with Z) or
    # a FLAG (note combo already present, no swap needed).
    if llm_client.is_available():
        deck, wild_warnings = _run_wild_combo_discovery(
            llm_client=llm_client,
            deck=deck,
            pool=pool,
            commander=commander.strip(),
            bracket=bracket,
            theme_hints=theme_hints,
            db_snapshot_id=db_snapshot_id,
            intent_analysis=intent_analysis,
            llm_metrics=llm_metrics,
            novel_combo_flags=novel_combo_flags,
            forbidden_set=forbidden_set,
            forbidden_prompt_block=forbidden_prompt_block,
            guard_fire_events=guard_fire_events,
        )
        warnings.extend(wild_warnings)

    # ---- Phase D: validation + swap iteration (≤12 iters, total ≤30 calls) ----
    # Note: we validate against the USER-STATED theme_hints, not the
    # LLM-augmented ones. Theme coherence is a "did we honor what the
    # user asked for" check; the LLM's inferred themes are bonuses, not
    # requirements.
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

    # ---- Iteration 2 Phase D2: final critic + rationale rewrite ----
    # Replace per-card `reason` fields with LLM-generated, deck-context-
    # aware text. Generate a summary_narrative paragraph and 0-3
    # consider_adding callouts. Skipped cleanly when LLM unavailable
    # (per-card reasons fall back to iteration-1 template-fill text).
    #
    # Iter 3 Phase 1: D2 only rewrites a priority-30 subset (must-includes,
    # commander, creative outliers, combo participants, corpus-delta fillers).
    # The other ~65 cards keep their iter-2 rationales (already substantive).
    if llm_client.is_available():
        deck, final_critic_warnings = _run_final_critic(
            llm_client=llm_client,
            deck=deck,
            commander=commander.strip(),
            bracket=bracket,
            theme_hints=theme_hints,
            intent_analysis=intent_analysis,
            last_findings=last_findings,
            llm_metrics=llm_metrics,
            must_include_cards=must_include_cards,
            novel_combo_flags=novel_combo_flags,
            archetype_brief=pool.get("archetype_brief") or {},
            forbidden_set=forbidden_set,
            forbidden_prompt_block=forbidden_prompt_block,
            guard_fire_events=guard_fire_events,
        )
        warnings.extend(final_critic_warnings)

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

    # Iteration 2 creativity_delta_count — count of non-commander, non-
    # basic cards in the final deck that are NOT in the top-30 corpus
    # staples for this commander cohort. Higher = more unique to this
    # specific build. The plan's success criterion is mean ≥ 8 across
    # 5 test cases.
    archetype_brief = pool.get("archetype_brief", {}) or {}
    staples_sorted = sorted(
        archetype_brief.get("staple_cards") or [],
        key=lambda s: float(s.get("usage_pct") or 0.0),
        reverse=True,
    )
    top30_staple_names_lower = {
        (s.get("name") or "").strip().lower() for s in staples_sorted[:30]
    }
    creativity_delta_count = 0
    basic_names_lower = {n.lower() for n in _BASIC_LAND_NAMES}
    for c in deck[1:]:  # skip commander
        cname = (c.get("card_name") or "").strip().lower()
        if not cname or cname in basic_names_lower:
            continue
        if cname not in top30_staple_names_lower:
            creativity_delta_count += 1
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
            "creativity_delta_count": creativity_delta_count,
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
        "novel_combo_flags": novel_combo_flags,
        "intent_analysis": intent_analysis,
        # Iter 3 Phase 2: combo-anchor guard report. `forbidden_set_size`
        # is the count of distinct cards forbidden by the guard;
        # `guard_fire_events` is the list of LLM-suggestion drops.
        "combo_anchor_guard": {
            "active": bool(forbidden_set),
            "forbidden_set_size": len(forbidden_set),
            "forbidden_set_sample": sorted(forbidden_set)[:20],
            "sources": forbidden_sources,
            "guard_fire_events": guard_fire_events,
            "guard_fire_count": len(guard_fire_events),
        },
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
        "combo_anchor_guard": {
            "active": False,
            "forbidden_set_size": 0,
            "forbidden_set_sample": [],
            "sources": [],
            "guard_fire_events": [],
            "guard_fire_count": 0,
        },
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
# Iteration 2 — Phase B2: additive boost for cards the LLM intent
# interpreter named in `suggested_extensions`. Strong enough to outrank
# pure staples and most theme-medium cards, but well below user picks
# (USER_PICK_SCORE=INF). The boost is ADDITIVE so a card that's also a
# theme match stays better than a card that's only an LLM suggestion.
LLM_EXTENSION_BOOST = 25.0


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
    suggested_extension_names: Optional[List[str]] = None,
    forbidden_set: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """Compose archetype_brief + theme_top_cards into a ranked candidate pool.

    `call_counter` is a single-key mutable dict ({"calls": int}) so callers
    can enforce the ENDPOINT_CALL_BUDGET across the whole build. Each upstream
    layer call increments this counter.

    `suggested_extension_names` (iteration 2): card names the LLM intent
    interpreter (Phase B2) flagged as likely-intended creative extensions.
    Each such name gets LLM_EXTENSION_BOOST added to its score, raising
    it above ordinary staples but never above a user must-include (which
    is INF-scored). Names not present in the theme/staple universe are
    NOT injected as new candidates — the LLM only re-ranks the
    deterministic pool. (Iteration 3 may add a separate broader-pool
    injection if Phase F2's report shows the LLM's suggestions are being
    drowned out by the deterministic top-N.)
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

    # Iteration 2 Phase B2: apply LLM-suggested-extension boost. Cards
    # named by the intent interpreter get LLM_EXTENSION_BOOST added to
    # their existing score (theme/staple/whatever). User picks (INF)
    # are unaffected (INF + 25 = INF).
    if suggested_extension_names:
        ext_lower = {n.strip().lower() for n in suggested_extension_names if isinstance(n, str)}
        for name, cand in by_name.items():
            if name.strip().lower() in ext_lower:
                old = cand["score"]
                if old == USER_PICK_SCORE:
                    continue
                cand["score"] = old + LLM_EXTENSION_BOOST
                rc = list(cand.get("rationale_components") or [])
                rc.append(
                    f"LLM intent interpreter flagged as a likely-intended creative "
                    f"extension (+{LLM_EXTENSION_BOOST:.0f} score boost)."
                )
                cand["rationale_components"] = rc
                # Combine the source string for downstream visibility.
                if "llm_intent_extension" not in cand["source"].split("|"):
                    cand["source"] = cand["source"] + "|llm_intent_extension"

    # Iter 3 Phase 2: drop forbidden cards from the deterministic pool
    # too. The kickoff rule applies the guard to LLM phases only, but
    # the Ur-Dragon test case demonstrated that some forbidden cards
    # (e.g. cards from the corpus archetype_staple list) enter via the
    # deterministic pool. Filtering the pool ensures the forbidden set
    # is the envelope of "things the user did not opt into", applied
    # universally. User picks (score=INF) are never filtered — the user
    # listing both halves of a combo IS the opt-in signal, and the
    # forbidden set is empty for those anchors.
    if forbidden_set:
        pre_count = len(by_name)
        forbidden_removed: List[str] = []
        for name in list(by_name.keys()):
            if name.strip().lower() in forbidden_set:
                # Defensive: never drop a user must-include. (This
                # shouldn't fire — must-includes are listed by user, and
                # build_forbidden_set excludes them from the partner set
                # — but belt-and-suspenders.)
                if by_name[name].get("is_user_pick"):
                    continue
                forbidden_removed.append(name)
                del by_name[name]
        if forbidden_removed:
            warnings.append({
                "code": "POOL_FORBIDDEN_FILTERED",
                "message": (
                    f"Combo-anchor guard removed {len(forbidden_removed)} "
                    f"cards from the deterministic candidate pool: "
                    f"{forbidden_removed[:10]}{'...' if len(forbidden_removed) > 10 else ''} "
                    f"(pool size {pre_count} -> {len(by_name)})."
                ),
            })

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
        # Iteration 2 — needed by Phase C2.1 candidate critic so it can
        # hydrate oracle text for the LLM via find_card_by_name.
        "db_snapshot_id": db_snapshot_id,
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


# ============================================================
# Iteration 2 Phase B2 — LLM call #1 (intent interpreter).
# ============================================================
#
# The intent interpreter runs BEFORE the deterministic candidate pool
# builds. It reads the user's stated intent (commander + bracket +
# theme_hints + must_includes) and:
#   1. Notes the type / abilities / signaled archetype of each must-include.
#   2. Proposes 3-5 IMPLICIT themes the user probably wants but didn't
#      state explicitly (e.g. user states "Vampires" with Vito as a must-
#      include → implicit theme is "lifegain payoffs").
#   3. Proposes 5-10 cards the user likely INTENDS but didn't list. These
#      are creative extensions of the stated request — NOT auto-expansion
#      of combo chains from a single anchor (the creativity-envelope rule
#      from iteration 1).
#   4. Flags conflicts (e.g. "bracket B2 + Thoracle+Consult must-includes"
#      is a B5-class combo at a casual bracket).
#   5. Identifies the deck's likely primary win condition.
#
# Output is folded into the build:
#   - implicit_themes → appended to theme_hints (purely additive; theme
#     coherence is still scored against the USER's hints, not the LLM's).
#   - suggested_extensions → flagged in the candidate pool with a
#     deterministic score boost (LLM_EXTENSION_BOOST=+25). They are NOT
#     score=INF — that's reserved for user must_include_cards.
#   - conflict_warnings → surfaced as INTENT_CONFLICT_WARNING entries.
#   - intent_analysis (full structured output) → exposed under
#     summary.intent_analysis for the UI to render.


_INTENT_INTERPRETER_SYSTEM_PROMPT = (
    "You are an expert MTG Commander (cEDH-literate) deck-building assistant. "
    "Your job is to read a user's deck-build request BEFORE the deterministic "
    "candidate-pool algorithm runs, and produce a structured analysis that "
    "improves the build.\n\n"
    "RULES — these are hard:\n"
    "1. NEVER auto-expand a combo chain from a single anchor card. If the user "
    "named one half of a known combo but not the other, do NOT propose the "
    "other half as a suggested_extension. That is the user's choice to make.\n"
    "2. Suggested extensions must be CREATIVE EXTENSIONS of the stated request "
    "— cards a thoughtful player would consider if they liked the user's picks "
    "and themes. Do NOT just list the top-frequency staples for the commander.\n"
    "3. Implicit themes should be themes the user CLEARLY wants but didn't say "
    "out loud (e.g. user picked Vito → +life payoffs implicit). Don't invent "
    "themes they'd plausibly reject.\n"
    "4. Output VALID JSON ONLY. No prose before or after, no markdown fences. "
    "If any field is unknown, return an empty list / empty string for it; "
    "never invent unknown card names.\n"
    "5. Card names must be EXACT Magic: The Gathering printed names "
    "(case-sensitive, with any apostrophes and commas).\n"
)


def _build_intent_interpreter_user_prompt(
    *, commander: str, bracket: str,
    theme_hints: List[str], must_include_cards: List[str],
) -> str:
    th_str = ", ".join(theme_hints) if theme_hints else "(none provided)"
    mi_str = ", ".join(must_include_cards) if must_include_cards else "(none provided)"
    return (
        f"Commander: {commander}\n"
        f"Bracket target: {bracket}  (B1=ultra-casual ... B5=cEDH)\n"
        f"User-stated theme hints: {th_str}\n"
        f"User must-include cards: {mi_str}\n\n"
        "For each must-include card, briefly note: its primary type, two or "
        "three key abilities, and what archetype/role it signals.\n\n"
        "Then output JSON exactly in this shape:\n"
        "{\n"
        '  "must_include_analysis": [\n'
        '    {"card": "...", "type": "...", "key_abilities": ["...", "..."], "signals_archetype": "..."}\n'
        "  ],\n"
        '  "implicit_themes": ["...", "..."],\n'
        '  "suggested_extensions": [\n'
        '    {"card": "Exact Card Name", "why": "one sentence reason grounded in the user picks/themes"}\n'
        "  ],\n"
        '  "conflict_warnings": ["..."],\n'
        '  "likely_win_condition": "one sentence"\n'
        "}\n\n"
        "Limits: implicit_themes 3-5 entries. suggested_extensions 5-10 entries. "
        "conflict_warnings 0-3 entries (only real conflicts, e.g. bracket vs combo).\n"
        "Remember: no combo auto-expansion. If a must-include is half of a "
        "known combo, the other half is NOT a suggested_extension."
    )


# Budget for the intent interpreter call. The plan target is ~$0.04 per
# build call at Sonnet 4.6 pricing: 3k input × $3/MT + 2k output × $15/MT
# = $0.009 + $0.030 = $0.039. Pre-call guard set at 3000 input tokens.
_INTENT_INTERPRETER_INPUT_TOKEN_BUDGET = 3000
_INTENT_INTERPRETER_OUTPUT_TOKEN_BUDGET = 2000


def _run_intent_interpreter(
    *,
    llm_client: Any,
    commander: str,
    bracket: str,
    theme_hints: List[str],
    must_include_cards: List[str],
    llm_metrics: Dict[str, Any],
    warnings: List[Dict[str, str]],
    forbidden_prompt_block: str = "",
) -> Optional[Dict[str, Any]]:
    """Run LLM call #1 (intent interpreter). Returns the parsed structured
    output dict on success, or None on any failure (which is surfaced as
    a warning; the build then proceeds without LLM augmentation for this
    phase — iteration-1 behavior).

    Tracks token / cost / latency in `llm_metrics["calls"]` so the
    response payload accumulates the cost across all four LLM phases.

    Iter 3 Phase 2: `forbidden_prompt_block` is appended to the system
    prompt so the LLM sees the combo-anchor guard list. The build_deck()
    caller also filters the suggested_extensions output against the
    forbidden_set after this returns — defense in depth.
    """
    system = _INTENT_INTERPRETER_SYSTEM_PROMPT + (forbidden_prompt_block or "")
    user = _build_intent_interpreter_user_prompt(
        commander=commander, bracket=bracket,
        theme_hints=theme_hints, must_include_cards=must_include_cards,
    )

    result = llm_client.call_with_budget(
        system=system,
        user=user,
        max_input_tokens=_INTENT_INTERPRETER_INPUT_TOKEN_BUDGET,
        max_output_tokens=_INTENT_INTERPRETER_OUTPUT_TOKEN_BUDGET,
    )

    # Always record metrics — even on failure — so the UI shows what the
    # iteration-2 layer attempted.
    llm_metrics["calls"].append({
        "phase": "B2_intent_interpreter",
        "ok": result.ok,
        "input_tokens": result.input_tokens,
        "output_tokens": result.output_tokens,
        "cost_usd": result.cost_usd,
        "latency_ms": result.latency_ms,
        "error_code": result.error_code,
        "retries": result.retries,
    })

    if not result.ok:
        warnings.append({
            "code": "INTENT_INTERPRETER_FAILED",
            "message": (
                f"LLM call #1 (intent interpreter) failed: "
                f"{result.error_code}: {result.error_message}. "
                "Falling back to iteration-1 deterministic pool build."
            ),
        })
        return None

    parsed = result.parsed_json
    if not isinstance(parsed, dict):
        warnings.append({
            "code": "INTENT_INTERPRETER_INVALID_JSON",
            "message": (
                "LLM call #1 succeeded but the response was not parseable JSON. "
                "Falling back to iteration-1 deterministic pool build. "
                f"Raw text head: {result.text[:200]!r}"
            ),
        })
        return None

    # Defensive shape check — make sure the keys we'll read exist with the
    # expected types. We do NOT fail hard on missing optional fields; we
    # just normalize.
    return {
        "must_include_analysis": _as_list_of_dicts(parsed.get("must_include_analysis")),
        "implicit_themes": _as_list_of_strings(parsed.get("implicit_themes")),
        "suggested_extensions": _as_list_of_dicts(parsed.get("suggested_extensions")),
        "conflict_warnings": _as_list_of_strings(parsed.get("conflict_warnings")),
        "likely_win_condition": str(parsed.get("likely_win_condition") or "").strip(),
    }


def _as_list_of_strings(v: Any) -> List[str]:
    """Normalize an unknown value into a list of stripped, non-empty strings."""
    if not isinstance(v, list):
        return []
    out: List[str] = []
    for item in v:
        if isinstance(item, str) and item.strip():
            out.append(item.strip())
    return out


def _as_list_of_dicts(v: Any) -> List[Dict[str, Any]]:
    """Normalize an unknown value into a list of dicts (drop non-dicts)."""
    if not isinstance(v, list):
        return []
    return [item for item in v if isinstance(item, dict)]


# ============================================================
# Iteration 2 Phase C2.1 — LLM call #2 (candidate critic).
# ============================================================
#
# Runs AFTER the deterministic _select_deck has produced a structurally-
# correct 99-card body. The LLM critic looks at the lowest-priority
# ~25-30 slots (basics first, then lowest-scored non-essentials) and
# proposes replacement cards from the broader candidate pool.
#
# Validation on every proposed swap:
#   1. Replacement is in the candidate pool (no hallucinated names).
#   2. Replacement passes color-identity (CI ⊆ commander CI).
#   3. Bracket combo policy (Fix 1) — replacement that would form a 2-
#      card pair with another card in the deck must be bracket-legal.
#   4. Singleton — replacement not already in the deck.
#
# Cards that fail validation are dropped with a warning, NOT
# substituted from a deterministic fallback list. Iteration 1's deck
# was already valid; the worst case is we keep iteration-1's pick.

# Budget targets (Sonnet 4.6 pricing): ~15k input × $3/MT + ~5k output ×
# $15/MT = $0.045 + $0.075 = $0.12 per call. We pad the input budget
# to 16k to accommodate full card text for ~80-120 candidates.
_CANDIDATE_CRITIC_INPUT_TOKEN_BUDGET = 16000
_CANDIDATE_CRITIC_OUTPUT_TOKEN_BUDGET = 5000

# How many flex/low-priority slots the LLM gets to re-pick. Iteration 1
# fills 99 slots; we want the LLM to influence the bottom 25-30. The
# actual number swapped may be lower if the LLM chooses to keep some
# deterministic picks.
_CANDIDATE_CRITIC_SWAPPABLE_SLOTS = 28

# How many candidates we surface to the LLM. Trade-off: more = more
# room for creativity but more tokens; fewer = tighter pool but less
# semantic exploration. 100 is a balanced default.
_CANDIDATE_CRITIC_POOL_SIZE = 100


_CANDIDATE_CRITIC_SYSTEM_PROMPT = (
    "You are an expert MTG Commander deck-builder. Iteration 1 of the "
    "deck-building agent has produced a structurally-correct 99-card "
    "deck (lands, ramp, draw, removal, must-includes are in place). Your "
    "job is to re-pick the {n_swappable} lowest-priority FLEX slots by "
    "choosing the most synergistic cards from a pre-filtered candidate "
    "pool. You are biased toward INTERESTING over SAFE — pick cards that "
    "reinforce the deck's stated direction in non-obvious ways.\n\n"
    "RULES — these are hard:\n"
    "1. Your replacement cards MUST come from the supplied candidate "
    "pool. Do NOT propose cards outside the pool (they'll be dropped).\n"
    "2. Color identity is enforced — every pick's CI must be a subset of "
    "the commander's CI (already filtered in the pool, just don't reverse "
    "that).\n"
    "3. Bracket constraint policy — your picks must NOT form 2-card combo "
    "pairs that violate the deck's target bracket. The combo policy will "
    "be applied after your output, but you should already avoid known "
    "early-game 2-card kills if the bracket is B1/B2/B3.\n"
    "4. Substantive rationale — each pick gets a one-sentence reason "
    "that references specific OTHER cards in the deck or specific play "
    "patterns. NO generic 'great fit for X tribal' fillers.\n"
    "5. is_creative_outlier=true is reserved for cards that are NOT top "
    "corpus staples but fit the deck's direction. Use sparingly (0-3 per "
    "deck).\n"
    "6. combo_lines_noted: 0-5 entries. Each entry is a 2-card combo the "
    "deck composition would enable. in_spellbook=true if it's a well-"
    "documented combo from MTG resources; false if you noticed a novel "
    "interaction. Honest assessment only.\n"
    "7. Output VALID JSON ONLY. No prose around it. Card names must be "
    "EXACT printed names."
)


def _build_candidate_critic_user_prompt(
    *,
    commander: str,
    bracket: str,
    theme_hints: List[str],
    intent_analysis: Optional[Dict[str, Any]],
    current_deck_summary: List[Dict[str, str]],
    swappable_slots: List[Dict[str, str]],
    candidate_pool: List[Dict[str, Any]],
    bracket_policy_summary: str,
) -> str:
    """Compose the user prompt. Card text is included for candidates so
    the model can reason semantically (which is the whole point of
    iteration 2 — corpus frequency alone misses non-obvious synergies)."""
    import json as _j
    deck_summary_lines = [
        f"  - {c['card_name']} ({c.get('source', 'agent')})"
        for c in current_deck_summary
    ]
    swappable_lines = [
        f"  - {c['card_name']} ({c.get('source', 'agent')})"
        for c in swappable_slots
    ]
    pool_lines = []
    for cand in candidate_pool:
        # Build a compact one-line summary per candidate.
        cname = cand.get("name", "?")
        ctype = cand.get("type_line") or ""
        ccmc = cand.get("cmc")
        cprim = ", ".join((cand.get("primitives") or [])[:4])
        rc = " | ".join((cand.get("rationale_components") or [])[:2])
        oracle_text = cand.get("oracle_text") or ""
        # Trim oracle text to keep tokens in budget; the first ~150 chars
        # carry the essential mechanic in 95% of cases.
        if oracle_text and len(oracle_text) > 180:
            oracle_text = oracle_text[:177] + "..."
        line = f"  - {cname} | {ctype} | CMC={ccmc} | primitives=[{cprim}] | {rc}"
        if oracle_text:
            line += f"\n      text: {oracle_text}"
        pool_lines.append(line)

    intent_block = ""
    if intent_analysis:
        wc = intent_analysis.get("likely_win_condition") or ""
        themes = intent_analysis.get("implicit_themes") or []
        intent_block = (
            f"\nLLM-inferred intent (from call #1):\n"
            f"  likely_win_condition: {wc}\n"
            f"  implicit_themes: {themes}\n"
        )

    return (
        f"Commander: {commander}\n"
        f"Bracket: {bracket}\n"
        f"User-stated themes: {theme_hints}\n"
        f"{intent_block}"
        f"\nBracket combo policy: {bracket_policy_summary}\n"
        f"\nCURRENT DECK ({len(current_deck_summary)} cards, locked):\n"
        + "\n".join(deck_summary_lines)
        + f"\n\nSWAPPABLE SLOTS ({len(swappable_slots)} cards, you can replace any):\n"
        + "\n".join(swappable_lines)
        + f"\n\nCANDIDATE POOL ({len(candidate_pool)} cards available for picks):\n"
        + "\n".join(pool_lines)
        + "\n\nOutput JSON exactly:\n"
        + "{\n"
        + '  "selected_cards": [\n'
        + '    {"name": "Exact Card Name", "category": "creature|removal|draw|ramp|flex|win_condition",\n'
        + '     "reason": "one substantive sentence referencing the deck",\n'
        + '     "is_creative_outlier": false}\n'
        + "  ],\n"
        + '  "combo_lines_noted": [\n'
        + '    {"cards": ["Card A", "Card B"], "outcome": "what happens", "in_spellbook": true}\n'
        + "  ]\n"
        + "}\n"
        + f"\nReturn AT MOST {len(swappable_slots)} selected_cards. You may "
        + "return fewer if you genuinely think iteration 1's picks for "
        + "some swappable slots are already optimal — that's a valid signal.\n"
    )


def _summarize_bracket_policy(bracket: str) -> str:
    """One-line policy summary for the prompt."""
    policy = BRACKET_COMBO_POLICY.get(bracket, {})
    if not policy.get("allow_early") and not policy.get("allow_late"):
        return f"Bracket {bracket}: 2-card combos rejected entirely (except when both halves are user must-includes)."
    if policy.get("allow_late") and not policy.get("allow_early"):
        return f"Bracket {bracket}: only late-game combos (Spellbook tags S, P) allowed. Early combos (tag R) forbidden."
    cap = policy.get("pair_cap")
    if cap is not None:
        return f"Bracket {bracket}: early + late combos allowed, max {cap} distinct 2-card pairs."
    return f"Bracket {bracket}: unrestricted; all combo pairs allowed."


def _select_swappable_slots(
    deck: List[Dict[str, str]], n: int,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """Pick the N lowest-priority cards in deck (commander + body) as the
    swap candidates. Returns (locked, swappable). Locked = the N highest-
    priority + commander + user picks; swappable = the rest (up to n).

    Priority order (lowest first = first to be swappable):
      1. Wastes / basic lands (deterministic padding)
      2. Cards sourced from `archetype_staple` only (no theme signal)
      3. Cards with only one rationale_component
      4. All other agent_select picks

    Never swap commander or user_intent picks.
    """
    locked: List[Dict[str, str]] = []
    candidates_for_swap: List[Dict[str, str]] = []
    for c in deck:
        src = c.get("source", "")
        name = c.get("card_name", "")
        if src == "user_intent" or name == deck[0]["card_name"]:
            locked.append(c)
            continue
        candidates_for_swap.append(c)

    # Order swappable candidates by priority: basics first, then staples,
    # then mana_base, then plain agent_select. Stable sort.
    def _swap_priority(c: Dict[str, str]) -> int:
        name = c.get("card_name", "")
        src = c.get("source", "")
        if name in _BASIC_LAND_NAMES or src == "mana_base":
            return 0
        if src == "archetype_staple":
            return 1
        if "theme" not in src and "user_intent" not in src and "llm_intent_extension" not in src:
            return 2
        return 3

    candidates_for_swap.sort(key=_swap_priority)
    swappable = candidates_for_swap[:n]
    locked.extend(candidates_for_swap[n:])
    return locked, swappable


def _candidate_pool_for_critic(pool: Dict[str, Any], db_snapshot_id: str,
                               exclude_names: set, size: int) -> List[Dict[str, Any]]:
    """Build the candidate-pool slice we pass to the LLM, with oracle
    text included. Excludes cards already in the deck. We lazy-load
    oracle text from the snapshot DB so the candidate pool can stay
    text-free for iteration-1 paths.

    Note: failure to fetch oracle text is non-fatal — the candidate
    line just omits the text field. The LLM still gets primitives +
    type_line which is enough for most decisions.
    """
    from engine.db import find_card_by_name

    out: List[Dict[str, Any]] = []
    for cand in pool.get("candidates", []) or []:
        if len(out) >= size:
            break
        name = cand.get("name") or ""
        if not name or name.strip().lower() in exclude_names:
            continue
        # User picks are already in the deck (locked); skip them in the
        # critic pool.
        if cand.get("is_user_pick"):
            continue
        # Hydrate oracle text if not already present.
        oracle_text = cand.get("oracle_text")
        if oracle_text is None:
            try:
                card = find_card_by_name(db_snapshot_id, name)
            except Exception:
                card = None
            if card and isinstance(card, dict):
                oracle_text = card.get("oracle_text") or card.get("text") or ""
        out.append({
            "name": name,
            "type_line": cand.get("type_line"),
            "cmc": cand.get("cmc"),
            "primitives": cand.get("primitives") or [],
            "color_identity": cand.get("color_identity") or [],
            "rationale_components": cand.get("rationale_components") or [],
            "score": cand.get("score"),
            "oracle_text": oracle_text or "",
        })
    return out


def _run_candidate_critic(
    *,
    llm_client: Any,
    deck: List[Dict[str, str]],
    pool: Dict[str, Any],
    commander: str,
    bracket: str,
    theme_hints: List[str],
    intent_analysis: Optional[Dict[str, Any]],
    llm_metrics: Dict[str, Any],
    novel_combo_flags: List[Dict[str, Any]],
    forbidden_set: Optional[Set[str]] = None,
    forbidden_prompt_block: str = "",
    guard_fire_events: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """Run the candidate critic. Returns (new_deck, warnings).

    new_deck contains the 100-card deck (commander + 99) with LLM-picked
    swaps applied where valid. novel_combo_flags is mutated in place
    with the LLM's noted combo_lines.

    Failure path: any LLM error → return deck unchanged, emit warning.
    Partial success path: hallucinated/invalid swaps are dropped, valid
    swaps are applied.
    """
    warnings: List[Dict[str, str]] = []

    color_identity = set(pool.get("color_identity") or [])
    if not color_identity:
        # Edge case: empty CI. Critic can't validate without it; bail.
        warnings.append({
            "code": "CRITIC_SKIPPED_NO_CI",
            "message": "Skipped candidate critic — empty commander color_identity in pool.",
        })
        return deck, warnings

    locked, swappable = _select_swappable_slots(deck, _CANDIDATE_CRITIC_SWAPPABLE_SLOTS)
    if not swappable:
        warnings.append({
            "code": "CRITIC_SKIPPED_NO_SWAPPABLE",
            "message": "Skipped candidate critic — no swappable slots identified.",
        })
        return deck, warnings

    # Build candidate pool excluding cards already in the LOCKED portion.
    # Swappable cards themselves stay eligible — the critic can choose to
    # re-pick the same card.
    locked_names_lower = {c["card_name"].strip().lower() for c in locked}
    db_snapshot_id = pool.get("db_snapshot_id") or ""
    # Phase B's pool doesn't currently expose db_snapshot_id — pull it
    # from build_deck's stack via the pool dict if available, else
    # accept that oracle text will be empty (LLM still has primitives).
    critic_pool = _candidate_pool_for_critic(
        pool, db_snapshot_id, locked_names_lower, _CANDIDATE_CRITIC_POOL_SIZE,
    )
    if not critic_pool:
        warnings.append({
            "code": "CRITIC_SKIPPED_EMPTY_POOL",
            "message": "Skipped candidate critic — no eligible candidates in pool.",
        })
        return deck, warnings

    # Make the call.
    system = _CANDIDATE_CRITIC_SYSTEM_PROMPT.format(n_swappable=len(swappable))
    system += (forbidden_prompt_block or "")
    user = _build_candidate_critic_user_prompt(
        commander=commander, bracket=bracket, theme_hints=theme_hints,
        intent_analysis=intent_analysis,
        current_deck_summary=locked,
        swappable_slots=swappable,
        candidate_pool=critic_pool,
        bracket_policy_summary=_summarize_bracket_policy(bracket),
    )

    result = llm_client.call_with_budget(
        system=system, user=user,
        max_input_tokens=_CANDIDATE_CRITIC_INPUT_TOKEN_BUDGET,
        max_output_tokens=_CANDIDATE_CRITIC_OUTPUT_TOKEN_BUDGET,
    )
    llm_metrics["calls"].append({
        "phase": "C2_1_candidate_critic",
        "ok": result.ok,
        "input_tokens": result.input_tokens,
        "output_tokens": result.output_tokens,
        "cost_usd": result.cost_usd,
        "latency_ms": result.latency_ms,
        "error_code": result.error_code,
        "retries": result.retries,
    })
    if not result.ok:
        warnings.append({
            "code": "CANDIDATE_CRITIC_FAILED",
            "message": (
                f"LLM call #2 (candidate critic) failed: {result.error_code}: "
                f"{result.error_message}. Deck unchanged from iteration-1 select."
            ),
        })
        return deck, warnings

    parsed = result.parsed_json
    if not isinstance(parsed, dict):
        warnings.append({
            "code": "CANDIDATE_CRITIC_INVALID_JSON",
            "message": (
                "LLM call #2 returned non-JSON output; deck unchanged. "
                f"Raw text head: {result.text[:200]!r}"
            ),
        })
        return deck, warnings

    # Record combo_lines_noted even when we can't apply any swaps —
    # these are useful to surface in the UI.
    for cl in _as_list_of_dicts(parsed.get("combo_lines_noted")):
        cards = cl.get("cards")
        if isinstance(cards, list) and len(cards) == 2:
            novel_combo_flags.append({
                "cards": [str(cards[0]), str(cards[1])],
                "outcome": str(cl.get("outcome") or "").strip(),
                "in_spellbook": bool(cl.get("in_spellbook", False)),
                "source": "C2_1_candidate_critic",
            })

    # Apply swaps. Build name → candidate map for O(1) lookup.
    pool_by_lower = {c["name"].strip().lower(): c for c in critic_pool}
    pair_index = _load_two_card_pair_index()
    selected = _as_list_of_dicts(parsed.get("selected_cards"))
    if not selected:
        warnings.append({
            "code": "CRITIC_NO_SELECTIONS",
            "message": "LLM critic chose to keep iteration-1's picks for all swappable slots.",
        })
        return deck, warnings

    # We need to know what's in the deck for color-identity + combo
    # checks; rebuild a set from the locked portion.
    deck_names_lower = {c["card_name"].strip().lower() for c in locked}
    swappable_idx = list(range(len(swappable)))  # available slots to fill
    new_swappable: List[Dict[str, str]] = []

    user_pick_names_lower: set = {
        c["card_name"].strip().lower() for c in locked if c.get("source") == "user_intent"
    }

    for entry in selected:
        if not swappable_idx:
            warnings.append({
                "code": "CRITIC_OVERFILL",
                "message": "LLM returned more selections than swappable slots; truncating.",
            })
            break
        name = str(entry.get("name") or "").strip()
        if not name:
            continue
        name_lower = name.lower()
        # Iter 3 Phase 2: reject combo-anchor-forbidden cards.
        if forbidden_set and name_lower in forbidden_set:
            warnings.append({
                "code": "CRITIC_REJECTED_FORBIDDEN",
                "message": (
                    f"Critic suggested {name!r} but it's on the combo-anchor "
                    f"forbidden list (would complete a combo with a user "
                    f"must-include); dropped."
                ),
            })
            if guard_fire_events is not None:
                guard_fire_events.append({
                    "phase": "C2_1_candidate_critic",
                    "field": "selected_cards",
                    "card": name,
                })
            continue
        # Reject duplicates of locked-or-already-picked.
        if name_lower in deck_names_lower:
            warnings.append({
                "code": "CRITIC_REJECTED_DUPLICATE",
                "message": f"Critic suggested {name!r} but it's already in the deck; dropped.",
            })
            continue
        # Reject hallucinated names.
        cand = pool_by_lower.get(name_lower)
        if not cand:
            warnings.append({
                "code": "CRITIC_REJECTED_HALLUCINATION",
                "message": (
                    f"Critic suggested {name!r} but it isn't in the supplied "
                    f"candidate pool; dropped."
                ),
            })
            continue
        # Reject color-identity violations.
        ci = set(cand.get("color_identity") or [])
        if ci and not ci.issubset(color_identity):
            warnings.append({
                "code": "CRITIC_REJECTED_CI_ILLEGAL",
                "message": (
                    f"Critic suggested {name!r} with CI={sorted(ci)} not subset "
                    f"of commander CI={sorted(color_identity)}; dropped."
                ),
            })
            continue
        # Reject bracket-policy violations.
        pair_count = _count_existing_combo_pairs(
            selected_names_lower=deck_names_lower, pair_index=pair_index,
        )
        violates, reason = _combo_violates_bracket(
            candidate_name=name,
            selected_names_lower=deck_names_lower,
            user_pick_names_lower=user_pick_names_lower,
            bracket=bracket,
            pair_index=pair_index,
            current_pair_count=pair_count,
        )
        if violates:
            warnings.append({
                "code": "CRITIC_REJECTED_BRACKET",
                "message": f"Critic suggested {name!r}: {reason}; dropped.",
            })
            continue

        # Accept. Compose the reason from the LLM's rationale (verbatim
        # so phase D2 can rewrite later; iteration-1 swap path is
        # preserved).
        reason = str(entry.get("reason") or "").strip() or "LLM candidate critic pick."
        category = str(entry.get("category") or "").strip()
        if category:
            reason = f"{reason} [slot={category}]"
        is_outlier = bool(entry.get("is_creative_outlier", False))
        source = "llm_candidate_critic"
        if is_outlier:
            source += "|creative_outlier"
        new_swappable.append({
            "card_name": name,
            "reason": reason,
            "source": source,
        })
        deck_names_lower.add(name_lower)
        swappable_idx.pop(0)

    # Any unused swappable slots stay as iteration-1's picks (preserve
    # the structurally-correct skeleton).
    if swappable_idx:
        kept = [swappable[i] for i in swappable_idx]
        new_swappable.extend(kept)

    # Reassemble deck: locked positions + the swapped flex slots.
    new_deck = locked + new_swappable
    # Defensive: keep exactly 100 cards.
    if len(new_deck) > 100:
        new_deck = new_deck[:100]
    elif len(new_deck) < 100:
        # Should not happen — locked + new_swappable preserves the
        # original count — but if it does, pad to 100 with basics.
        deficit = 100 - len(new_deck)
        ci_list = sorted(color_identity) if color_identity else []
        new_deck.extend(_fill_mana_base(ci_list, deficit))
        warnings.append({
            "code": "CRITIC_DECK_UNDERFILL_PADDED",
            "message": (
                f"After applying critic swaps the deck was {100 - deficit} "
                f"cards; padded with {deficit} basics."
            ),
        })

    return new_deck, warnings


# ============================================================
# Iteration 2 Phase C2.2 — LLM call #2.5 (wild combo discovery).
# ============================================================
#
# The candidate critic (Phase C2.1) is bounded by Phase B's corpus-prior
# pool — it can re-rank what the deterministic skeleton already
# surfaced. Phase C2.2 expands the search space: a separate 300-500
# card pool (color-legal + theme-adjacent, NOT pre-narrowed by corpus
# frequency) is computed and offered to the LLM along with the near-
# final deck. The LLM is asked to find wild synergies the iteration-1
# pipeline could not — non-Spellbook combos, engine+payoff pairings,
# underexplored mechanic interactions.
#
# Two suggestion modes:
#   ADD swap — drop card X from the deck, add card Y. Bracket policy +
#     color identity re-checked on every applied swap.
#   FLAG only — the LLM noticed a combo already present (no swap
#     needed); recorded in novel_combo_flags so the UI can render it.

# C2.2 input budget — observed at 28-29k estimated tokens with a 350-
# card pool + full oracle text. Bumped from 22k → 35k so the call lands
# on the live test cases; cost rises by ~$0.04 per build, well within
# the iteration-2 $0.50/build envelope. If iteration 3 prunes pool
# size or oracle-text length, this can come back down.
_WILD_COMBO_INPUT_TOKEN_BUDGET = 35000
_WILD_COMBO_OUTPUT_TOKEN_BUDGET = 3500
_WILD_COMBO_POOL_SIZE = 350  # smaller than the wide-pool max so token
                              # budget fits at the call_with_budget guard.
_WILD_COMBO_MAX_SUGGESTIONS = 5


_WILD_COMBO_SYSTEM_PROMPT = (
    "You are an expert MTG Commander deck-builder with deep cEDH and "
    "casual-EDH literacy. The deck is essentially complete — your job is "
    "to find WILD synergies and combos the deck doesn't currently have "
    "but COULD. Read the card text and reason about interactions; don't "
    "just list well-known Spellbook combos.\n\n"
    "Look for:\n"
    "  - Cards that would close a near-combo (2 of 3 pieces present — "
    "what's the third?).\n"
    "  - Novel 2-card combos not in Spellbook (either because the cards "
    "are new or because the combo is non-obvious).\n"
    "  - 'Engine + payoff' synergies (e.g. recursion engine + payoff "
    "that triggers off recursion).\n"
    "  - Underexplored mechanic interactions.\n"
    "  - Cards that would let a borderline-functional combo go off "
    "reliably (tutor effects for fragile combos).\n\n"
    "RULES — hard:\n"
    "1. Each suggestion is either an ADD (with a corresponding REMOVE) "
    "or a FLAG (combo already present in the deck — no swap needed).\n"
    "2. ADD swaps must respect the bracket combo policy. If the bracket "
    "doesn't allow the combo type, FLAG it instead and explain.\n"
    "3. ADD cards must come from the supplied candidate pool. The pool "
    "is wider than the iteration-1 pool on purpose — use it.\n"
    "4. Bias toward INTERESTING over SAFE. If nothing creative jumps "
    "out, return an empty list — don't pad. A short, sharp list beats "
    "a long boring one.\n"
    "5. Maximum {n_max} suggestions total (ADD + FLAG combined).\n"
    "6. Output VALID JSON ONLY. Card names must be EXACT printed names."
)


def _build_wild_combo_user_prompt(
    *,
    commander: str,
    bracket: str,
    theme_hints: List[str],
    intent_analysis: Optional[Dict[str, Any]],
    deck: List[Dict[str, str]],
    wide_pool: List[Dict[str, Any]],
    bracket_policy_summary: str,
) -> str:
    deck_lines = [f"  - {c['card_name']} ({c.get('source', 'agent')})" for c in deck]
    pool_lines: List[str] = []
    for cand in wide_pool:
        name = cand.get("name", "?")
        type_line = cand.get("type_line") or ""
        cmc = cand.get("cmc")
        prims = ", ".join((cand.get("primitives") or [])[:3])
        oracle_text = cand.get("oracle_text") or ""
        if oracle_text and len(oracle_text) > 200:
            oracle_text = oracle_text[:197] + "..."
        line = f"  - {name} | {type_line} | CMC={cmc} | primitives=[{prims}]"
        if oracle_text:
            line += f"\n      text: {oracle_text}"
        pool_lines.append(line)

    intent_block = ""
    if intent_analysis:
        wc = intent_analysis.get("likely_win_condition") or ""
        if wc:
            intent_block = f"\nLikely win condition (from intent interpreter): {wc}\n"

    return (
        f"Commander: {commander}\n"
        f"Bracket: {bracket}\n"
        f"Themes: {theme_hints}\n"
        f"{intent_block}"
        f"\nBracket combo policy: {bracket_policy_summary}\n"
        f"\nCURRENT 99-CARD DECK:\n"
        + "\n".join(deck_lines)
        + f"\n\nWIDE CANDIDATE POOL ({len(wide_pool)} cards):\n"
        + "\n".join(pool_lines)
        + "\n\nOutput JSON exactly:\n"
        + "{\n"
        + '  "suggestions": [\n'
        + '    {"action": "add_swap",\n'
        + '     "add_card": "Card to add (must be from pool)",\n'
        + '     "remove_card": "Card to remove (must be in current deck)",\n'
        + '     "combo_partner": "Card already in deck that the add interacts with",\n'
        + '     "outcome": "what the interaction produces",\n'
        + '     "is_known_spellbook_combo": false,\n'
        + '     "is_creative_outlier": true},\n'
        + '    {"action": "flag_only",\n'
        + '     "combo_cards": ["Card A in deck", "Card B in deck"],\n'
        + '     "outcome": "what they produce together",\n'
        + '     "is_known_spellbook_combo": true,\n'
        + '     "is_creative_outlier": false}\n'
        + "  ]\n"
        + "}\n"
        + f"\nReturn 0 to {_WILD_COMBO_MAX_SUGGESTIONS} suggestions. Empty list is fine."
    )


def _run_wild_combo_discovery(
    *,
    llm_client: Any,
    deck: List[Dict[str, str]],
    pool: Dict[str, Any],
    commander: str,
    bracket: str,
    theme_hints: List[str],
    db_snapshot_id: str,
    intent_analysis: Optional[Dict[str, Any]],
    llm_metrics: Dict[str, Any],
    novel_combo_flags: List[Dict[str, Any]],
    forbidden_set: Optional[Set[str]] = None,
    forbidden_prompt_block: str = "",
    guard_fire_events: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """Run the wild-combo-discovery pass. Returns (new_deck, warnings).
    Applies ADD swaps that pass color/bracket/singleton validation; logs
    FLAG-only suggestions and dropped ADD swaps in novel_combo_flags."""
    warnings: List[Dict[str, str]] = []
    color_identity = set(pool.get("color_identity") or [])
    if not color_identity:
        warnings.append({
            "code": "WILD_COMBO_SKIPPED_NO_CI",
            "message": "Skipped wild-combo discovery — empty commander color identity.",
        })
        return deck, warnings

    # Collect theme primitives from the existing pool's candidates so
    # the wide pool can rank theme-adjacent cards higher.
    theme_primitives_set: set = set()
    for cand in pool.get("candidates", []) or []:
        for p in cand.get("primitives") or []:
            if isinstance(p, str):
                theme_primitives_set.add(p)

    # Build the wide pool.
    deck_names = [c["card_name"] for c in deck]
    try:
        from api.engine.layers.agent_wide_candidate_pool_v1 import (
            compute_agent_wide_candidate_pool_v1,
        )
        wide_pool_result = compute_agent_wide_candidate_pool_v1(
            db_snapshot_id=db_snapshot_id,
            commander=commander,
            color_identity=sorted(color_identity),
            theme_primitives=sorted(theme_primitives_set),
            pool_size=_WILD_COMBO_POOL_SIZE,
            exclude_names=deck_names,
        )
    except Exception as exc:
        warnings.append({
            "code": "WILD_COMBO_POOL_FAILED",
            "message": f"{exc.__class__.__name__}: {exc}",
        })
        return deck, warnings

    wide_candidates = wide_pool_result.get("candidates") or []
    for w in wide_pool_result.get("warnings") or []:
        warnings.append({
            "code": f"WIDE_POOL_{w.get('code', 'WARNING')}",
            "message": w.get("message", ""),
        })
    if not wide_candidates:
        warnings.append({
            "code": "WILD_COMBO_SKIPPED_EMPTY_WIDE_POOL",
            "message": "Wide candidate pool returned 0 cards; skipping wild-combo discovery.",
        })
        return deck, warnings

    # Call the LLM.
    system = _WILD_COMBO_SYSTEM_PROMPT.format(n_max=_WILD_COMBO_MAX_SUGGESTIONS)
    system += (forbidden_prompt_block or "")
    user = _build_wild_combo_user_prompt(
        commander=commander, bracket=bracket, theme_hints=theme_hints,
        intent_analysis=intent_analysis, deck=deck, wide_pool=wide_candidates,
        bracket_policy_summary=_summarize_bracket_policy(bracket),
    )
    result = llm_client.call_with_budget(
        system=system, user=user,
        max_input_tokens=_WILD_COMBO_INPUT_TOKEN_BUDGET,
        max_output_tokens=_WILD_COMBO_OUTPUT_TOKEN_BUDGET,
    )
    llm_metrics["calls"].append({
        "phase": "C2_2_wild_combo_discovery",
        "ok": result.ok,
        "input_tokens": result.input_tokens,
        "output_tokens": result.output_tokens,
        "cost_usd": result.cost_usd,
        "latency_ms": result.latency_ms,
        "error_code": result.error_code,
        "retries": result.retries,
    })
    if not result.ok:
        warnings.append({
            "code": "WILD_COMBO_FAILED",
            "message": (
                f"LLM call #2.5 (wild-combo discovery) failed: "
                f"{result.error_code}: {result.error_message}. "
                "Deck unchanged."
            ),
        })
        return deck, warnings
    parsed = result.parsed_json
    if not isinstance(parsed, dict):
        warnings.append({
            "code": "WILD_COMBO_INVALID_JSON",
            "message": (
                "LLM call #2.5 returned non-JSON output; deck unchanged. "
                f"Raw text head: {result.text[:200]!r}"
            ),
        })
        return deck, warnings

    suggestions = _as_list_of_dicts(parsed.get("suggestions"))
    if not suggestions:
        # Empty list is a valid outcome — bias was toward "interesting only".
        return deck, warnings

    pool_by_lower = {c["name"].strip().lower(): c for c in wide_candidates}
    deck_names_lower = {c["card_name"].strip().lower() for c in deck}
    user_pick_names_lower = {
        c["card_name"].strip().lower() for c in deck if c.get("source") == "user_intent"
    }
    pair_index = _load_two_card_pair_index()
    deck = list(deck)  # work on a copy

    for sug in suggestions[:_WILD_COMBO_MAX_SUGGESTIONS]:
        action = str(sug.get("action") or "").strip().lower()
        outcome = str(sug.get("outcome") or "").strip()

        if action == "flag_only":
            cards = sug.get("combo_cards")
            if not isinstance(cards, list) or len(cards) != 2:
                continue
            # Iter 3 Phase 2: a flag mentioning a forbidden card means the
            # LLM is asserting a combo line involving a card that shouldn't
            # be in the deck. Drop and log; don't surface that combo.
            blocked = False
            if forbidden_set:
                for c in cards:
                    if isinstance(c, str) and c.strip().lower() in forbidden_set:
                        blocked = True
                        if guard_fire_events is not None:
                            guard_fire_events.append({
                                "phase": "C2_2_wild_combo_discovery",
                                "field": "flag_only.combo_cards",
                                "card": c,
                            })
                        break
            if blocked:
                warnings.append({
                    "code": "WILD_COMBO_FLAG_REJECTED_FORBIDDEN",
                    "message": (
                        f"Wild-combo flagged {cards!r} but one card is on the "
                        f"combo-anchor forbidden list; flag dropped."
                    ),
                })
                continue
            novel_combo_flags.append({
                "cards": [str(cards[0]), str(cards[1])],
                "outcome": outcome,
                "in_spellbook": bool(sug.get("is_known_spellbook_combo", False)),
                "source": "C2_2_wild_combo_discovery_flag",
                "applied_swap": False,
            })
            continue

        if action != "add_swap":
            continue

        add_name = str(sug.get("add_card") or "").strip()
        remove_name = str(sug.get("remove_card") or "").strip()
        if not add_name or not remove_name:
            continue
        add_lower = add_name.lower()
        remove_lower = remove_name.lower()

        # Iter 3 Phase 2: reject combo-anchor-forbidden adds.
        if forbidden_set and add_lower in forbidden_set:
            warnings.append({
                "code": "WILD_COMBO_REJECTED_FORBIDDEN",
                "message": (
                    f"Wild-combo suggested adding {add_name!r} but it's on the "
                    f"combo-anchor forbidden list (would complete a combo with "
                    f"a user must-include); dropped."
                ),
            })
            if guard_fire_events is not None:
                guard_fire_events.append({
                    "phase": "C2_2_wild_combo_discovery",
                    "field": "add_swap.add_card",
                    "card": add_name,
                })
            continue

        cand = pool_by_lower.get(add_lower)
        if not cand:
            warnings.append({
                "code": "WILD_COMBO_REJECTED_HALLUCINATION",
                "message": (
                    f"Wild-combo suggested adding {add_name!r} but it isn't "
                    f"in the wide pool; dropped."
                ),
            })
            continue
        if add_lower in deck_names_lower:
            warnings.append({
                "code": "WILD_COMBO_REJECTED_DUPLICATE",
                "message": f"Wild-combo suggested {add_name!r} already in deck; dropped.",
            })
            continue
        ci = set(cand.get("color_identity") or [])
        if ci and not ci.issubset(color_identity):
            warnings.append({
                "code": "WILD_COMBO_REJECTED_CI_ILLEGAL",
                "message": (
                    f"Wild-combo add {add_name!r} CI={sorted(ci)} not subset of "
                    f"commander CI={sorted(color_identity)}; dropped."
                ),
            })
            continue

        # Remove target must exist and not be a user pick.
        remove_idx = None
        remove_is_user_pick = False
        for idx, deck_card in enumerate(deck):
            if deck_card["card_name"].strip().lower() == remove_lower:
                if deck_card.get("source") == "user_intent":
                    remove_is_user_pick = True
                    break
                remove_idx = idx
                break
        if remove_idx is None and not remove_is_user_pick:
            warnings.append({
                "code": "WILD_COMBO_REJECTED_REMOVE_MISSING",
                "message": (
                    f"Wild-combo suggested removing {remove_name!r} but it isn't "
                    f"in the deck; dropped."
                ),
            })
            continue
        if remove_is_user_pick:
            warnings.append({
                "code": "WILD_COMBO_REJECTED_REMOVE_USER_PICK",
                "message": (
                    f"Wild-combo suggested removing {remove_name!r} but it's a "
                    f"user must-include; dropped."
                ),
            })
            continue

        # Bracket combo policy on the post-swap deck.
        deck_after = (deck_names_lower - {remove_lower}) | {add_lower}
        pair_count_after = _count_existing_combo_pairs(
            selected_names_lower=deck_after - {add_lower},
            pair_index=pair_index,
        )
        violates, reason = _combo_violates_bracket(
            candidate_name=add_name,
            selected_names_lower=deck_after - {add_lower},
            user_pick_names_lower=user_pick_names_lower,
            bracket=bracket,
            pair_index=pair_index,
            current_pair_count=pair_count_after,
        )
        if violates:
            warnings.append({
                "code": "WILD_COMBO_REJECTED_BRACKET",
                "message": f"Wild-combo add {add_name!r}: {reason}; dropped to flag instead.",
            })
            partner = str(sug.get("combo_partner") or "").strip() or remove_name
            novel_combo_flags.append({
                "cards": [add_name, partner],
                "outcome": outcome,
                "in_spellbook": bool(sug.get("is_known_spellbook_combo", False)),
                "source": "C2_2_wild_combo_discovery_bracket_demoted",
                "applied_swap": False,
            })
            continue

        # Apply swap.
        partner = str(sug.get("combo_partner") or "").strip()
        is_outlier = bool(sug.get("is_creative_outlier", False))
        new_reason = (
            f"Wild-combo discovery: {outcome or 'synergy add'} "
            f"(partners with {partner!r})." if partner else
            f"Wild-combo discovery: {outcome or 'synergy add'}."
        )
        source = "llm_wild_combo_discovery"
        if is_outlier:
            source += "|creative_outlier"
        deck[remove_idx] = {
            "card_name": add_name,
            "reason": new_reason,
            "source": source,
        }
        deck_names_lower = (deck_names_lower - {remove_lower}) | {add_lower}
        novel_combo_flags.append({
            "cards": [add_name, partner] if partner else [add_name],
            "outcome": outcome,
            "in_spellbook": bool(sug.get("is_known_spellbook_combo", False)),
            "source": "C2_2_wild_combo_discovery_added",
            "applied_swap": True,
            "removed_card": remove_name,
        })

    return deck, warnings


# ============================================================
# Iteration 2 Phase D2 — LLM call #3 (final critic + rationale rewrite).
# ============================================================
#
# After validation passes, the LLM does a final pass over the finished
# deck. Three jobs:
#   1. Rewrite each card's `reason` field so it's substantive and deck-
#      context-aware — referencing specific other cards in THIS deck or
#      specific play patterns. Iteration 1's template-fill reasons
#      ("Theme 'TYPAL_VAMPIRES' signal_count=2") get replaced.
#   2. Compose a 3-5 sentence summary_narrative for the deck.
#   3. Suggest 0-3 "consider adding..." cards that the build pipeline
#      ruled out (NOT added to the deck — surfaced only).

# Iter 3 Phase 1: D2 only rewrites 30 priority cards (down from ~95 in
# iter 2). Output tokens drop, but the Edgar smoke test hit the 2500
# ceiling exactly (Tier-1 self-correction during Phase 1): 30 rewrites
# at ~80-100 tokens + summary_narrative + consider_adding peaked at
# ~2900. Bumped to 3500 for headroom. Phase 3 batched rewrites will
# divide this further across 3 calls, so the per-call ceiling will drop
# again at that point.
_FINAL_CRITIC_INPUT_TOKEN_BUDGET = 12000
_FINAL_CRITIC_OUTPUT_TOKEN_BUDGET = 3500

_FINAL_CRITIC_SYSTEM_PROMPT = (
    "You are reviewing a finalized 100-card MTG Commander deck. Your job "
    "is to write per-card rationale and a deck-level summary that read "
    "like the player's own deck notes — specific, opinionated, grounded "
    "in the actual cards in this deck.\n\n"
    "RULES — hard:\n"
    "1. Per-card rationale: ONE sentence per card. Reference at least "
    "one specific other card in the deck OR a specific play pattern this "
    "deck enables. DO NOT use template phrases like 'fits the theme', "
    "'great staple', 'strong curve' — be concrete. If two cards combo or "
    "synergize, mention each other in their rationale.\n"
    "2. Skip cards that don't need a rewrite (lands and basics — the "
    "iteration-1 reason is fine). For every card you DO include, make it "
    "count.\n"
    "3. summary_narrative: 3-5 sentences. Describe the deck's primary "
    "plan, secondary plan if any, and 1-2 notable tech choices. Avoid "
    "generic 'this deck is a tribal vampire deck' — be specific.\n"
    "4. consider_adding: 0-3 entries. These are cards NOT in the deck "
    "that the player should evaluate. Don't list cards already in the "
    "deck. Each entry must have a one-sentence reason.\n"
    "5. Output VALID JSON ONLY. Card names must be EXACT printed names.\n"
)


def _build_final_critic_user_prompt(
    *,
    commander: str,
    bracket: str,
    theme_hints: List[str],
    intent_analysis: Optional[Dict[str, Any]],
    deck: List[Dict[str, str]],
    priority_cards: List[Dict[str, str]],
    classified_themes: List[Dict[str, Any]],
    strength_check_summary: Optional[Dict[str, Any]],
) -> str:
    # Iter 3 Phase 1: only the priority cards get sent for rewriting.
    # The full-deck list (all 100) still ships as context so the LLM can
    # ground rationales in what's actually around the priority card —
    # but the OUTPUT contract is "rewrite ONLY the priority list."
    priority_names_lower = {
        (c.get("card_name") or "").strip().lower() for c in priority_cards
    }
    deck_lines = []
    for c in deck:
        name = c["card_name"]
        src = c.get("source", "")
        reason = (c.get("reason") or "")[:80]
        marker = " [PRIORITY]" if (name or "").strip().lower() in priority_names_lower else ""
        deck_lines.append(f"  - {name}{marker} | source={src} | iter1_reason='{reason}'")

    theme_block = ""
    if classified_themes:
        ids = [t.get("theme_id") or t.get("name") or "" for t in classified_themes[:5]]
        theme_block = f"\nClassified themes (from deck_analyze_v1): {ids}"

    sc_block = ""
    if strength_check_summary:
        sc_block = (
            f"\nStrength check: bracket_signal={strength_check_summary.get('bracket_signal')!r}, "
            f"mean_similarity={strength_check_summary.get('mean_similarity')}"
        )

    intent_block = ""
    if intent_analysis:
        wc = intent_analysis.get("likely_win_condition") or ""
        themes = intent_analysis.get("implicit_themes") or []
        if wc or themes:
            intent_block = (
                f"\nLLM-inferred intent: win_condition={wc!r}, "
                f"implicit_themes={themes}"
            )

    priority_names = [c.get("card_name", "") for c in priority_cards]

    return (
        f"Commander: {commander}\n"
        f"Bracket: {bracket}\n"
        f"User themes: {theme_hints}{theme_block}{sc_block}{intent_block}\n"
        f"\nFINAL 100-CARD DECK (cards tagged [PRIORITY] are the ones to rewrite):\n"
        + "\n".join(deck_lines)
        + f"\n\nPRIORITY REWRITE LIST ({len(priority_cards)} cards):\n"
        + "\n".join(f"  - {n}" for n in priority_names)
        + "\n\nOutput JSON exactly:\n"
        + "{\n"
        + '  "card_rationales": [\n'
        + '    {"card": "Exact Card Name", "reason": "one specific deck-context sentence"}\n'
        + "  ],\n"
        + '  "summary_narrative": "3-5 sentences describing primary plan, secondary plan, notable tech",\n'
        + '  "consider_adding": [\n'
        + '    {"card": "Card not in deck", "why": "one sentence reason to consider"}\n'
        + "  ]\n"
        + "}\n"
        + "\nRULES:\n"
        + f"- Rewrite EXACTLY the {len(priority_cards)} [PRIORITY] cards listed above. "
        + "Do NOT rewrite any other deck card — those keep their existing rationale.\n"
        + "- Each rewrite is one substantive sentence grounded in specific other cards in this deck.\n"
        + "- consider_adding entries must NOT already be in the deck.\n"
        + "- summary_narrative is 3-5 sentences covering primary plan + secondary plan + notable tech."
    )


def _select_priority_rewrite_cards(
    *,
    deck: List[Dict[str, str]],
    must_include_cards: List[str],
    novel_combo_flags: List[Dict[str, Any]],
    archetype_brief: Dict[str, Any],
    cap: int = 30,
) -> List[Dict[str, str]]:
    """Iter 3 Phase 1 priority selection for D2 rationale rewrites.

    Priority order (highest first):
      1. Commander (always)
      2. Must-include cards (typically 2-5)
      3. Cards flagged as creative_outlier (source contains 'creative_outlier')
      4. Cards participating in novel_combo_flags (cards: [a, b], applied_swap)
      5. Cards with the highest corpus-delta (cards NOT in top-30 staples;
         we fill remaining slots with these because they're the most "this
         deck specifically" picks — exactly where deck-context rationale
         adds the most signal)

    Returns a list of deck card dicts; len <= cap. The other (~65) cards
    keep their iteration-2 rationales.
    """
    selected: List[Dict[str, str]] = []
    selected_names_lower: set = set()

    def _add(card: Dict[str, str]) -> bool:
        n = (card.get("card_name") or "").strip().lower()
        if not n or n in selected_names_lower:
            return False
        selected.append(card)
        selected_names_lower.add(n)
        return True

    if not deck:
        return selected

    # 1. Commander (always the first deck entry).
    _add(deck[0])

    # 2. Must-includes.
    mi_lower = {(m or "").strip().lower() for m in (must_include_cards or [])}
    for card in deck:
        if (card.get("card_name") or "").strip().lower() in mi_lower:
            _add(card)
            if len(selected) >= cap:
                return selected

    # 3. Creative outliers.
    for card in deck:
        src = card.get("source") or ""
        if "creative_outlier" in src:
            _add(card)
            if len(selected) >= cap:
                return selected

    # 4. Cards participating in novel_combo_flags.
    combo_card_names: set = set()
    for flag in novel_combo_flags or []:
        for n in flag.get("cards") or []:
            if isinstance(n, str) and n.strip():
                combo_card_names.add(n.strip().lower())
    for card in deck:
        if (card.get("card_name") or "").strip().lower() in combo_card_names:
            _add(card)
            if len(selected) >= cap:
                return selected

    # 5. Highest-corpus-delta cards (not in top-30 corpus staples, not
    #    basics, not already selected).
    staples_sorted = sorted(
        (archetype_brief or {}).get("staple_cards") or [],
        key=lambda s: float(s.get("usage_pct") or 0.0),
        reverse=True,
    )
    top30_staple_names_lower = {
        (s.get("name") or "").strip().lower() for s in staples_sorted[:30]
    }
    basic_names_lower = {n.lower() for n in _BASIC_LAND_NAMES}
    # Fill in deck order (Phase C2.1 + C2.2 already put high-priority
    # picks near the top of the body section).
    for card in deck[1:]:
        cname_lower = (card.get("card_name") or "").strip().lower()
        if not cname_lower or cname_lower in basic_names_lower:
            continue
        if cname_lower in top30_staple_names_lower:
            continue
        if _add(card) and len(selected) >= cap:
            return selected

    # 6. Backstop — if we still haven't hit the cap, fill with any non-
    #    basic deck cards not already selected.
    if len(selected) < cap:
        for card in deck[1:]:
            cname_lower = (card.get("card_name") or "").strip().lower()
            if not cname_lower or cname_lower in basic_names_lower:
                continue
            if _add(card) and len(selected) >= cap:
                break

    return selected


def _run_final_critic(
    *,
    llm_client: Any,
    deck: List[Dict[str, str]],
    commander: str,
    bracket: str,
    theme_hints: List[str],
    intent_analysis: Optional[Dict[str, Any]],
    last_findings: Dict[str, Any],
    llm_metrics: Dict[str, Any],
    must_include_cards: Optional[List[str]] = None,
    novel_combo_flags: Optional[List[Dict[str, Any]]] = None,
    archetype_brief: Optional[Dict[str, Any]] = None,
    forbidden_set: Optional[Set[str]] = None,
    forbidden_prompt_block: str = "",
    guard_fire_events: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """Run the final critic. Returns (deck_with_rewritten_reasons, warnings).
    Mutates `last_findings` to add `summary_narrative` + `consider_adding`.

    Iter 3 Phase 1: D2 only rewrites a priority-30 subset of the deck
    instead of the full ~95 non-basic cards. The other cards keep their
    iteration-2 rationales (which were already substantive).
    """
    warnings: List[Dict[str, str]] = []

    classified_themes = last_findings.get("themes_classified") or []
    strength_check_summary = last_findings.get("strength_check_summary")

    # Iter 3 Phase 1: select priority-30 cards for rewriting.
    priority_cards = _select_priority_rewrite_cards(
        deck=deck,
        must_include_cards=must_include_cards or [],
        novel_combo_flags=novel_combo_flags or [],
        archetype_brief=archetype_brief or {},
        cap=30,
    )

    system = _FINAL_CRITIC_SYSTEM_PROMPT + (forbidden_prompt_block or "")
    user = _build_final_critic_user_prompt(
        commander=commander, bracket=bracket,
        theme_hints=theme_hints, intent_analysis=intent_analysis,
        deck=deck, priority_cards=priority_cards,
        classified_themes=classified_themes,
        strength_check_summary=strength_check_summary,
    )

    result = llm_client.call_with_budget(
        system=system, user=user,
        max_input_tokens=_FINAL_CRITIC_INPUT_TOKEN_BUDGET,
        max_output_tokens=_FINAL_CRITIC_OUTPUT_TOKEN_BUDGET,
    )
    llm_metrics["calls"].append({
        "phase": "D2_final_critic",
        "ok": result.ok,
        "input_tokens": result.input_tokens,
        "output_tokens": result.output_tokens,
        "cost_usd": result.cost_usd,
        "latency_ms": result.latency_ms,
        "error_code": result.error_code,
        "retries": result.retries,
    })
    if not result.ok:
        warnings.append({
            "code": "FINAL_CRITIC_FAILED",
            "message": (
                f"LLM call #3 (final critic + rationale rewrite) failed: "
                f"{result.error_code}: {result.error_message}. "
                "Per-card reasons remain from iteration 1; summary_narrative empty."
            ),
        })
        return deck, warnings
    parsed = result.parsed_json
    if not isinstance(parsed, dict):
        warnings.append({
            "code": "FINAL_CRITIC_INVALID_JSON",
            "message": (
                "LLM call #3 returned non-JSON output; per-card reasons unchanged. "
                f"Raw text head: {result.text[:200]!r}"
            ),
        })
        return deck, warnings

    # Apply per-card rationale rewrites. Match on card name (case-
    # insensitive). Cards the LLM didn't address keep their iteration-1
    # reason — that's intentional per the prompt.
    rationales = _as_list_of_dicts(parsed.get("card_rationales"))
    rewrites_by_name_lower: Dict[str, str] = {}
    for entry in rationales:
        name = str(entry.get("card") or "").strip()
        reason = str(entry.get("reason") or "").strip()
        if name and reason:
            rewrites_by_name_lower[name.lower()] = reason

    rewrite_count = 0
    for card in deck:
        cname_lower = card["card_name"].strip().lower()
        new_reason = rewrites_by_name_lower.get(cname_lower)
        if new_reason:
            # Preserve original reason as a debug field; bump source.
            card["reason"] = new_reason
            existing_src = card.get("source") or ""
            if "llm_rationale_rewrite" not in existing_src.split("|"):
                card["source"] = (existing_src + "|llm_rationale_rewrite") if existing_src else "llm_rationale_rewrite"
            rewrite_count += 1

    if rewrite_count == 0:
        warnings.append({
            "code": "FINAL_CRITIC_NO_REWRITES",
            "message": "LLM call #3 produced 0 applicable per-card rationale rewrites.",
        })

    # Stash narrative + consider_adding on last_findings — the summary
    # block at the top of compute_agent_build_deck_v1 reads from there.
    summary_narrative = str(parsed.get("summary_narrative") or "").strip()
    if summary_narrative:
        last_findings["summary_narrative"] = summary_narrative

    consider_adding_raw = _as_list_of_dicts(parsed.get("consider_adding"))
    deck_names_lower = {c["card_name"].strip().lower() for c in deck}
    consider_adding: List[Dict[str, str]] = []
    for entry in consider_adding_raw:
        name = str(entry.get("card") or "").strip()
        why = str(entry.get("why") or "").strip()
        if not name or not why:
            continue
        # Suppress entries that turn out to already be in the deck.
        if name.lower() in deck_names_lower:
            continue
        # Iter 3 Phase 2: reject combo-anchor-forbidden adds.
        if forbidden_set and name.lower() in forbidden_set:
            if guard_fire_events is not None:
                guard_fire_events.append({
                    "phase": "D2_final_critic",
                    "field": "consider_adding",
                    "card": name,
                })
            continue
        consider_adding.append({"card": name, "why": why})
        if len(consider_adding) >= 3:
            break
    last_findings["consider_adding"] = consider_adding

    return deck, warnings
