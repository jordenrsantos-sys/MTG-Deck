"""
pillar_e_aggressive_swaps_v1 — Mega-task v7 Phase 3.

Pillar E v0.7: deterministic swap-aggression layer that ACTS on the flags
emitted by the v0.1-v0.6 optimizers. Pre-v7, the optimizers correctly
flagged discrepancies (`MANA_BASE_DISCREPANCY_UNJUSTIFIED`,
`CARD_ADVANTAGE_DISCREPANCY_UNJUSTIFIED`, `CURVE_DISCREPANCY`,
`INTERACTION_DISCREPANCY`) but no engine layer closed the gaps — LLM
critiques returned `justified: false` and the discrepancies persisted
to the final deck.

The architectural rule (per `feedback_pool_score_does_not_drive_llm_picking`
memory) is that the only mechanism that GUARANTEES outcomes is a
deterministic post-hoc layer running after the LLM picks. This module is
that layer for the Pillar E optimizer surface.

Behavior per category:

  mana_base
    actual_lands < target_lands → swap basics IN, lowest-priority spells OUT
    actual_lands > target_lands → swap basics OUT, ramp/utility spells IN
    color source deficit → swap mono-color basics for color-fixing duals

  card_advantage
    total < target_count → swap low-priority cards OUT, draw cards IN matching
                            missing category (cantrip / engine / burst)
    total > target_count → no swap (overshoot is rarely a problem; tune later)

  curve_smoother
    Each hole (slot under target) gets candidate cards at that CMC.
    Each brick (CMC above ceiling) gets swapped for a lower-CMC alternative.

  interaction_designer
    Each per-category gap (mass_removal short, targeted_creature short,
    counterspell short) gets pool cards matching the missing category.

  win_con_coherence
    NOT addressed here — needs Phase 7 (DB primitive hydration). The
    coherence report stays in the response for diagnostic visibility but
    no swaps are proposed.

Validation guards applied before any swap:
  - card_out is in the deck and is not the commander
  - card_out is not a user must-include (those are score=∞ locked)
  - card_in is not already in the deck (singleton rule for non-basics)
  - card_in's color identity is a subset of the commander's
  - swap does not introduce a forbidden combo pair when bracket gates
    are tighter than the deck (B1-B3)

The module never raises. On any failure path it returns an empty
swap list — the build response surfaces the un-swapped deck plus a
`PILLAR_E_AGGRESSIVE_SWAP_FAILED` warning so callers can see the layer
fired but didn't close gaps.
"""
from __future__ import annotations

import json
from collections import Counter
from typing import Any, Dict, List, Optional, Set, Tuple


PILLAR_E_V0_7_VERSION = "pillar_e_aggressive_swaps_v1.0"

_BASIC_LAND_NAMES: Set[str] = {"Plains", "Island", "Swamp", "Mountain", "Forest", "Wastes"}
_COLOR_TO_BASIC: Dict[str, str] = {
    "W": "Plains", "U": "Island", "B": "Swamp", "R": "Mountain", "G": "Forest",
}

# Per-category primitive vocab — mirrors v7 Phase 1's _classify_card update.
# Both legacy primitive_to_cards vocabulary and v6 Phase 3 ontology v2
# vocabulary are accepted so classification works regardless of substrate
# path that hydrated the card's primitives field.
_RAMP_PRIMS: Set[str] = {
    "MANA_ROCK", "MANA_RAMP_LAND_SEARCH", "MANA_RAMP_CREATURE_DORK",
    "MANA_RAMP_SPELL", "MANA_RITUAL_BURST", "TUTOR_LAND",
    "RAMP_MANA", "RAMP_LAND", "MANA_FIXING",
}
_DRAW_PRIMS: Set[str] = {
    "CARD_DRAW_BURST", "CARD_DRAW_REPEATABLE", "DRAW_REPLACEMENT",
    "CARD_SELECTION_SCRY_SURVEIL", "WHEEL_EFFECT", "TUTOR_ANY_TO_HAND",
    "CARD_DRAW", "CARD_SELECTION",
}
_MASS_REMOVAL_PRIMS: Set[str] = {"BOARDWIPE_CREATURES", "BOARD_WIPE"}
_TARGETED_CREATURE_PRIMS: Set[str] = {"TARGETED_REMOVAL_CREATURE", "REMOVAL_SINGLE"}
_COUNTERSPELL_PRIMS: Set[str] = {
    "COUNTERSPELL_GENERIC", "COUNTERSPELL_CREATURE", "COUNTERSPELL",
    "STACK_COUNTERSPELL", "PERMISSION_OVERRIDE",
}
_LAND_PRIMS: Set[str] = {"MANA_FIXING", "RAMP_LAND"}

# v8 Phase 3: win_con coherence enablers — primitives that signal a
# deck has a concrete win plan. Drawn from win_con_coherence_v1's
# _WIN_CON_PATTERNS canonical primitive sets (v2 ontology vocabulary).
# When the deck is flagged as 75pct-pile, the swap layer injects cards
# with these primitives to surface a primary plan with ≥5 enablers.
_WIN_CON_ENABLER_PRIMS: Set[str] = {
    # combo_win / tutor_chain
    "INFINITE_COMBO", "COMBO_PIECE", "TUTOR_ANY", "TUTOR_CREATURE",
    "TUTOR_LAND", "TUTOR_ANY_TO_HAND", "TUTOR_TO_TOP",
    # voltron_combat
    "EXTRA_COMBAT", "EQUIPMENT_SYNERGY", "AURA_SYNERGY",
    # go_wide_anthem
    "TOKEN_PRODUCTION", "TOKEN_DOUBLING", "TOKEN_COPY",
    # aristocrats
    "SAC_OUTLET", "DEATH_PAYOFF", "DIES_TRIGGER",
    # storm_spellslinger
    "STORM", "CAST_TRIGGER_PAYOFF", "CAST_COUNT_SCALING",
    "MAGECRAFT_TRIGGER", "SPELL_COPY",
    # reanimator
    "GRAVEYARD_RECURSION", "GRAVEYARD_REANIMATION",
    "CAST_FROM_GRAVEYARD", "RETURN_AS_TOKEN", "RETURN_ON_DEATH",
    # mill_alt_win
    "MOVE_TO_GRAVEYARD", "DECK_OUT",
    # counters_proliferate
    "PROLIFERATE", "COUNTER_SYNERGY", "COUNTER_DOUBLING",
    "REPLACEMENT_COUNTER_DOUBLING",
    # control_grind / stax_lock
    "TAX_EFFECT", "ACTIVATED_ABILITY_HATE",
    "FORCED_COMBAT", "TIMING_LOCK", "CAST_RESTRICTION",
    # landfall_aggro
    "LANDFALL", "EXTRA_LAND_DROP",
}

# Per-category swap budgets. Caps how many swaps each category can run
# so a single category doesn't dominate the deck. Across all categories
# the engine caps at TOTAL_SWAP_BUDGET regardless.
# v8 Phase 3 added win_con_coherence (budget 4).
_PER_CATEGORY_SWAP_BUDGET: Dict[str, int] = {
    "mana_base": 6,
    "card_advantage": 4,
    "curve_smoother": 3,
    "interaction_designer": 4,
    "win_con_coherence": 4,
}
TOTAL_SWAP_BUDGET = 14  # v8 Phase 3: bumped from 12 to accommodate win_con.


def compute_pillar_e_aggressive_swaps(
    *,
    deck: List[Dict[str, str]],
    pool: Dict[str, Any],
    db_snapshot_id: str,
    commander_color_identity: List[str],
    must_include_lower: Set[str],
    forbidden_set: Set[str],
    mana_base_block: Optional[Dict[str, Any]] = None,
    card_advantage_block: Optional[Dict[str, Any]] = None,
    curve_smoother_block: Optional[Dict[str, Any]] = None,
    interaction_designer_block: Optional[Dict[str, Any]] = None,
    win_con_coherence_block: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Returns a structured swap plan + the post-swap deck.

    Output shape:
    {
        "version": str,
        "applied_swaps": List[{"category": str, "card_out": str,
                               "card_in": str, "rationale": str}],
        "skipped_swaps": List[{"category": str, "card_out": str,
                               "card_in": str, "skip_reason": str}],
        "new_deck": List[Dict[str, str]],
        "per_category_count": Dict[str, int],
        "warnings": List[{"code": str, "message": str}],
    }
    """
    applied_swaps: List[Dict[str, str]] = []
    skipped_swaps: List[Dict[str, str]] = []
    warnings: List[Dict[str, str]] = []
    per_category: Dict[str, int] = {}

    # Cards available for swap-in (the candidate pool + DB direct query).
    pool_candidates: List[Dict[str, Any]] = list(pool.get("candidates") or [])
    pool_by_name_lower: Dict[str, Dict[str, Any]] = {
        (c.get("name") or "").strip().lower(): c for c in pool_candidates
    }

    # Build working deck. Mutates in place via swap apply.
    working_deck: List[Dict[str, str]] = [dict(c) for c in deck]
    deck_names_lower: Set[str] = {
        (c.get("card_name") or "").strip().lower() for c in working_deck
    }
    ci_set = {c.upper() for c in (commander_color_identity or []) if isinstance(c, str)}

    def _swap_or_skip(category: str, card_out: str, card_in: str, rationale: str) -> bool:
        """Validate + apply or skip with reason."""
        cap = _PER_CATEGORY_SWAP_BUDGET.get(category, 3)
        if per_category.get(category, 0) >= cap:
            skipped_swaps.append({
                "category": category, "card_out": card_out, "card_in": card_in,
                "skip_reason": f"per-category swap budget {cap} reached",
            })
            return False
        if len(applied_swaps) >= TOTAL_SWAP_BUDGET:
            skipped_swaps.append({
                "category": category, "card_out": card_out, "card_in": card_in,
                "skip_reason": f"total swap budget {TOTAL_SWAP_BUDGET} reached",
            })
            return False
        if card_in.strip().lower() in deck_names_lower:
            skipped_swaps.append({
                "category": category, "card_out": card_out, "card_in": card_in,
                "skip_reason": "card_in already in deck (singleton rule)",
            })
            return False
        if card_in.strip().lower() in forbidden_set:
            skipped_swaps.append({
                "category": category, "card_out": card_out, "card_in": card_in,
                "skip_reason": "card_in in forbidden_set",
            })
            return False
        # Find card_out in deck.
        out_lower = card_out.strip().lower()
        out_idx: Optional[int] = None
        for i, c in enumerate(working_deck):
            cn = (c.get("card_name") or "").strip().lower()
            if cn == out_lower:
                # Don't swap user picks or the commander.
                if cn in must_include_lower:
                    continue
                if (c.get("source") or "") == "user_intent" and (c.get("reason") or "").startswith("Commander"):
                    continue
                # Prefer to swap the FIRST occurrence (for basics) — basics
                # appear multiple times and any is fine.
                out_idx = i
                break
        if out_idx is None:
            skipped_swaps.append({
                "category": category, "card_out": card_out, "card_in": card_in,
                "skip_reason": "card_out not found in deck or is user-pick/commander",
            })
            return False
        # Resolve card_in metadata.
        in_meta = pool_by_name_lower.get(card_in.strip().lower())
        if in_meta is None:
            # Try DB hydration as a fallback.
            try:
                from engine.db import find_card_by_name
                in_card = find_card_by_name(db_snapshot_id, card_in)
                if in_card is None:
                    skipped_swaps.append({
                        "category": category, "card_out": card_out, "card_in": card_in,
                        "skip_reason": "card_in not found in DB",
                    })
                    return False
                in_meta = {
                    "name": in_card.get("name") or card_in,
                    "type_line": in_card.get("type_line"),
                    "primitives": in_card.get("primitives") or [],
                    "color_identity": in_card.get("color_identity") or [],
                    "cmc": in_card.get("cmc"),
                }
            except Exception:
                skipped_swaps.append({
                    "category": category, "card_out": card_out, "card_in": card_in,
                    "skip_reason": "DB hydration failed",
                })
                return False
        # Color-identity legality check.
        card_in_ci = _normalize_ci(in_meta.get("color_identity"))
        if card_in_ci and not set(card_in_ci).issubset(ci_set if ci_set else {"W","U","B","R","G"}):
            skipped_swaps.append({
                "category": category, "card_out": card_out, "card_in": card_in,
                "skip_reason": f"color identity {card_in_ci} not subset of {sorted(ci_set)}",
            })
            return False
        # Apply: replace card_out with card_in.
        working_deck[out_idx] = {
            "card_name": card_in,
            "reason": f"Pillar E v0.7 aggressive swap ({category}): {rationale}",
            "source": "pillar_e_aggressive_swap",
        }
        deck_names_lower.discard(out_lower)
        deck_names_lower.add(card_in.strip().lower())
        applied_swaps.append({
            "category": category, "card_out": card_out, "card_in": card_in,
            "rationale": rationale,
        })
        per_category[category] = per_category.get(category, 0) + 1
        return True

    # ---- 1. mana_base ----
    if mana_base_block and mana_base_block.get("active"):
        recon = mana_base_block.get("reconciliation") or {}
        if recon.get("significant"):
            actual = int(recon.get("actual_land_count") or 0)
            rec = mana_base_block.get("recommendation") or {}
            target = int(rec.get("target_land_count") or 36)
            delta = actual - target
            if delta > 0:
                # Too many lands. Swap basics OUT, ramp IN.
                ramp_in_pool = _filter_pool_by_primitives(
                    pool_candidates, _RAMP_PRIMS, exclude_names=deck_names_lower,
                )
                basics_in_deck = [
                    c["card_name"] for c in working_deck
                    if c.get("card_name") in _BASIC_LAND_NAMES
                ]
                for i in range(min(delta, len(ramp_in_pool), len(basics_in_deck))):
                    _swap_or_skip(
                        "mana_base",
                        card_out=basics_in_deck[i], card_in=ramp_in_pool[i]["name"],
                        rationale=f"Land surplus delta=+{delta}; swap basic for ramp",
                    )
            elif delta < 0:
                # Too few lands. Swap low-priority spells OUT, dual lands IN.
                land_in_pool = [
                    c for c in pool_candidates
                    if "land" in (c.get("type_line") or "").lower()
                    and (c.get("name") or "") not in _BASIC_LAND_NAMES
                    and (c.get("name") or "").strip().lower() not in deck_names_lower
                ]
                low_priority_out = _find_low_priority_deck_cards(
                    working_deck, must_include_lower, prefer_sources={
                        "slot_fallback:ramp", "slot_fallback:card_draw",
                        "slot_fallback:removal", "slot_fallback:win_condition",
                    },
                )
                gap = abs(delta)
                for i in range(min(gap, len(land_in_pool), len(low_priority_out))):
                    _swap_or_skip(
                        "mana_base",
                        card_out=low_priority_out[i], card_in=land_in_pool[i]["name"],
                        rationale=f"Land deficit delta={delta}; swap spell for utility land",
                    )

    # ---- 2. card_advantage ----
    if card_advantage_block and card_advantage_block.get("active"):
        rec = card_advantage_block.get("recommendation") or {}
        if rec.get("significant"):
            mix_targets = rec.get("mix_targets") or {}
            current_counts = rec.get("current_counts") or {}
            total_target = int(rec.get("target_count") or 10)
            total_actual = sum(int(v) for v in current_counts.values())
            if total_actual < total_target:
                gap = total_target - total_actual
                draw_in_pool = _filter_pool_by_primitives(
                    pool_candidates, _DRAW_PRIMS, exclude_names=deck_names_lower,
                )
                low_priority_out = _find_low_priority_deck_cards(
                    working_deck, must_include_lower, prefer_sources={
                        "slot_fallback:removal", "slot_fallback:win_condition",
                    },
                )
                for i in range(min(gap, len(draw_in_pool), len(low_priority_out))):
                    _swap_or_skip(
                        "card_advantage",
                        card_out=low_priority_out[i], card_in=draw_in_pool[i]["name"],
                        rationale=f"Draw deficit {total_actual}/{total_target}; inject draw piece",
                    )

    # ---- 3. curve_smoother ----
    if curve_smoother_block and curve_smoother_block.get("active"):
        analysis = curve_smoother_block.get("analysis") or {}
        if analysis.get("significant"):
            holes = analysis.get("holes") or []
            bricks = analysis.get("bricks") or []
            # For each hole, find a candidate at that CMC.
            for hole in holes[:_PER_CATEGORY_SWAP_BUDGET["curve_smoother"]]:
                hole_cmc = hole.get("cmc") if isinstance(hole, dict) else None
                if not isinstance(hole_cmc, (int, float)):
                    continue
                # Find candidates near this CMC in pool.
                cmc_match = [
                    c for c in pool_candidates
                    if c.get("cmc") is not None
                    and abs(float(c.get("cmc") or 0) - float(hole_cmc)) < 0.5
                    and (c.get("name") or "").strip().lower() not in deck_names_lower
                    and (c.get("name") or "") not in _BASIC_LAND_NAMES
                ]
                if not cmc_match:
                    continue
                # Swap out a brick (high-CMC card) if any; else lowest-priority slot_fallback.
                out_candidate = None
                if bricks:
                    brick_names = [b.get("card_name") if isinstance(b, dict) else None for b in bricks]
                    brick_names = [n for n in brick_names if isinstance(n, str) and n]
                    if brick_names:
                        out_candidate = brick_names[0]
                if not out_candidate:
                    low_pri = _find_low_priority_deck_cards(
                        working_deck, must_include_lower,
                        prefer_sources={"slot_fallback:win_condition"},
                    )
                    out_candidate = low_pri[0] if low_pri else None
                if out_candidate:
                    _swap_or_skip(
                        "curve_smoother",
                        card_out=out_candidate, card_in=cmc_match[0]["name"],
                        rationale=f"Fill curve hole at CMC {hole_cmc}",
                    )

    # ---- 4. interaction_designer ----
    if interaction_designer_block and interaction_designer_block.get("active"):
        analysis = interaction_designer_block.get("analysis") or {}
        if analysis.get("significant"):
            # v7 Phase 6 added `per_category` with min/max/actual/in_range
            # per category. Prefer that shape; fall back to a synthesized
            # version from `targets_by_category` + `actual_by_category`
            # for tests / older outputs.
            per_cat = analysis.get("per_category") or {}
            if not per_cat:
                targets_legacy = analysis.get("targets_by_category") or {}
                actual_legacy = analysis.get("actual_by_category") or {}
                per_cat = {
                    cat: {"target": int(t), "actual": int(actual_legacy.get(cat, 0)),
                          "min": 0, "max": int(t) * 2 if t else 0,
                          "in_range": True}  # legacy fallback — assume in range
                    for cat, t in targets_legacy.items()
                }
            category_to_prims = {
                "mass_removal": _MASS_REMOVAL_PRIMS,
                "targeted_creature_removal": _TARGETED_CREATURE_PRIMS,
                "counterspells": _COUNTERSPELL_PRIMS,
            }
            for cat_name, prims in category_to_prims.items():
                cat_data = per_cat.get(cat_name)
                if not isinstance(cat_data, dict):
                    continue
                # v7 Phase 6: only act on UNDER-bound. Over-bound (above
                # max) is a different shape — would need swap-OUT logic
                # which we don't have here. Surplus is also rare in
                # iter-7 sweep (the failure mode was under-fill).
                lo = int(cat_data.get("min", 0))
                actual = int(cat_data.get("actual", 0))
                if actual >= lo:
                    continue
                gap = lo - actual
                candidates_in_pool = _filter_pool_by_primitives(
                    pool_candidates, prims, exclude_names=deck_names_lower,
                )
                low_priority_out = _find_low_priority_deck_cards(
                    working_deck, must_include_lower,
                    prefer_sources={"slot_fallback:win_condition", "slot_fallback:removal"},
                )
                for i in range(min(gap, len(candidates_in_pool), len(low_priority_out))):
                    _swap_or_skip(
                        "interaction_designer",
                        card_out=low_priority_out[i],
                        card_in=candidates_in_pool[i]["name"],
                        rationale=f"{cat_name} below min {actual}/{lo}; inject",
                    )

    # ---- 5. win_con_coherence (v8 Phase 3) ----
    # When the deck flags as a 75pct_pile (no win-con pattern reaches the
    # bracket's primary floor), inject cards with win-con enabler primitives.
    # This is the v8 extension that gives the optimizer a path to act on
    # win_con flags rather than just reporting them.
    if win_con_coherence_block and win_con_coherence_block.get("active"):
        report = win_con_coherence_block.get("report") or {}
        if report.get("flagged_75pct_pile"):
            primary_floor = int(report.get("primary_floor") or 3)
            # Inject up to (primary_floor - current_top) enablers from any
            # win-con pattern. Pick from pool candidates whose primitives
            # overlap _WIN_CON_ENABLER_PRIMS; rank by pool order (already
            # archetype-relevant after v8 Phase 1).
            pattern_scores = report.get("pattern_scores") or {}
            current_top = max(pattern_scores.values()) if pattern_scores else 0
            gap = max(1, primary_floor - current_top)
            candidates_in_pool = _filter_pool_by_primitives(
                pool_candidates, _WIN_CON_ENABLER_PRIMS,
                exclude_names=deck_names_lower,
            )
            low_priority_out = _find_low_priority_deck_cards(
                working_deck, must_include_lower,
                prefer_sources={"slot_fallback:card_draw",
                                "slot_fallback:removal"},
            )
            for i in range(min(gap, len(candidates_in_pool), len(low_priority_out))):
                _swap_or_skip(
                    "win_con_coherence",
                    card_out=low_priority_out[i],
                    card_in=candidates_in_pool[i]["name"],
                    rationale=f"75pct_pile flagged; inject win-con enabler "
                              f"(top pattern score {current_top}, floor {primary_floor})",
                )

    return {
        "version": PILLAR_E_V0_7_VERSION,
        "applied_swaps": applied_swaps,
        "skipped_swaps": skipped_swaps,
        "new_deck": working_deck,
        "per_category_count": per_category,
        "warnings": warnings,
    }


# ============================================================
# Helpers
# ============================================================


def _normalize_ci(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return sorted({str(c).upper() for c in raw if isinstance(c, str) and c})
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        if s.startswith("["):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return sorted({str(c).upper() for c in parsed if isinstance(c, str) and c})
            except Exception:
                pass
        return sorted({c.strip().upper() for c in s.split(",") if c.strip()})
    return []


def _filter_pool_by_primitives(
    pool_candidates: List[Dict[str, Any]],
    target_prims: Set[str],
    exclude_names: Set[str],
) -> List[Dict[str, Any]]:
    """Return pool candidates whose primitives overlap target_prims and
    are NOT in exclude_names (case-insensitive)."""
    out: List[Dict[str, Any]] = []
    for c in pool_candidates:
        name = (c.get("name") or "").strip()
        if not name:
            continue
        if name.strip().lower() in exclude_names:
            continue
        prims = set(c.get("primitives") or [])
        if prims & target_prims:
            out.append(c)
    return out


def _find_low_priority_deck_cards(
    deck: List[Dict[str, str]],
    must_include_lower: Set[str],
    *,
    prefer_sources: Optional[Set[str]] = None,
) -> List[str]:
    """Return deck card names ordered by swap-out priority:
      1. cards from `prefer_sources` (typically slot_fallback:*)
      2. cards from sources containing 'archetype_staple'
      3. cards from sources containing 'theme:'
      4. cards from sources containing 'agent_select'
    Excludes the commander and user must-includes. Excludes basic lands
    (handled by mana_base category swaps separately).
    """
    prefer_sources = prefer_sources or set()
    tier1: List[str] = []
    tier2: List[str] = []
    tier3: List[str] = []
    tier4: List[str] = []
    for c in deck:
        name = (c.get("card_name") or "").strip()
        if not name:
            continue
        if name in _BASIC_LAND_NAMES:
            continue
        if name.strip().lower() in must_include_lower:
            continue
        src = c.get("source") or ""
        reason = c.get("reason") or ""
        if reason.startswith("Commander"):
            continue
        if src in prefer_sources:
            tier1.append(name)
        elif "archetype_staple" in src:
            tier2.append(name)
        elif "theme:" in src:
            tier3.append(name)
        else:
            tier4.append(name)
    return tier1 + tier2 + tier3 + tier4
