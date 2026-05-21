"""
mana_base_optimizer_v1 — Pillar E v0.1.

Deterministic mana-base recommender. Takes a deck's nonland card list +
commander color identity + bracket and outputs Karsten-compliant
source-count targets and a paragraph rationale. Iter 4+ may extend
with curve-aware utility land selection; v0.1 ships the source-count
math and the integration hook.

Hybrid architecture (per the 5-pillar plan): this module is
DETERMINISTIC. It produces a recommendation; the LLM in the agent
flow critiques discrepancies between recommendation and what
iteration 1/2/3 actually picked. Mana-base mechanics (Karsten's
color-source formula) are NOT subject to the creativity envelope —
they're mechanical heuristics, not creative picks.

Reference: Frank Karsten's "How Many Colored Mana Sources Do You
Need..." article. The published table maps (CMC, requirement) →
sources needed in a 60-card deck; we scale to 100-card Commander
(roughly +5 sources for each Karsten target, per the same article's
Commander update).

Public API:
  - compute_mana_base(commander_color_identity, nonland_cards,
                      bracket, archetype_hint=None) -> ManaBaseRecommendation
  - ManaBaseRecommendation: dataclass with target_land_count,
    color_source_targets, tap_land_tolerance, utility_land_budget,
    basic_nonbasic_ratio, rationale
"""
from __future__ import annotations

import re
from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Sequence


MANA_BASE_OPTIMIZER_VERSION = "mana_base_optimizer_v1.0"


# ============================================================
# Karsten table (Commander / 100-card).
# ============================================================
#
# Sources needed by (CMC, color_pip_count) — values derived from
# Karsten's 60-card table + his Commander adjustment (≈ +5 sources per
# row to compensate for the larger library + extra opening-hand
# distribution).
#
# Index: row[cmc][pip_count] = sources needed.
#   - pip_count = 1 means "X" cost at CMC, e.g. {1}{B} at CMC 2 has 1 B pip.
#   - pip_count = 2 means "XX" cost, e.g. {B}{B} or {2}{B}{B}.
#   - pip_count = 3 means "XXX" cost.
#
# CMCs 1-7 are explicit; CMC 8+ uses the CMC=7 row (rare for casting
# costs to demand more than 7 with double/triple-pip beyond that).

KARSTEN_TABLE_COMMANDER: Dict[int, Dict[int, int]] = {
    1: {1: 19, 2: 0,  3: 0},
    2: {1: 18, 2: 23, 3: 0},
    3: {1: 16, 2: 20, 3: 23},
    4: {1: 15, 2: 18, 3: 22},
    5: {1: 14, 2: 17, 3: 20},
    6: {1: 13, 2: 16, 3: 19},
    7: {1: 12, 2: 15, 3: 18},
}


def _pip_sources_required(cmc: int, pip_count: int) -> int:
    """Look up Karsten sources required at (CMC, pip count). CMC<1
    treated as 1; CMC>7 treated as 7. pip_count<1 returns 0."""
    if pip_count <= 0:
        return 0
    cmc = max(1, min(7, int(cmc or 1)))
    pip_count = min(3, pip_count)
    return KARSTEN_TABLE_COMMANDER.get(cmc, {}).get(pip_count, 0)


# ============================================================
# Bracket policy.
# ============================================================


# Per-bracket land count targets (commander = 100-card; lands include
# basics, duals, fetches, utility lands).
_BRACKET_LAND_TARGET: Dict[str, int] = {
    "B1": 38,
    "B2": 37,
    "B3": 36,
    "B4": 35,
    "B5": 32,  # cEDH baseline; storm and other fast strategies can go lower
}

_BRACKET_TAP_LAND_TOLERANCE: Dict[str, int] = {
    "B1": 12,
    "B2": 10,
    "B3": 6,
    "B4": 3,
    "B5": 0,
}

_BRACKET_BASIC_NONBASIC_RATIO: Dict[str, float] = {
    "B1": 0.50,
    "B2": 0.40,
    "B3": 0.30,
    "B4": 0.20,
    "B5": 0.12,
}

_BRACKET_UTILITY_LAND_BUDGET: Dict[str, int] = {
    "B1": 2,
    "B2": 3,
    "B3": 5,
    "B4": 7,
    "B5": 8,
}


# Archetype adjustments — applied additively after the bracket base.
_ARCHETYPE_LAND_DELTA: Dict[str, int] = {
    "storm":      -4,   # rituals replace lands
    "reanimator": -2,   # less land-dependent
    "landfall":   +2,   # more lands = more triggers
    "control":    +1,   # extra mana on opp's turn
    "voltron":    -1,   # cheaper deck, lower CMC
    "aristocrats": 0,
    "tribal":      0,
    "combo":      -1,
    "tokens":      0,
    "blink":       0,
    "group_hug":   0,
    "default":     0,
}


# ============================================================
# Output dataclass.
# ============================================================


@dataclass
class ManaBaseRecommendation:
    target_land_count: int
    color_source_targets: Dict[str, int]
    tap_land_tolerance: int
    utility_land_budget: int
    basic_nonbasic_ratio: float
    rationale: str
    # Per-card analysis (helpful for the LLM critique pass).
    requirements_summary: List[Dict[str, Any]] = field(default_factory=list)
    version: str = MANA_BASE_OPTIMIZER_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================
# Mana-cost parser.
# ============================================================


_PIP_RE = re.compile(r"\{([^}]+)\}")


def _parse_color_pips(mana_cost: str) -> Counter:
    """Return a Counter mapping color letter (W/U/B/R/G) → pip count.
    Hybrid pips (e.g. {W/B}) count toward BOTH colors at 0.5 each;
    we round up to 1 in either color to be safe (Karsten formula
    assumes worst case for a single color's source needs)."""
    if not mana_cost:
        return Counter()
    pips: Counter = Counter()
    for token in _PIP_RE.findall(mana_cost):
        # Generic numbers (e.g. "3", "X") contribute no color.
        if token.isdigit() or token in {"X", "Y", "Z"}:
            continue
        # Hybrid: "W/B" → both W and B count this pip.
        if "/" in token:
            parts = [p for p in token.split("/") if p in {"W", "U", "B", "R", "G"}]
            for p in parts:
                pips[p] += 1
            continue
        # Phyrexian: "W/P" already handled by the slash above.
        # Pure color letter.
        if token in {"W", "U", "B", "R", "G"}:
            pips[token] += 1
            continue
        # Snow / colorless / other — no color contribution.
    return pips


# ============================================================
# Main entry point.
# ============================================================


def compute_mana_base(
    *,
    commander_color_identity: Sequence[str],
    nonland_cards: Sequence[Dict[str, Any]],
    bracket: str,
    archetype_hint: Optional[str] = None,
) -> ManaBaseRecommendation:
    """Compute mana-base recommendation.

    Args:
        commander_color_identity: e.g. ["B", "R", "W"] for a Mardu commander.
            Determines which colors need sources.
        nonland_cards: list of dicts with `mana_cost` and `cmc` fields.
            Lands should be filtered out before calling.
        bracket: B1..B5.
        archetype_hint: optional archetype from Phase 6's detector
            (storm/reanimator/landfall/etc.) — adjusts land count.

    Returns:
        ManaBaseRecommendation with all fields populated.
    """
    ci_set = {c.upper() for c in commander_color_identity if isinstance(c, str)}
    bracket = bracket if bracket in _BRACKET_LAND_TARGET else "B3"
    arch = archetype_hint or "default"

    # ---- per-color source requirement ----
    # For each color in CI: find MAX of Karsten requirements across all
    # nonland cards that pip that color at their CMC.
    color_max_required: Dict[str, int] = {c: 0 for c in ci_set}
    # Also build a requirements summary for the LLM critique.
    requirements_summary: List[Dict[str, Any]] = []

    for card in nonland_cards:
        mana_cost = card.get("mana_cost") or ""
        cmc = int(card.get("cmc") or 0)
        pips = _parse_color_pips(mana_cost)
        if not pips:
            continue
        for color, pip_count in pips.items():
            if color not in ci_set:
                continue  # off-CI pip — shouldn't happen given CI filter
            req = _pip_sources_required(cmc, pip_count)
            if req > color_max_required.get(color, 0):
                color_max_required[color] = req
                requirements_summary.append({
                    "card": card.get("name") or card.get("card_name") or "?",
                    "color": color,
                    "pip_count": pip_count,
                    "cmc": cmc,
                    "sources_required": req,
                })

    # ---- land count ----
    target_land_count = _BRACKET_LAND_TARGET[bracket] + _ARCHETYPE_LAND_DELTA.get(arch, 0)
    target_land_count = max(28, min(42, target_land_count))

    # ---- tap land tolerance / basic ratio / utility budget ----
    tap_tolerance = _BRACKET_TAP_LAND_TOLERANCE[bracket]
    basic_ratio = _BRACKET_BASIC_NONBASIC_RATIO[bracket]
    utility_budget = _BRACKET_UTILITY_LAND_BUDGET[bracket]
    # cEDH (B5) penalizes utility lands too — but they're still
    # allowed (e.g. Bojuka Bog is fine in B5).

    # ---- rationale ----
    rationale_lines: List[str] = []
    rationale_lines.append(
        f"Bracket {bracket} baseline: {_BRACKET_LAND_TARGET[bracket]} lands."
    )
    if arch != "default" and _ARCHETYPE_LAND_DELTA.get(arch, 0) != 0:
        delta = _ARCHETYPE_LAND_DELTA[arch]
        rationale_lines.append(
            f"Archetype {arch}: {'+' if delta > 0 else ''}{delta} adjustment → {target_land_count} lands."
        )
    if not color_max_required:
        rationale_lines.append("Colorless or no color-pip nonland cards detected.")
    else:
        for color in sorted(color_max_required):
            req = color_max_required[color]
            if req > 0:
                # Find the triggering card for rationale.
                trigger = next(
                    (r for r in requirements_summary
                     if r["color"] == color and r["sources_required"] == req),
                    None,
                )
                tname = trigger["card"] if trigger else "?"
                tcmc = trigger["cmc"] if trigger else "?"
                tpips = trigger["pip_count"] if trigger else "?"
                rationale_lines.append(
                    f"{color}: max requirement is {req} sources (driven by "
                    f"{tname} at CMC {tcmc}, {tpips} {color}-pip)."
                )
    rationale_lines.append(
        f"Tap-land tolerance: {tap_tolerance}. Basic/nonbasic ratio target: "
        f"~{basic_ratio:.0%}. Utility-land budget: {utility_budget}."
    )

    return ManaBaseRecommendation(
        target_land_count=target_land_count,
        color_source_targets=dict(color_max_required),
        tap_land_tolerance=tap_tolerance,
        utility_land_budget=utility_budget,
        basic_nonbasic_ratio=basic_ratio,
        rationale=" ".join(rationale_lines),
        requirements_summary=requirements_summary,
    )


# ============================================================
# Reconciliation: compare actual deck lands to recommendation.
# ============================================================


_BASIC_LAND_NAMES = {"Plains", "Island", "Swamp", "Mountain", "Forest", "Wastes"}


def reconcile_deck_lands(
    *,
    deck: Sequence[Dict[str, Any]],
    recommendation: ManaBaseRecommendation,
) -> Dict[str, Any]:
    """Compare the actual deck's lands to the recommendation. Returns
    a dict with:
      - actual_land_count
      - actual_color_sources: dict color -> count of sources currently
        in deck (approximate — counts each basic + assumed each land
        named with the color)
      - land_count_delta: actual - target (negative = too few lands)
      - color_source_deltas: per-color actual - target
      - discrepancies: list of human-readable discrepancy strings
      - significant: True if any delta > 2 (triggers LLM critique pass)
    """
    actual_lands = []
    for c in deck:
        name = c.get("card_name") or c.get("name") or ""
        if not name:
            continue
        # Detect land by source string or basic-name match — heuristic
        # because the deck dict from build_deck doesn't always carry
        # type_line.
        is_land = (
            c.get("source") == "mana_base"
            or "[slot=land]" in (c.get("reason") or "")
            or name in _BASIC_LAND_NAMES
        )
        if is_land:
            actual_lands.append(c)
    actual_count = len(actual_lands)

    # Approximate color-source count by basic name.
    color_to_basic = {
        "W": "Plains", "U": "Island", "B": "Swamp",
        "R": "Mountain", "G": "Forest",
    }
    actual_sources: Dict[str, int] = {c: 0 for c in recommendation.color_source_targets}
    for land in actual_lands:
        nm = land.get("card_name") or land.get("name") or ""
        # Basic-land detection.
        for color, basic in color_to_basic.items():
            if nm == basic and color in actual_sources:
                actual_sources[color] += 1

    land_delta = actual_count - recommendation.target_land_count
    color_deltas = {
        color: actual_sources.get(color, 0) - target
        for color, target in recommendation.color_source_targets.items()
    }

    discrepancies: List[str] = []
    if abs(land_delta) > 2:
        discrepancies.append(
            f"Land count: actual={actual_count}, target={recommendation.target_land_count} "
            f"(delta {land_delta:+d})"
        )
    for color, delta in color_deltas.items():
        if abs(delta) > 2:
            target = recommendation.color_source_targets[color]
            discrepancies.append(
                f"{color} sources: actual={actual_sources[color]}, "
                f"target={target} (delta {delta:+d})"
            )

    return {
        "actual_land_count": actual_count,
        "actual_color_sources": actual_sources,
        "land_count_delta": land_delta,
        "color_source_deltas": color_deltas,
        "discrepancies": discrepancies,
        "significant": bool(discrepancies),
    }
