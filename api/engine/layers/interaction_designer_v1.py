"""
interaction_designer_v1 — Pillar E v0.4 (Mega-task v5 Phase 10).

Computes target counts for each interaction category given a commander's
color identity, bracket, and (optionally) archetype hint, plus how the
deck currently meets those targets.

Categories (per the kickoff spec):
  - counterspells              (U-color only)
  - targeted-creature-removal
  - targeted-artifact-removal
  - targeted-enchantment-removal
  - mass-removal (board wipes)
  - graveyard-interaction

Per-bracket totals + sorcery-speed mix:
  B1/B2: ~9 total, 70% sorcery-speed, 1-2 mass-removal
  B3/B4: ~11 total, 50% sorcery-speed, 2-3 mass-removal
  B5:    ~13 total, 20% sorcery-speed (instant-speed dominates), 0-1 mass-removal

Color policy:
  W in CI: enables wraths + exiles
  U in CI: enables counterspells + bounce
  B in CI: enables targeted-creature-removal + graveyard-interaction
  R in CI: enables damage-based-removal
  G in CI: enables artifact/enchantment removal

The output is informational (same pattern as v0.1 mana_base / v0.2
card_advantage / v0.3 curve_smoother): pure analysis, no deck mutation,
exposed under response.summary.pillar_e_v0_4_interaction_check.

Public API:
  - compute_interaction_targets(*, commander_color_identity, bracket,
        archetype_hint=None, deck=None, pool=None) -> InteractionTargets
  - InteractionTargets dataclass
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Set


INTERACTION_DESIGNER_VERSION = "interaction_designer_v1.1_per_category_bounds"

_BRACKET_POLICY: Dict[str, Dict[str, Any]] = {
    "B1": {"total": 9,  "sorcery_pct": 0.70, "mass_removal": 2},
    "B2": {"total": 9,  "sorcery_pct": 0.70, "mass_removal": 2},
    "B3": {"total": 11, "sorcery_pct": 0.50, "mass_removal": 2},
    "B4": {"total": 11, "sorcery_pct": 0.50, "mass_removal": 3},
    "B5": {"total": 13, "sorcery_pct": 0.20, "mass_removal": 1},
}
_DEFAULT_POLICY = {"total": 10, "sorcery_pct": 0.50, "mass_removal": 2}

# v7 Phase 6: per-category min-max bounds. Pre-v7 the discrepancy check
# used a single ±50% band on the sum-based target, which overshot
# 1.5×target on every iter-7 sweep case once multi-primitive counting
# in v6 Phase 4 inflated multi-mode card counts. Per-category bounds
# replaced the sum-based check.
#
# v8 Phase 5: bracket-proportional bounds. Pre-v8 the bounds were
# universal — the per-category min/max applied to every bracket equally.
# Iter-8 sweep flagged a real problem: kickoff's targeted_creature_removal
# bound [4,7] exceeds bracket B2's interaction allocation (B2 total=9
# with mass_removal=2 leaves 7 for ALL 6 other categories — can't fit
# 4-7 in any single one). Higher brackets (B4/B5) also legitimately run
# more interaction than the universal bound permits. Bracket-proportional
# bounds make each bracket's per-category range realistic given its
# overall interaction budget.
#
# Each category gets {bracket: (min, max)} table. Reads the row matching
# the deck's bracket. Falls back to the v7 universal bounds when bracket
# key not present.
_PER_CATEGORY_BOUNDS_BY_BRACKET: Dict[str, Dict[str, tuple]] = {
    "mass_removal": {
        "B1": (1, 3), "B2": (1, 3), "B3": (2, 4), "B4": (2, 4), "B5": (0, 2),
    },
    "targeted_creature_removal": {
        "B1": (2, 4), "B2": (2, 5), "B3": (3, 6), "B4": (3, 7), "B5": (4, 8),
    },
    "targeted_artifact_removal": {
        "B1": (0, 2), "B2": (0, 2), "B3": (1, 3), "B4": (1, 4), "B5": (1, 4),
    },
    "targeted_enchantment_removal": {
        "B1": (0, 1), "B2": (0, 2), "B3": (0, 2), "B4": (0, 3), "B5": (0, 3),
    },
    "counterspells": {  # U-only
        "B1": (1, 3), "B2": (1, 4), "B3": (2, 6), "B4": (3, 7), "B5": (4, 10),
    },
    "graveyard_interaction": {
        "B1": (0, 2), "B2": (0, 2), "B3": (0, 3), "B4": (1, 4), "B5": (1, 4),
    },
}

# v7 fallback bounds when bracket lookup misses (e.g., custom bracket).
_PER_CATEGORY_BOUNDS_DEFAULT: Dict[str, tuple] = {
    "mass_removal":                 (2, 4),
    "targeted_creature_removal":    (4, 7),
    "targeted_artifact_removal":    (1, 3),
    "targeted_enchantment_removal": (0, 2),
    "counterspells":                (4, 8),
    "graveyard_interaction":        (0, 3),
}


def _resolve_bracket_bounds(category: str, bracket: str) -> Optional[tuple]:
    """v8 Phase 5: return (min, max) for category at bracket. Falls back
    to universal default when bracket not in the proportional table."""
    by_bracket = _PER_CATEGORY_BOUNDS_BY_BRACKET.get(category)
    if by_bracket and bracket in by_bracket:
        return by_bracket[bracket]
    return _PER_CATEGORY_BOUNDS_DEFAULT.get(category)


# v7 Phase 6 (deprecated, kept for backward compat with v8 Phase 4 dual-
# vocab test): the universal bounds — superseded by the bracket-
# proportional version above. Consumers should call
# `_resolve_bracket_bounds(cat, bracket)` instead.
_PER_CATEGORY_BOUNDS: Dict[str, tuple] = _PER_CATEGORY_BOUNDS_DEFAULT

# Map primitive tags to interaction categories.
#
# v7 Phase 8 self-correction: vocab now includes BOTH the legacy lowercase
# hyphenated names (e.g., counterspell-hard) AND the v6 Phase 3 cards.
# primitives_v1_json uppercase names (e.g., COUNTERSPELL, REMOVAL_SINGLE,
# BOARD_WIPE). Pre-v7P8 the per-category counts were always 0 because the
# DB-hydrated primitives via win_con's hydration helper carry the v2 vocab,
# and the legacy lowercase keys never matched. Same pattern as the Phase 1
# fix for _classify_card.
_PRIMITIVES_TO_CATEGORY: Dict[str, str] = {
    # Legacy primitive_to_cards inverted-index vocabulary (lowercase-hyphenated).
    "counterspell-hard":          "counterspells",
    "counterspell-soft":          "counterspells",
    "free-counter":               "counterspells",
    "removal-creature":           "targeted_creature_removal",
    "removal-artifact":           "targeted_artifact_removal",
    "removal-enchantment":        "targeted_enchantment_removal",
    "removal-mass-creatures":     "mass_removal",
    "removal-mass-board":         "mass_removal",
    "bounce":                     "targeted_creature_removal",
    "tap-down":                   "targeted_creature_removal",
    "recursion-exile":            "graveyard_interaction",
    "mill-all":                   "graveyard_interaction",
    # v6 Phase 3 cards.primitives_v1_json vocabulary (UPPERCASE_UNDERSCORED).
    "COUNTERSPELL":               "counterspells",
    "STACK_COUNTERSPELL":         "counterspells",
    "PERMISSION_OVERRIDE":        "counterspells",
    "COUNTERSPELL_PROTECTION":    "counterspells",
    "TARGETED_REMOVAL_CREATURE":  "targeted_creature_removal",
    "REMOVAL_SINGLE":             "targeted_creature_removal",
    "DIRECT_DAMAGE":              "targeted_creature_removal",
    "BOARDWIPE_CREATURES":        "mass_removal",
    "BOARD_WIPE":                 "mass_removal",
    "REMOVAL_ARTIFACT_ENCHANTMENT": "targeted_artifact_removal",
    "GRAVEYARD_RECURSION_TO_HAND": "graveyard_interaction",
}

# Per-color enablement — when a color is NOT in the commander's CI, the
# category is suppressed (target = 0).
_COLOR_GATES: Dict[str, Set[str]] = {
    "counterspells":                {"U"},
    "targeted_creature_removal":    {"W", "U", "B", "R", "G"},  # every color has SOME
    "targeted_artifact_removal":    {"W", "G", "R"},
    "targeted_enchantment_removal": {"W", "G", "U"},  # white/green primary; blue via bounce
    "mass_removal":                 {"W", "U", "B", "R", "G"},  # every color has SOMETHING
    "graveyard_interaction":        {"W", "B", "G"},
}


@dataclass
class InteractionTargets:
    bracket: str
    color_identity: List[str]
    archetype_hint: Optional[str]
    total_target: int
    sorcery_speed_target: int
    instant_speed_target: int
    mass_removal_target: int
    targets_by_category: Dict[str, int] = field(default_factory=dict)
    actual_by_category: Dict[str, int] = field(default_factory=dict)
    discrepancies: List[str] = field(default_factory=list)
    significant: bool = False
    # v7 Phase 6: per-category bounds + in-range report. Each entry:
    #   {"target": <orig allocation>, "min": <bound>, "max": <bound>,
    #    "actual": <count>, "in_range": <bool>}
    # Color-gated-off categories (e.g. counterspells in WUG without U)
    # are not included.
    per_category: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    version: str = INTERACTION_DESIGNER_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _resolve_policy(bracket: str) -> Dict[str, Any]:
    return _BRACKET_POLICY.get(bracket, _DEFAULT_POLICY)


def _allocate_targets_per_category(
    color_identity_set: Set[str],
    total_target: int,
    mass_removal_target: int,
) -> Dict[str, int]:
    """Split the total interaction target across the 6 categories,
    honoring color-gating. The allocation roughly mirrors EDH-community
    norms: mass_removal carved out first, then a weighted split of the
    remainder across the color-enabled categories."""
    # Gate the categories by color identity.
    enabled = {
        cat for cat, gate in _COLOR_GATES.items()
        if not gate.isdisjoint(color_identity_set)
    }
    # mass_removal is always enabled by the policy (every color has one).
    # Subtract it from the budget first.
    remaining = max(0, total_target - mass_removal_target)

    # Heuristic weights for the targeted categories. counterspells get more
    # weight in U; targeted-creature-removal is the workhorse in every CI;
    # graveyard interaction is a smaller share unless specifically built.
    raw_weights = {
        "counterspells":                3.0 if "U" in color_identity_set else 0.0,
        "targeted_creature_removal":    3.0,
        "targeted_artifact_removal":    1.5,
        "targeted_enchantment_removal": 1.0,
        "graveyard_interaction":        1.0,
    }
    # Zero out gated-off categories.
    for cat in list(raw_weights):
        if cat not in enabled:
            raw_weights[cat] = 0.0
    total_w = sum(raw_weights.values())
    targets: Dict[str, int] = {}
    if total_w > 0:
        for cat, w in raw_weights.items():
            targets[cat] = int(round(remaining * (w / total_w)))
    else:
        for cat in raw_weights:
            targets[cat] = 0
    # Mass removal is its own line.
    targets["mass_removal"] = mass_removal_target
    return targets


def _classify_card_interaction(primitives: List[str]) -> Set[str]:
    """Return ALL interaction categories the card's primitives map to.

    Mega-task v6 Phase 4 (BLOCKING) fix: replaces the v5 first-match
    classification. The previous behavior returned only the first
    matching category, so cards with multiple interaction-relevant
    primitives (e.g. counterspell-hard + removal-creature on Fierce
    Guardianship variants; bounce + tap-down + removal-creature on
    multi-mode spells) were undercounted because only the first match
    contributed to ``total_actual``. The iter 6 sweep landed 0/5 on
    pillar_e_v0_4_interaction_within target as a direct consequence.

    The new behavior counts a card in EVERY category it matches (once
    per category — bounce + tap-down both mapping to
    targeted_creature_removal still adds 1, not 2). Cards spanning
    distinct categories (counterspell + removal) contribute to both.
    """
    if not primitives:
        return set()
    cats: Set[str] = set()
    for p in primitives:
        cat = _PRIMITIVES_TO_CATEGORY.get(p)
        if cat:
            cats.add(cat)
    return cats


def _count_actual_interaction(
    deck: List[Dict[str, Any]],
    pool: Optional[Dict[str, Any]],
    color_identity_set: Set[str],
) -> Dict[str, int]:
    """Count how many cards in the deck fall into each interaction
    category, using primitives. Pool-hydrated primitives win over deck-
    inlined ones (same pattern as the candidate critic).

    Mega-task v6 Phase 4: a card now contributes to EVERY interaction
    category its primitives match (was: only the first). See
    ``_classify_card_interaction`` for the why.
    """
    pool_by_name_lower = {
        (c.get("name") or "").strip().lower(): c
        for c in (pool or {}).get("candidates") or []
    }
    counts: Dict[str, int] = {
        "counterspells": 0,
        "targeted_creature_removal": 0,
        "targeted_artifact_removal": 0,
        "targeted_enchantment_removal": 0,
        "mass_removal": 0,
        "graveyard_interaction": 0,
    }
    for card in deck:
        name = (card.get("card_name") or "").strip()
        if not name:
            continue
        pool_match = pool_by_name_lower.get(name.lower())
        prims = []
        if pool_match is not None:
            prims = list(pool_match.get("primitives") or [])
        if not prims:
            prims = list(card.get("primitives") or [])
        if not prims:
            continue
        cats = _classify_card_interaction(prims)
        if not cats:
            continue
        for cat in cats:
            # Color-gate: only count counterspells when U in CI (per the
            # kickoff spec). Other categories are gated softer.
            if cat == "counterspells" and "U" not in color_identity_set:
                continue
            counts[cat] = counts.get(cat, 0) + 1
    return counts


def compute_interaction_targets(
    *,
    commander_color_identity: List[str],
    bracket: str,
    archetype_hint: Optional[str] = None,
    deck: Optional[List[Dict[str, Any]]] = None,
    pool: Optional[Dict[str, Any]] = None,
    discrepancy_pct: float = 0.5,
) -> InteractionTargets:
    """Compute the InteractionTargets for the given commander.

    Args:
      commander_color_identity: e.g. ["W", "U", "B", "G"] for Atraxa.
      bracket: "B1" through "B5".
      archetype_hint: optional. Not currently used to adjust targets but
        is recorded on the output for downstream consumers.
      deck: optional. When provided, the function also counts the deck's
        actual interaction cards per category and produces discrepancies.
      pool: optional candidate pool dict, used to hydrate primitives that
        aren't inlined on deck entries.
      discrepancy_pct: a category is flagged as a discrepancy when the
        actual count is below `target * (1 - discrepancy_pct)` or above
        `target * (1 + discrepancy_pct)`. Default 0.5 (±50%).

    Returns: InteractionTargets dataclass.
    """
    color_set = {(c or "").upper() for c in (commander_color_identity or []) if c}
    policy = _resolve_policy(bracket)
    total_target = int(policy["total"])
    mass_removal_target = int(policy["mass_removal"])
    sorcery_pct = float(policy["sorcery_pct"])
    sorcery_speed_target = int(round(total_target * sorcery_pct))
    instant_speed_target = total_target - sorcery_speed_target

    targets_by_category = _allocate_targets_per_category(
        color_set, total_target, mass_removal_target,
    )

    actual_by_category: Dict[str, int] = {}
    discrepancies: List[str] = []
    per_category: Dict[str, Dict[str, Any]] = {}
    if deck is not None:
        actual_by_category = _count_actual_interaction(deck, pool, color_set)
        # v7 Phase 6: per-category bounds replace the sum-based ±50% check.
        # Each enabled category has a hard (min, max) range; discrepancy
        # fires only when actual is outside that range. Color-gated-off
        # categories (counterspells without U) are excluded entirely.
        gated_off_cats: Set[str] = set()
        for cat, gate in _COLOR_GATES.items():
            if gate.isdisjoint(color_set):
                gated_off_cats.add(cat)
        for cat, t in targets_by_category.items():
            actual = actual_by_category.get(cat, 0)
            # Color-gate first — never flag a gated-off category as
            # under-target (e.g., counterspells in BRG).
            if cat in gated_off_cats:
                continue
            # v8 Phase 5: bracket-proportional bounds lookup.
            bounds = _resolve_bracket_bounds(cat, bracket)
            if bounds:
                lo, hi = bounds
                in_range = lo <= actual <= hi
                per_category[cat] = {
                    "target": int(t), "min": int(lo), "max": int(hi),
                    "actual": int(actual), "in_range": in_range,
                }
                if not in_range:
                    if actual < lo:
                        discrepancies.append(
                            f"{cat} below per-category min: {actual} vs [{lo},{hi}]"
                        )
                    else:
                        discrepancies.append(
                            f"{cat} above per-category max: {actual} vs [{lo},{hi}]"
                        )
            else:
                # No per-category bound defined — fall back to legacy ±50%
                # sum-based check (preserves backward-compat for any
                # category not in _PER_CATEGORY_BOUNDS).
                if t <= 0:
                    continue
                low = t * (1 - discrepancy_pct)
                high = t * (1 + discrepancy_pct)
                if actual < low:
                    discrepancies.append(f"{cat} under target: {actual} vs {t}")
                elif actual > high:
                    discrepancies.append(f"{cat} over target: {actual} vs {t}")

    return InteractionTargets(
        bracket=bracket,
        color_identity=sorted(color_set),
        archetype_hint=archetype_hint,
        total_target=total_target,
        sorcery_speed_target=sorcery_speed_target,
        instant_speed_target=instant_speed_target,
        mass_removal_target=mass_removal_target,
        targets_by_category=targets_by_category,
        actual_by_category=actual_by_category,
        discrepancies=discrepancies,
        significant=bool(discrepancies),
        per_category=per_category,
    )
