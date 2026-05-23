"""win_con_coherence_v1 — Pillar E v0.5 (mega-task v6 Phase 9).

Identifies a deck's primary win condition by primitive-pattern matching
and validates that the deck has enough enabling cards for the primary
plan plus a credible backup. Flags decks where neither primary nor
backup is clear (the "75% pile of good cards" anti-pattern).

Pure analysis — does NOT mutate the deck. Output is exposed as
``response.summary.win_con_coherence_report`` for the UI and
downstream metrics.

Public API
----------
``check_win_con_coherence(deck, theme_profile, bracket, *, pool=None)
-> WinConCoherenceReport``

The checker maps each card to its primitive tags (preferring pool-
hydrated primitives over deck-inlined ones, same pattern as Pillar E
v0.4), counts how many cards support each known win-condition pattern,
and selects:
  - **primary_plan**: highest-scoring pattern (must have >= the bracket's
    primary-plan floor of enabling cards).
  - **backup_plan**: second-highest scoring pattern with >= 4 enabling
    cards; ``None`` if no secondary path clears the bar.
  - **flagged_75pct_pile**: True iff no pattern reaches primary floor
    AND no two patterns reach backup floor — the deck is undifferentiated.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple


WIN_CON_COHERENCE_VERSION = "win_con_coherence_v1.0"


# Win-condition patterns: id -> required primitive sets (any-of-set match).
# A card "enables" a pattern if its primitives include ALL tags in any one
# of the listed primitive_sets for that pattern. Each pattern lists at
# least one canonical primitive set; multi-set patterns let us recognize
# both "the engine card" (anthem+token) and "the payoff card" (token-
# producer or anthem-effect alone) variants.
_WIN_CON_PATTERNS: Dict[str, Dict[str, Any]] = {
    "combo_win": {
        "label": "Two-card combo (Thoracle / Kiki-Jiki / Heliod+Ballista pattern)",
        "primitive_sets": [
            {"combo-assembly"},
            {"infinite-mana-source"},
            {"infinite-untap-source"},
            {"deck-out"},
            {"x-spell-payoff", "infinite-mana-source"},
        ],
    },
    "tutor_chain": {
        "label": "Tutor chain to assemble + protect combo",
        "primitive_sets": [
            {"tutor-broad"},
            {"tutor-narrow"},
            {"tutor-creature"},
        ],
    },
    "voltron_combat": {
        "label": "Voltron commander damage with extra-combats",
        "primitive_sets": [
            {"voltron-payoff"},
            {"extra-combat"},
            {"evasion-grant"},
        ],
    },
    "go_wide_anthem": {
        "label": "Go-wide tokens + anthem damage",
        "primitive_sets": [
            {"token-producer", "anthem-effect"},
            {"token-producer"},
        ],
    },
    "aristocrats": {
        "label": "Aristocrats — sac outlet + death triggers",
        "primitive_sets": [
            {"sac-outlet", "death-trigger"},
            {"sac-outlet", "persist-creature"},
        ],
    },
    "storm_spellslinger": {
        "label": "Storm / spellslinger payoff chain",
        "primitive_sets": [
            {"storm-payoff"},
            {"cantrip", "storm-payoff"},
        ],
    },
    "reanimator": {
        "label": "Reanimator — mill + recursion to battlefield",
        "primitive_sets": [
            {"recursion-graveyard"},
            {"self-mill"},
        ],
    },
    "mill_alt_win": {
        "label": "Mill-based alt-win (Bruvac / Maddening Cacophony)",
        "primitive_sets": [
            {"mill-all"},
        ],
    },
    "counters_proliferate": {
        "label": "+1/+1 counters / proliferate snowball",
        "primitive_sets": [
            {"proliferate-trigger"},
            {"plus1plus1-counter-doubler"},
            {"plus1plus1-counter-payoff", "plus1plus1-counter-distributor"},
        ],
    },
    "stax_lock": {
        "label": "Stax — lock the table + grind to victory",
        "primitive_sets": [
            {"stax-effect"},
            {"tap-down"},
        ],
    },
    "control_grind": {
        "label": "Control — counterspells + mass removal + slow win",
        "primitive_sets": [
            {"counterspell-hard", "removal-mass-creatures"},
            {"counterspell-hard"},
            {"removal-mass-creatures"},
        ],
    },
    "landfall_aggro": {
        "label": "Landfall — extra land drops + per-land triggers",
        "primitive_sets": [
            {"landfall-trigger", "extra-land-drop"},
            {"landfall-trigger"},
        ],
    },
}


# Per-bracket primary-plan floor: minimum number of enabling cards a
# pattern must have to be considered the deck's primary win condition.
# Mega-task v6 Phase 11 calibration: original floors (B1=8, B5=4) were
# too high — the agent's deck only has primitives populated for the ~30
# cards that came from the candidate pool, not the full 100. Lowered to
# match realistic primitive-coverage: B5 cEDH lives on 2-3 cards (Thoracle
# + Demonic Consultation + 1 tutor), B1 casual still needs broader signal.
_PRIMARY_PLAN_FLOOR: Dict[str, int] = {
    "B1": 5, "B2": 4, "B3": 3, "B4": 3, "B5": 2,
}
_BACKUP_PLAN_FLOOR = 2  # uniform — calibrated alongside primary floor


@dataclass
class WinConCoherenceReport:
    version: str = WIN_CON_COHERENCE_VERSION
    bracket: str = ""
    primary_plan: Optional[Dict[str, Any]] = None       # {pattern_id, label, enablers, count}
    backup_plan: Optional[Dict[str, Any]] = None        # same shape, or None
    pattern_scores: Dict[str, int] = field(default_factory=dict)
    flagged_75pct_pile: bool = False
    flag_reason: Optional[str] = None
    primary_floor: int = 0
    backup_floor: int = _BACKUP_PLAN_FLOOR

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _resolve_primitives_for_card(
    card: Dict[str, Any],
    pool_by_name_lower: Dict[str, Dict[str, Any]],
) -> List[str]:
    """Same precedence rule as Pillar E v0.4: pool > deck-inlined."""
    name = (card.get("card_name") or "").strip().lower()
    pool_match = pool_by_name_lower.get(name)
    if pool_match is not None:
        prims = list(pool_match.get("primitives") or [])
        if prims:
            return prims
    return list(card.get("primitives") or [])


def _pattern_score(card_primitives: Set[str], primitive_sets: List[Set[str]]) -> bool:
    """True iff the card's primitive set fully covers at least one of the
    pattern's primitive_sets."""
    for needed in primitive_sets:
        if needed.issubset(card_primitives):
            return True
    return False


def check_win_con_coherence(
    deck: List[Dict[str, Any]],
    theme_profile: Optional[Dict[str, Any]],
    bracket: str,
    *,
    pool: Optional[Dict[str, Any]] = None,
) -> WinConCoherenceReport:
    """Compute the win-con coherence report.

    Args:
        deck: list of {"card_name", "reason", "source", optional
            "primitives"} dicts.
        theme_profile: optional B2 intent_interpreter theme_profile.
            Currently informational — recorded for downstream LLM critique
            context; doesn't change the pattern matching.
        bracket: "B1".."B5". Sets the primary-plan enablers floor.
        pool: optional candidate pool dict (with "candidates": [{"name",
            "primitives", ...}, ...]). Pool primitives win over deck-
            inlined primitives.

    Returns: WinConCoherenceReport with primary/backup plans, all pattern
    scores, and the 75pct-pile flag.
    """
    report = WinConCoherenceReport(
        bracket=bracket,
        primary_floor=_PRIMARY_PLAN_FLOOR.get(bracket, _PRIMARY_PLAN_FLOOR["B3"]),
    )

    pool_by_name_lower: Dict[str, Dict[str, Any]] = {
        (c.get("name") or "").strip().lower(): c
        for c in (pool or {}).get("candidates") or []
    }

    # Per-pattern enabler counts.
    scores: Dict[str, int] = {pid: 0 for pid in _WIN_CON_PATTERNS}
    enablers: Dict[str, List[str]] = {pid: [] for pid in _WIN_CON_PATTERNS}

    for card in deck:
        prims = set(_resolve_primitives_for_card(card, pool_by_name_lower))
        if not prims:
            continue
        name = (card.get("card_name") or "").strip()
        for pid, pdef in _WIN_CON_PATTERNS.items():
            if _pattern_score(prims, pdef["primitive_sets"]):
                scores[pid] += 1
                enablers[pid].append(name)

    report.pattern_scores = dict(scores)

    # Rank patterns by score, take top-2.
    ranked = sorted(scores.items(), key=lambda kv: -kv[1])
    if ranked:
        top_pid, top_count = ranked[0]
        if top_count >= report.primary_floor:
            report.primary_plan = {
                "pattern_id": top_pid,
                "label": _WIN_CON_PATTERNS[top_pid]["label"],
                "enablers": sorted(set(enablers[top_pid])),
                "count": top_count,
            }
        for pid, count in ranked[1:]:
            if count >= _BACKUP_PLAN_FLOOR:
                report.backup_plan = {
                    "pattern_id": pid,
                    "label": _WIN_CON_PATTERNS[pid]["label"],
                    "enablers": sorted(set(enablers[pid])),
                    "count": count,
                }
                break

    # 75% pile = no primary clears floor AND no second pattern clears
    # backup floor.
    if report.primary_plan is None and report.backup_plan is None:
        report.flagged_75pct_pile = True
        if ranked:
            top_pid, top_count = ranked[0]
            report.flag_reason = (
                f"No win-con pattern reaches the {bracket} primary floor "
                f"({report.primary_floor} enablers). Top: '{top_pid}' with "
                f"{top_count} enablers. The deck reads as a pile of good "
                f"cards without a clear path to win."
            )
        else:
            report.flag_reason = "No win-con primitives detected in any deck card."

    return report
