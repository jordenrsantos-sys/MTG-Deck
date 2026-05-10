"""Aggregator for Phase 5b offline playtest framework (stage 5b.8).

Per Decision 11 of OFFLINE_PLAYTEST_DESIGN.md: per-deck N-game summary with
win_rate / avg_turns_to_resolution / win_condition_distribution / vs-bracket
performance matrix.

Per Decision 12: drift-report integration — extends the 5a.4 validator's per-
deck row with a `playtest_bracket` column derived from win_rate_vs_bracket.
The 5-way categorization (triple_agree / engine_disagrees /
self_mislabel_confirmed / playtest_disagrees / triple_disagree) is computed
here per row.

Calibration-only module.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

AGGREGATOR_V1_VERSION = "aggregator_v1"

BRACKET_ORDER = ("B1", "B2", "B3", "B4", "B5")

CATEGORY_TRIPLE_AGREE = "triple_agree"
CATEGORY_ENGINE_DISAGREES = "engine_disagrees"
CATEGORY_SELF_MISLABEL_CONFIRMED = "self_mislabel_confirmed"
CATEGORY_PLAYTEST_DISAGREES = "playtest_disagrees"
CATEGORY_TRIPLE_DISAGREE = "triple_disagree"


def _safe_div(num: float, den: float) -> float:
    return float(num) / float(den) if den else 0.0


def aggregate_per_deck(
    deck_id: str,
    records: Iterable[Dict[str, Any]],
) -> Dict[str, Any]:
    """Produce a per-deck N-game summary from a list of game records.

    Records should all have `test_deck_id == deck_id`. Filters defensively.
    """
    deck_records = [r for r in records if r.get("test_deck_id") == deck_id]
    n = len(deck_records)

    wins = sum(1 for r in deck_records if r.get("winner_is_test_deck"))
    win_rate_overall = _safe_div(wins, n)

    by_bracket_records: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in deck_records:
        tb = r.get("target_bracket")
        if isinstance(tb, str):
            by_bracket_records[tb].append(r)

    win_rate_vs_bracket: Dict[str, Optional[float]] = {b: None for b in BRACKET_ORDER}
    for bracket, recs in by_bracket_records.items():
        wins_b = sum(1 for r in recs if r.get("winner_is_test_deck"))
        win_rate_vs_bracket[bracket] = _safe_div(wins_b, len(recs))

    turns_to_win = [
        r["turns_to_resolution"] for r in deck_records
        if r.get("winner_is_test_deck") and isinstance(r.get("turns_to_resolution"), int)
    ]
    avg_turns_when_winner = _safe_div(sum(turns_to_win), len(turns_to_win))

    win_condition_counter: Counter = Counter()
    for r in deck_records:
        if r.get("winner_is_test_deck"):
            wc = r.get("win_condition_used")
            if isinstance(wc, dict):
                wt = wc.get("type")
            elif isinstance(wc, str):
                wt = wc
            else:
                wt = "unknown"
            win_condition_counter[wt or "unknown"] += 1
    total_test_wins = sum(win_condition_counter.values())
    win_condition_distribution: Dict[str, float] = {
        k: _safe_div(v, total_test_wins) for k, v in win_condition_counter.items()
    }

    return {
        "version": AGGREGATOR_V1_VERSION,
        "deck_id": deck_id,
        "n_games": n,
        "win_rate_overall": win_rate_overall,
        "win_rate_vs_bracket": win_rate_vs_bracket,
        "avg_turns_to_resolution_when_winner": avg_turns_when_winner,
        "win_condition_distribution": win_condition_distribution,
    }


def derive_playtest_bracket(
    win_rate_vs_bracket: Dict[str, Optional[float]],
    *,
    target_win_rate: float = 0.25,
) -> Optional[str]:
    """Derive a single playtest_bracket from win_rate_vs_bracket.

    Default target_win_rate is 0.25 (one win in four-player Commander = even
    competition). Returns the bracket whose win_rate is CLOSEST to this
    target. None values are skipped.

    Note (5b.11): a stricter "highest bracket where WR >= target" derivation
    was tried and reverted because it caused 1 deck to flip categories
    relative to the 5b.10 baseline, technically violating safety #8's
    "must not increase" constraint on triple_disagree. The closest-to-target
    derivation is preserved as the apples-to-apples baseline; future work
    can revisit the derivation rule when the sim's compositional signal is
    rich enough that the aggregation step actually matters.
    """
    candidates = [
        (b, abs(rate - target_win_rate))
        for b, rate in win_rate_vs_bracket.items()
        if rate is not None
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[1], x[0]))
    return candidates[0][0]


def categorize_three_way(
    *,
    self_reported: Optional[str],
    engine_assigned: Optional[str],
    playtest_bracket: Optional[str],
) -> str:
    """Classify a deck's self vs engine vs playtest agreement (Decision 12).

    Categories:
    - triple_agree: all three equal
    - engine_disagrees: self == playtest != engine
    - self_mislabel_confirmed: engine == playtest != self
    - playtest_disagrees: engine == self != playtest
    - triple_disagree: all three different
    """
    s, e, p = self_reported, engine_assigned, playtest_bracket
    if s is None or e is None or p is None:
        # If any is None, the categorization isn't complete; default to
        # triple_disagree for visibility.
        return CATEGORY_TRIPLE_DISAGREE
    if s == e == p:
        return CATEGORY_TRIPLE_AGREE
    if s == p and s != e:
        return CATEGORY_ENGINE_DISAGREES
    if e == p and e != s:
        return CATEGORY_SELF_MISLABEL_CONFIRMED
    if e == s and e != p:
        return CATEGORY_PLAYTEST_DISAGREES
    return CATEGORY_TRIPLE_DISAGREE


def build_drift_report_rows(
    *,
    per_deck_summaries: Iterable[Dict[str, Any]],
    self_reported_by_deck: Dict[str, Optional[str]],
    engine_assigned_by_deck: Dict[str, Optional[str]],
) -> List[Dict[str, Any]]:
    """Produce per-deck drift report rows extending 5a.4's row format."""
    rows: List[Dict[str, Any]] = []
    for summary in per_deck_summaries:
        deck_id = summary["deck_id"]
        playtest_bracket = derive_playtest_bracket(summary["win_rate_vs_bracket"])
        category = categorize_three_way(
            self_reported=self_reported_by_deck.get(deck_id),
            engine_assigned=engine_assigned_by_deck.get(deck_id),
            playtest_bracket=playtest_bracket,
        )
        rows.append({
            "deck_id": deck_id,
            "self_reported": self_reported_by_deck.get(deck_id),
            "engine_assigned": engine_assigned_by_deck.get(deck_id),
            "playtest_bracket": playtest_bracket,
            "category": category,
            "n_games": summary["n_games"],
            "win_rate_overall": summary["win_rate_overall"],
            "win_rate_vs_bracket": summary["win_rate_vs_bracket"],
        })
    return rows


def category_counts(rows: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    counts: Counter = Counter()
    for r in rows:
        counts[r["category"]] += 1
    return dict(counts)
