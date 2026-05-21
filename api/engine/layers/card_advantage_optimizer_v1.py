"""
card_advantage_optimizer_v1 — Pillar E v0.2.

Deterministic card-advantage recommender. Takes a deck + bracket +
archetype hint and outputs a target draw count (8-12 typical) with a
mix breakdown (cantrip / engine / burst). Reconciliation against the
deck's actual draw shape produces a discrepancy report that the agent
flow uses to fire an LLM critique pass.

Hybrid architecture follows the Pillar E v0.1 mana-base pattern: this
module is DETERMINISTIC. It does not auto-apply swaps; it surfaces a
recommendation and the LLM critic either justifies the deviation or
suggests swaps.

Detection heuristic (v0.2): keyword-based pattern matching on the
card's oracle_text. Iter 4 Phase 5 ships a real primitive extractor;
iter 5+ can re-route this module to read `cards.primitive_tags_v1`
instead of keywords.

Public API:
  - compute_card_advantage(deck, bracket, archetype_hint=None,
                           pool=None) -> CardAdvantageRecommendation
  - CardAdvantageRecommendation: dataclass with target_count,
    mix_targets, current_counts, recommended_swaps, rationale
  - reconcile_card_advantage(rec) -> Dict (kept inline for symmetry
    with mana_base_optimizer; the deck reconciliation is computed
    inside compute_card_advantage).
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Sequence


CARD_ADVANTAGE_OPTIMIZER_VERSION = "card_advantage_optimizer_v1.0"


# ============================================================
# Targets per bracket / archetype.
# ============================================================
#
# Total target = base bracket + archetype delta. Mix is split via the
# archetype mix profile (or default if unknown).

_BRACKET_BASE_TARGET: Dict[str, int] = {
    "B1": 8,
    "B2": 9,
    "B3": 10,
    "B4": 10,
    "B5": 10,
}

_ARCHETYPE_TOTAL_DELTA: Dict[str, int] = {
    "control":         +2,  # slow win needs more draw
    "combo":            0,
    "tribal":           0,
    "voltron":         -1,
    "storm":           -3,  # rituals replace draw
    "aristocrats":      0,
    "counters_matter":  0,
    "blink":           +1,
    "reanimator":       0,
    "landfall":         0,
    "group_hug":       +1,
    "tokens":           0,
    "default":          0,
}

# Mix profile per archetype as (cantrip_w, engine_w, burst_w). Normalized
# at apply time to the total target count (rounded to integers,
# residual goes to engine).
_ARCHETYPE_MIX_PROFILE: Dict[str, Sequence[float]] = {
    "control":         (5.0, 5.0, 2.0),
    "combo":           (5.0, 1.0, 2.0),
    "tribal":          (4.0, 4.0, 2.0),
    "voltron":         (4.0, 2.0, 2.0),
    "storm":           (4.0, 0.0, 2.0),
    "aristocrats":     (3.0, 4.0, 2.0),
    "counters_matter": (3.0, 5.0, 2.0),
    "blink":           (3.0, 5.0, 2.0),
    "reanimator":      (4.0, 3.0, 2.0),
    "landfall":        (3.0, 5.0, 2.0),
    "group_hug":       (3.0, 5.0, 2.0),
    "tokens":          (3.0, 4.0, 3.0),
    "default":         (4.0, 4.0, 2.0),
}


# ============================================================
# Keyword detection on oracle_text.
# ============================================================
#
# The patterns are intentionally conservative: a card must clearly fit
# the category, not just incidentally reference drawing. False
# positives are worse than false negatives in this context — the
# discrepancy threshold (>= 2) absorbs occasional misses.

_CANTRIP_PATTERNS = (
    # "Draw a card." as the primary effect — match anchored or as a
    # complete clause (not "draw a card. then discard..." which is a
    # rummage, not a true cantrip).
    re.compile(r"\bdraw a card\b", re.IGNORECASE),
    # Scry+draw cantrip variants captured by "draw a card" plus low CMC
    # is the actual filter; the keyword alone is enough.
)

_ENGINE_PATTERNS = (
    # Recurring per-turn or per-attack draw.
    re.compile(r"at the beginning of (your |each |the next )?(upkeep|draw|combat)", re.IGNORECASE),
    re.compile(r"whenever (a|you|the|an|another) [\w \-,]{0,80}\bdraw\b", re.IGNORECASE),
    re.compile(r"whenever .{0,80}(attacks?|deals? (combat )?damage).{0,80}draw", re.IGNORECASE),
    re.compile(r"you draw an additional card", re.IGNORECASE),
    re.compile(r"if you would draw .{0,40}draw (twice|two)", re.IGNORECASE),
)

_BURST_PATTERNS = (
    re.compile(r"\bdraw (three|four|five|six|seven|eight|nine|ten) cards?\b", re.IGNORECASE),
    re.compile(r"\bdraw cards? equal to\b", re.IGNORECASE),
    re.compile(r"\bwheel\b|\beach player draws\b", re.IGNORECASE),
    re.compile(r"\bdiscard your hand.{0,40}draw\b", re.IGNORECASE),
)


def _classify_card_advantage(oracle_text: str, cmc: Optional[float],
                             type_line: str) -> Optional[str]:
    """Return one of {'cantrip', 'engine', 'burst'} or None.

    Cards are tried in burst → engine → cantrip order so a card that
    matches multiple categories is bucketed under the strongest one.
    """
    if not oracle_text:
        return None
    text = oracle_text.lower()
    for pat in _BURST_PATTERNS:
        if pat.search(text):
            return "burst"
    for pat in _ENGINE_PATTERNS:
        if pat.search(text):
            # Permanents with per-turn triggers are engines; spells
            # with a one-shot "whenever you cast" are not.
            tl = (type_line or "").lower()
            if any(t in tl for t in ("creature", "enchantment", "artifact",
                                     "planeswalker", "land")):
                return "engine"
            return "cantrip"  # one-shot whenever-cast on a sorcery is closer to a cantrip
    for pat in _CANTRIP_PATTERNS:
        if pat.search(text):
            tl = (type_line or "").lower()
            # Big creatures with "enters: draw" aren't cantrips; flag as
            # engines if low-CMC, otherwise as cantrips for cheap spells.
            if cmc is None or cmc <= 3:
                return "cantrip"
            return "engine"
    return None


# ============================================================
# Output dataclass.
# ============================================================


@dataclass
class CardAdvantageRecommendation:
    target_count: int
    mix_targets: Dict[str, int]   # cantrip / engine / burst → count
    current_counts: Dict[str, int]
    discrepancies: List[str]
    recommended_swaps: List[Dict[str, Any]]
    rationale: str
    significant: bool   # True if total deficit > 2 OR any mix mismatch > 2
    version: str = CARD_ADVANTAGE_OPTIMIZER_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================
# Helpers.
# ============================================================


def _mix_for(archetype: str, total: int) -> Dict[str, int]:
    """Apportion `total` across (cantrip, engine, burst) per the
    archetype profile. Cantrip + engine + burst == total."""
    weights = list(_ARCHETYPE_MIX_PROFILE.get(archetype) or _ARCHETYPE_MIX_PROFILE["default"])
    s = sum(weights)
    if s <= 0:
        return {"cantrip": total, "engine": 0, "burst": 0}
    cantrip = int(round(weights[0] / s * total))
    burst = int(round(weights[2] / s * total))
    engine = max(0, total - cantrip - burst)
    return {"cantrip": cantrip, "engine": engine, "burst": burst}


# ============================================================
# Public API.
# ============================================================


def _load_deck_card_metadata_from_db(
    deck_names: List[str], db_snapshot_id: Optional[str],
) -> Dict[str, Dict[str, Any]]:
    """Pull oracle_text + cmc + type_line from the cards table for the
    given names, scoped to the active snapshot. Returns name.lower() ->
    {oracle_text, cmc, type_line}.
    Falls back to {} on any error; caller proceeds with empty counts.
    """
    if not deck_names or not db_snapshot_id:
        return {}
    try:
        import sqlite3
        from engine.db import resolve_db_path
        con = sqlite3.connect(str(resolve_db_path()))
        con.row_factory = sqlite3.Row
        try:
            qmarks = ",".join("?" * len(deck_names))
            rows = con.execute(
                f"SELECT name, oracle_text, cmc, type_line "
                f"FROM cards WHERE snapshot_id=? AND name IN ({qmarks})",
                tuple([db_snapshot_id] + list(deck_names)),
            ).fetchall()
        finally:
            con.close()
        return {
            (r["name"] or "").strip().lower(): {
                "oracle_text": r["oracle_text"] or "",
                "cmc": r["cmc"],
                "type_line": r["type_line"] or "",
            }
            for r in rows
        }
    except Exception:
        return {}


def compute_card_advantage(
    *,
    deck: List[Dict[str, Any]],
    bracket: str,
    archetype_hint: Optional[str] = None,
    pool: Optional[Dict[str, Any]] = None,
) -> CardAdvantageRecommendation:
    """Compute Pillar E v0.2 card-advantage recommendation + reconciliation.

    Args:
      deck: list of card entries (card_name, source, reason).
      bracket: B1..B5.
      archetype_hint: archetype key (tribal, control, ...) for mix profile.
      pool: optional candidate pool from compute_agent_build_deck_v1 —
        used FIRST for oracle_text / cmc / type_line lookups. When a
        deck card isn't in the pool (basics, C2.2 picks, semantic-
        neighbor picks), the optimizer falls back to the cards table
        via `pool.get('db_snapshot_id')`.

    Returns:
      CardAdvantageRecommendation. `significant=True` means the agent
      flow should fire the LLM critique pass.
    """
    archetype = (archetype_hint or "default").strip().lower()
    base_total = _BRACKET_BASE_TARGET.get(bracket, 10)
    delta = _ARCHETYPE_TOTAL_DELTA.get(archetype, 0)
    target_total = max(4, base_total + delta)
    mix_targets = _mix_for(archetype, target_total)

    # Build a name → pool-card lookup for oracle_text / cmc / type_line.
    pool_by_name_lower: Dict[str, Dict[str, Any]] = {}
    db_snapshot_id: Optional[str] = None
    if pool:
        db_snapshot_id = pool.get("db_snapshot_id")
        for c in pool.get("candidates") or []:
            name = (c.get("name") or "").strip().lower()
            if name:
                pool_by_name_lower[name] = c

    # Fall back to direct cards-table lookup for any deck card missing
    # from the narrow pool. This is the difference between v0.2 and the
    # mana_base optimizer's pool-only lookup: card-advantage detection
    # is meaningful across the whole 100-card deck, not just the C2.1
    # candidate pool.
    missing_names = [
        (c.get("card_name") or "").strip()
        for c in deck
        if (c.get("card_name") or "").strip()
           and (c.get("card_name") or "").strip().lower() not in pool_by_name_lower
    ]
    db_lookup = _load_deck_card_metadata_from_db(missing_names, db_snapshot_id)

    counts = {"cantrip": 0, "engine": 0, "burst": 0}
    classified_cards: List[Dict[str, Any]] = []
    for c in deck:
        name = (c.get("card_name") or "").strip()
        if not name:
            continue
        match = pool_by_name_lower.get(name.lower()) or db_lookup.get(name.lower())
        if not match:
            continue
        category = _classify_card_advantage(
            oracle_text=match.get("oracle_text") or "",
            cmc=match.get("cmc"),
            type_line=match.get("type_line") or "",
        )
        if category:
            counts[category] += 1
            classified_cards.append({"name": name, "category": category})

    discrepancies: List[str] = []
    total_current = sum(counts.values())
    total_deficit = target_total - total_current
    if total_deficit > 2:
        discrepancies.append(
            f"Total card-advantage count {total_current} is "
            f"{total_deficit} below the target of {target_total} "
            f"(bracket={bracket}, archetype={archetype})."
        )
    elif total_deficit < -2:
        discrepancies.append(
            f"Total card-advantage count {total_current} is "
            f"{-total_deficit} ABOVE the target of {target_total}; "
            f"consider trimming for tempo or curve."
        )
    for cat in ("cantrip", "engine", "burst"):
        cat_delta = mix_targets[cat] - counts[cat]
        if abs(cat_delta) >= 2:
            sign = "below" if cat_delta > 0 else "above"
            discrepancies.append(
                f"{cat.capitalize()} count {counts[cat]} is "
                f"{abs(cat_delta)} {sign} the target {mix_targets[cat]} "
                f"for archetype {archetype}."
            )

    significant = bool(discrepancies)

    rationale = (
        f"Pillar E v0.2 card-advantage target: {target_total} draw pieces "
        f"for {bracket} {archetype} ({mix_targets['cantrip']} cantrip / "
        f"{mix_targets['engine']} engine / {mix_targets['burst']} burst). "
        f"Deck currently has {total_current} pieces "
        f"({counts['cantrip']} cantrip / {counts['engine']} engine / "
        f"{counts['burst']} burst). "
    )
    if significant:
        rationale += (
            f"{len(discrepancies)} discrepancy/discrepancies flagged for "
            f"LLM review."
        )
    else:
        rationale += "Counts within tolerance — no LLM critique needed."

    return CardAdvantageRecommendation(
        target_count=target_total,
        mix_targets=mix_targets,
        current_counts=dict(counts),
        discrepancies=discrepancies,
        recommended_swaps=[],   # iter 4 stub: LLM critique fills this
        rationale=rationale,
        significant=significant,
    )
