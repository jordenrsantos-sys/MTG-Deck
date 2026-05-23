"""anti_meta_hate_v1 — Pillar E v0.6 (mega-task v6 Phase 10).

The final Pillar E optimizer. Reads the tiered opposition registry
(`opposition_decks_v1.json`) to characterize the expected meta for a
deck's bracket, then recommends hate piece counts per category +
specific candidate cards. Pure analysis — does NOT mutate the deck.

Public API
----------
``recommend_anti_meta_hate(deck, bracket, *, opposition_data=None) ->
AntiMetaRecommendations``

Categories:
  - graveyard_hate (fires when reanimator/dredge present in expected meta)
  - artifact_hate (fires when combo/rocks/storm meta)
  - stax_tax (always tier-relevant for B4/B5)
  - counterspell_density (B5 cEDH only)
  - format_specific_tech (catch-all per bracket)

Per-bracket recommended hate counts are conservative — the goal is a
narrow set of techy slots (~1-3 cards in B2-B3, ~3-5 in B4-B5) rather
than a hate-heavy main deck.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
import json
import re


ANTI_META_HATE_VERSION = "anti_meta_hate_v1.0"

_OPPOSITION_DECKS_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "playtest" / "opposition_decks_v1.json"
)


# Keyword patterns in opposition archetype_hint strings → meta theme tag.
_ARCHETYPE_HINT_PATTERNS: Dict[str, List[str]] = {
    "reanimator": [r"\breanimat", r"\bdredge\b", r"\bgraveyard\b"],
    "combo": [r"\bcombo\b", r"\bthoracle\b", r"\bkiki", r"\bheliod"],
    "storm": [r"\bstorm\b", r"\bspellslinger"],
    "rocks_artifacts": [r"\bartifact", r"\brocks?\b", r"\baffin"],
    "stax": [r"\bstax\b", r"\bpillow ?fort\b"],
    "tribal": [r"\btribal\b", r"\bgoblin\b", r"\bvampire\b", r"\bdragon\b"],
    "control": [r"\bcontrol\b", r"\bcounter", r"\bremoval"],
    "tokens": [r"\btoken\b", r"\bgo[- ]wide\b"],
}


# Bracket → recommended hate piece allocation per category.
# Counts are deck slots to TARGET, not absolutes. The actual flagging
# logic compares actual_in_deck to these targets.
_BRACKET_TARGETS: Dict[str, Dict[str, int]] = {
    "B1": {"graveyard_hate": 0, "artifact_hate": 0, "stax_tax": 0,
           "counterspell_density": 0, "format_specific_tech": 1},
    "B2": {"graveyard_hate": 1, "artifact_hate": 0, "stax_tax": 0,
           "counterspell_density": 0, "format_specific_tech": 1},
    "B3": {"graveyard_hate": 1, "artifact_hate": 1, "stax_tax": 0,
           "counterspell_density": 0, "format_specific_tech": 1},
    "B4": {"graveyard_hate": 2, "artifact_hate": 1, "stax_tax": 1,
           "counterspell_density": 1, "format_specific_tech": 2},
    "B5": {"graveyard_hate": 1, "artifact_hate": 1, "stax_tax": 0,
           "counterspell_density": 2, "format_specific_tech": 1},
}


# Canonical hate-piece example cards per category (no commitment to
# specific colors — the agent / D2 critic decides what color-correct
# substitutes to actually run).
_HATE_CARD_EXAMPLES: Dict[str, List[str]] = {
    "graveyard_hate": [
        "Rest in Peace", "Leyline of the Void", "Bojuka Bog", "Soul-Guide Lantern",
        "Tormod's Crypt", "Grafdigger's Cage",
    ],
    "artifact_hate": [
        "Vandalblast", "Bane of Progress", "Collector Ouphe", "Null Rod",
        "Stony Silence", "By Force",
    ],
    "stax_tax": [
        "Thalia, Guardian of Thraben", "Aven Mindcensor", "Glowrider",
        "Drannith Magistrate", "Cursed Totem",
    ],
    "counterspell_density": [
        "Force of Will", "Force of Negation", "Mana Drain", "Counterspell",
        "Swan Song", "Fierce Guardianship",
    ],
    "format_specific_tech": [
        "Carpet of Flowers", "Boseiju, Who Endures", "Cyclonic Rift",
        "Notion Thief", "Drannith Magistrate",
    ],
}


@dataclass
class AntiMetaRecommendations:
    version: str = ANTI_META_HATE_VERSION
    bracket: str = ""
    expected_meta: List[str] = field(default_factory=list)        # ordered theme tags
    targets_by_category: Dict[str, int] = field(default_factory=dict)
    suggested_candidates: Dict[str, List[str]] = field(default_factory=dict)
    rationale: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _load_opposition() -> List[Dict[str, Any]]:
    try:
        with _OPPOSITION_DECKS_PATH.open(encoding="utf-8") as f:
            data = json.load(f)
        return data.get("entries") or []
    except Exception:
        return []


def _characterize_meta(opposition: List[Dict[str, Any]], bracket: str) -> List[str]:
    """Walk opposition entries for `bracket`, extract archetype themes
    from their `archetype_hint` strings, return distinct themes ordered
    by frequency (most-common first)."""
    counts: Dict[str, int] = {}
    for entry in opposition:
        if entry.get("bracket") != bracket:
            continue
        hint = (entry.get("archetype_hint") or "").lower()
        if not hint:
            continue
        for theme, patterns in _ARCHETYPE_HINT_PATTERNS.items():
            for p in patterns:
                if re.search(p, hint):
                    counts[theme] = counts.get(theme, 0) + 1
                    break  # don't double-count this hint for this theme
    return [t for t, _ in sorted(counts.items(), key=lambda kv: -kv[1])]


def recommend_anti_meta_hate(
    deck: List[Dict[str, Any]],
    bracket: str,
    *,
    opposition_data: Optional[List[Dict[str, Any]]] = None,
) -> AntiMetaRecommendations:
    """Compute anti-meta hate recommendations.

    Args:
        deck: list of {card_name, ...} dicts. Currently unused for
            target counts (those are pure bracket → target lookups) but
            reserved for a future per-color filter when D2 critique
            picks specific cards.
        bracket: "B1".."B5". Drives `_BRACKET_TARGETS`.
        opposition_data: optional pre-loaded opposition entries; loads
            from `opposition_decks_v1.json` when None.

    Returns: AntiMetaRecommendations with targets per category +
        suggested candidate cards + per-category rationale strings.
    """
    rec = AntiMetaRecommendations(bracket=bracket)

    opposition = opposition_data if opposition_data is not None else _load_opposition()
    meta_themes = _characterize_meta(opposition, bracket)
    rec.expected_meta = meta_themes

    targets = dict(_BRACKET_TARGETS.get(bracket, _BRACKET_TARGETS["B3"]))

    # Meta-conditional adjustments — bump categories that match the
    # bracket's expected meta.
    if "reanimator" in meta_themes and targets.get("graveyard_hate", 0) < 2:
        targets["graveyard_hate"] = max(targets.get("graveyard_hate", 0), 2)
        rec.rationale.append(
            "Reanimator present in expected meta → bumped graveyard_hate target to 2."
        )
    if any(t in meta_themes for t in ("combo", "rocks_artifacts", "storm")):
        if targets.get("artifact_hate", 0) < 1:
            targets["artifact_hate"] = 1
        rec.rationale.append(
            "Combo / artifacts / storm in expected meta → ensure artifact_hate ≥ 1."
        )
    if bracket == "B5" and "control" in meta_themes:
        if targets.get("counterspell_density", 0) < 3:
            targets["counterspell_density"] = 3
        rec.rationale.append(
            "B5 + control in expected meta → bumped counterspell_density to 3."
        )

    rec.targets_by_category = targets

    # Always include the canonical example pool — the actual color-correct
    # subset selection is left to D2 critique / user discretion.
    for cat, count in targets.items():
        if count > 0:
            examples = _HATE_CARD_EXAMPLES.get(cat, [])
            rec.suggested_candidates[cat] = examples[: max(count, 3)]

    if not meta_themes:
        rec.rationale.append(
            f"No archetype_hint data for bracket {bracket} in opposition "
            f"registry — using flat bracket targets only."
        )

    return rec
