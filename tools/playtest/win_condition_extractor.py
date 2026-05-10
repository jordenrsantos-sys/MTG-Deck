"""Win-condition extraction layer for Phase 5b offline playtest framework
(stage 5b.7).

Per Decision 6 of OFFLINE_PLAYTEST_DESIGN.md: pre-game analysis of each deck's
primary + redundant win lines from existing combo packs + win-condition
primitives + commander role.

Per OQ-6: high-level granularity (combat / combo / mill / alt_win / value_grind).
Per safety #8: must distinguish Heliod (B4 user-rated; combat-primary) from
Varis Power Ranger (B5 user-rated; combo-primary) — the engine-axis-coverage
ceiling memo's canonical test case.

Approach: read 5a-era diagnostics already on each deck record:
- combo_count via existing `detect_two_card_combos` (consumed by Phase 5a's
  validator; available via the engine's combo machinery)
- power_staple_diagnostic per-axis counts (TRACK_ONLY since 5a.6; cheap_tutor
  count is the signal that distinguishes Heliod 0 vs Varis 3 — both have
  combo count=2 but Varis assembles combos via tutors, Heliod via natural
  draw)
- gc count
- engine_assigned_bracket

Output: per-deck win-condition envelope (Decision 6 shape).

Calibration-only module.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

WIN_CONDITION_EXTRACTOR_V1_VERSION = "win_condition_extractor_v1"

WIN_LINE_COMBO = "combo"
WIN_LINE_COMBAT = "combat"
WIN_LINE_MILL = "mill"
WIN_LINE_ALT_WIN = "alt_win"
WIN_LINE_VALUE_GRIND = "value_grind"

WIN_LINE_TYPES = (
    WIN_LINE_COMBO,
    WIN_LINE_COMBAT,
    WIN_LINE_MILL,
    WIN_LINE_ALT_WIN,
    WIN_LINE_VALUE_GRIND,
)


@dataclass(frozen=True)
class DeckSignals:
    """Pre-computed signals from the corpus deck dict."""
    deck_id: str
    gc: int
    combo_count: int
    cheap_tutor: int
    fast_mana: int
    interaction_staple: int
    value_staple: int
    engine_assigned_bracket: Optional[str]
    self_reported_bracket: Optional[str]


def signals_from_deck_dict(deck: Dict[str, Any]) -> DeckSignals:
    """Extract DeckSignals from a corpus deck dict (post-5a.6.2 shape).

    The corpus pack stores `power_staple_diagnostic` per deck (added in 5a.6).
    GC count + combo_count are NOT directly stored on the corpus pack, so this
    helper reads them from a `signals` field if present, else returns 0
    (caller may inject these from the validator's `_classify_corpus` output).
    """
    psd = deck.get("power_staple_diagnostic") or {}
    signals = deck.get("signals") or {}
    return DeckSignals(
        deck_id=deck["deck_id"],
        gc=int(signals.get("gc", 0)),
        combo_count=int(signals.get("combo_count", 0)),
        cheap_tutor=int(psd.get("cheap_tutor", 0)),
        fast_mana=int(psd.get("fast_mana", 0)),
        interaction_staple=int(psd.get("interaction_staple", 0)),
        value_staple=int(psd.get("value_staple", 0)),
        engine_assigned_bracket=deck.get("engine_assigned_bracket"),
        self_reported_bracket=deck.get("self_reported_bracket"),
    )


def extract_envelope(signals: DeckSignals) -> Dict[str, Any]:
    """Compute the win-condition envelope for a deck given its signals.

    Per Decision 6 + OQ-6 (high-level granularity):
    - primary_win_lines: ordered list of high-confidence win-line types
    - secondary_win_lines: lower-confidence alternates
    - win_line_count: count of distinct types present
    - redundancy_total: sum of per-line redundancy counts

    Disambiguation rule for safety #8 (Heliod vs Varis):
    - Both have combo_count=2 + gc=9
    - Heliod has cheap_tutor=0 -> combo line lacks tutor enablers; primary=combat
    - Varis has cheap_tutor=3 -> tutor-driven combo assembly; primary=combo

    The cheap_tutor axis is the high-level signal that distinguishes
    "deck has a combo line but not a combo deck" from "deck assembles combos
    every game" — it's the difference between Heliod's combo as a backup
    finisher and Varis's combo as the primary path.
    """
    primary: List[Dict[str, Any]] = []
    secondary: List[Dict[str, Any]] = []

    # Combo presence + assembly capability.
    combo_present = signals.combo_count > 0
    combo_assembled = combo_present and signals.cheap_tutor >= 2

    # Combat presence: every deck has combat; only meaningful if NOT
    # outclassed by combo.
    combat_present = True

    # Value grind: low gc + low combos + high value_staple.
    value_grind_present = (
        signals.combo_count <= 1
        and signals.gc <= 3
        and signals.value_staple >= 3
    )

    # Mill: not detected at this stage (would need primitive flags).
    mill_present = False

    # Alt-win: not detected at this stage (would need primitive flags).
    alt_win_present = False

    # Decide primary vs secondary placement.
    if combo_assembled:
        # Combo with tutor support is the primary win line.
        primary.append({
            "type": WIN_LINE_COMBO,
            "redundancy_count": signals.combo_count,
            "enabler_density": signals.cheap_tutor + signals.fast_mana,
        })
        if combat_present:
            secondary.append({
                "type": WIN_LINE_COMBAT,
                "redundancy_count": 1,
                "enabler_density": 0,
            })
    elif combo_present:
        # Combo present but lacks tutor support; combat is the actual primary
        # win line (the combo is opportunistic, not the plan).
        primary.append({
            "type": WIN_LINE_COMBAT,
            "redundancy_count": 1,
            "enabler_density": 0,
        })
        secondary.append({
            "type": WIN_LINE_COMBO,
            "redundancy_count": signals.combo_count,
            "enabler_density": signals.cheap_tutor + signals.fast_mana,
        })
    else:
        # No combos — primary is combat, possibly value-grind secondary.
        primary.append({
            "type": WIN_LINE_COMBAT,
            "redundancy_count": 1,
            "enabler_density": 0,
        })
        if value_grind_present:
            secondary.append({
                "type": WIN_LINE_VALUE_GRIND,
                "redundancy_count": 1,
                "enabler_density": signals.value_staple,
            })

    win_line_count = len(primary) + len(secondary)
    redundancy_total = sum(
        line["redundancy_count"] for line in primary + secondary
    )

    return {
        "version": WIN_CONDITION_EXTRACTOR_V1_VERSION,
        "deck_id": signals.deck_id,
        "primary_win_lines": primary,
        "secondary_win_lines": secondary,
        "win_line_count": win_line_count,
        "redundancy_total": redundancy_total,
        "primary_type": primary[0]["type"] if primary else None,
        "signals_used": {
            "gc": signals.gc,
            "combo_count": signals.combo_count,
            "cheap_tutor": signals.cheap_tutor,
            "fast_mana": signals.fast_mana,
            "interaction_staple": signals.interaction_staple,
            "value_staple": signals.value_staple,
        },
    }


def envelope_for_deck_dict(
    deck: Dict[str, Any],
    *,
    signals_override: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    """Convenience: extract envelope from a corpus deck dict.

    `signals_override` lets callers (e.g., 5b.10's full-corpus run) inject
    pre-computed gc + combo_count values from the validator's classifier
    output, since those signals aren't stored on the corpus pack itself.
    """
    if signals_override is not None:
        deck = dict(deck)
        existing_signals = deck.get("signals") or {}
        merged = dict(existing_signals)
        merged.update(signals_override)
        deck["signals"] = merged
    sig = signals_from_deck_dict(deck)
    return extract_envelope(sig)
