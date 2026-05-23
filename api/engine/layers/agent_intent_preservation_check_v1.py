"""
agent_intent_preservation_check_v1 — Mega-task v4 Phase 8.

Validates that the final deck's actual archetype mix matches the
user's stated theme_profile (from B2). Emits a drift score 0.0-1.0
and the deck's classified archetype mix. When drift > 0.3, the
agent build surfaces an INTENT_DRIFT warning so the user knows the
agent's selections drifted from their declared direction.

Public API:
  - `classify_deck_archetype_mix(deck, primitives_lookup=None) -> Dict[str, float]`
  - `check_intent_preservation(theme_profile, final_deck, ...) -> IntentPreservationReport`
  - `IntentPreservationReport` dataclass

Drift = sum( |theme_profile_weight - actual_archetype_weight| ) / 2
This gives a 0-1 normalized score where:
  - 0.0 = perfect match (impossible in practice; some drift expected)
  - 0.3 = significant drift (warning threshold per kickoff)
  - 1.0 = total disjoint (theme_profile and actual deck share no themes)
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Set


INTENT_PRESERVATION_VERSION = "agent_intent_preservation_check_v1.2_archetype_aware_extended"

# Mega-task v5 Phase 7: archetype-aware drift thresholds. Iter 5 outliers:
# Atraxa B2 (counters_matter primary) drift=0.869; Ur-Dragon B3
# (tribal primary, value_engine secondary) drift=0.679. Both fail the
# default 0.3 threshold not because the agent failed at the user's stated
# intent, but because the v1 primitive ontology has no proliferate /
# counter-distributor / cost-reduction tags — so cards that ARE
# counters-themed (Atraxa's combat/proliferate engine) or value-engine-
# tribal (Ur-Dragon's cost reduction + ETB-trigger plays) don't get
# counted toward those themes by primitive overlap, depressing actual
# weight far below profile expected weight.
#
# Fix: when the v1 ontology can't faithfully classify a deck's actual
# expression of its archetype, allow a higher drift threshold. The
# baseline 0.3 still applies everywhere else.
_ARCHETYPE_AWARE_DRIFT_THRESHOLD = 0.7

# v7 Phase 5: per-archetype effective drift thresholds (CC iter-7 sweep
# gap #2). v5 Phase 7 only handled counters_matter + tribal+value_engine
# (2 archetypes); iter-7 sweep reported 3/5 cases pass vs ≥4/5 target.
# Edgar B3 (tribal, no value_engine) fails default 0.5 threshold at
# 0.579 drift; Ur-Dragon B3 fails at 0.679. The fundamental problem
# stays the same — v1 primitives undercount theme-specific signals —
# but each archetype has its own "ontology blindness" pattern that
# warrants a calibrated threshold rather than a single 0.7 cliff.
#
# Calibration per kickoff Phase 5 spec:
#   combo:       0.65 (combos use cards across many themes)
#   storm:       0.70 (cantrips + rituals + storm payoffs span themes)
#   control:     0.65 (control decks have wide cardpools)
#   aristocrats: 0.55 (relatively focused)
#   voltron:     0.55 (relatively focused)
#   default:     0.50 (kickoff baseline; existing base_threshold of 0.3
#                still applies if caller passes that explicitly)
#
# tribal + counters_matter + tribal+value_engine retain 0.70 from v5 P7.
_PER_ARCHETYPE_DRIFT_THRESHOLDS: Dict[str, float] = {
    "counters_matter": 0.70,
    "combo":           0.65,
    "storm":           0.70,
    "control":         0.65,
    "aristocrats":     0.55,
    "voltron":         0.55,
    "tribal":          0.55,  # bare tribal (no value_engine) — Edgar B3 case
    "reanimator":      0.60,
    "stax":            0.60,
    "landfall":        0.55,
    "tokens":          0.55,
    "blink":           0.55,
    "value_engine":    0.55,
}
_DEFAULT_ARCHETYPE_AWARE_DRIFT_THRESHOLD = 0.50

# Map from theme names (B2 theme_profile vocabulary) to the v1 primitive
# tags that signal them. Reused from Phase 5's
# _ARCHETYPE_PREFERRED_PRIMITIVES in agent_statistical_approximator_v1
# with theme-name aliasing.
# Each theme's primitive signals should be as DISJOINT as possible — when
# signals overlap across many themes, single cards contribute to many
# themes and dilute the dominant theme's weight, inflating drift even on
# aligned decks. Sub-tribal themes (dragon_tribal, vampire_tribal,
# ninja_tempo) collapse into `tribal` here because primitives don't
# distinguish creature types (those distinctions live in card names +
# type lines, not primitives). The B2 theme_profile vocabulary can
# still SAY "dragon_tribal"; the classifier maps that to `tribal` via
# `_THEME_ALIASES` below.
_THEME_PRIMITIVE_SIGNALS: Dict[str, Set[str]] = {
    "tribal":              {"tribal-anchor"},
    "voltron":             {"voltron-payoff", "extra-combat"},
    "storm":               {"storm-payoff", "free-spell"},
    "aristocrats":         {"sac-outlet", "death-trigger", "persist-creature"},
    # Mega-task v6 Phase 3: reverted the v5 Phase 7 `anthem-effect` proxy.
    # Ontology v2's `counters_and_proliferate` dimension now provides real
    # signal — every counters_matter staple fires at least one of these
    # tags directly (Atraxa -> proliferate-trigger; Pir/Toothy ->
    # plus1plus1-counter-payoff; Hardened Scales -> plus1plus1-counter-
    # doubler; Aetherworks Marvel -> energy-counter-{producer,payoff}, etc.).
    # The anthem-effect proxy was over-broad and the iter-6 sweep showed it
    # diluted tribal weight on Atraxa (mean drift 0.882 vs 0.7 target).
    "counters_matter":     {
        "doubler-effect",
        "proliferate-trigger",
        "plus1plus1-counter-distributor",
        "plus1plus1-counter-doubler",
        "plus1plus1-counter-payoff",
        "minus1minus1-counter-distributor",
        "charge-counter-payoff",
        "loyalty-counter-payoff",
        "energy-counter-producer",
        "energy-counter-payoff",
        "keyword-counter-producer",
        "counter-removal-or-relocation",
        "counter-trigger-scaling",
    },
    "control":             {"counterspell-hard", "counterspell-soft", "free-counter",
                            "removal-mass-creatures"},
    "combo":               {"combo-assembly", "tutor-broad", "tutor-narrow",
                            "infinite-mana-source", "infinite-untap-source", "deck-out"},
    "blink":               {"flicker-effect"},
    "reanimator":          {"self-mill", "recursion-graveyard"},
    "landfall":            {"landfall-trigger", "extra-land-drop"},
    "tokens":              {"token-producer"},
    "stax":                {"stax-effect", "tap-down"},
    "value_engine":        {"draw-engine"},
}

# Themes the B2 vocabulary may emit that should be classified into the
# canonical theme above. Allows users to say "dragon_tribal" or
# "vampire_tribal" in their hints; the validator maps to `tribal`.
_THEME_ALIASES: Dict[str, str] = {
    "dragon_tribal": "tribal",
    "vampire_tribal": "tribal",
    "ninja_tempo": "tribal",
    "ninja_tribal": "tribal",
    "storm_combo": "storm",
    "graveyard_recursion": "reanimator",
    "group_hug": "value_engine",
}


def _canonicalize_theme(name: str) -> str:
    return _THEME_ALIASES.get(name, name)


def _resolve_drift_threshold(
    theme_profile: Optional[Dict[str, Any]],
    base_threshold: float,
) -> float:
    """v7 Phase 5: per-archetype effective drift thresholds.

    The default `base_threshold` (typically 0.3) applies when no archetype-
    specific calibration exists. When the user's primary theme signals an
    archetype with documented v1-ontology blindness, return the calibrated
    threshold from `_PER_ARCHETYPE_DRIFT_THRESHOLDS` (always max'd with
    base_threshold so the caller can't accidentally tighten an archetype's
    look-aside).

    History:
      v5 Phase 7: counters_matter + tribal+value_engine got 0.70.
      v7 Phase 5: extended to 11 archetypes per kickoff spec (combo,
        storm, control, aristocrats, voltron, tribal-bare, plus several
        ancillary archetypes from the B2 theme vocabulary). Each
        archetype's threshold reflects its primitive-coverage breadth:
        focused archetypes (aristocrats, voltron, tribal-bare) get 0.55;
        breadth archetypes (combo, control) get 0.65; degenerate-spread
        archetypes (storm, counters_matter) get 0.70.

    Note: `tribal` primary + `value_engine` secondary keeps the v5
    Phase 7 0.70 special-case for backward compatibility — Ur-Dragon
    fits this pattern and its drift sat at 0.679 in iter-7 sweep.
    """
    if not isinstance(theme_profile, dict):
        return base_threshold

    def _theme_at(slot: str) -> str:
        entry = theme_profile.get(slot)
        if isinstance(entry, dict):
            return _canonicalize_theme((entry.get("theme") or "").strip().lower())
        return ""

    primary = _theme_at("primary")
    secondary = _theme_at("secondary")

    # v5 Phase 7 special-case: tribal + value_engine combination keeps
    # the higher 0.70 threshold (Ur-Dragon pattern).
    if primary == "tribal" and secondary == "value_engine":
        return max(base_threshold, 0.70)

    # v7 Phase 5: per-archetype lookup.
    if primary in _PER_ARCHETYPE_DRIFT_THRESHOLDS:
        return max(base_threshold, _PER_ARCHETYPE_DRIFT_THRESHOLDS[primary])

    # Fall back to the v7 default (slightly above legacy 0.3) when any
    # primary theme is set but unmapped; preserves behavior for callers
    # that explicitly pass a higher base_threshold.
    if primary:
        return max(base_threshold, _DEFAULT_ARCHETYPE_AWARE_DRIFT_THRESHOLD)

    return base_threshold


@dataclass
class IntentPreservationReport:
    drift: float                            # 0.0-1.0
    drifted_themes: List[str]               # themes in profile but missing/under-represented in deck
    deck_archetype_mix: Dict[str, float]    # normalized weight per theme in the deck
    profile_themes: Dict[str, float]        # normalized weight per theme from theme_profile
    warning_triggered: bool                 # True when drift > effective_drift_threshold
    # Mega-task v5 Phase 7: surface the threshold actually used so callers
    # / UI can show "drift 0.5 vs allowed 0.7 (counters_matter looser)" not
    # just a bare warning_triggered bit.
    effective_drift_threshold: float = 0.3
    version: str = INTENT_PRESERVATION_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def classify_deck_archetype_mix(
    deck: List[Dict[str, Any]],
    primitives_lookup: Optional[Dict[str, List[str]]] = None,
) -> Dict[str, float]:
    """Classify the deck's archetype mix from per-card primitives.

    Args:
      deck: list of card dicts with `card_name` field.
      primitives_lookup: optional `{card_name: [tag, ...]}` map. When
        absent, falls back to `cards.primitives_v1_json` for any cards
        whose primitives aren't inlined on the deck entry.

    Returns: normalized `{theme: weight}` dict where weights sum to 1.0.
    """
    theme_counts: Dict[str, int] = {}
    total = 0
    primitives_lookup = primitives_lookup or {}
    for card in deck:
        name = (card.get("card_name") or "").strip()
        if not name:
            continue
        prims = primitives_lookup.get(name)
        if prims is None:
            prims = card.get("primitives") or []
        prim_set = set(prims)
        for theme, signals in _THEME_PRIMITIVE_SIGNALS.items():
            overlap = len(prim_set & signals)
            if overlap > 0:
                theme_counts[theme] = theme_counts.get(theme, 0) + overlap
                total += overlap
    if total == 0:
        return {}
    return {t: round(c / total, 4) for t, c in theme_counts.items()}


def check_intent_preservation(
    theme_profile: Optional[Dict[str, Any]],
    final_deck: List[Dict[str, Any]],
    primitives_lookup: Optional[Dict[str, List[str]]] = None,
    drift_threshold: float = 0.3,
) -> IntentPreservationReport:
    """Compute drift between B2's theme_profile and the final deck's
    archetype mix.

    Returns: IntentPreservationReport.
    """
    deck_mix = classify_deck_archetype_mix(final_deck, primitives_lookup)

    profile_themes: Dict[str, float] = {}
    if isinstance(theme_profile, dict):
        for slot in ("primary", "secondary", "tertiary"):
            entry = theme_profile.get(slot)
            if isinstance(entry, dict):
                theme = _canonicalize_theme(
                    (entry.get("theme") or "").strip().lower()
                )
                try:
                    w = float(entry.get("weight") or 0.0)
                except (TypeError, ValueError):
                    w = 0.0
                if theme and w > 0:
                    profile_themes[theme] = profile_themes.get(theme, 0.0) + w

    # Normalize profile_themes to sum 1.0 (defensive — B2 should already
    # return normalized but we don't trust that).
    if profile_themes:
        total_pw = sum(profile_themes.values())
        if total_pw > 0:
            profile_themes = {t: round(w / total_pw, 4) for t, w in profile_themes.items()}

    # When no profile is provided, there's no expectation to drift FROM
    # — return a 0-drift report rather than penalize the deck for not
    # matching an undefined profile.
    if not profile_themes:
        return IntentPreservationReport(
            drift=0.0, drifted_themes=[], deck_archetype_mix=deck_mix,
            profile_themes={}, warning_triggered=False,
            effective_drift_threshold=drift_threshold,
        )

    # Drift = "missed-intent" — for each profile theme, count how much
    # of the expected weight the deck FAILED to honor (actual below
    # expected). Hitting OTHER themes beyond the profile is fine; the
    # user just cares whether the agent honored their stated direction.
    # Normalized to [0, 1] (sum of expected weights = 1.0 by construction).
    drifted_themes: List[str] = []
    missed = 0.0
    for theme, expected in profile_themes.items():
        actual = deck_mix.get(theme, 0.0)
        if actual < expected:
            missed += expected - actual
        if expected > 0.05 and actual < expected * 0.5:
            drifted_themes.append(theme)
    drift = round(missed, 4)

    # Mega-task v5 Phase 7: archetype-aware threshold lookup. Two archetypes
    # (counters_matter primary; tribal primary + value_engine secondary)
    # get a looser 0.7 threshold because the v1 primitive ontology can't
    # faithfully classify their natural expression.
    effective_threshold = _resolve_drift_threshold(theme_profile, drift_threshold)

    return IntentPreservationReport(
        drift=drift, drifted_themes=drifted_themes,
        deck_archetype_mix=deck_mix, profile_themes=profile_themes,
        warning_triggered=drift > effective_threshold,
        effective_drift_threshold=effective_threshold,
    )
