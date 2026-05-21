"""
agent_c22_prompt_templates_v1 — Iter 3 Phase 6.

Per-archetype prompt fragments for C2.2 (wild-combo discovery). The
generic C2.2 prompt asks the LLM to find "wild synergies"; this module
adds archetype-specific guidance so the LLM looks in the right
directions for the deck's actual strategy.

Public surface:

  - `ARCHETYPES`: tuple of supported archetype keys.
  - `detect_archetype(intent_analysis, theme_hints, commander) -> str`:
    classifier returning one of the keys (or "default").
  - `prompt_fragment_for(archetype) -> str`: returns the archetype-
    specific text that gets appended to the C2.2 system prompt.

Both detection and fragments are pure-string operations — testable
without LLM calls.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence


C22_PROMPT_TEMPLATES_VERSION = "agent_c22_prompt_templates_v1.0"


ARCHETYPES = (
    "tribal",         # creature-type-focused (vampires, goblins, dragons, ninjas, ...)
    "voltron",        # commander-damage / single-creature-amplification
    "storm",          # ritual chains + free spells → kill spell
    "aristocrats",    # sacrifice outlets + death triggers
    "control",        # counter/removal heavy → slow win
    "combo",          # explicit 2-3-card kill loop
    "blink",          # ETB-trigger-abuse + flicker effects
    "reanimator",     # graveyard recursion of fatties
    "landfall",       # landfall triggers + ramp
    "group_hug",      # politics / give-cards engines
    "tokens",         # wide-go-broad token swarms
    "default",        # generic fallback
)


# ============================================================
# Detection — keyword-based classifier.
# ============================================================


_KEYWORD_PATTERNS: Dict[str, List[str]] = {
    "tribal":      [r"\btribal\b", r"\btypal\b", r"\bvampire", r"\bgoblin",
                    r"\belf\b|\belves\b", r"\bzombie", r"\bdragon", r"\bninja",
                    r"\bmerfolk", r"\bcat\b|\bfeline", r"\bspirit", r"\bsoldier",
                    r"\bbird", r"\bdemon", r"\bangel", r"\bhuman",
                    r"creature[- ]type"],
    "voltron":     [r"\bvoltron\b", r"\bcommander damage\b", r"\bequipment\b",
                    r"\bauras?\b", r"\bsingle creature", r"\battach"],
    "storm":       [r"\bstorm\b", r"\britual chain\b", r"\bfree spell",
                    r"\bcost reduction", r"\bcast many spells"],
    "aristocrats": [r"\baristocrat", r"\bsacrifice outlet", r"\bsac outlet",
                    r"\bdeath trigger", r"\bdies trigger", r"\bblood artist",
                    r"\bdrain"],
    "control":     [r"\bcontrol\b", r"\bcounter(spell|magic)?\b",
                    r"\bremoval\b", r"\bboard wipe", r"\bwipe creatures",
                    r"\bstax\b"],
    "combo":       [r"\binfinite\b", r"\bcombo line\b", r"\bgame-ending combo",
                    r"\bthassa's oracle", r"\bdemonic consultation",
                    r"\btainted pact", r"\bkiki-jiki", r"\bsplinter twin",
                    r"\bbreach\b.*storm"],
    "blink":       [r"\bblink\b", r"\bflicker\b", r"\bexile (and return|and bring)",
                    r"\betb (trigger|abuse)", r"\bdeja vu"],
    "reanimator":  [r"\breanimat", r"\bgraveyard recursion", r"\bfrom graveyard to (battlefield|hand)",
                    r"\bdredge\b", r"\bunearth\b"],
    "landfall":    [r"\blandfall\b", r"\bland drop", r"\bramp.*landfall",
                    r"\bextra land", r"\bfetchland (engine|chain)"],
    "group_hug":   [r"\bgroup hug\b", r"\bevery opponent draws",
                    r"\bopponents draw cards", r"\bpolitics"],
    "tokens":      [r"\btoken (swarm|army|strategy|go.wide)",
                    r"\bgo wide\b", r"\bmass tokens?\b",
                    r"\beminence (token|trigger)", r"\bcreate (many|swarms?) (of )?creature tokens?"],
}


def _build_haystack(
    *,
    intent_analysis: Optional[Dict[str, Any]],
    theme_hints: Sequence[str],
    commander: str,
) -> str:
    """Compose a lowercased text blob from the inputs that
    `_KEYWORD_PATTERNS` can match against. Higher-signal fields
    (win condition, implicit themes) are repeated to give them more
    weight in tied matches."""
    parts: List[str] = []
    parts.append((commander or "").lower())
    for h in theme_hints or []:
        if isinstance(h, str):
            parts.append(h.lower())
    if intent_analysis:
        wc = intent_analysis.get("likely_win_condition")
        if isinstance(wc, str):
            # Win condition gets ×3 weight — it's the strongest signal.
            parts.append(wc.lower())
            parts.append(wc.lower())
            parts.append(wc.lower())
        themes = intent_analysis.get("implicit_themes") or []
        for t in themes:
            if isinstance(t, str):
                # Implicit themes get ×2 weight.
                parts.append(t.lower())
                parts.append(t.lower())
        sigs = intent_analysis.get("must_include_analysis") or []
        for s in sigs:
            if isinstance(s, dict):
                sig = s.get("signals_archetype")
                if isinstance(sig, str):
                    parts.append(sig.lower())
                # Card type words are useful too.
                tp = s.get("type")
                if isinstance(tp, str):
                    parts.append(tp.lower())
                abs_ = s.get("key_abilities") or []
                for a in abs_:
                    if isinstance(a, str):
                        parts.append(a.lower())
    return " | ".join(parts)


def detect_archetype(
    *,
    intent_analysis: Optional[Dict[str, Any]],
    theme_hints: Optional[Sequence[str]] = None,
    commander: str = "",
) -> str:
    """Classify the deck's archetype based on B2's intent_analysis +
    theme_hints + commander name. Returns one of `ARCHETYPES`. Falls
    back to "default" if no archetype scored higher than 0 matches.

    Scoring: each archetype's regex patterns count distinct matches in
    the composed haystack. The winning archetype is the one with the
    highest count; ties resolve via the archetype's order in
    `ARCHETYPES` (earlier = higher priority — tribal beats voltron beats
    storm, etc.).
    """
    haystack = _build_haystack(
        intent_analysis=intent_analysis,
        theme_hints=theme_hints or [],
        commander=commander,
    )
    if not haystack:
        return "default"

    scores: Dict[str, int] = {}
    for arch, patterns in _KEYWORD_PATTERNS.items():
        total = 0
        for pat in patterns:
            total += len(re.findall(pat, haystack))
        if total > 0:
            scores[arch] = total

    if not scores:
        return "default"

    # Tiebreaker: order in ARCHETYPES tuple (excluding "default").
    arch_order = {a: i for i, a in enumerate(ARCHETYPES) if a != "default"}
    best = max(
        scores.items(),
        key=lambda kv: (kv[1], -arch_order.get(kv[0], 999)),
    )
    return best[0]


# ============================================================
# Prompt fragments — appended to the C2.2 system prompt.
# ============================================================


_FRAGMENTS: Dict[str, str] = {
    "tribal": (
        "\n[ARCHETYPE: tribal] Look for off-tribe enablers that synergize with "
        "the tribe — creature-type-doubling effects (e.g. Mirror Entity), anthem "
        "lords (e.g. Cordial Vampire), and effects that count creatures of the "
        "shared type. Reanimate-the-tribe or recur-on-death effects often "
        "outperform pure tribal lords because they handle the trade-down problem."
    ),
    "voltron": (
        "\n[ARCHETYPE: voltron] Look for equipment/aura redundancy and "
        "protection: hexproof/shroud grants, indestructible turns, attack "
        "untap effects. Combat-step damage doubling (e.g. Berserkers' Onslaught) "
        "is dramatically stronger here than in other decks. Card-draw on combat "
        "damage closes the engine."
    ),
    "storm": (
        "\n[ARCHETYPE: storm] Look for ritual chains, cost reduction stacks, "
        "and free spells that don't consume mana net. Also: replicate-cantrip "
        "loops, X-spell payoffs, and storm-count enablers. A single payoff "
        "(Aetherflux Reservoir, Brain Freeze) closes a long enabler chain."
    ),
    "aristocrats": (
        "\n[ARCHETYPE: aristocrats] Look for sacrifice outlets, death triggers, "
        "persist/undying enablers, and recurring sacrifice fodder. The win-path "
        "is often: persist creature + sacrifice outlet + death trigger = "
        "engine. Drain effects (Blood Artist, Zulaport Cutthroat) close the "
        "game. Cards that reanimate after sac (Reassembling Skeleton, Bloodghast) "
        "are gold."
    ),
    "control": (
        "\n[ARCHETYPE: control] Look for cheap interaction (1-2 mana removal/"
        "counters), card-advantage engines (Rhystic Study, Mystic Remora), "
        "and reach (planeswalkers, X-spells). The win-con is often slow but "
        "inevitable (Approach of the Second Sun, planeswalker ultimates). "
        "Watch for redundancy in interaction types."
    ),
    "combo": (
        "\n[ARCHETYPE: combo] Look for tutor pieces, mana production for the "
        "kill turn, and counter-magic protection for the combo. Also: "
        "alternative kill lines from the same engine (e.g. a deck with "
        "Thoracle should also pack Lab Maniac + DC + Pact). Combo-protection "
        "(Silence, Veil of Summer, Allosaurus Shepherd) is high-priority."
    ),
    "blink": (
        "\n[ARCHETYPE: blink] Look for cards with strong ETB triggers and "
        "for repeated flicker effects (Ephemerate, Eldrazi Displacer, Charming "
        "Prince). The win-path is incremental value compounding into "
        "inevitability. Cards that flicker the WHOLE BOARD (Brago, Roon, "
        "Conjurer's Closet) are 2-for-1 engines."
    ),
    "reanimator": (
        "\n[ARCHETYPE: reanimator] Look for fast self-mill (Buried Alive, "
        "Entomb), efficient reanimation spells (Animate Dead, Reanimate), "
        "and game-ending reanimation targets (Razaketh, Jin-Gitaxias, "
        "Sheoldred). Discard outlets (Faithless Looting, Cathartic Reunion) "
        "double as setup. Graveyard hate protection (Loaming Shaman, "
        "regrowth effects) is essential."
    ),
    "landfall": (
        "\n[ARCHETYPE: landfall] Look for extra-land-drop effects (Azusa, "
        "Oracle of Mul Daya), fetchlands and landfall-cycling lands, and "
        "landfall payoffs that scale (Avenger of Zendikar, Scute Swarm, "
        "Lotus Cobra). Double-landfall enablers (Roost of Drakes, Wood "
        "Elves) compound."
    ),
    "group_hug": (
        "\n[ARCHETYPE: group_hug] Look for symmetric draw/ramp effects that "
        "the deck can break (Howling Mine + Underworld Dreams). Politics "
        "cards (Vow of cycle, monarch-grant effects) that incentivize "
        "opponents to NOT attack you. Win-cons that flip the symmetry "
        "(Approach, Maze's End)."
    ),
    "tokens": (
        "\n[ARCHETYPE: tokens] Look for token-doublers (Anointed Procession, "
        "Parallel Lives), anthem effects scaling with creature count (Shared "
        "Animosity, Cathars' Crusade), and mass haste/evasion grants. Sac "
        "outlets that turn dying tokens into value (Ashnod's Altar, Skullclamp) "
        "are exceptional. Token producers WITH built-in payoffs (Beast Whisperer "
        "for tokens) are gold."
    ),
    "default": (
        "\n[ARCHETYPE: general] Look for high-impact engine pieces that "
        "compound with what's in the deck. Read the candidate's text against "
        "specific cards in the current deck — synergies that name-check a "
        "specific other card are usually stronger than generic value cards."
    ),
}


def prompt_fragment_for(archetype: str) -> str:
    """Return the prompt fragment for the given archetype. Falls back
    to 'default' on unknown keys (defensive — caller passes a key from
    ARCHETYPES, but if it doesn't match exactly we still get something)."""
    return _FRAGMENTS.get(archetype, _FRAGMENTS["default"])
