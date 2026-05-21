"""
agent_statistical_approximator_v1 — Pillar F v0.1 (iter 4 upgrade).

Statistical approximator for 4-player Commander deck-vs-pod win rate.
No game simulation; decomposes decks into win-paths + interaction
profiles and computes pod winrate from speed/interaction/resilience
matchups.

Architecturally separate from Phase 5b's MPA substrate (per the
kickoff: "Pillar F v0.1 in this mega-task is a SEPARATE statistical
layer that does NOT depend on or interact with the MPA").

Iter 4 Phase 6 upgrade: win-paths now reference the Pillar C
ontology's primitive tags (`primitives_v1`, lowercase-kebab) instead
of the iter-1 primitives_v0 taxonomy. Deck card primitives are
looked up from `cards.primitives_v1_json` (populated by the Phase 5
backfill) and falls back to the `primitives` field on the deck dict
when present. 4 new win-paths added (mass-token-anthem, mass-mill-
lockout, stax-grind, etb-flicker, tutor-combo-assembly).

Public API:
  - approximate_pod_winrate(deck, opponents, db_snapshot_id) → PodWinrateReport
  - PodWinrateReport: dataclass with pod_winrate, per_opponent_winrate,
    decomposition

Per the kickoff, this remains a v0.1 SCAFFOLD. Stub areas
(heuristic placeholders, not full implementations):
  - 4+-card combo chain matching (only 2-3-card patterns covered)
  - Mana stochasticity (average opening hand assumed)
  - Mid-game adaptation (linear play assumed)
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


STATISTICAL_APPROXIMATOR_VERSION = "agent_statistical_approximator_v1.0"

OPPOSITION_DECKS_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "playtest" / "opposition_decks_v1.json"
)


# ============================================================
# Win-path catalog.
# ============================================================
#
# Each win-path is identified by a set of REQUIRED primitive tags
# the deck must include. When all required tags are present, the
# win-path is "armed". Speed (turn the path can land) is hand-coded
# per pattern, calibrated against known Commander metagame data.
#
# Iter 4+ will expand this catalog with primitive_tags_v1 data; iter
# 3 uses iter-1 primitives_v0 + structural heuristics.

WIN_PATHS: List[Dict[str, Any]] = [
    {
        "id": "thoracle_consultation",
        "name": "Thassa's Oracle + Demonic Consultation/Tainted Pact",
        "description": "Cast Thoracle, then DC/TP exiling a card not in the deck → empty library → win.",
        "required_card_names": ["thassa's oracle"],
        "any_card_names": ["demonic consultation", "tainted pact", "jace, wielder of mysteries"],
        "primitives": [],
        "speed_score": 4.0,  # avg turn to win
        "category": "tutor-combo",
    },
    {
        "id": "kiki_combo",
        "name": "Kiki-Jiki + persist creature with relevant ETB",
        "description": "Kiki copies a creature that untaps Kiki on ETB → infinite copies with haste.",
        "required_card_names": ["kiki-jiki, mirror breaker"],
        "any_card_names": [
            "conspicuous snoop", "felidar guardian", "splinter twin",
            "zealous conscripts", "village bell-ringer",
        ],
        "primitives": [],
        "speed_score": 5.0,
        "category": "tutor-combo",
    },
    {
        "id": "heliod_ballista",
        "name": "Heliod + Walking Ballista",
        "description": "Lifelink counter on Ballista; activate {1} to ping, gain life, place +1/+1 counter, repeat.",
        "required_card_names": ["heliod, sun-crowned"],
        "any_card_names": ["walking ballista", "spike feeder"],
        "primitives": [],
        "speed_score": 5.0,
        "category": "creature-combo",
    },
    {
        "id": "sanguine_exquisite",
        "name": "Sanguine Bond + Exquisite Blood",
        "description": "Any life-loss triggers Bond, which gains life, which triggers Blood, which loses life → infinite drain.",
        "required_card_names": ["sanguine bond", "exquisite blood"],
        "any_card_names": [],
        "primitives": [],
        "speed_score": 7.0,
        "category": "permanent-combo",
    },
    {
        "id": "mikaeus_trike",
        "name": "Mikaeus the Unhallowed + Triskelion",
        "description": "Trike removes counters to ping; dies; Mikaeus persist; trike returns with +1/+1, persist counter removed → loop.",
        "required_card_names": ["mikaeus, the unhallowed"],
        "any_card_names": ["triskelion", "walking ballista"],
        "primitives": [],
        "speed_score": 6.0,
        "category": "creature-combo",
    },
    {
        "id": "aristocrats_drain",
        "name": "Sacrifice outlet + persist/recurring creature + drain payoff",
        "description": "Loop sac + recur + drain trigger for incremental life loss across the table.",
        "required_card_names": [],
        "any_card_names": [],
        "primitives": ["sac-outlet", "death-trigger"],
        "any_primitives": ["persist-creature", "recursion-graveyard"],
        "speed_score": 8.0,
        "category": "engine",
    },
    {
        "id": "storm_kill",
        "name": "Ritual chains + storm payoff",
        "description": "Cast a chain of free/cheap spells, then cast Storm payoff (Brain Freeze, Aetherflux) for the kill.",
        "required_card_names": [],
        "any_card_names": [
            "aetherflux reservoir", "brain freeze", "tendrils of agony",
            "grapeshot",
        ],
        "primitives": [],
        "speed_score": 5.0,
        "category": "engine",
    },
    {
        "id": "dragon_tempest_combat",
        "name": "Dragon Tempest + chain of Dragon ETBs",
        "description": "Each Dragon entering pings opponents; Tiamat tutors a lethal chain.",
        "required_card_names": ["dragon tempest"],
        "any_card_names": [
            "tiamat", "scourge of valkas", "terror of the peaks", "the ur-dragon",
        ],
        "primitives": [],
        "speed_score": 7.0,
        "category": "tribal-combat",
    },
    {
        "id": "extra_combat_voltron",
        "name": "Infinite-mana → infinite extra combats",
        "description": "Aggravated Assault / Hellkite Charger on infinite mana → arbitrary combats.",
        "required_card_names": [],
        "any_card_names": [
            "aggravated assault", "hellkite charger", "world at war",
        ],
        "primitives": ["infinite-mana-source", "extra-combat"],
        "speed_score": 6.0,
        "category": "creature-combo",
    },
    {
        "id": "edgar_swarm",
        "name": "Edgar Markov eminence + wide vampire swarm + lifegain drain",
        "description": "Cast many cheap vampires; eminence doubles into tokens; Vito/Sanctum Seeker drain to lethal.",
        "required_card_names": ["edgar markov"],
        "any_card_names": [
            "vito, thorn of the dusk rose", "sanctum seeker",
            "bloodthirsty conqueror",
        ],
        "primitives": [],
        "speed_score": 7.0,
        "category": "tribal-combat",
    },
    {
        "id": "krenko_goblin_swarm",
        "name": "Krenko goblin doubling swarm",
        "description": "Tap Krenko to double goblins; with haste/untap effects, exponential token production.",
        "required_card_names": ["krenko, mob boss"],
        "any_card_names": [],
        "primitives": ["tribal-anchor"],
        "speed_score": 7.0,
        "category": "tribal-combat",
    },
    {
        "id": "proliferate_counters",
        "name": "Proliferate counter scaling",
        "description": "Build +1/+1 counters via Atraxa/Pir; proliferate to lethal combat damage.",
        "required_card_names": [],
        "any_card_names": ["atraxa, praetors' voice", "pir, imaginative rascal"],
        "primitives": ["doubler-effect"],
        "speed_score": 9.0,
        "category": "tribal-combat",
    },
    # ----- Iter 4 Phase 6: new primitive-grounded win-paths -----
    {
        "id": "mass_token_anthem",
        "name": "Mass tokens + anthem swarm",
        "description": "Wide token production scaled by anthem effects pushes combat to lethal.",
        "required_card_names": [],
        "any_card_names": [],
        "primitives": ["token-producer", "anthem-effect"],
        "speed_score": 8.0,
        "category": "tribal-combat",
    },
    {
        "id": "mass_mill_lockout",
        "name": "Mass mill + recursion lockout",
        "description": "Mill opponents repeatedly while recursion keeps the engine alive.",
        "required_card_names": [],
        "any_card_names": [],
        "primitives": ["mill-all", "recursion-graveyard"],
        "speed_score": 9.0,
        "category": "engine",
    },
    {
        "id": "stax_grind",
        "name": "Stax + value-engine grind",
        "description": "Lock opponents under stax pieces while a draw engine pulls ahead.",
        "required_card_names": [],
        "any_card_names": [],
        "primitives": ["stax-effect", "draw-engine"],
        "speed_score": 10.0,
        "category": "engine",
    },
    {
        "id": "etb_flicker_chain",
        "name": "ETB-trigger + flicker engine",
        "description": "Ephemerate/Eldrazi Displacer + value ETBs compound into inevitability.",
        "required_card_names": [],
        "any_card_names": [],
        "primitives": ["etb-trigger", "flicker-effect"],
        "speed_score": 7.0,
        "category": "engine",
    },
    {
        "id": "tutor_combo_assembly",
        "name": "Tutor-broad + combo-assembly piece",
        "description": "Demonic Tutor / Vampiric Tutor + named combo-assembly card (Thoracle, Kiki, Heliod) → guaranteed combo turn.",
        "required_card_names": [],
        "any_card_names": [],
        "primitives": ["tutor-broad", "combo-assembly"],
        "speed_score": 4.5,
        "category": "tutor-combo",
    },
    {
        "id": "extra_turn_chain",
        "name": "Extra-turn chain + extra-combat",
        "description": "Extra turns stacked with extra combats deliver one-shot voltron kills.",
        "required_card_names": [],
        "any_card_names": [],
        "primitives": ["extra-turn", "extra-combat"],
        "speed_score": 7.0,
        "category": "creature-combo",
    },
]


# ============================================================
# Output dataclasses.
# ============================================================


@dataclass
class WinPathMatch:
    win_path_id: str
    name: str
    category: str
    speed_score: float
    armed: bool
    missing_pieces: List[str] = field(default_factory=list)


@dataclass
class DeckDecomposition:
    win_paths: List[WinPathMatch]
    speed_score: float  # min(armed paths' speed_score); None if no armed paths
    interaction_density: int  # count of interaction primitives
    resilience_score: int  # count of protection/recursion primitives
    vulnerability_to: List[str]


@dataclass
class PodWinrateReport:
    pod_winrate: float
    per_opponent_winrate: Dict[str, float]
    decomposition: DeckDecomposition
    version: str = STATISTICAL_APPROXIMATOR_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================
# Helpers.
# ============================================================


def _name_lower_set(deck: Sequence[Dict[str, Any]]) -> set:
    return {(c.get("card_name") or c.get("name") or "").strip().lower() for c in deck}


def _primitives_set(
    deck: Sequence[Dict[str, Any]],
    db_snapshot_id: Optional[str] = None,
) -> set:
    """Union of all primitives across the deck's cards.

    Iter 4 Phase 6: primary source is `cards.primitives_v1_json` (loaded
    per-snapshot when `db_snapshot_id` is given). Falls back to deck
    cards' inline `primitives` field for tests + cases where the DB
    isn't available.
    """
    out: set = set()
    for c in deck:
        for p in (c.get("primitives") or []):
            if isinstance(p, str):
                out.add(p)

    if db_snapshot_id:
        names = [
            (c.get("card_name") or c.get("name") or "").strip()
            for c in deck
        ]
        names = [n for n in names if n]
        try:
            import sqlite3
            from engine.db import resolve_db_path
            con = sqlite3.connect(str(resolve_db_path()))
            try:
                # Chunk to avoid massive IN-lists.
                for i in range(0, len(names), 500):
                    chunk = names[i:i + 500]
                    qmarks = ",".join("?" * len(chunk))
                    rows = con.execute(
                        f"SELECT primitives_v1_json FROM cards "
                        f"WHERE snapshot_id=? AND name IN ({qmarks})",
                        tuple([db_snapshot_id] + chunk),
                    ).fetchall()
                    for (raw,) in rows:
                        if not raw:
                            continue
                        try:
                            for p in json.loads(raw):
                                if isinstance(p, str):
                                    out.add(p)
                        except json.JSONDecodeError:
                            pass
            finally:
                con.close()
        except Exception:
            pass

    return out


def _match_win_paths(
    deck: Sequence[Dict[str, Any]],
    deck_names_lower: Optional[set] = None,
    deck_primitives: Optional[set] = None,
) -> List[WinPathMatch]:
    """For each win-path in the catalog, check if the deck arms it."""
    if deck_names_lower is None:
        deck_names_lower = _name_lower_set(deck)
    if deck_primitives is None:
        deck_primitives = _primitives_set(deck)

    matches: List[WinPathMatch] = []
    for wp in WIN_PATHS:
        required_names = set(wp.get("required_card_names") or [])
        any_names = set(wp.get("any_card_names") or [])
        required_prims = set(wp.get("primitives") or [])
        any_prims = set(wp.get("any_primitives") or [])

        # All required names must be in the deck.
        names_present = required_names.issubset(deck_names_lower)
        # All required primitives.
        prims_present = required_prims.issubset(deck_primitives)
        # Either the win-path doesn't list `any_*` (so it's automatically
        # satisfied), or at least one is in the deck.
        any_names_present = (not any_names) or bool(any_names & deck_names_lower)
        any_prims_present = (not any_prims) or bool(any_prims & deck_primitives)

        armed = names_present and prims_present and any_names_present and any_prims_present

        missing: List[str] = []
        if not names_present:
            missing.extend(sorted(required_names - deck_names_lower))
        if not prims_present:
            missing.extend(sorted(required_prims - deck_primitives))
        if not any_names_present and any_names:
            missing.append(f"any of: {sorted(any_names)}")
        if not any_prims_present and any_prims:
            missing.append(f"any prim of: {sorted(any_prims)}")

        matches.append(WinPathMatch(
            win_path_id=wp["id"], name=wp["name"], category=wp["category"],
            speed_score=wp["speed_score"], armed=armed,
            missing_pieces=missing,
        ))
    return matches


def _interaction_density(deck_primitives: set) -> int:
    """Count interaction primitives — accepts both v0 (UPPERCASE) and
    v1 (kebab-case) tags for backwards compatibility."""
    interaction_prims = {
        # v1 (Pillar C ontology)
        "counterspell-hard", "counterspell-soft", "free-counter",
        "removal-creature", "removal-artifact", "removal-enchantment",
        "removal-mass-creatures", "removal-mass-board",
        "bounce",
        # v0 (legacy primitives_v0)
        "COUNTERSPELL_GENERIC", "COUNTERSPELL_CREATURE",
        "TARGETED_REMOVAL_CREATURE", "TARGETED_REMOVAL_ARTIFACT",
        "TARGETED_REMOVAL_ENCHANTMENT", "TARGETED_REMOVAL_PLANESWALKER",
        "BOARDWIPE_CREATURES",
    }
    return len(deck_primitives & interaction_prims)


def _resilience_score(deck_primitives: set, deck_names_lower: set) -> int:
    """Count of protection / recursion primitives + a few known
    high-value protection cards by name."""
    resilience_prims = {
        # v1
        "recursion-graveyard", "fizzle-prevention", "creature-protection",
        "combo-protection",
        # v0
        "RECURSION_GRAVEYARD",
    }
    score = len(deck_primitives & resilience_prims)
    # Known by-name protection picks (any of these is worth +1).
    protection_cards = {
        "lightning greaves", "swiftfoot boots", "teferi's protection",
        "heroic intervention", "veil of summer", "silence",
        "allosaurus shepherd", "boseiju, who endures",
    }
    score += sum(1 for n in protection_cards if n in deck_names_lower)
    return score


# Opposition vulnerability — quick mapping of opponent archetype →
# strategies this deck is weak against.
_VULNERABILITY_BY_DECK_TYPE: Dict[str, List[str]] = {
    "combo_low_interaction": ["fast cEDH combo", "storm chains"],
    "creature_focused": ["board wipes", "lockdown stax"],
    "graveyard_dependent": ["graveyard hate", "Rest in Peace"],
    "tribal_swarm": ["board wipes", "mass-removal control"],
    "voltron": ["unconditional removal", "indestructible blockers"],
    "control_slow": ["fast combo turns 2-4"],
}


# ============================================================
# Opposition deck loading.
# ============================================================


def load_opposition_decks(
    path: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    """Load opposition_decks_v1.json. Returns list of entries with
    `commander`, `bracket`, `archetype_hint`, `role_tag`."""
    p = path or OPPOSITION_DECKS_PATH
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        entries = data.get("entries") or []
        return entries if isinstance(entries, list) else []
    except Exception:
        return []


# ============================================================
# Matchup logic.
# ============================================================


# Bracket "expected speed" — what turn a deck of that bracket typically
# wins on. Used as a proxy when a deck has no armed win-paths in our
# catalog.
_BRACKET_EXPECTED_SPEED: Dict[str, float] = {
    "B1": 12.0,
    "B2": 10.0,
    "B3": 8.0,
    "B4": 6.0,
    "B5": 4.0,
}


def _expected_speed_for_opponent(opponent: Dict[str, Any]) -> float:
    bracket = opponent.get("bracket") or "B3"
    return _BRACKET_EXPECTED_SPEED.get(bracket, 8.0)


def _expected_interaction_for_opponent(opponent: Dict[str, Any]) -> int:
    bracket = opponent.get("bracket") or "B3"
    return {"B1": 4, "B2": 6, "B3": 8, "B4": 10, "B5": 12}.get(bracket, 8)


def _matchup_winrate(
    own_speed: float,
    own_interaction: int,
    own_resilience: int,
    opp_speed: float,
    opp_interaction: int,
) -> float:
    """Compute heads-up matchup winrate (0-1). Heuristic:
      - Speed delta: 1 turn faster ≈ +0.10 winrate.
      - Interaction delta: 1 extra interaction (vs opponent's resilience
        baseline of 2) ≈ +0.03 winrate.
      - Our resilience scales linearly into the answer-vs-disrupt
        equation: each point of resilience cancels ~1 opponent
        interaction.

    Result clamped to [0.05, 0.95].
    """
    base = 0.50
    speed_delta = (opp_speed - own_speed) * 0.10  # we're faster = positive
    interaction_advantage = (own_interaction - 2 - max(0, opp_interaction - own_resilience)) * 0.03
    score = base + speed_delta + interaction_advantage
    return max(0.05, min(0.95, score))


# ============================================================
# Main entry point.
# ============================================================


def approximate_pod_winrate(
    *,
    deck: Sequence[Dict[str, Any]],
    opponents: Optional[Sequence[Dict[str, Any]]] = None,
    db_snapshot_id: Optional[str] = None,
) -> PodWinrateReport:
    """Compute pod winrate for `deck` against `opponents` (or 3
    bracket-distributed opponents from opposition_decks_v1.json if
    None is passed).

    Iter 4 Phase 6: when `db_snapshot_id` is provided, the function
    loads primitives_v1 tags for each deck card from the cards table
    so win-path detection uses the Pillar C ontology vocabulary.
    """
    if opponents is None:
        all_opps = load_opposition_decks()
        # Default: 3 opponents — first B2, first B3, first B4 — to
        # span the bracket range.
        opponents = []
        for target_bracket in ("B2", "B3", "B4"):
            match = next((o for o in all_opps if o.get("bracket") == target_bracket), None)
            if match:
                opponents.append(match)
        # Fallback: just take the first 3.
        if not opponents:
            opponents = all_opps[:3]

    deck_names_lower = _name_lower_set(deck)
    deck_primitives = _primitives_set(deck, db_snapshot_id=db_snapshot_id)

    matches = _match_win_paths(deck, deck_names_lower, deck_primitives)
    armed = [m for m in matches if m.armed]
    own_speed = min((m.speed_score for m in armed), default=10.0)
    own_interaction = _interaction_density(deck_primitives)
    own_resilience = _resilience_score(deck_primitives, deck_names_lower)

    # Vulnerability mapping (heuristic).
    vulnerability: List[str] = []
    if own_resilience < 2:
        vulnerability.append("targeted removal of key combo pieces")
    if own_interaction < 3:
        vulnerability.append("opposing fast-combo turns 2-4")
    if not armed:
        vulnerability.append("no identified win-path — likely incomplete deck or unseen archetype")

    decomposition = DeckDecomposition(
        win_paths=matches,
        speed_score=own_speed,
        interaction_density=own_interaction,
        resilience_score=own_resilience,
        vulnerability_to=vulnerability,
    )

    per_opp: Dict[str, float] = {}
    for opp in opponents:
        name = (
            (opp.get("commander") or "?")
            + " ("
            + (opp.get("bracket") or "?")
            + ")"
        )
        opp_speed = _expected_speed_for_opponent(opp)
        opp_interaction = _expected_interaction_for_opponent(opp)
        wr = _matchup_winrate(
            own_speed=own_speed,
            own_interaction=own_interaction,
            own_resilience=own_resilience,
            opp_speed=opp_speed,
            opp_interaction=opp_interaction,
        )
        per_opp[name] = round(wr, 3)

    # Pod winrate ≈ product of head-to-head wins (we need to beat all 3),
    # but more realistically the lowest-matchup tends to dominate since
    # we only need ONE opponent to put us in a bad spot. Use the GM
    # of head-to-head winrates with a small adjustment for the 1/N
    # baseline.
    if per_opp:
        import math as _m
        gm = _m.exp(sum(_m.log(max(0.01, w)) for w in per_opp.values()) / len(per_opp))
        # 1-of-4 baseline = 0.25; our pod winrate is gm × (4 * 0.25) = gm.
        # Cap at 0.85 because no Commander deck is a sure thing.
        pod_winrate = round(min(0.85, gm), 3)
    else:
        pod_winrate = 0.25  # no opponents — assume neutral 1/N.

    return PodWinrateReport(
        pod_winrate=pod_winrate,
        per_opponent_winrate=per_opp,
        decomposition=decomposition,
    )


# ============================================================
# Iter 5 / mega-task v3 Phase 5 — new-card archetype-impact scoring.
# ============================================================
#
# `score_card_archetype_impact(new_card, archetypes=None)` returns a
# per-archetype shift score that quantifies how much the new card
# would change a representative deck's pod_winrate if substituted in.
#
# Architectural choice (v0.1): we do not run an actual substitution
# simulation against a reference deck. Instead, we compute a primitive-
# matching impact score against per-archetype "preferred primitive"
# weights. The output `delta` is a calibrated proxy for pod_winrate
# shift in the [-0.05, +0.15] range (anchored against Phase 6's
# sweep results). Future iterations may replace this with a real
# substitution sim once a "typical deck per archetype" snapshot is
# materialized.

# Per-archetype preferred-primitive weights. Tuned so that a card with
# all primitives matching a single archetype scores ~+0.10 delta, and
# a card with mixed signals across archetypes scores partial credit on
# each.
_ARCHETYPE_PREFERRED_PRIMITIVES: Dict[str, Dict[str, float]] = {
    "tribal": {
        "tribal-anchor": 1.0, "anthem-effect": 0.7, "token-producer": 0.5,
        "attack-trigger": 0.5, "haste-grant": 0.4, "cost-discount": 0.4,
    },
    "voltron": {
        "voltron-payoff": 1.0, "creature-protection": 0.8, "evasion-grant": 0.7,
        "haste-grant": 0.5, "extra-combat": 0.6, "vigilance-grant": 0.4,
    },
    "storm": {
        "storm-payoff": 1.0, "cantrip": 0.7, "free-spell": 0.7,
        "cost-discount": 0.6, "infinite-mana-source": 0.6, "x-spell-payoff": 0.5,
    },
    "aristocrats": {
        "sac-outlet": 1.0, "death-trigger": 1.0, "persist-creature": 0.8,
        "recursion-graveyard": 0.6, "token-producer": 0.4,
    },
    "counters_matter": {
        "doubler-effect": 1.0, "anthem-effect": 0.6,
    },
    "control": {
        "counterspell-hard": 1.0, "counterspell-soft": 0.7,
        "free-counter": 0.8, "removal-creature": 0.5,
        "removal-mass-creatures": 0.7, "removal-mass-board": 0.7,
        "draw-engine": 0.6, "stax-effect": 0.4,
    },
    "combo": {
        "combo-assembly": 1.0, "tutor-broad": 0.8, "tutor-narrow": 0.5,
        "combo-protection": 0.6, "free-counter": 0.5,
        "infinite-mana-source": 0.5, "infinite-untap-source": 0.5,
        "deck-out": 0.7,
    },
    "blink": {
        "etb-trigger": 1.0, "flicker-effect": 1.0,
        "creature-protection": 0.4,
    },
    "reanimator": {
        "recursion-graveyard": 1.0, "self-mill": 0.8,
        "alternative-cost": 0.5,
    },
    "landfall": {
        "landfall-trigger": 1.0, "extra-land-drop": 0.8, "land-ramp": 0.6,
    },
    "group_hug": {
        "draw-engine": 0.6, "extra-land-drop": 0.4,
    },
    "tokens": {
        "token-producer": 1.0, "anthem-effect": 0.7,
        "infinite-tokens-with-evasion": 0.6, "doubler-effect": 0.6,
        "sac-outlet": 0.3,
    },
}

# Calibration: each unit of primitive-weight match contributes this
# much to delta. Tuned so a perfect-match (sum_weight=1.0 + extra)
# scores ~+0.10 - +0.15 delta.
_IMPACT_CALIBRATION = 0.08


def score_card_archetype_impact(
    new_card: Dict[str, Any],
    archetypes: Optional[List[str]] = None,
    *,
    primitives_field: str = "primitives",
) -> Dict[str, Dict[str, Any]]:
    """Score the new card's potential impact on each archetype.

    Args:
      new_card: dict with at least `name` + a primitives field
        (`primitives` by default; pass `primitives_field="primitives_v1"`
        to use a different key).
      archetypes: optional subset of `_ARCHETYPE_PREFERRED_PRIMITIVES`
        keys. Default: all 12 archetypes.
      primitives_field: which key on `new_card` holds the v1 tag list.

    Returns:
      {archetype: {delta: float, fits_role: str, displaces: None,
                   matched_primitives: list[str]}}
      sorted by descending |delta|. Vanilla cards (no primitives)
      return zero-delta on every archetype.
    """
    target_archetypes = archetypes or list(_ARCHETYPE_PREFERRED_PRIMITIVES.keys())
    card_prims = list(new_card.get(primitives_field) or [])
    if not card_prims:
        # Empty primitives → zero impact across the board.
        return {
            a: {
                "delta": 0.0, "fits_role": "vanilla",
                "displaces": None, "matched_primitives": [],
            }
            for a in target_archetypes
        }

    out: Dict[str, Dict[str, Any]] = {}
    for arch in target_archetypes:
        weights = _ARCHETYPE_PREFERRED_PRIMITIVES.get(arch) or {}
        matched: List[str] = []
        total_weight = 0.0
        for p in card_prims:
            w = weights.get(p, 0.0)
            if w > 0:
                total_weight += w
                matched.append(p)
        delta = round(total_weight * _IMPACT_CALIBRATION, 4)
        # Cap delta at +0.15 to avoid hyper-stacking outliers.
        delta = min(delta, 0.15)
        if matched:
            top_match = max(matched, key=lambda p: weights.get(p, 0.0))
            fits_role = f"primary primitive: {top_match}"
        else:
            fits_role = "no archetype-relevant primitives"
        out[arch] = {
            "delta": delta,
            "fits_role": fits_role,
            "displaces": None,   # v0.1 stub; real sub-sim future work
            "matched_primitives": matched,
        }
    return out


def top_archetypes_for_card(
    new_card: Dict[str, Any],
    k: int = 3,
    primitives_field: str = "primitives",
) -> List[Tuple[str, Dict[str, Any]]]:
    """Convenience wrapper: return the top-k archetypes by |delta|."""
    scores = score_card_archetype_impact(
        new_card, primitives_field=primitives_field,
    )
    ranked = sorted(scores.items(), key=lambda kv: -abs(kv[1]["delta"]))
    return ranked[:k]
