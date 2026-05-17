"""
mpa_game_state — Phase 5b.1a.

Game state dataclass for the MTG Playing Agent. Models the seven zones,
permanents, lifetotals, hand, library, graveyard, exile, stack, command zone,
plus turn/phase/priority bookkeeping.

This is the minimal viable model — captures enough state for the MPA's
heuristic decision policy to make legal plays. Does NOT model:
  - Continuous effects layer system (CR 613)
  - Replacement effects layer (CR 614)
  - State-based action loops (CR 704) beyond basic creature-toughness-0
  - Multiplayer politics state
These extensions land in 5b.4+ as the calibration suite reveals what's needed.

Architectural rules served (DESIGN_DECISIONS.md):
  - 1.4 Pilot competence: this is the substrate the playing agent reasons over.
    Must be expressive enough that legal_actions() can correctly enumerate.
  - 1.4 Anti-bias: state is symmetric — Seat-A vs Seat-B is just an index, no
    structural asymmetry beyond turn order.

Pure dataclass — no DB, no network. Serializable to JSON via dataclass.asdict.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


# ============================================================
# Enums
# ============================================================


class Zone(str, Enum):
    LIBRARY = "library"
    HAND = "hand"
    BATTLEFIELD = "battlefield"
    GRAVEYARD = "graveyard"
    EXILE = "exile"
    STACK = "stack"
    COMMAND = "command"


class Phase(str, Enum):
    BEGINNING = "beginning"        # untap, upkeep, draw
    PRECOMBAT_MAIN = "precombat_main"
    COMBAT = "combat"              # beginning, declare attackers, declare blockers, damage, end
    POSTCOMBAT_MAIN = "postcombat_main"
    ENDING = "ending"              # end, cleanup


class Step(str, Enum):
    UNTAP = "untap"
    UPKEEP = "upkeep"
    DRAW = "draw"
    PRECOMBAT_MAIN = "precombat_main"
    BEGINNING_OF_COMBAT = "beginning_of_combat"
    DECLARE_ATTACKERS = "declare_attackers"
    DECLARE_BLOCKERS = "declare_blockers"
    COMBAT_DAMAGE = "combat_damage"
    END_OF_COMBAT = "end_of_combat"
    POSTCOMBAT_MAIN = "postcombat_main"
    END = "end"
    CLEANUP = "cleanup"


# ============================================================
# Permanent + Card
# ============================================================


@dataclass
class CardInGame:
    """A specific card instance in some zone. Identity = oracle_id + instance index."""
    oracle_id: str
    name: str
    type_line: str = ""
    cmc: int = 0
    mana_cost: str = ""
    color_identity: Tuple[str, ...] = ()
    power: Optional[int] = None
    toughness: Optional[int] = None
    primitives: Tuple[str, ...] = ()
    instance_id: int = 0  # unique within a game

    # Battlefield state
    tapped: bool = False
    summoning_sick: bool = False
    plus1_counters: int = 0
    minus1_counters: int = 0
    loyalty_counters: int = 0
    attached_to_instance: Optional[int] = None

    def is_land(self) -> bool:
        return "Land" in self.type_line.split("—")[0]

    def is_creature(self) -> bool:
        return "Creature" in self.type_line.split("—")[0]

    def is_instant_or_sorcery(self) -> bool:
        head = self.type_line.split("—")[0]
        return "Instant" in head or "Sorcery" in head

    def is_artifact(self) -> bool:
        return "Artifact" in self.type_line.split("—")[0]

    def is_legendary(self) -> bool:
        return "Legendary" in self.type_line.split("—")[0]


@dataclass
class PlayerState:
    """Per-player state. Seat-symmetric — no implicit "you" vs "opponent"."""
    seat_index: int
    life: int = 40  # Commander default
    poison: int = 0
    library: List[CardInGame] = field(default_factory=list)
    hand: List[CardInGame] = field(default_factory=list)
    battlefield: List[CardInGame] = field(default_factory=list)
    graveyard: List[CardInGame] = field(default_factory=list)
    exile: List[CardInGame] = field(default_factory=list)
    command_zone: List[CardInGame] = field(default_factory=list)
    commander_tax_paid: int = 0  # additional {2} per cast from command
    mulligans_taken: int = 0

    def lost(self) -> bool:
        """CR 104.3 — player loses if life <= 0, draws from empty, or 10+ poison."""
        if self.life <= 0:
            return True
        if self.poison >= 10:
            return True
        return False

    def lands_played_this_turn(self) -> int:
        # For tracking land-per-turn limit; reset on phase transitions in runner
        return 0  # placeholder — runner tracks this externally


@dataclass
class GameState:
    """Complete game state. Symmetric in players[] — seat 0 vs seat 1 is just index."""
    players: List[PlayerState]
    active_player_index: int = 0
    priority_player_index: int = 0
    phase: Phase = Phase.BEGINNING
    step: Step = Step.UNTAP
    turn_number: int = 1
    stack: List[Dict[str, Any]] = field(default_factory=list)  # objects on the stack
    lands_played_this_turn_by_seat: Dict[int, int] = field(default_factory=dict)
    mana_pool_by_seat: Dict[int, Dict[str, int]] = field(default_factory=dict)
    rng_seed: int = 0
    rng: Optional[random.Random] = None
    history: List[Dict[str, Any]] = field(default_factory=list)
    game_over: bool = False
    winner_seat: Optional[int] = None
    loss_reason: Optional[str] = None

    def opponent_seats(self, of_seat: int) -> List[int]:
        return [i for i in range(len(self.players)) if i != of_seat]

    def active(self) -> PlayerState:
        return self.players[self.active_player_index]

    def priority(self) -> PlayerState:
        return self.players[self.priority_player_index]

    def __post_init__(self):
        if self.rng is None:
            self.rng = random.Random(self.rng_seed)
        for p in self.players:
            self.lands_played_this_turn_by_seat.setdefault(p.seat_index, 0)
            self.mana_pool_by_seat.setdefault(p.seat_index, {})

    def shuffle_library(self, seat_index: int) -> None:
        """Shuffle a player's library using game RNG (deterministic for given seed)."""
        if self.rng is None:
            self.rng = random.Random(self.rng_seed)
        self.rng.shuffle(self.players[seat_index].library)

    def draw(self, seat_index: int, n: int = 1) -> List[CardInGame]:
        """Draw n cards. CR 121.1 — drawing from empty library = lose state-based check."""
        drawn: List[CardInGame] = []
        p = self.players[seat_index]
        for _ in range(n):
            if not p.library:
                # State-based loss — runner will detect
                p.life = -100  # sentinel "decked"
                if not self.loss_reason:
                    self.loss_reason = "deck_out"
                return drawn
            card = p.library.pop()
            p.hand.append(card)
            drawn.append(card)
        return drawn

    def to_summary(self) -> Dict[str, Any]:
        """Compact summary for logs / decision-quality tracking."""
        return {
            "turn": self.turn_number,
            "phase": self.phase.value,
            "step": self.step.value,
            "active": self.active_player_index,
            "priority": self.priority_player_index,
            "lives": [p.life for p in self.players],
            "hand_sizes": [len(p.hand) for p in self.players],
            "library_sizes": [len(p.library) for p in self.players],
            "battlefield_counts": [len(p.battlefield) for p in self.players],
            "graveyard_sizes": [len(p.graveyard) for p in self.players],
            "stack_size": len(self.stack),
            "game_over": self.game_over,
            "winner": self.winner_seat,
            "loss_reason": self.loss_reason,
        }


# ============================================================
# Game construction
# ============================================================


def new_commander_game(
    deck_a_oracle_ids: List[Tuple[str, str, str]],  # [(oracle_id, name, type_line), ...]
    deck_b_oracle_ids: List[Tuple[str, str, str]],
    commander_a: Tuple[str, str, str],
    commander_b: Tuple[str, str, str],
    starting_life: int = 40,
    seed: int = 0,
) -> GameState:
    """Construct a fresh 1v1 Commander game state.

    Decks are provided as lists of (oracle_id, name, type_line) tuples — caller
    is responsible for hydrating from the cards table. Commanders go to the
    command zone. Libraries shuffled with the game RNG.

    Each player draws 7. Mulligan decision is handed off to the decision policy
    via the runner (not here).
    """
    rng = random.Random(seed)
    p_a = PlayerState(seat_index=0, life=starting_life)
    p_b = PlayerState(seat_index=1, life=starting_life)

    instance_counter = [0]
    def _mk(oid: str, name: str, type_line: str) -> CardInGame:
        instance_counter[0] += 1
        return CardInGame(
            oracle_id=oid, name=name, type_line=type_line,
            instance_id=instance_counter[0],
        )

    for (oid, name, tl) in deck_a_oracle_ids:
        p_a.library.append(_mk(oid, name, tl))
    for (oid, name, tl) in deck_b_oracle_ids:
        p_b.library.append(_mk(oid, name, tl))

    p_a.command_zone.append(_mk(commander_a[0], commander_a[1], commander_a[2]))
    p_b.command_zone.append(_mk(commander_b[0], commander_b[1], commander_b[2]))

    state = GameState(players=[p_a, p_b], rng_seed=seed, rng=rng)
    state.shuffle_library(0)
    state.shuffle_library(1)
    state.draw(0, 7)
    state.draw(1, 7)
    return state
