"""Game-state object model.

GameState aggregates:
  - per-player PlayerState (life, zones, mana pool, per-turn counters)
  - global state (turn_number, phase, step_within_phase, active_player,
    priority_holder, stack, monarch, initiative, day_or_night)
  - effect registries (replacement_effects_active, continuous_effects_active,
    delayed_triggers_pending)
  - cards_by_id: master dict {card_id → Card}; zones reference card_ids

JSON round-trip preserves all state with version field for forward-compat.
`perspective_view(player_id)` returns a same-shape state with opposing
zones redacted (hand contents → counts only, library → count, face-down
cards → opaque markers).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set
import json

from api.engine.pillar_f.v0_2.state.card import Card
from api.engine.pillar_f.v0_2.state.player import PlayerState


STATE_VERSION = "pillar_f_v0_2_state_v1"


class Phase(str, Enum):
    """Turn phases per CR 5.1. Sub-steps live in Step."""
    BEGINNING = "beginning"
    PRECOMBAT_MAIN = "precombat_main"
    COMBAT = "combat"
    POSTCOMBAT_MAIN = "postcombat_main"
    ENDING = "ending"


class Step(str, Enum):
    """Steps within phases. See CR 5.1 for the canonical list."""
    UNTAP = "untap"
    UPKEEP = "upkeep"
    DRAW = "draw"
    MAIN_1 = "main_1"
    BEGINNING_OF_COMBAT = "beginning_of_combat"
    DECLARE_ATTACKERS = "declare_attackers"
    DECLARE_BLOCKERS = "declare_blockers"
    FIRST_STRIKE_DAMAGE = "first_strike_damage"
    COMBAT_DAMAGE = "combat_damage"
    END_OF_COMBAT = "end_of_combat"
    MAIN_2 = "main_2"
    END_STEP = "end_step"
    CLEANUP = "cleanup"


class DayNight(str, Enum):
    DAY = "day"
    NIGHT = "night"
    NEITHER = "neither"


@dataclass
class StackEntry:
    """A spell or activated/triggered ability on the stack."""
    entry_id: str                       # unique stack-entry id
    card_id: Optional[str]              # source card_id (None for emblems)
    controller: int                     # player_id who controls the entry
    entry_type: str                     # "spell" | "activated" | "triggered"
    targets: List[Any] = field(default_factory=list)  # card_ids or player_ids
    payment: Dict[str, Any] = field(default_factory=dict)  # mana paid + alternative-cost flags
    description: str = ""               # human-readable for debug + reports

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "card_id": self.card_id,
            "controller": self.controller,
            "entry_type": self.entry_type,
            "targets": list(self.targets),
            "payment": dict(self.payment),
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "StackEntry":
        return cls(
            entry_id=d.get("entry_id", ""),
            card_id=d.get("card_id"),
            controller=int(d.get("controller", 0)),
            entry_type=d.get("entry_type", "spell"),
            targets=list(d.get("targets") or []),
            payment=dict(d.get("payment") or {}),
            description=d.get("description", ""),
        )


@dataclass
class ReplacementEffect:
    """A registered replacement effect. event_pattern keys off event type;
    replacement_fn (referenced by name; resolved by registry) returns the
    replacement event or None to leave unchanged. Controller resolves
    ordering when multiple replacements apply to the same event."""
    effect_id: str
    source_card_id: Optional[str]
    controller: int
    event_pattern: Dict[str, Any]       # e.g. {"type": "DrawEvent", "player_id": 0}
    replacement_fn_name: str             # name string; engine resolves to function
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "effect_id": self.effect_id, "source_card_id": self.source_card_id,
            "controller": self.controller,
            "event_pattern": dict(self.event_pattern),
            "replacement_fn_name": self.replacement_fn_name,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ReplacementEffect":
        return cls(
            effect_id=d.get("effect_id", ""),
            source_card_id=d.get("source_card_id"),
            controller=int(d.get("controller", 0)),
            event_pattern=dict(d.get("event_pattern") or {}),
            replacement_fn_name=d.get("replacement_fn_name", ""),
            description=d.get("description", ""),
        )


@dataclass
class ContinuousEffect:
    """A static/continuous effect registered in the layer system (Phase 5).
    layer is 1-7; sublayer is "a"/"b"/"c"/"d" for layer 7. effect_fn_name
    keys into the application registry."""
    effect_id: str
    source_card_id: Optional[str]
    controller: int
    layer: int
    sublayer: Optional[str] = None     # "a"/"b"/"c"/"d" for layer 7 only
    effect_fn_name: str = ""
    target_pattern: Dict[str, Any] = field(default_factory=dict)
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "effect_id": self.effect_id, "source_card_id": self.source_card_id,
            "controller": self.controller, "layer": self.layer,
            "sublayer": self.sublayer, "effect_fn_name": self.effect_fn_name,
            "target_pattern": dict(self.target_pattern),
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ContinuousEffect":
        return cls(
            effect_id=d.get("effect_id", ""),
            source_card_id=d.get("source_card_id"),
            controller=int(d.get("controller", 0)),
            layer=int(d.get("layer", 6)),
            sublayer=d.get("sublayer"),
            effect_fn_name=d.get("effect_fn_name", ""),
            target_pattern=dict(d.get("target_pattern") or {}),
            description=d.get("description", ""),
        )


@dataclass
class GameState:
    """Aggregate game state. Mutable — engine code mutates this in place."""
    version: str = STATE_VERSION

    # Players. Indexed by player_id (0-based, clockwise from starting player).
    players: List[PlayerState] = field(default_factory=list)

    # Master card registry. card_id → Card. Zone members reference card_ids.
    cards_by_id: Dict[str, Card] = field(default_factory=dict)

    # Global turn state.
    turn_number: int = 1
    phase: Phase = Phase.BEGINNING
    step: Step = Step.UNTAP
    active_player: int = 0                      # whose turn it is
    priority_holder: Optional[int] = None       # who currently has priority (None between)
    # Track who's passed in succession this priority round (cleared on stack mutation).
    priority_passes_this_round: Set[int] = field(default_factory=set)

    # Stack — LIFO order, last in = top of stack.
    stack: List[StackEntry] = field(default_factory=list)

    # Optional designations.
    the_monarch: Optional[int] = None
    the_initiative: Optional[int] = None
    day_or_night: DayNight = DayNight.NEITHER

    # Effect registries.
    replacement_effects: List[ReplacementEffect] = field(default_factory=list)
    continuous_effects: List[ContinuousEffect] = field(default_factory=list)
    delayed_triggers_pending: List[Dict[str, Any]] = field(default_factory=list)
    # v14 Phase 2: counter for auto-generated UEOT effect ids.
    _ueot_effect_counter: int = 0

    # Commander identity per player. commander_card_ids[player_id] = card_id of
    # that player's commander (in command zone at game start).
    commander_card_ids: Dict[int, str] = field(default_factory=dict)

    # Game-result fields.
    winner_player_id: Optional[int] = None
    game_over: bool = False

    # ------- Card / zone API -------

    def get_card(self, card_id: str) -> Optional[Card]:
        return self.cards_by_id.get(card_id)

    def add_card(self, card: Card) -> None:
        self.cards_by_id[card.card_id] = card

    def move_card(self, card_id: str, *, from_player: int,
                  from_zone: str, to_player: int, to_zone: str,
                  to_top: bool = False) -> None:
        """Move a card from one zone to another. Both players + zones
        identified explicitly. Updates Card.controller to to_player when
        moving to battlefield (CR 110.2 — controller of permanent is its
        controller). Other zones inherit owner-controller default."""
        from_p = self.players[from_player]
        to_p = self.players[to_player]
        # Remove from source.
        removed = from_p.zones.remove_card(card_id)
        if removed is None:
            raise ValueError(
                f"card {card_id!r} not in player {from_player} zone {from_zone!r}"
            )
        # Add to destination.
        to_p.zones.add_card(card_id, to_zone, to_top=to_top)
        card = self.get_card(card_id)
        if card and to_zone == "battlefield":
            card.controller = to_player
            # ETB tapped/untapped: callers handle via replacement effects.

    # ------- v14 Phase 2: until-end-of-turn helper -------

    def register_until_end_of_turn_effect(
        self,
        *,
        source_card_id: Optional[str],
        controller: int,
        layer: int,
        sublayer: Optional[str] = None,
        effect_fn_name: str = "",
        target_pattern: Optional[Dict[str, Any]] = None,
        description: str = "",
    ) -> ContinuousEffect:
        """Register a continuous effect that automatically expires
        at the cleanup step of the current turn.

        Sub-mega-task v14 Phase 2: wraps the existing
        `continuous_effects` registry with the
        `target_pattern["until_end_of_turn"] = True` flag the
        substrate's `cleanup_step` already removes. Also stamps the
        turn number into `applies_during_turn_number` so callers can
        audit which turn registered the effect.

        Returns the registered ContinuousEffect (for caller diagnostics).

        Iter-11+ cards emitting until-end-of-turn buffs (Giant Growth,
        Berserk, Threaten, etc.) should call this helper at resolve
        time instead of directly mutating Card.power / Card.toughness
        / Card.controller -- the layer engine then applies the buff
        each layer pass, and cleanup_step removes it automatically.
        """
        self._ueot_effect_counter += 1
        tp: Dict[str, Any] = dict(target_pattern or {})
        tp["until_end_of_turn"] = True
        tp.setdefault("applies_during_turn_number", self.turn_number)
        effect = ContinuousEffect(
            effect_id=(
                f"ueot_{self.turn_number}_{self._ueot_effect_counter}"
            ),
            source_card_id=source_card_id,
            controller=controller,
            layer=layer,
            sublayer=sublayer,
            effect_fn_name=effect_fn_name,
            target_pattern=tp,
            description=description,
        )
        self.continuous_effects.append(effect)
        return effect

    # ------- Serialization -------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "players": [p.to_dict() for p in self.players],
            "cards_by_id": {cid: c.to_dict() for cid, c in self.cards_by_id.items()},
            "turn_number": self.turn_number,
            "phase": self.phase.value,
            "step": self.step.value,
            "active_player": self.active_player,
            "priority_holder": self.priority_holder,
            "priority_passes_this_round": sorted(list(self.priority_passes_this_round)),
            "stack": [e.to_dict() for e in self.stack],
            "the_monarch": self.the_monarch,
            "the_initiative": self.the_initiative,
            "day_or_night": self.day_or_night.value,
            "replacement_effects": [r.to_dict() for r in self.replacement_effects],
            "continuous_effects": [c.to_dict() for c in self.continuous_effects],
            "delayed_triggers_pending": [dict(t) for t in self.delayed_triggers_pending],
            "commander_card_ids": dict(self.commander_card_ids),
            "winner_player_id": self.winner_player_id,
            "game_over": self.game_over,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "GameState":
        gs = cls(
            version=d.get("version", STATE_VERSION),
            players=[PlayerState.from_dict(p) for p in (d.get("players") or [])],
            cards_by_id={cid: Card.from_dict(c) for cid, c in (d.get("cards_by_id") or {}).items()},
            turn_number=int(d.get("turn_number", 1)),
            phase=Phase(d.get("phase", Phase.BEGINNING.value)),
            step=Step(d.get("step", Step.UNTAP.value)),
            active_player=int(d.get("active_player", 0)),
            priority_holder=d.get("priority_holder"),
            priority_passes_this_round=set(d.get("priority_passes_this_round") or []),
            stack=[StackEntry.from_dict(e) for e in (d.get("stack") or [])],
            the_monarch=d.get("the_monarch"),
            the_initiative=d.get("the_initiative"),
            day_or_night=DayNight(d.get("day_or_night", DayNight.NEITHER.value)),
            replacement_effects=[ReplacementEffect.from_dict(r) for r in (d.get("replacement_effects") or [])],
            continuous_effects=[ContinuousEffect.from_dict(c) for c in (d.get("continuous_effects") or [])],
            delayed_triggers_pending=[dict(t) for t in (d.get("delayed_triggers_pending") or [])],
            commander_card_ids={int(k): v for k, v in (d.get("commander_card_ids") or {}).items()},
            winner_player_id=d.get("winner_player_id"),
            game_over=bool(d.get("game_over", False)),
        )
        return gs

    @classmethod
    def from_json(cls, s: str) -> "GameState":
        return cls.from_dict(json.loads(s))

    # ------- Perspective view (hidden-information redaction) -------

    def perspective_view(self, viewer_player_id: int) -> Dict[str, Any]:
        """Returns the same-shape state with hidden zones redacted from
        the viewer's perspective per scoping doc section (a):

        - Other players' hands: contents become counts only (opaque marker
          per card_id, no names/oracle data).
        - All libraries (including viewer's own): contents become counts only,
          opaque markers per card. Engine knows the order; players don't.
        - Face-down exile or battlefield cards: opaque markers.
        - Stack entries: visible to all (public information per CR).
        - Battlefield, graveyard, exile (face-up), command, and game-state
          fields are fully visible.

        Returns a dict mirror of to_dict() with redactions applied. The
        viewer's OWN hand IS visible (top-down view from their seat).
        """
        view = self.to_dict()

        # Redact each player's hand (except viewer's) + every library.
        opaque_cards: Dict[str, Dict[str, Any]] = {}
        for i, player_d in enumerate(view["players"]):
            zones = player_d.get("zones") or {}
            # Library always opaque even to owner.
            for cid in zones.get("library") or []:
                opaque_cards[cid] = {"card_id": cid, "opaque": True,
                                     "zone": "library", "owner": i}
            if i != viewer_player_id:
                # Opponent's hand opaque.
                for cid in zones.get("hand") or []:
                    opaque_cards[cid] = {"card_id": cid, "opaque": True,
                                         "zone": "hand", "owner": i}

        # Face-down battlefield + exile cards.
        for cid, card_d in view.get("cards_by_id", {}).items():
            if card_d.get("face_down"):
                # Visible to controller, hidden to others.
                if card_d.get("controller") != viewer_player_id:
                    opaque_cards[cid] = {"card_id": cid, "opaque": True,
                                         "zone": "face_down",
                                         "controller": card_d.get("controller")}

        # Apply redactions: replace cards_by_id entries with opaque markers
        # for the hidden ones.
        new_cards_by_id: Dict[str, Any] = {}
        for cid, card_d in view.get("cards_by_id", {}).items():
            if cid in opaque_cards:
                new_cards_by_id[cid] = opaque_cards[cid]
            else:
                new_cards_by_id[cid] = card_d
        view["cards_by_id"] = new_cards_by_id

        # Add a "viewer_player_id" field so consumers know whose view it is.
        view["viewer_player_id"] = viewer_player_id
        return view
