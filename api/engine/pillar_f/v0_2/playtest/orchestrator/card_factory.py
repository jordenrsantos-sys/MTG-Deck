"""Card factory: build a Card instance from a card name.

Sub-C Phase 3. Handles iter-11 card categories the orchestrator's
Stage 2 decks need:

- **Basic lands** (Plains/Island/Swamp/Mountain/Forest): is_land()=True,
  no iter10_annotation needed (lands aren't cast — sub-B's eligible_actions
  emits play_land actions for them directly).
- **Counterspells** (Counterspell, Negate, Force of Will, ...): use
  sub-C Phase 2's `make_counterspell_annotation` to attach the
  target_stack_top annotation.
- **Simple damage instants** (Lightning Bolt, Shock, ...): set
  iter10_annotation with payment.resolver="deal_damage_to_player" +
  default_targets=[next_opponent_pid_placeholder].
- **Creatures** (Goblin Guide, Llanowar Elves, ...): minimal Card with
  type_line="Creature", power/toughness from a lookup table; no
  iter10_annotation (creatures aren't cast through sub-B's pipeline
  in iter-11, but they CAN attack via combat hook glue).

Iter-12+ will replace this with a per-card data registry.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from api.engine.pillar_f.v0_2.state import Card
from api.engine.pillar_f.v0_2.playtest.counter_war import (
    COUNTERSPELL_FAMILY_NAMES, make_counterspell_annotation,
)


CARD_FACTORY_VERSION = "pillar_f_v0_2_playtest_card_factory_v1"


# Basic-land name -> (type_line, color produced) lookup.
_BASIC_LANDS: Dict[str, str] = {
    "Plains": "Basic Land — Plains",
    "Island": "Basic Land — Island",
    "Swamp": "Basic Land — Swamp",
    "Mountain": "Basic Land — Mountain",
    "Forest": "Basic Land — Forest",
}

# Simple damage instants. Maps name -> (amount, cmc, mana_cost).
_DAMAGE_INSTANTS: Dict[str, Dict[str, Any]] = {
    "Lightning Bolt": {"amount": 3, "cmc": 1, "mana_cost": "{R}"},
    "Shock": {"amount": 2, "cmc": 1, "mana_cost": "{R}"},
    "Lava Spike": {"amount": 3, "cmc": 1, "mana_cost": "{R}"},
    "Searing Blaze": {"amount": 3, "cmc": 2, "mana_cost": "{R}{R}"},
    "Skewer the Critics": {"amount": 3, "cmc": 1, "mana_cost": "{R}"},
}

# Simple creature stat lookup. Iter-11 cards/ has richer entries; this
# table is the Stage 2 control-pool subset that decks here use.
# Maps name -> (power, toughness, keywords, cmc, mana_cost, type_line).
_CREATURE_STATS: Dict[str, Dict[str, Any]] = {
    # Mono-R goblin tribal subset:
    "Goblin Guide": {"power": "2", "toughness": "2",
                     "keywords": ["haste"], "cmc": 1, "mana_cost": "{R}"},
    "Goblin Piledriver": {"power": "1", "toughness": "2",
                          "keywords": [], "cmc": 2, "mana_cost": "{R}{R}"},
    "Krenko, Mob Boss": {"power": "3", "toughness": "3",
                          "keywords": [], "cmc": 4, "mana_cost": "{2}{R}{R}",
                          "type_line": "Legendary Creature — Goblin Warrior"},
    "Skirk Prospector": {"power": "1", "toughness": "1",
                          "keywords": [], "cmc": 1, "mana_cost": "{R}"},
    "Battle Cry Goblin": {"power": "2", "toughness": "2",
                          "keywords": ["haste"], "cmc": 2,
                          "mana_cost": "{1}{R}"},
    # Mono-W soldiers subset:
    "Soldier of the Pantheon": {"power": "2", "toughness": "1",
                                "keywords": [], "cmc": 1,
                                "mana_cost": "{W}"},
    "Thalia, Guardian of Thraben": {"power": "2", "toughness": "1",
                                     "keywords": ["first strike"], "cmc": 2,
                                     "mana_cost": "{1}{W}",
                                     "type_line": "Legendary Creature — Human Soldier"},
    "Heliod, Sun-Crowned": {"power": "5", "toughness": "5",
                             "keywords": ["lifelink", "indestructible"],
                             "cmc": 3, "mana_cost": "{1}{W}{W}",
                             "type_line": "Legendary Enchantment Creature — God"},
    # Mono-U tempo subset:
    "Delver of Secrets": {"power": "1", "toughness": "1",
                          "keywords": [], "cmc": 1, "mana_cost": "{U}"},
    "Snapcaster Mage": {"power": "2", "toughness": "1",
                        "keywords": ["flash"], "cmc": 2,
                        "mana_cost": "{1}{U}"},
    # Mono-B reanimator subset:
    "Putrid Imp": {"power": "1", "toughness": "1",
                   "keywords": ["flying"], "cmc": 1, "mana_cost": "{B}"},
    "Reassembling Skeleton": {"power": "1", "toughness": "1",
                              "keywords": [], "cmc": 2,
                              "mana_cost": "{1}{B}"},
    # Vampire tribal subset (for Edgar Markov B3 calibration):
    "Edgar Markov": {"power": "4", "toughness": "4",
                     "keywords": ["first strike", "haste"],
                     "cmc": 6, "mana_cost": "{3}{R}{W}{B}",
                     "type_line": "Legendary Creature — Vampire Knight"},
    "Vampire Nighthawk": {"power": "2", "toughness": "3",
                          "keywords": ["flying", "deathtouch", "lifelink"],
                          "cmc": 3, "mana_cost": "{1}{B}{B}"},
    # Dragon tribal subset (for Ur-Dragon B4 calibration):
    "The Ur-Dragon": {"power": "10", "toughness": "10",
                       "keywords": ["flying"],
                       "cmc": 9, "mana_cost": "{4}{W}{U}{B}{R}{G}",
                       "type_line": "Legendary Creature — Dragon Avatar"},
    "Dragonlord Atarka": {"power": "8", "toughness": "8",
                           "keywords": ["flying", "trample"],
                           "cmc": 7, "mana_cost": "{5}{R}{G}",
                           "type_line": "Legendary Creature — Elder Dragon"},
}


def is_basic_land(name: str) -> bool:
    return name in _BASIC_LANDS


def is_counterspell(name: str) -> bool:
    return name in COUNTERSPELL_FAMILY_NAMES


def is_damage_instant(name: str) -> bool:
    return name in _DAMAGE_INSTANTS


def is_known_creature(name: str) -> bool:
    return name in _CREATURE_STATS


def make_card_from_name(
    name: str, owner: int,
    *,
    next_opponent_pid: Optional[int] = None,
) -> Card:
    """Build a Card instance from a card name.

    Args:
        name: card name (case-sensitive; must match _BASIC_LANDS /
            COUNTERSPELL_FAMILY_NAMES / _DAMAGE_INSTANTS /
            _CREATURE_STATS).
        owner: player_id who owns this card (drives controller too).
        next_opponent_pid: for damage instants only — sets the
            default target so the LLM's first option is to hit the
            next-clockwise opponent. None defaults to (owner + 1) % 4.
    """
    if is_basic_land(name):
        c = Card(name=name, owner=owner, controller=owner,
                 type_line=_BASIC_LANDS[name])
        return c

    if is_counterspell(name):
        c = Card(name=name, owner=owner, controller=owner,
                 type_line="Instant",
                 mana_cost="{U}{U}" if name == "Counterspell" else "{1}{U}",
                 cmc=2)
        c.iter10_annotation = make_counterspell_annotation(name)
        return c

    if is_damage_instant(name):
        info = _DAMAGE_INSTANTS[name]
        c = Card(name=name, owner=owner, controller=owner,
                 type_line="Instant",
                 mana_cost=info["mana_cost"],
                 cmc=info["cmc"])
        target = next_opponent_pid if next_opponent_pid is not None \
            else (owner + 1) % 4
        c.iter10_annotation = {
            "description": f"deals {info['amount']} damage to P{target}",
            "payment": {
                "resolver": "deal_damage_to_player",
                "amount": info["amount"],
            },
            "default_targets": [target],
        }
        return c

    if is_known_creature(name):
        info = _CREATURE_STATS[name]
        c = Card(name=name, owner=owner, controller=owner,
                 type_line=info.get("type_line", "Creature"),
                 power=info["power"], toughness=info["toughness"],
                 keywords=list(info["keywords"]),
                 cmc=info.get("cmc", 0),
                 mana_cost=info.get("mana_cost", ""))
        return c

    # Unknown card: create a vanilla placeholder so deck construction
    # doesn't blow up. Marked with a note for diagnostics.
    c = Card(name=name, owner=owner, controller=owner,
             type_line="(unknown placeholder)",
             oracle_text=f"[iter-11 stub: {name!r} not in known card factory]")
    return c
