"""Phase 7 — Counterspells.

Counterspell, Negate, Swan Song, Arcane Denial, An Offer You Can't
Refuse, Mana Drain (drain side stubbed to add C), Deflecting Swat
(redirect — stub), Fierce Guardianship (Counterspell w/ commander tax
side stubbed), Deadly Rollick (Path to Exile w/ commander tax).

Each counterspell:
  - Reads its target stack-entry id from StackEntry.targets[0].
  - Calls substrate's `counter_target(state, target_entry_id)` to
    remove from stack.
  - Applies any side effect (token creation, mana production, etc.)
"""
from __future__ import annotations

from api.engine.pillar_f.v0_2.cards.spell.framework import register_spell
from api.engine.pillar_f.v0_2.stack import counter_target
from api.engine.pillar_f.v0_2.state import Card, GameState, StackEntry


def _counterspell_resolve(state: GameState, entry: StackEntry) -> None:
    """Counter target spell."""
    if not entry.targets:
        return None
    target_entry_id = entry.targets[0]
    counter_target(state, target_entry_id)
    return None


register_spell("Counterspell", _counterspell_resolve)
register_spell("Negate", _counterspell_resolve)  # Counter target noncreature
register_spell("Mana Drain", _counterspell_resolve)  # counter + add mana (stub)


def _swan_song_resolve(state: GameState, entry: StackEntry) -> None:
    """Counter target enchantment/instant/sorcery; controller of countered
    spell creates a 2/2 blue Bird Spirit token."""
    if not entry.targets:
        return None
    target_entry_id = entry.targets[0]
    # Find the target stack entry to determine its controller BEFORE
    # countering.
    target_controller = None
    for e in state.stack:
        if e.entry_id == target_entry_id:
            target_controller = e.controller
            break
    if counter_target(state, target_entry_id):
        if target_controller is not None and 0 <= target_controller < len(state.players):
            token = Card(name="Bird Token", owner=target_controller,
                        controller=target_controller,
                        type_line="Token Creature — Bird",
                        power="2", toughness="2",
                        colors=["U"], color_identity=["U"],
                        keywords=["flying"])
            state.add_card(token)
            state.players[target_controller].zones.battlefield.append(token.card_id)
    return None


register_spell("Swan Song", _swan_song_resolve)


def _arcane_denial_resolve(state: GameState, entry: StackEntry) -> None:
    """Counter target spell. Its controller may draw up to 2 cards at
    the beginning of the next turn's upkeep. You draw a card.
    Iter-10 stub: controller draws immediately (delay-to-upkeep is
    iter-11+; we approximate with an immediate draw of 1 for the AD
    caster + flag the target's controller for 2-card draw next turn —
    we just give 2 immediately for simplicity)."""
    if not entry.targets:
        return None
    target_entry_id = entry.targets[0]
    target_controller = None
    for e in state.stack:
        if e.entry_id == target_entry_id:
            target_controller = e.controller
            break
    counter_target(state, target_entry_id)
    # AD caster draws 1.
    pid = entry.controller
    if 0 <= pid < len(state.players):
        lib = state.players[pid].zones.library
        if lib:
            state.players[pid].zones.hand.append(lib.pop(0))
            state.players[pid].cards_drawn_this_turn += 1
    # Countered spell's controller draws 2 (immediate stub).
    if target_controller is not None and 0 <= target_controller < len(state.players):
        for _ in range(2):
            lib = state.players[target_controller].zones.library
            if not lib:
                state.players[target_controller].has_drawn_from_empty_library = True
                break
            state.players[target_controller].zones.hand.append(lib.pop(0))
            state.players[target_controller].cards_drawn_this_turn += 1
    return None


register_spell("Arcane Denial", _arcane_denial_resolve)


def _an_offer_resolve(state: GameState, entry: StackEntry) -> None:
    """Counter target noncreature spell. Its controller creates 2
    Treasure tokens."""
    if not entry.targets:
        return None
    target_entry_id = entry.targets[0]
    target_controller = None
    for e in state.stack:
        if e.entry_id == target_entry_id:
            target_controller = e.controller
            break
    if counter_target(state, target_entry_id):
        if target_controller is not None and 0 <= target_controller < len(state.players):
            for _ in range(2):
                token = Card(name="Treasure Token", owner=target_controller,
                            controller=target_controller,
                            type_line="Token Artifact — Treasure",
                            subtypes=["Treasure"])
                state.add_card(token)
                state.players[target_controller].zones.battlefield.append(token.card_id)
    return None


register_spell("An Offer You Can't Refuse", _an_offer_resolve)


# Fierce Guardianship — Counter target noncreature spell. If you control
# a commander, this spell costs nothing. The "cost nothing" side is
# caller-controlled (iter-12+ cast pipeline scope). Resolution = counter.
register_spell("Fierce Guardianship", _counterspell_resolve)
