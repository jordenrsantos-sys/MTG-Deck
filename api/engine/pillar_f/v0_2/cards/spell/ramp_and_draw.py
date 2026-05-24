"""Phase 7 — Ramp spells + tutors + card-draw spells.

Ramp:
  - Cultivate / Kodama's Reach (2 basics: 1 hand, 1 onto battlefield tapped)
  - Nature's Lore / Three Visits (Forest onto battlefield, untapped)
  - Rampant Growth (basic onto battlefield tapped)
  - Dark Ritual (add {B}{B}{B})

Tutors:
  - Demonic Tutor (any card to hand)
  - Vampiric Tutor (any card to top of library)
  - Enlightened Tutor (artifact or enchantment to top)

Card draw:
  - Brainstorm (draw 3, put 2 back on top)
  - Faithless Looting (draw 2, discard 2; flashback is iter-12+ scope)

Reanimation:
  - Reanimate (target creature card from any graveyard onto battlefield;
    lose life equal to its mana value)

Finisher:
  - Thassa's Oracle (win if library empty after scry) — kickoff Phase 7
    explicitly calls this out.
"""
from __future__ import annotations

from api.engine.pillar_f.v0_2.cards.spell.framework import register_spell
from api.engine.pillar_f.v0_2.state import Card, GameState, StackEntry


# ==========================================================
# Cultivate — search 2 basics; one to hand, one to battlefield tapped.
# ==========================================================


def _cultivate_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    found = 0
    for cid in list(state.players[pid].zones.library):
        if found >= 2:
            break
        card = state.get_card(cid)
        if card is None or not card.is_land() or "Basic" not in card.type_line:
            continue
        if found == 0:
            # First basic → battlefield tapped.
            state.move_card(cid, from_player=pid, from_zone="library",
                           to_player=pid, to_zone="battlefield")
            card.tapped = True
        else:
            # Second basic → hand.
            state.move_card(cid, from_player=pid, from_zone="library",
                           to_player=pid, to_zone="hand")
        found += 1
    return None


register_spell("Cultivate", _cultivate_resolve)
register_spell("Kodama's Reach", _cultivate_resolve)  # Same effect


# ==========================================================
# Nature's Lore — Forest onto battlefield (untapped).
# ==========================================================


def _natures_lore_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    for cid in list(state.players[pid].zones.library):
        card = state.get_card(cid)
        if card is None:
            continue
        if card.name == "Forest" or "Forest" in (card.subtypes or []):
            state.move_card(cid, from_player=pid, from_zone="library",
                           to_player=pid, to_zone="battlefield")
            return None
    return None


register_spell("Nature's Lore", _natures_lore_resolve)
register_spell("Three Visits", _natures_lore_resolve)  # Functional reprint


# ==========================================================
# Rampant Growth — basic onto battlefield TAPPED.
# ==========================================================


def _rampant_growth_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    for cid in list(state.players[pid].zones.library):
        card = state.get_card(cid)
        if card is None or not card.is_land() or "Basic" not in card.type_line:
            continue
        state.move_card(cid, from_player=pid, from_zone="library",
                       to_player=pid, to_zone="battlefield")
        card.tapped = True
        return None
    return None


register_spell("Rampant Growth", _rampant_growth_resolve)


# ==========================================================
# Dark Ritual — add {B}{B}{B}.
# ==========================================================


def _dark_ritual_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    state.players[pid].mana_pool.B += 3
    return None


register_spell("Dark Ritual", _dark_ritual_resolve)


# ==========================================================
# Demonic Tutor — search library for any card → hand.
# ==========================================================


def _demonic_tutor_resolve(state: GameState, entry: StackEntry) -> None:
    """Iter-10 stub: caller passes targets[0] = the card_id to tutor.
    If no target specified, picks the first card in library."""
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    if entry.targets:
        target_cid = entry.targets[0]
    elif state.players[pid].zones.library:
        target_cid = state.players[pid].zones.library[0]
    else:
        return None
    state.move_card(target_cid, from_player=pid, from_zone="library",
                   to_player=pid, to_zone="hand")
    return None


register_spell("Demonic Tutor", _demonic_tutor_resolve)


# Vampiric Tutor — same shape but puts on TOP of library, -2 life.
def _vampiric_tutor_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    state.players[pid].life_total -= 2
    if entry.targets:
        target_cid = entry.targets[0]
        # Move from library to top of library (no-op for first
        # position, but more meaningful if caller picked a non-top card).
        if target_cid in state.players[pid].zones.library:
            state.players[pid].zones.library.remove(target_cid)
            state.players[pid].zones.library.insert(0, target_cid)
    return None


register_spell("Vampiric Tutor", _vampiric_tutor_resolve)


# Enlightened Tutor — find artifact/enchantment → top.
def _enlightened_tutor_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    for cid in list(state.players[pid].zones.library):
        card = state.get_card(cid)
        if card is None:
            continue
        tl = card.type_line.lower()
        if "artifact" in tl or "enchantment" in tl:
            state.players[pid].zones.library.remove(cid)
            state.players[pid].zones.library.insert(0, cid)
            return None
    return None


register_spell("Enlightened Tutor", _enlightened_tutor_resolve)


# Mystical Tutor — find instant/sorcery → top.
def _mystical_tutor_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    for cid in list(state.players[pid].zones.library):
        card = state.get_card(cid)
        if card is None:
            continue
        tl = card.type_line.lower()
        if "instant" in tl or "sorcery" in tl:
            state.players[pid].zones.library.remove(cid)
            state.players[pid].zones.library.insert(0, cid)
            return None
    return None


register_spell("Mystical Tutor", _mystical_tutor_resolve)


# ==========================================================
# Brainstorm — draw 3, put 2 back on top in any order.
# Iter-10 stub: draw 3, then put the LAST 2 hand cards back on top.
# (LLM choice deferred to iter-11+.)
# ==========================================================


def _brainstorm_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    lib = state.players[pid].zones.library
    drawn = 0
    for _ in range(3):
        if not lib:
            state.players[pid].has_drawn_from_empty_library = True
            break
        state.players[pid].zones.hand.append(lib.pop(0))
        state.players[pid].cards_drawn_this_turn += 1
        drawn += 1
    # Put 2 back on top from hand (LIFO: last drawn).
    for _ in range(2):
        if not state.players[pid].zones.hand:
            break
        cid = state.players[pid].zones.hand.pop()
        state.players[pid].zones.library.insert(0, cid)
    return None


register_spell("Brainstorm", _brainstorm_resolve)


# ==========================================================
# Faithless Looting — draw 2, discard 2.
# ==========================================================


def _faithless_looting_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    lib = state.players[pid].zones.library
    # Draw 2.
    for _ in range(2):
        if not lib:
            state.players[pid].has_drawn_from_empty_library = True
            break
        state.players[pid].zones.hand.append(lib.pop(0))
        state.players[pid].cards_drawn_this_turn += 1
    # Discard 2 (LIFO).
    for _ in range(2):
        if not state.players[pid].zones.hand:
            break
        cid = state.players[pid].zones.hand.pop()
        state.players[pid].zones.graveyard.append(cid)
    return None


register_spell("Faithless Looting", _faithless_looting_resolve)


# ==========================================================
# Reanimate — target creature card from a graveyard → battlefield.
# Lose life = its mana value.
# ==========================================================


def _reanimate_resolve(state: GameState, entry: StackEntry) -> None:
    if not entry.targets:
        return None
    target_cid = entry.targets[0]
    card = state.get_card(target_cid)
    if card is None or not card.is_creature():
        return None
    # Find which graveyard holds it.
    gy_owner = None
    for ps in state.players:
        if target_cid in ps.zones.graveyard:
            gy_owner = ps.player_id
            break
    if gy_owner is None:
        return None
    pid = entry.controller
    # Move to controller's battlefield (Reanimate puts it under YOUR
    # control regardless of original owner).
    state.move_card(target_cid, from_player=gy_owner,
                   from_zone="graveyard",
                   to_player=pid, to_zone="battlefield")
    # Lose life = card's mana value.
    if 0 <= pid < len(state.players):
        state.players[pid].life_total -= int(card.cmc or 0)
    return None


register_spell("Reanimate", _reanimate_resolve)


# ==========================================================
# Thassa's Oracle (Phase 7 kickoff highlight) — "When this creature
# enters the battlefield, look at the top X cards of your library,
# where X is your devotion to blue. Put up to one of them on top of
# your library and the rest on the bottom in a random order. If X is
# greater than or equal to the number of cards in your library, you
# win the game."
#
# Iter-10 stub: skip the scry; just check the win condition.
# Devotion to blue = count of U pips in controller's permanents' costs.
# ==========================================================


from api.engine.pillar_f.v0_2.cards.etb.creatures import (
    _devotion_to_color as _existing_devotion,
)


def _thassas_oracle_resolve(state: GameState, entry: StackEntry) -> None:
    pid = entry.controller
    if not (0 <= pid < len(state.players)):
        return None
    x = _existing_devotion(state, pid, "U")
    lib_count = len(state.players[pid].zones.library)
    if x >= lib_count:
        state.winner_player_id = pid
        state.game_over = True
    return None


register_spell("Thassa's Oracle", _thassas_oracle_resolve)


# Also wire Thoracle's ETB-trigger as if it lands as a creature →
# resolves ETB win-check via the etb pipeline. But since it's in the
# spell bucket in our categorization, we treat it as a resolved spell
# that does the check.
