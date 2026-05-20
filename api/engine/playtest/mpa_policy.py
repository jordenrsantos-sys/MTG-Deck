"""mpa_policy — Phase 5b.1c heuristic baseline + 5b.3 block selection."""
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
from .mpa_game_state import GameState, Phase, Step
from .mpa_actions import Action, ActionType, FAST_MANA_PRODUCERS

POLICY_VERSION = "mpa_policy_v0.6_extra_combat_wincon"


def _untapped_mana_sources_count(state, seat_index):
    """Phase 3 — count untapped lands AND fast-mana artifacts, weighting
    each by mana produced. Used by tutor/wincon checks where we want a
    coarse "do I have enough mana" gate, not a strict color check."""
    me = state.players[seat_index]
    total = 0
    for c in me.battlefield:
        if c.tapped:
            continue
        if c.is_land():
            total += 1
        elif c.name in FAST_MANA_PRODUCERS:
            total += FAST_MANA_PRODUCERS[c.name]
    return total


# ============================================================
# v0.3 — assembled win-condition recognition
# ============================================================

# Pattern A: Thassa's Oracle + Demonic Consultation/Tainted Pact.
# Canonical cEDH wincon. Cast Consultation (or Tainted Pact) naming a card
# not in your deck → exile entire library → Thoracle's ETB / static ability
# wins because library count < devotion-to-blue.
#
# MPA does not yet model triggered abilities or library exile, so the
# detection returns a synthetic WIN_THE_GAME action that the runner applies
# as terminal. The PoC question this targets: does the detection→decision→
# execution architecture work, independent of full MTG rules modeling?
_THORACLE_NAME = "Thassa's Oracle"
_CONSULT_NAMES = ("Demonic Consultation", "Tainted Pact")

# Phase 4 — Pattern B: infinite combat phases via extra-combat anchor +
# enabler. Each (anchor, enabler) pair on battlefield produces unbounded
# combat steps, terminal for the defender. The MPA can't model the
# activation loop, so we collapse to WIN_THE_GAME the same way the
# Thoracle combo does.
#
# Anchors are cards that grant an extra combat (or repeatable combat).
# Enablers either produce enough mana per attack to fund the next
# activation OR untap the lands/creatures needed.
_EXTRA_COMBAT_ANCHORS = frozenset({
    "Aggravated Assault",   # {5}: extra combat (sorcery)
    "Hellkite Charger",     # {5}{R}{R}: untap attackers + extra combat
    "Combat Celebrant",     # exert: extra combat (once per turn — combos w/ blink)
})
_EXTRA_COMBAT_ENABLERS = frozenset({
    # Mana per attack — fully fund the next anchor activation
    "Old Gnawbone",            # 8 treasure per attack
    "Savage Ventmaw",          # 6 mana on attack ({R}{R}{R}{G}{G}{G})
    "Ancient Copper Dragon",   # treasures = damage dealt (~6-12)
    # Untap effects that recycle the attacker / lands for re-activation
    "Bear Umbra",              # untap all lands when enchanted attacker triggers
    "Sword of Feast and Famine",  # untap all lands when combat damage dealt
    "Nature's Will",           # untap lands when creature attacks
    "Aurelia, the Warleader",  # this is a different anchor — untap creatures on attack
})


def check_win_conditions(state, seat_index):
    """v0.3 — recognize assembled win conditions. Returns a WIN_THE_GAME
    Action if a known wincon is in place and executable this priority window,
    else None.

    Currently recognized:
      Thassa's Oracle + Demonic Consultation/Tainted Pact (cEDH instant-win):
        Conditions:
          - Thoracle in battlefield OR in hand
          - Consultation or Tainted Pact in hand
          - Sufficient untapped lands to cast the remaining piece(s).
            We approximate as a land count, not a strict color check —
            cEDH manabases run nonbasics that current _land_color doesn't
            recognize; gating on color would prevent the wincon from ever
            firing despite the deck being capable of casting it.

    Color-check looseness is a deliberate PoC simplification. If the
    architecture validates here, the next pass tightens mana modeling.
    """
    me = state.players[seat_index]

    # Pattern A: Thassa's Oracle + Demonic Consultation / Tainted Pact (Phase 1)
    thoracle_in_play = any(c.name == _THORACLE_NAME for c in me.battlefield)
    thoracle_in_hand = any(c.name == _THORACLE_NAME for c in me.hand)
    consult_in_hand = any(c.name in _CONSULT_NAMES for c in me.hand)
    if (thoracle_in_play or thoracle_in_hand) and consult_in_hand:
        available_mana = _untapped_mana_sources_count(state, seat_index)
        needed = 1 if thoracle_in_play else 3
        if available_mana >= needed:
            return Action(
                type=ActionType.WIN_THE_GAME,
                seat_index=seat_index,
                notes="thoracle_consult_combo",
            )

    # Pattern B: extra-combat anchor + enabler in play (Phase 4)
    combat = _check_extra_combat_wincon(state, seat_index)
    if combat is not None:
        return combat

    return None


def _check_extra_combat_wincon(state, seat_index):
    """Pattern B (Phase 4): anchor + enabler on battlefield → infinite combats."""
    me = state.players[seat_index]
    anchors_in_play = [c for c in me.battlefield if c.name in _EXTRA_COMBAT_ANCHORS]
    enablers_in_play = [c for c in me.battlefield if c.name in _EXTRA_COMBAT_ENABLERS]
    if not anchors_in_play or not enablers_in_play:
        return None
    anchor_name = anchors_in_play[0].name
    enabler_name = enablers_in_play[0].name
    return Action(
        type=ActionType.WIN_THE_GAME,
        seat_index=seat_index,
        notes=f"infinite_combat_combo:{anchor_name}+{enabler_name}",
    )


_TUTOR_NAMES = frozenset({
    # Strict library tutors that put a card in hand (or top of library — we
    # simplify to "into hand"). Approximations preserve PoC behavior: each
    # of these in real MTG moves the searched card somewhere accessible
    # within a turn or two; for the MPA's coarse simulation we collapse
    # that to "into hand immediately."
    "Demonic Tutor",       # {1}{B} sorcery, into hand
    "Vampiric Tutor",      # {B} instant, top of library
    "Imperial Seal",       # {B} sorcery, top of library
    "Enlightened Tutor",   # {W} instant, top of library — artifacts/enchantments
    "Mystical Tutor",      # {U} instant, top of library — instants/sorceries
    "Gamble",              # {R} sorcery, into hand then random discard
    "Beseech the Mirror",  # {2}{B}{B} sorcery with bargain — into hand
    "Diabolic Tutor",      # {2}{B} sorcery, into hand
    "Worldly Tutor",       # {G} instant, top of library — creatures
    "Wishclaw Talisman",   # {2} artifact activation — opponent picks card (we waive)
    "Grim Tutor",          # {1}{B}{B} sorcery
    "Personal Tutor",      # {U} sorcery — sorceries
    "Vampiric Tutor",      # duplicate-safe
    "Mastermind's Acquisition",  # {3}{B}{B} sorcery
    "Increasing Ambition", # {3}{B} sorcery — flashback
})


def check_tutor_actions(state, seat_index):
    """v0.4 — recognize library tutors and fire them to fetch wincon pieces.

    Conditions:
      - Recognized tutor card is in hand
      - At least one priority target (Thoracle, Consultation, Tainted Pact)
        is in library AND not already in hand
      - Untapped land count >= tutor.cmc (loose mana check — same simplification
        as check_win_conditions; nonbasic manabases would block strict color
        check at this stage)

    Returns a TUTOR_FOR_TARGET Action or None.

    Priority within wincon targets matches check_win_conditions:
      1. Thassa's Oracle (the engine half)
      2. Demonic Consultation (1-mana enabler)
      3. Tainted Pact (2-mana enabler)
    """
    me = state.players[seat_index]

    # Find a tutor in hand we can pay for. Phase 3: include fast-mana
    # artifacts in the mana count so cEDH decks can tutor on turn 1-2.
    available_mana = _untapped_mana_sources_count(state, seat_index)
    tutors_in_hand = [c for c in me.hand if c.name in _TUTOR_NAMES]
    if not tutors_in_hand:
        return None

    targets_priority = (_THORACLE_NAME, _CONSULT_NAMES[0], _CONSULT_NAMES[1])

    # For each tutor (cheapest first), try to find a missing target in library.
    for tutor in sorted(tutors_in_hand, key=lambda c: c.cmc or 0):
        if available_mana < (tutor.cmc or 0):
            continue
        for target_name in targets_priority:
            if any(c.name == target_name for c in me.hand):
                continue  # Already have it; no point tutoring
            target_card = next(
                (c for c in me.library if c.name == target_name), None
            )
            if target_card is None:
                continue
            return Action(
                type=ActionType.TUTOR_FOR_TARGET,
                seat_index=seat_index,
                source_instance_id=tutor.instance_id,
                target_instance_id=target_card.instance_id,
                notes=f"tutor:{tutor.name}->target:{target_name}",
            )
    return None


def _pick_fast_mana_cast(legal_actions, hand):
    """Phase 3 — prefer casting fast-mana producers (Sol Ring, Mox cycle,
    Lotus Petal, etc.) over higher-CMC spells. Cheap rocks should ramp the
    combo even when bigger creatures are also legal. Returns a CAST_FROM_HAND
    Action or None.
    """
    hand_by_id = {c.instance_id: c for c in hand}
    for a in legal_actions:
        if a.type != ActionType.CAST_FROM_HAND:
            continue
        c = hand_by_id.get(a.source_instance_id)
        if c is None:
            continue
        if c.name in FAST_MANA_PRODUCERS:
            return a
    return None


def choose_action(state, seat_index, legal_actions):
    if not legal_actions:
        return Action(type=ActionType.PASS_PRIORITY, seat_index=seat_index), 1.0, "no_legal_actions"
    if state.game_over:
        return Action(type=ActionType.PASS_PRIORITY, seat_index=seat_index), 1.0, "game_over"
    # v0.3: scan for Thoracle wincon before any other action consideration.
    wincon = check_win_conditions(state, seat_index)
    if wincon is not None:
        return wincon, 1.0, f"wincon_{wincon.notes}"
    # v0.6 / Phase 4: extra-combat anchor + enabler on battlefield → infinite combats.
    combat_wincon = _check_extra_combat_wincon(state, seat_index)
    if combat_wincon is not None:
        return combat_wincon, 1.0, f"wincon_{combat_wincon.notes}"
    # v0.4: if we have a tutor and are missing a wincon piece, search for it.
    tutor = check_tutor_actions(state, seat_index)
    if tutor is not None:
        return tutor, 0.95, f"tutor_{tutor.notes}"
    me = state.players[seat_index]
    # Land drop always before fast mana — lands enable everything else.
    land_actions = [a for a in legal_actions if a.type == ActionType.PLAY_LAND]
    if land_actions:
        return land_actions[0], 0.95, "always_play_land_t1"
    # Phase 3: cast fast mana before any other spell. This is the difference
    # between assembling combo on turn 3 vs turn 6.
    fast_mana_cast = _pick_fast_mana_cast(legal_actions, me.hand)
    if fast_mana_cast is not None:
        # Resolve the name from the action's source_instance_id for the rationale.
        fm_name = next(
            (c.name for c in me.hand if c.instance_id == fast_mana_cast.source_instance_id),
            "fast_mana",
        )
        return fast_mana_cast, 0.9, f"cast_fast_mana:{fm_name}"
    commander_actions = [a for a in legal_actions if a.type == ActionType.CAST_COMMANDER]
    if commander_actions:
        return commander_actions[0], 0.85, "cast_commander_on_curve"
    cast_actions = [a for a in legal_actions if a.type == ActionType.CAST_FROM_HAND]
    if cast_actions:
        def cmc_for(action):
            for c in me.hand:
                if c.instance_id == action.source_instance_id:
                    return c.cmc or 0
            return 0
        chosen = max(cast_actions, key=cmc_for)
        return chosen, 0.7, "cast_highest_cmc_spell"
    attack_actions = [a for a in legal_actions if a.type == ActionType.DECLARE_ATTACKER]
    if attack_actions and _should_attack(state, seat_index):
        return attack_actions[0], 0.65, "attack_when_opp_low_life_or_open"
    block_actions = [a for a in legal_actions if a.type == ActionType.DECLARE_BLOCKER]
    if block_actions and _should_block(state, seat_index):
        return block_actions[0], 0.65, "block_to_prevent_damage"
    return Action(type=ActionType.PASS_PRIORITY, seat_index=seat_index), 0.9, "no_productive_action"


def should_mulligan(state, seat_index, mulligans_taken):
    me = state.players[seat_index]
    if mulligans_taken >= 3:
        return False, "max_mulligans_reached"
    land_count = sum(1 for c in me.hand if c.is_land())
    castable = sum(1 for c in me.hand if not c.is_land() and (c.cmc or 0) <= 4)
    if land_count < 2:
        return True, "only_" + str(land_count) + "_lands_in_opening_hand"
    if land_count > 5:
        return True, "flooded_" + str(land_count) + "_lands"
    if castable == 0:
        return True, "no_castable_threats_in_opening_hand"
    return False, "keep_hand"


def _should_attack(state, seat_index):
    opps = state.opponent_seats(seat_index)
    if not opps:
        return False
    my_life = state.players[seat_index].life
    for o in opps:
        if state.players[o].life <= my_life:
            return True
    for o in opps:
        if not any(c.is_creature() for c in state.players[o].battlefield):
            return True
    return False


def _should_block(state, seat_index):
    incoming = 0
    for obj in state.stack:
        if obj.get("type") == "attack" and obj.get("defender_seat") == seat_index:
            incoming += int(obj.get("damage", 0))
    my_life = state.players[seat_index].life
    if incoming >= my_life:
        return True
    if incoming >= 10:
        return True
    return False


def choose_blocks(state, defender_seat, attackers):
    """Phase 5b.3 block selection. Returns list of (attacker, blocker) tuples.

    Heuristic: profitable blocks first (blocker survives + kills attacker);
    chump-block lethal swings; otherwise let damage through. Always returns
    a list (possibly empty); never None.
    """
    if not attackers:
        return []
    defender = state.players[defender_seat]
    available = [c for c in defender.battlefield if c.is_creature() and not c.tapped]
    if not available:
        return []
    total_incoming = sum((a.power or 0) for a in attackers)
    is_lethal_swing = total_incoming >= defender.life
    threats = sorted(attackers, key=lambda a: -(a.power or 0))
    blockers_pool = sorted(available, key=lambda b: (b.toughness or 0))
    assignments = []
    used = set()
    for atk in threats:
        atk_pow = atk.power or 0
        atk_tou = atk.toughness or 0
        best = None
        for b in blockers_pool:
            if b.instance_id in used:
                continue
            b_pow = b.power or 0
            b_tou = b.toughness or 0
            if b_tou > atk_pow and b_pow >= atk_tou:
                best = b
                break
        if best is None and is_lethal_swing:
            for b in blockers_pool:
                if b.instance_id in used:
                    continue
                best = b
                break
        if best is not None:
            assignments.append((atk, best))
            used.add(best.instance_id)
    return assignments
