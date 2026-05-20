"""Phase 1 (capability upgrade): MPA tutor recognition.

The MPA v0.3 wincon recognition fires only when the wincon pieces happen
to be drawn naturally. Ezio's deck runs 8+ library tutors (Demonic Tutor,
Vampiric Tutor, etc.) which the policy currently can't pilot — they sit
in hand uncast because the policy's heuristic doesn't model "search
library, put a card into hand" effects.

This pass adds `check_tutor_actions(state, seat_index)` that emits a
synthetic `TUTOR_FOR_TARGET` action when:
  - A recognized tutor card is in hand
  - At least one wincon target card is in library but NOT in hand
  - Sufficient untapped lands to pay the tutor's cost (count-based, no
    color check — same simplification as wincon recognition)

The runner applies the action atomically: the tutor moves to graveyard,
the target moves from library to hand, library shuffles.

Architecture parity with wincon: tutoring bypasses the legal-action
enumeration (which would block tutors at Ezio's colorless-typed
nonbasic manabase). Justified as PoC simplification; tighter mana
modeling lands in Phase 2.
"""
from __future__ import annotations

import pytest


def _mk_card(name, type_line, cmc=0, mana_cost="", instance_id=0, color_identity=()):
    from api.engine.playtest.mpa_game_state import CardInGame
    return CardInGame(
        oracle_id=f"oid-{name.lower().replace(' ', '-')}",
        name=name,
        type_line=type_line,
        cmc=cmc,
        mana_cost=mana_cost,
        color_identity=color_identity,
        instance_id=instance_id,
    )


def _build_minimal_state(seat_a_hand, seat_a_library, seat_a_battlefield=None):
    from api.engine.playtest.mpa_game_state import GameState, PlayerState
    p0 = PlayerState(seat_index=0)
    p0.hand = list(seat_a_hand)
    p0.library = list(seat_a_library)
    p0.battlefield = list(seat_a_battlefield or [])
    p1 = PlayerState(seat_index=1)
    return GameState(players=[p0, p1])


def test_tutor_recognition_demonic_tutor_for_thoracle():
    """Player has Demonic Tutor in hand + Thoracle in library + 2 untapped lands → fire TUTOR_FOR_TARGET."""
    from api.engine.playtest.mpa_policy import check_tutor_actions
    from api.engine.playtest.mpa_actions import ActionType

    tutor = _mk_card("Demonic Tutor", "Sorcery", cmc=2, mana_cost="{1}{B}", instance_id=1, color_identity=("B",))
    thoracle = _mk_card("Thassa's Oracle", "Creature — Merfolk Wizard", cmc=2, mana_cost="{U}{U}", instance_id=2, color_identity=("U",))
    lands = [_mk_card(f"Underground Sea {i}", "Land", instance_id=100 + i) for i in range(2)]
    state = _build_minimal_state([tutor], [thoracle], lands)

    action = check_tutor_actions(state, 0)
    assert action is not None, "Tutor recognition should fire"
    assert action.type == ActionType.TUTOR_FOR_TARGET
    assert action.source_instance_id == tutor.instance_id
    assert action.target_instance_id == thoracle.instance_id
    assert "Thassa's Oracle" in (action.notes or "")


def test_tutor_recognition_skips_when_target_already_in_hand():
    """Don't waste a tutor — if Thoracle is already in hand, don't search for it."""
    from api.engine.playtest.mpa_policy import check_tutor_actions

    tutor = _mk_card("Demonic Tutor", "Sorcery", cmc=2, mana_cost="{1}{B}", instance_id=1)
    thoracle_in_hand = _mk_card("Thassa's Oracle", "Creature", cmc=2, instance_id=2)
    consult_in_lib = _mk_card("Demonic Consultation", "Instant", cmc=1, instance_id=3)
    pact_in_lib = _mk_card("Tainted Pact", "Instant", cmc=2, instance_id=4)
    lands = [_mk_card(f"L{i}", "Land", instance_id=100 + i) for i in range(2)]

    # Thoracle in hand; tutor should target Consultation or Pact instead.
    state = _build_minimal_state([tutor, thoracle_in_hand], [consult_in_lib, pact_in_lib], lands)
    action = check_tutor_actions(state, 0)
    assert action is not None
    assert action.target_instance_id in (consult_in_lib.instance_id, pact_in_lib.instance_id)


def test_tutor_recognition_no_fire_if_no_targets_in_library():
    """All wincon pieces already in hand → no tutor needed."""
    from api.engine.playtest.mpa_policy import check_tutor_actions

    tutor = _mk_card("Demonic Tutor", "Sorcery", cmc=2, mana_cost="{1}{B}", instance_id=1)
    thoracle = _mk_card("Thassa's Oracle", "Creature", cmc=2, instance_id=2)
    consult = _mk_card("Demonic Consultation", "Instant", cmc=1, instance_id=3)
    lands = [_mk_card(f"L{i}", "Land", instance_id=100 + i) for i in range(2)]
    state = _build_minimal_state([tutor, thoracle, consult], [], lands)

    action = check_tutor_actions(state, 0)
    assert action is None


def test_tutor_recognition_respects_mana_cost():
    """Demonic Tutor cmc=2; if player has <2 untapped lands, can't tutor."""
    from api.engine.playtest.mpa_policy import check_tutor_actions

    tutor = _mk_card("Demonic Tutor", "Sorcery", cmc=2, mana_cost="{1}{B}", instance_id=1)
    thoracle = _mk_card("Thassa's Oracle", "Creature", cmc=2, instance_id=2)
    one_land = [_mk_card("L0", "Land", instance_id=100)]
    state = _build_minimal_state([tutor], [thoracle], one_land)

    action = check_tutor_actions(state, 0)
    assert action is None, "Insufficient mana should block tutor"


def test_runner_applies_tutor_action():
    """Apply TUTOR_FOR_TARGET → tutor goes to graveyard, target moves to hand, library shuffles."""
    from api.engine.playtest.mpa_actions import Action, ActionType
    from api.engine.playtest.mpa_runner import _apply_action

    tutor = _mk_card("Demonic Tutor", "Sorcery", cmc=2, mana_cost="{1}{B}", instance_id=1)
    thoracle = _mk_card("Thassa's Oracle", "Creature", cmc=2, instance_id=2)
    extra_lib_card = _mk_card("Mountain", "Land", instance_id=999)
    lands = [_mk_card(f"L{i}", "Land", instance_id=100 + i) for i in range(2)]
    state = _build_minimal_state([tutor], [thoracle, extra_lib_card], lands)

    action = Action(
        type=ActionType.TUTOR_FOR_TARGET,
        seat_index=0,
        source_instance_id=tutor.instance_id,
        target_instance_id=thoracle.instance_id,
        notes="tutor:Demonic Tutor->target:Thassa's Oracle",
    )
    _apply_action(state, action)

    p0 = state.players[0]
    # Tutor moved to graveyard
    assert tutor not in p0.hand
    assert tutor in p0.graveyard
    # Target moved from library to hand
    assert thoracle in p0.hand
    assert thoracle not in p0.library
    # Other library cards still present
    assert extra_lib_card in p0.library
    # At least one land tapped to pay
    assert sum(1 for c in p0.battlefield if c.tapped) >= 1


def test_choose_action_emits_tutor_when_no_wincon():
    """Integration: choose_action returns TUTOR_FOR_TARGET when no wincon assembled but tutor available."""
    from api.engine.playtest.mpa_policy import choose_action
    from api.engine.playtest.mpa_actions import Action, ActionType

    tutor = _mk_card("Demonic Tutor", "Sorcery", cmc=2, mana_cost="{1}{B}", instance_id=1)
    thoracle = _mk_card("Thassa's Oracle", "Creature", cmc=2, instance_id=2)
    lands = [_mk_card(f"L{i}", "Land", instance_id=100 + i) for i in range(2)]
    state = _build_minimal_state([tutor], [thoracle], lands)

    # legal_actions provided externally to choose_action; the wincon/tutor
    # checks act on state directly so we can pass minimal legal list.
    legal = [Action(type=ActionType.PASS_PRIORITY, seat_index=0)]
    action, conf, rationale = choose_action(state, 0, legal)
    assert action.type == ActionType.TUTOR_FOR_TARGET
    assert "tutor" in rationale.lower()


def test_tutor_targets_thoracle_before_consult_when_both_in_library():
    """Priority: Thoracle > Consultation > Tainted Pact."""
    from api.engine.playtest.mpa_policy import check_tutor_actions

    tutor = _mk_card("Demonic Tutor", "Sorcery", cmc=2, mana_cost="{1}{B}", instance_id=1)
    consult = _mk_card("Demonic Consultation", "Instant", cmc=1, instance_id=2)
    thoracle = _mk_card("Thassa's Oracle", "Creature", cmc=2, instance_id=3)
    pact = _mk_card("Tainted Pact", "Instant", cmc=2, instance_id=4)
    lands = [_mk_card(f"L{i}", "Land", instance_id=100 + i) for i in range(2)]
    # Library order: consult first, then thoracle, then pact.
    state = _build_minimal_state([tutor], [consult, thoracle, pact], lands)

    action = check_tutor_actions(state, 0)
    assert action is not None
    assert action.target_instance_id == thoracle.instance_id, "Thoracle should be highest priority target"


def test_tutor_recognition_works_for_vampiric_tutor():
    """Sanity: at least one other tutor name is recognized."""
    from api.engine.playtest.mpa_policy import check_tutor_actions

    vt = _mk_card("Vampiric Tutor", "Instant", cmc=1, mana_cost="{B}", instance_id=1)
    thoracle = _mk_card("Thassa's Oracle", "Creature", cmc=2, instance_id=2)
    lands = [_mk_card("L0", "Land", instance_id=100)]
    state = _build_minimal_state([vt], [thoracle], lands)
    action = check_tutor_actions(state, 0)
    assert action is not None
    assert action.source_instance_id == vt.instance_id
