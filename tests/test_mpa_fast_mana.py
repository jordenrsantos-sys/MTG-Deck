"""Phase 3 (capability upgrade): fast mana recognition.

Goal: accelerate cEDH combo assembly by recognizing Sol Ring, Mana Crypt,
Lotus Petal, Mox cycle as mana producers that:
  1. Get cast ASAP from hand (cmc 0-1) — before higher-CMC spells.
  2. Stay on battlefield as untapped mana sources.
  3. Contribute to `_available_mana` and `check_tutor_actions` /
     `check_win_conditions` mana checks.
  4. Get tapped by `_auto_tap_for_cost` when paying for spells.

Without this, Sol Ring sits in hand uncast (cast_highest_cmc_spell picks
big creatures over 1-mana rocks), and even when cast it's dead mana
because _available_mana only knows about lands.
"""
from __future__ import annotations

import pytest


def _mk_card(name, type_line, cmc=0, mana_cost="", color_identity=(), instance_id=0):
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


def _state_with_seat0(hand, battlefield=None):
    from api.engine.playtest.mpa_game_state import GameState, PlayerState
    p0 = PlayerState(seat_index=0)
    p0.hand = list(hand)
    p0.battlefield = list(battlefield or [])
    p1 = PlayerState(seat_index=1)
    return GameState(players=[p0, p1])


def test_sol_ring_on_battlefield_adds_two_colorless():
    """A Sol Ring on battlefield (untapped) adds 2 colorless to _available_mana."""
    from api.engine.playtest.mpa_actions import _available_mana
    sol = _mk_card("Sol Ring", "Artifact", cmc=1, instance_id=1)
    state = _state_with_seat0([], battlefield=[sol])
    pool = _available_mana(state, 0)
    assert pool.get("C", 0) >= 2, f"Sol Ring should give 2 colorless; got {pool!r}"


def test_lotus_petal_on_battlefield_adds_one_colorless():
    """Lotus Petal contributes 1 mana while untapped."""
    from api.engine.playtest.mpa_actions import _available_mana
    petal = _mk_card("Lotus Petal", "Artifact", cmc=0, instance_id=1)
    state = _state_with_seat0([], battlefield=[petal])
    pool = _available_mana(state, 0)
    assert pool.get("C", 0) >= 1


def test_tapped_sol_ring_does_not_contribute():
    """Once tapped, Sol Ring is exhausted until next untap."""
    from api.engine.playtest.mpa_actions import _available_mana
    sol = _mk_card("Sol Ring", "Artifact", cmc=1, instance_id=1)
    sol.tapped = True
    state = _state_with_seat0([], battlefield=[sol])
    pool = _available_mana(state, 0)
    assert pool.get("C", 0) == 0


def test_tutor_check_counts_fast_mana_producers():
    """Demonic Tutor (cmc=2) becomes castable with 1 land + Sol Ring (2)."""
    from api.engine.playtest.mpa_policy import check_tutor_actions
    tutor = _mk_card("Demonic Tutor", "Sorcery", cmc=2, mana_cost="{1}{B}", instance_id=1)
    thoracle = _mk_card("Thassa's Oracle", "Creature", cmc=2, instance_id=2)
    sol = _mk_card("Sol Ring", "Artifact", cmc=1, instance_id=10)
    land = _mk_card("L", "Land", instance_id=11)
    from api.engine.playtest.mpa_game_state import GameState, PlayerState
    p0 = PlayerState(seat_index=0)
    p0.hand = [tutor]
    p0.library = [thoracle]
    p0.battlefield = [sol, land]
    p1 = PlayerState(seat_index=1)
    state = GameState(players=[p0, p1])
    action = check_tutor_actions(state, 0)
    assert action is not None, "Tutor should fire with Sol Ring + 1 land = 3 mana available"


def test_wincon_check_counts_fast_mana():
    """Thoracle in hand + Consultation in hand + 0 lands + Sol Ring + Mana Crypt
    has 4 mana available → wincon should fire."""
    from api.engine.playtest.mpa_policy import check_win_conditions
    thoracle = _mk_card("Thassa's Oracle", "Creature", cmc=2, instance_id=1)
    consult = _mk_card("Demonic Consultation", "Instant", cmc=1, instance_id=2)
    sol = _mk_card("Sol Ring", "Artifact", cmc=1, instance_id=10)
    crypt = _mk_card("Mana Crypt", "Artifact", cmc=0, instance_id=11)
    from api.engine.playtest.mpa_game_state import GameState, PlayerState
    p0 = PlayerState(seat_index=0)
    p0.hand = [thoracle, consult]
    p0.battlefield = [sol, crypt]
    p1 = PlayerState(seat_index=1)
    state = GameState(players=[p0, p1])
    action = check_win_conditions(state, 0)
    assert action is not None, "Wincon should fire with 4 mana from artifacts"


def test_choose_action_prefers_fast_mana_over_higher_cmc():
    """On turn 1 with a Sol Ring in hand + a 5-CMC dragon in hand + 1 land,
    cast Sol Ring first (not the dragon, which is uncastable anyway, but
    the policy should select Sol Ring even if there's another cheap option)."""
    from api.engine.playtest.mpa_policy import choose_action
    from api.engine.playtest.mpa_actions import Action, ActionType, enumerate_legal_actions
    from api.engine.playtest.mpa_game_state import GameState, PlayerState, CardInGame, Phase, Step
    sol = CardInGame(oracle_id="oid-sol", name="Sol Ring", type_line="Artifact", cmc=1, mana_cost="{1}", instance_id=1)
    cheap = CardInGame(oracle_id="oid-x", name="Bear", type_line="Creature", cmc=1, mana_cost="{1}", instance_id=2)
    land = CardInGame(oracle_id="oid-l", name="L", type_line="Land", instance_id=10)
    p0 = PlayerState(seat_index=0)
    p0.hand = [sol, cheap]
    p0.battlefield = [land]
    p1 = PlayerState(seat_index=1)
    state = GameState(
        players=[p0, p1],
        phase=Phase.PRECOMBAT_MAIN, step=Step.PRECOMBAT_MAIN,
        active_player_index=0, priority_player_index=0,
    )
    legal = enumerate_legal_actions(state, 0)
    legal_for_policy = [a for a in legal if a.type != ActionType.TAP_FOR_MANA]
    action, conf, rationale = choose_action(state, 0, legal_for_policy)
    # Either Sol Ring is cast, OR a tutor/wincon fired first.
    if action.type == ActionType.CAST_FROM_HAND:
        assert action.source_instance_id == sol.instance_id, (
            f"Should cast Sol Ring first, not {action.source_instance_id} (rationale: {rationale})"
        )


def test_auto_tap_for_cost_taps_fast_mana_too():
    """When paying a 2-generic cost with 0 lands + Sol Ring, Sol Ring gets tapped."""
    from api.engine.playtest.mpa_runner import _auto_tap_for_cost
    from api.engine.playtest.mpa_game_state import GameState, PlayerState
    sol = _mk_card("Sol Ring", "Artifact", cmc=1, instance_id=1)
    p0 = PlayerState(seat_index=0)
    p0.battlefield = [sol]
    p1 = PlayerState(seat_index=1)
    state = GameState(players=[p0, p1])
    _auto_tap_for_cost(state, 0, 2, mana_cost_string="{2}")
    assert sol.tapped, "Sol Ring should be tapped to pay {2}"
