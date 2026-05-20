"""Phase 6 (capability upgrade): better attack heuristic.

Current `_should_attack`: attack if opponent life <= my life OR opponent
has no creatures. Misses two critical cases:
  1. **Race-or-die:** if opponent has lethal swing on board (their
     untapped non-summoning-sick power ≥ my life), I'll be dead next
     turn — must attack ALL-IN now, even if attacking is "bad value."
  2. **Lethal swing:** if my total attacking power ≥ opponent's life,
     swing for game regardless of board state.

These shift behavior in tight midgames (B5_vs_B4, mirror_B3) where the
current heuristic stalls when neither side has clear lifetotal lead.
"""
from __future__ import annotations

import pytest


def _mk_creature(name, power=2, toughness=2, summoning_sick=False, tapped=False, instance_id=0):
    from api.engine.playtest.mpa_game_state import CardInGame
    c = CardInGame(
        oracle_id=f"oid-{name.lower().replace(' ', '-')}",
        name=name,
        type_line="Creature — Warrior",
        cmc=power + toughness,
        instance_id=instance_id,
        power=power,
        toughness=toughness,
    )
    c.summoning_sick = summoning_sick
    c.tapped = tapped
    return c


def _state(life_a=40, life_b=40, battlefield_a=(), battlefield_b=()):
    from api.engine.playtest.mpa_game_state import GameState, PlayerState
    p0 = PlayerState(seat_index=0, life=life_a)
    p0.battlefield = list(battlefield_a)
    p1 = PlayerState(seat_index=1, life=life_b)
    p1.battlefield = list(battlefield_b)
    return GameState(players=[p0, p1])


def test_race_or_die_attack_when_opp_has_lethal_swing():
    """I'm at 5 life, opponent has 8 power untapped → attack to race."""
    from api.engine.playtest.mpa_policy import _should_attack
    opp_creature = _mk_creature("Big Beast", power=8, toughness=8, instance_id=1)
    state = _state(life_a=5, life_b=40, battlefield_b=[opp_creature])
    assert _should_attack(state, 0) is True


def test_race_or_die_ignores_summoning_sick_opp():
    """Opp creature is summoning sick → won't swing this turn → race-or-die
    branch must NOT trigger. With no other "attack" trigger (opp at higher
    life, has a creature blocker available), expect False."""
    from api.engine.playtest.mpa_policy import _should_attack
    opp_creature = _mk_creature("Big Beast", power=8, toughness=8, summoning_sick=True, instance_id=1)
    state = _state(life_a=5, life_b=40, battlefield_b=[opp_creature])
    # opp life (40) > mine (5), opp has a creature, opp's threat is summoning
    # sick so no race-or-die. No lethal swing for me. Should NOT attack.
    assert _should_attack(state, 0) is False


def test_lethal_swing_attack_when_i_can_kill():
    """I have 25 power on board, opp at 20 life → swing for lethal."""
    from api.engine.playtest.mpa_policy import _should_attack
    my_creatures = [_mk_creature(f"Beater{i}", power=5, toughness=5, instance_id=i) for i in range(5)]
    state = _state(life_a=40, life_b=20, battlefield_a=my_creatures)
    assert _should_attack(state, 0) is True


def test_no_attack_when_no_urgency_and_higher_life():
    """I'm at 40 life, opp at 30 — opp has creatures, no race. Existing
    heuristic returns True because opp life <= mine. Verify it still does.
    """
    from api.engine.playtest.mpa_policy import _should_attack
    opp_creature = _mk_creature("Defender", power=2, toughness=4, instance_id=1)
    state = _state(life_a=40, life_b=30, battlefield_b=[opp_creature])
    assert _should_attack(state, 0) is True  # opp life lower → attack


def test_no_attack_when_both_high_life_and_opp_has_blockers():
    """I'm at 30, opp at 40 (opp higher) — defensive heuristic should not attack."""
    from api.engine.playtest.mpa_policy import _should_attack
    opp_creature = _mk_creature("Big Defender", power=2, toughness=5, instance_id=1)
    state = _state(life_a=30, life_b=40, battlefield_b=[opp_creature])
    assert _should_attack(state, 0) is False


def test_attack_with_lethal_overrides_higher_opp_life():
    """Even if opp life > mine, if I have lethal on board, swing."""
    from api.engine.playtest.mpa_policy import _should_attack
    my_creatures = [_mk_creature("Big", power=15, toughness=15, instance_id=1)]
    opp_creature = _mk_creature("D", power=1, toughness=3, instance_id=2)
    state = _state(life_a=10, life_b=12, battlefield_a=my_creatures, battlefield_b=[opp_creature])
    # opp life (12) > mine (10), but my power (15) >= opp life (12) → lethal swing
    assert _should_attack(state, 0) is True
