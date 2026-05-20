"""Phase 4 (capability upgrade): additional wincon patterns.

Phase 1 added Thassa's Oracle + Demonic Consultation/Tainted Pact, which
is the cEDH wincon but doesn't help non-cEDH decks. Ur-Dragon B4
(Dragon Tribal/Goodstuff/cEDH-adjacent) has a different wincon family:
infinite combat phases via an extra-combat anchor + a mana-per-attack
creature.

Pattern B: Extra-combat infinite loop.
  Anchor (extra-combat enabler):
    - Aggravated Assault  (pay {5} for extra combat as sorcery)
    - Hellkite Charger    (pay {5}{R}{R} for extra combat)
    - Combat Celebrant    (one extra combat per turn, exerts)
  Enabler (mana per attack ≥ anchor's cost, or untap):
    - Old Gnawbone         (8 power → 8 treasures on attack)
    - Savage Ventmaw       ({R}{R}{R}{G}{G}{G} on attack)
    - Ancient Copper Dragon (treasures = damage dealt)
    - Bear Umbra           (untap all lands when enchanted creature attacks)

If both an anchor and an enabler are on battlefield, the loop produces
infinite combat phases — terminal for the opponent. The MPA can't model
the chain of activations/untaps; same WIN_THE_GAME synthetic action
pattern as Phase 1.

Helps: B4_vs_B2 (Ur-Dragon vs Atraxa) and B4_vs_B3 (vs Edgar Markov).
"""
from __future__ import annotations

import pytest


def _mk_card(name, type_line="Creature", cmc=0, mana_cost="", color_identity=(), instance_id=0):
    from api.engine.playtest.mpa_game_state import CardInGame
    return CardInGame(
        oracle_id=f"oid-{name.lower().replace(' ', '-').replace(chr(39), '')}",
        name=name,
        type_line=type_line,
        cmc=cmc,
        mana_cost=mana_cost,
        color_identity=color_identity,
        instance_id=instance_id,
    )


def _state_with_battlefield(battlefield, hand=None):
    from api.engine.playtest.mpa_game_state import GameState, PlayerState
    p0 = PlayerState(seat_index=0)
    p0.battlefield = list(battlefield)
    p0.hand = list(hand or [])
    p1 = PlayerState(seat_index=1)
    return GameState(players=[p0, p1])


def test_aggravated_assault_plus_old_gnawbone_fires_wincon():
    from api.engine.playtest.mpa_policy import check_win_conditions
    from api.engine.playtest.mpa_actions import ActionType
    aa = _mk_card("Aggravated Assault", "Enchantment", instance_id=1)
    og = _mk_card("Old Gnawbone", "Creature — Dragon", instance_id=2)
    state = _state_with_battlefield([aa, og])
    action = check_win_conditions(state, 0)
    assert action is not None
    assert action.type == ActionType.WIN_THE_GAME
    assert "combat" in action.notes.lower()


def test_hellkite_charger_plus_savage_ventmaw_fires_wincon():
    from api.engine.playtest.mpa_policy import check_win_conditions
    hc = _mk_card("Hellkite Charger", "Creature — Dragon", instance_id=1)
    sv = _mk_card("Savage Ventmaw", "Creature — Dragon", instance_id=2)
    state = _state_with_battlefield([hc, sv])
    action = check_win_conditions(state, 0)
    assert action is not None


def test_aggravated_assault_plus_ancient_copper_dragon_fires_wincon():
    from api.engine.playtest.mpa_policy import check_win_conditions
    aa = _mk_card("Aggravated Assault", "Enchantment", instance_id=1)
    acd = _mk_card("Ancient Copper Dragon", "Creature — Dragon", instance_id=2)
    state = _state_with_battlefield([aa, acd])
    action = check_win_conditions(state, 0)
    assert action is not None


def test_anchor_alone_does_not_fire():
    """Aggravated Assault alone is not an instant win — it's an expensive
    redundancy effect. Needs an enabler creature."""
    from api.engine.playtest.mpa_policy import check_win_conditions
    aa = _mk_card("Aggravated Assault", "Enchantment", instance_id=1)
    state = _state_with_battlefield([aa])
    action = check_win_conditions(state, 0)
    assert action is None or "combat" not in (action.notes or "").lower()


def test_enabler_alone_does_not_fire():
    """Old Gnawbone alone is a value creature, not a wincon."""
    from api.engine.playtest.mpa_policy import check_win_conditions
    og = _mk_card("Old Gnawbone", "Creature — Dragon", instance_id=1)
    state = _state_with_battlefield([og])
    action = check_win_conditions(state, 0)
    assert action is None or "combat" not in (action.notes or "").lower()


def test_thoracle_combo_still_works_in_hand():
    """Regression: Phase 1 Thoracle combo still detected when both pieces
    in hand alongside the new pattern."""
    from api.engine.playtest.mpa_policy import check_win_conditions
    thoracle = _mk_card("Thassa's Oracle", "Creature", cmc=2, instance_id=1)
    consult = _mk_card("Demonic Consultation", "Instant", cmc=1, instance_id=2)
    lands = [_mk_card(f"L{i}", "Land", instance_id=100 + i) for i in range(3)]
    state = _state_with_battlefield(lands, hand=[thoracle, consult])
    action = check_win_conditions(state, 0)
    assert action is not None
    assert "thoracle_consult_combo" in (action.notes or "")


def test_bear_umbra_plus_hellkite_charger_fires_wincon():
    """Bear Umbra untaps lands when enchanted creature attacks. With
    Hellkite Charger as the anchor, untapped lands fund repeated activations."""
    from api.engine.playtest.mpa_policy import check_win_conditions
    bu = _mk_card("Bear Umbra", "Enchantment — Aura", instance_id=1)
    hc = _mk_card("Hellkite Charger", "Creature — Dragon", instance_id=2)
    state = _state_with_battlefield([bu, hc])
    action = check_win_conditions(state, 0)
    assert action is not None
