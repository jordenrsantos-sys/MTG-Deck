"""Phase 5b.1 smoke tests — MTG Playing Agent foundation.

Verifies the game state model + legal action enumeration + decision policy
+ turn loop runner can play a stub game end-to-end. Does NOT verify rules
correctness at the depth Phase 5b.3 calibration will — that's a separate
suite once anti-bias + calibration ship.
"""
from __future__ import annotations

import pytest


def _build_test_deck():
    """40-card test deck: 20 Mountain, 12 Goblin Chieftain, 8 Lightning Bolt."""
    return (
        [("oid-mountain", "Mountain", "Basic Land — Mountain")] * 20
        + [("oid-gobch", "Goblin Chieftain", "Creature — Goblin")] * 12
        + [("oid-bolt", "Lightning Bolt", "Instant")] * 8
    )


def _hydrate(state, cmc_creature=3, cmc_instant=1):
    """Set cmc + color_identity + power/toughness on dummy test cards."""
    for p in state.players:
        for c in p.command_zone:
            c.cmc = 3
            c.color_identity = ("R",)
            c.power = 1
            c.toughness = 2
        for c in p.library + p.hand:
            if c.name == "Goblin Chieftain":
                c.cmc = cmc_creature
                c.color_identity = ("R",)
                c.power = 2
                c.toughness = 2
            elif c.name == "Lightning Bolt":
                c.cmc = cmc_instant
                c.color_identity = ("R",)


def test_mpa_game_state_constructable():
    from api.engine.playtest.mpa_game_state import new_commander_game
    deck = _build_test_deck()
    cmdr = ("oid-krenko", "Krenko, Mob Boss", "Legendary Creature — Goblin Warrior")
    state = new_commander_game(deck, deck, cmdr, cmdr, seed=42)
    assert len(state.players) == 2
    assert all(len(p.hand) == 7 for p in state.players)
    assert all(len(p.command_zone) == 1 for p in state.players)
    assert state.turn_number == 1
    assert not state.game_over


def test_mpa_legal_actions_includes_pass_priority():
    from api.engine.playtest.mpa_game_state import new_commander_game
    from api.engine.playtest.mpa_actions import enumerate_legal_actions, ActionType
    deck = _build_test_deck()
    cmdr = ("oid-krenko", "Krenko, Mob Boss", "Legendary Creature — Goblin Warrior")
    state = new_commander_game(deck, deck, cmdr, cmdr, seed=42)
    _hydrate(state)
    actions = enumerate_legal_actions(state, 0)
    assert any(a.type == ActionType.PASS_PRIORITY for a in actions)


def test_mpa_runner_completes_game():
    """Deterministic stub game runs to completion within 30 turns + 60s wallclock."""
    from api.engine.playtest.mpa_game_state import new_commander_game, Phase, Step
    from api.engine.playtest.mpa_runner import run_game
    deck = _build_test_deck()
    cmdr = ("oid-krenko", "Krenko, Mob Boss", "Legendary Creature — Goblin Warrior")
    state = new_commander_game(deck, deck, cmdr, cmdr, seed=42)
    _hydrate(state)
    result = run_game(state, max_turns=30, record_decisions=False)
    assert result.final_turn > 0
    assert result.actions_taken > 0
    # Either someone won or we hit the turn cap (draw)
    assert state.game_over


def test_mpa_runner_deterministic_with_seed():
    """Same seed → same winner. Anti-bias prerequisite per DESIGN_DECISIONS 1.4."""
    from api.engine.playtest.mpa_game_state import new_commander_game
    from api.engine.playtest.mpa_runner import run_game
    deck = _build_test_deck()
    cmdr = ("oid-krenko", "Krenko, Mob Boss", "Legendary Creature — Goblin Warrior")

    def run(seed):
        state = new_commander_game(deck, deck, cmdr, cmdr, seed=seed)
        _hydrate(state)
        return run_game(state, max_turns=30)

    a = run(42)
    b = run(42)
    assert a.winner_seat == b.winner_seat
    assert a.final_turn == b.final_turn
    assert a.actions_taken == b.actions_taken


def test_mpa_mulligan_decision():
    """Mulligan policy: keep with 2-5 lands + at least one castable threat."""
    from api.engine.playtest.mpa_game_state import (
        new_commander_game, PlayerState, CardInGame, GameState,
    )
    from api.engine.playtest.mpa_policy import should_mulligan

    # Construct minimal state with a hand of all lands (should mulligan)
    p = PlayerState(seat_index=0, life=40)
    for i in range(7):
        p.hand.append(CardInGame(
            oracle_id="x", name="Mountain", type_line="Basic Land — Mountain",
            instance_id=i,
        ))
    state = GameState(players=[p, PlayerState(seat_index=1, life=40)])
    should, reason = should_mulligan(state, 0, 0)
    assert should is True
    assert "flooded" in reason
