"""Phase 1 of mega-task v12 -- combat hook glue unit tests.

Covers sub-B Phase 9 deferred gate 5:
- Eligible-attackers filter respects substrate's can_attack rules
  (tapped, summoning-sick without haste, non-creature, defender keyword).
- Attack targets enumerate alive opponents + planeswalkers.
- Attacker decider asks LLM via attackers prompt, parses, converts to
  AttackerDeclaration list, falls back to "no attack" on 3rd failure.
- Blocker decider runs once per defending player, handles multi-block
  assignment ordering.
- run_llm_combat_phase end-to-end: declarations -> substrate's
  run_combat_phase -> damage applied.

Live multi-block + first-strike + trample integration deferred to
Phase 7 cycle smoke (saves $ on every pytest invocation; integration
tests in this file use mock LLM client).
"""
from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones, Step, Phase,
)
from api.engine.pillar_f.v0_2.combat import (
    AttackerDeclaration, BlockerAssignment, CombatState,
    declare_attackers as substrate_declare_attackers,
)
from api.engine.pillar_f.v0_2.playtest.combat_glue import (
    compute_eligible_attackers, compute_attack_targets,
    compute_eligible_blockers, compute_attackers_to_block,
    make_llm_attacker_decider, make_llm_blocker_decider,
    run_llm_combat_phase,
)
from api.engine.pillar_f.v0_2.policy.cost import CostTracker


# ============================================================
# Mock LLM client
# ============================================================


@dataclass
class MockCallResult:
    ok: bool = True
    text: str = ""
    cost_usd: float = 0.002
    error_code: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class MockLLMClient:
    responses: List[MockCallResult] = field(default_factory=list)
    calls: List[Dict[str, Any]] = field(default_factory=list)
    available: bool = True

    def is_available(self) -> bool:
        return self.available

    def call_with_budget(self, *, system, user, max_input_tokens,
                         max_output_tokens, **kwargs) -> MockCallResult:
        self.calls.append({"system": system, "user": user})
        if self.responses:
            return self.responses.pop(0)
        return MockCallResult(
            ok=False, text="",
            error_code="no_response_queued",
            error_message="MockLLMClient ran out of responses.",
        )


def _empty_4p_game() -> GameState:
    gs = GameState()
    for pid in range(4):
        gs.players.append(PlayerState(player_id=pid, name=f"P{pid}",
                                      life_total=40, zones=PlayerZones()))
    gs.active_player = 0
    gs.step = Step.DECLARE_ATTACKERS
    gs.phase = Phase.COMBAT
    gs.turn_number = 3
    return gs


def _make_creature(
    gs: GameState, owner: int, *, name: str,
    power: str = "2", toughness: str = "2",
    keywords=None, tapped: bool = False,
    summoning_sick: bool = False,
    type_line: str = "Creature -- Bear",
) -> Card:
    c = Card(name=name, owner=owner, controller=owner,
             type_line=type_line, power=power, toughness=toughness,
             keywords=list(keywords or []), cmc=2,
             tapped=tapped, summoning_sick=summoning_sick)
    gs.add_card(c)
    gs.players[owner].zones.battlefield.append(c.card_id)
    return c


# ============================================================
# compute_eligible_attackers / targets / blockers
# ============================================================


class EligibleAttackersTests(unittest.TestCase):
    def test_untapped_creature_eligible(self) -> None:
        gs = _empty_4p_game()
        _make_creature(gs, 0, name="Bear")
        eligible = compute_eligible_attackers(gs, active_player=0)
        self.assertEqual(len(eligible), 1)
        self.assertEqual(eligible[0]["name"], "Bear")

    def test_tapped_creature_excluded(self) -> None:
        gs = _empty_4p_game()
        _make_creature(gs, 0, name="Bear", tapped=True)
        self.assertEqual(compute_eligible_attackers(gs, 0), [])

    def test_summoning_sick_no_haste_excluded(self) -> None:
        gs = _empty_4p_game()
        _make_creature(gs, 0, name="Slow", summoning_sick=True)
        self.assertEqual(compute_eligible_attackers(gs, 0), [])

    def test_summoning_sick_with_haste_included(self) -> None:
        gs = _empty_4p_game()
        _make_creature(gs, 0, name="Hasty", summoning_sick=True,
                       keywords=["haste"])
        eligible = compute_eligible_attackers(gs, 0)
        self.assertEqual(len(eligible), 1)
        self.assertIn("haste", eligible[0]["keywords"])

    def test_defender_keyword_excluded(self) -> None:
        gs = _empty_4p_game()
        _make_creature(gs, 0, name="Wall", keywords=["defender"])
        self.assertEqual(compute_eligible_attackers(gs, 0), [])

    def test_non_creature_excluded(self) -> None:
        gs = _empty_4p_game()
        _make_creature(gs, 0, name="Rock",
                       type_line="Artifact", power="0", toughness="0")
        self.assertEqual(compute_eligible_attackers(gs, 0), [])


class AttackTargetsTests(unittest.TestCase):
    def test_targets_include_alive_opponents(self) -> None:
        gs = _empty_4p_game()
        targets = compute_attack_targets(gs, active_player=0)
        # P0 excluded; P1/P2/P3 included.
        target_ids = [t["target_id"] for t in targets]
        self.assertEqual(sorted(target_ids), [1, 2, 3])

    def test_eliminated_opponent_excluded(self) -> None:
        gs = _empty_4p_game()
        gs.players[2].has_lost = True
        targets = compute_attack_targets(gs, 0)
        target_ids = [t["target_id"] for t in targets if t["kind"] == "player"]
        self.assertEqual(sorted(target_ids), [1, 3])

    def test_planeswalker_target_added(self) -> None:
        gs = _empty_4p_game()
        pw = Card(name="Jace", owner=1, controller=1,
                  type_line="Legendary Planeswalker -- Jace",
                  loyalty=3)
        gs.add_card(pw)
        gs.players[1].zones.battlefield.append(pw.card_id)
        targets = compute_attack_targets(gs, 0)
        pw_targets = [t for t in targets if t["kind"] == "planeswalker"]
        self.assertEqual(len(pw_targets), 1)
        self.assertEqual(pw_targets[0]["target_id"], pw.card_id)


class EligibleBlockersTests(unittest.TestCase):
    def test_summoning_sick_blocker_legal(self) -> None:
        """CR 509.1: only attacking + tap-activation are restricted by
        summoning sick. Blocking is fine."""
        gs = _empty_4p_game()
        _make_creature(gs, 1, name="Soldier", summoning_sick=True)
        eligible = compute_eligible_blockers(gs, defending_player=1)
        self.assertEqual(len(eligible), 1)

    def test_tapped_blocker_excluded(self) -> None:
        gs = _empty_4p_game()
        _make_creature(gs, 1, name="Soldier", tapped=True)
        eligible = compute_eligible_blockers(gs, 1)
        self.assertEqual(eligible, [])


class AttackersToBlockTests(unittest.TestCase):
    def test_only_incoming_attackers_shown(self) -> None:
        gs = _empty_4p_game()
        bear = _make_creature(gs, 0, name="Bear")
        # Manually build combat state with 1 attacker targeting P1.
        cs = CombatState(
            attackers=[AttackerDeclaration(
                attacker_card_id=bear.card_id, target=1,
            )],
        )
        for_p1 = compute_attackers_to_block(cs, 1, gs)
        for_p2 = compute_attackers_to_block(cs, 2, gs)
        self.assertEqual(len(for_p1), 1)
        self.assertEqual(for_p1[0]["name"], "Bear")
        self.assertEqual(for_p2, [])


# ============================================================
# LLM attacker decider end-to-end
# ============================================================


class AttackerDeciderTests(unittest.TestCase):
    def test_decider_returns_parsed_declarations(self) -> None:
        gs = _empty_4p_game()
        bear = _make_creature(gs, 0, name="Bear")
        cost = CostTracker()
        mock = MockLLMClient(responses=[MockCallResult(
            ok=True,
            text=('{"attackers": [{"attacker_index": 0, '
                  '"target_index": 0}], '
                  '"rationale": "open attack"}'),
            cost_usd=0.003,
        )])
        decider = make_llm_attacker_decider(
            llm_client=mock, cost_tracker=cost,
        )
        decls = decider(gs, active_player=0)
        self.assertEqual(len(decls), 1)
        self.assertEqual(decls[0].attacker_card_id, bear.card_id)
        # First target in compute_attack_targets list is P1.
        self.assertEqual(decls[0].target, 1)
        self.assertEqual(len(mock.calls), 1)

    def test_decider_no_eligible_returns_empty(self) -> None:
        gs = _empty_4p_game()
        cost = CostTracker()
        mock = MockLLMClient()
        decider = make_llm_attacker_decider(
            llm_client=mock, cost_tracker=cost,
        )
        decls = decider(gs, active_player=0)
        self.assertEqual(decls, [])
        # No LLM call when nothing eligible.
        self.assertEqual(len(mock.calls), 0)

    def test_decider_falls_back_on_repeated_parse_failures(self) -> None:
        gs = _empty_4p_game()
        _make_creature(gs, 0, name="Bear")
        cost = CostTracker()
        mock = MockLLMClient(responses=[
            MockCallResult(ok=True, text='not json'),
            MockCallResult(ok=True, text='also bad'),
            MockCallResult(ok=True, text='{"bad": true}'),
        ])
        decider = make_llm_attacker_decider(
            llm_client=mock, cost_tracker=cost,
        )
        decls = decider(gs, active_player=0)
        # Fallback = no attack.
        self.assertEqual(decls, [])
        # 3 LLM calls = initial + 2 re-prompts.
        self.assertEqual(len(mock.calls), 3)

    def test_decider_skips_when_game_halted_for_cost(self) -> None:
        gs = _empty_4p_game()
        _make_creature(gs, 0, name="Bear")
        cost = CostTracker()
        cost.game_halted_for_cost = True
        mock = MockLLMClient()
        decider = make_llm_attacker_decider(
            llm_client=mock, cost_tracker=cost,
        )
        decls = decider(gs, active_player=0)
        self.assertEqual(decls, [])
        self.assertEqual(len(mock.calls), 0)

    def test_decider_skips_when_player_in_fallback(self) -> None:
        gs = _empty_4p_game()
        _make_creature(gs, 0, name="Bear")
        cost = CostTracker()
        cost.fallback_until_turn_end[0] = gs.turn_number
        mock = MockLLMClient()
        decider = make_llm_attacker_decider(
            llm_client=mock, cost_tracker=cost,
        )
        self.assertEqual(decider(gs, 0), [])

    def test_decider_records_cost(self) -> None:
        gs = _empty_4p_game()
        _make_creature(gs, 0, name="Bear")
        cost = CostTracker()
        mock = MockLLMClient(responses=[MockCallResult(
            ok=True,
            text='{"attackers": [{"attacker_index": 0, "target_index": 0}]}',
            cost_usd=0.005,
        )])
        decider = make_llm_attacker_decider(
            llm_client=mock, cost_tracker=cost,
        )
        decider(gs, 0)
        self.assertAlmostEqual(
            cost.spend_for_player_turn(0, gs.turn_number), 0.005, places=5,
        )
        # Purpose recorded.
        self.assertEqual(cost.events[0]["purpose"], "combat_attackers")


# ============================================================
# LLM blocker decider
# ============================================================


class BlockerDeciderTests(unittest.TestCase):
    def test_decider_returns_assignments(self) -> None:
        gs = _empty_4p_game()
        bear = _make_creature(gs, 0, name="Bear")
        soldier = _make_creature(gs, 1, name="Soldier", power="2", toughness="2")
        cs = CombatState(attackers=[
            AttackerDeclaration(attacker_card_id=bear.card_id, target=1),
        ])
        cost = CostTracker()
        mock = MockLLMClient(responses=[MockCallResult(
            ok=True,
            text=('{"blocks": [{"attacker_index": 0, '
                  '"blocker_indices": [0]}]}'),
        )])
        decider = make_llm_blocker_decider(
            llm_client=mock, cost_tracker=cost,
        )
        assignments = decider(gs, cs, defending_player=1)
        self.assertEqual(len(assignments), 1)
        self.assertEqual(assignments[0].attacker_card_id, bear.card_id)
        self.assertEqual(assignments[0].blocker_card_ids, [soldier.card_id])

    def test_decider_no_attackers_returns_empty(self) -> None:
        gs = _empty_4p_game()
        _make_creature(gs, 1, name="Soldier")
        cs = CombatState()
        cost = CostTracker()
        mock = MockLLMClient()
        decider = make_llm_blocker_decider(
            llm_client=mock, cost_tracker=cost,
        )
        self.assertEqual(decider(gs, cs, 1), [])
        self.assertEqual(len(mock.calls), 0)

    def test_decider_no_blockers_returns_empty(self) -> None:
        gs = _empty_4p_game()
        bear = _make_creature(gs, 0, name="Bear")
        cs = CombatState(attackers=[
            AttackerDeclaration(attacker_card_id=bear.card_id, target=1),
        ])
        # P1 has no creatures.
        cost = CostTracker()
        mock = MockLLMClient()
        decider = make_llm_blocker_decider(
            llm_client=mock, cost_tracker=cost,
        )
        self.assertEqual(decider(gs, cs, 1), [])
        self.assertEqual(len(mock.calls), 0)

    def test_multi_block_assignment_order_preserved(self) -> None:
        gs = _empty_4p_game()
        big = _make_creature(gs, 0, name="BigBoy", power="5", toughness="5")
        b0 = _make_creature(gs, 1, name="Wall0", power="1", toughness="3")
        b1 = _make_creature(gs, 1, name="Wall1", power="1", toughness="3")
        cs = CombatState(attackers=[
            AttackerDeclaration(attacker_card_id=big.card_id, target=1),
        ])
        cost = CostTracker()
        mock = MockLLMClient(responses=[MockCallResult(
            ok=True,
            text=('{"blocks": [{"attacker_index": 0, '
                  '"blocker_indices": [1, 0]}]}'),  # order: b1 then b0
        )])
        decider = make_llm_blocker_decider(
            llm_client=mock, cost_tracker=cost,
        )
        assignments = decider(gs, cs, defending_player=1)
        self.assertEqual(len(assignments), 1)
        self.assertEqual(
            assignments[0].blocker_card_ids,
            [b1.card_id, b0.card_id],
        )


# ============================================================
# Full combat phase end-to-end (with mocked LLM, real substrate)
# ============================================================


class RunLLMCombatPhaseTests(unittest.TestCase):
    def test_unopposed_attack_damages_player(self) -> None:
        gs = _empty_4p_game()
        bear = _make_creature(gs, 0, name="Bear",
                              power="2", toughness="2")
        cost = CostTracker()
        # P0 attacker decider says attack with bear targeting P1.
        # P1 blocker decider would NOT fire (no blockers exist).
        mock = MockLLMClient(responses=[MockCallResult(
            ok=True,
            text=('{"attackers": [{"attacker_index": 0, '
                  '"target_index": 0}]}'),
        )])
        atk_decider = make_llm_attacker_decider(
            llm_client=mock, cost_tracker=cost,
        )
        blk_decider = make_llm_blocker_decider(
            llm_client=mock, cost_tracker=cost,
        )
        starting_life = gs.players[1].life_total
        cs, actions = run_llm_combat_phase(
            gs, active_player=0,
            attacker_decider=atk_decider,
            blocker_decider=blk_decider,
        )
        self.assertEqual(len(cs.attackers), 1)
        self.assertEqual(
            gs.players[1].life_total, starting_life - 2,
        )

    def test_blocked_attack_applies_damage_to_creatures(self) -> None:
        gs = _empty_4p_game()
        big = _make_creature(gs, 0, name="Big", power="3", toughness="3")
        sm = _make_creature(gs, 1, name="Soldier", power="2", toughness="2")
        cost = CostTracker()
        mock = MockLLMClient(responses=[
            # Attacker prompt (P0).
            MockCallResult(
                ok=True,
                text=('{"attackers": [{"attacker_index": 0, '
                      '"target_index": 0}]}'),
            ),
            # Blocker prompt (P1).
            MockCallResult(
                ok=True,
                text=('{"blocks": [{"attacker_index": 0, '
                      '"blocker_indices": [0]}]}'),
            ),
        ])
        atk = make_llm_attacker_decider(
            llm_client=mock, cost_tracker=cost,
        )
        blk = make_llm_blocker_decider(
            llm_client=mock, cost_tracker=cost,
        )
        starting_life = gs.players[1].life_total
        cs, actions = run_llm_combat_phase(
            gs, active_player=0,
            attacker_decider=atk, blocker_decider=blk,
        )
        # Player took no damage.
        self.assertEqual(gs.players[1].life_total, starting_life)
        # Combat state recorded the block.
        self.assertEqual(len(cs.blocker_assignments), 1)

    def test_no_attack_returns_empty(self) -> None:
        gs = _empty_4p_game()
        cost = CostTracker()
        mock = MockLLMClient()
        atk = make_llm_attacker_decider(
            llm_client=mock, cost_tracker=cost,
        )
        blk = make_llm_blocker_decider(
            llm_client=mock, cost_tracker=cost,
        )
        cs, actions = run_llm_combat_phase(
            gs, active_player=0,
            attacker_decider=atk, blocker_decider=blk,
        )
        self.assertEqual(cs.attackers, [])
        self.assertEqual(actions, [])


if __name__ == "__main__":
    unittest.main()
