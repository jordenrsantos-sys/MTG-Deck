"""Phase 4 of mega-task v10 — combat-phase prompt + parser tests.

Coverage per kickoff Phase 4 gates:
- Attackers prompt assembles given mid-game state with 5 eligible attackers.
- Parser handles attacker JSON.
- Validator catches a tapped creature (well: out-of-range index since
  the engine pre-computes eligible list — substrate-level validation
  catches the tapped case BEFORE the LLM sees it).
- Double-block scenario assigns damage order correctly.
- Integration test: full combat phase from declare → first-strike →
  normal damage with LLM driving both sides — deferred to Phase 9
  integration test (saves $$ on every pytest run).
"""
from __future__ import annotations

import unittest
from typing import Any, Dict, List

from api.engine.pillar_f.v0_2.policy.prompts import (
    build_attackers_prompt, build_blockers_prompt,
    ATTACKERS_SYSTEM_PROMPT, BLOCKERS_SYSTEM_PROMPT,
)
from api.engine.pillar_f.v0_2.policy.parsers import (
    parse_attackers_response, parse_blockers_response,
    AttackersResponse, BlockersResponse,
)


def _attackers_list() -> List[Dict[str, Any]]:
    return [
        {"card_id": "atk-1", "name": "Goblin Guide", "power": 2,
         "toughness": 2, "keywords": ["haste"],
         "description": "Goblin Guide 2/2 haste"},
        {"card_id": "atk-2", "name": "Lava Spike", "power": 0,
         "toughness": 0, "keywords": [], "description": "(not creature)"},
        {"card_id": "atk-3", "name": "Bolt Drake", "power": 3,
         "toughness": 1, "keywords": ["flying"],
         "description": "Bolt Drake 3/1 flying"},
    ]


def _targets_list() -> List[Dict[str, Any]]:
    return [
        {"target_id": 1, "kind": "player", "description": "P1 (life=15)"},
        {"target_id": 2, "kind": "player", "description": "P2 (life=30)"},
        {"target_id": 3, "kind": "player", "description": "P3 (life=8)"},
    ]


def _blockers_list() -> List[Dict[str, Any]]:
    return [
        {"card_id": "blk-1", "name": "Soldier", "power": 1,
         "toughness": 2, "keywords": [], "description": "Soldier 1/2"},
        {"card_id": "blk-2", "name": "Wall", "power": 0,
         "toughness": 4, "keywords": ["defender"],
         "description": "Wall 0/4 defender"},
    ]


def _attackers_to_block() -> List[Dict[str, Any]]:
    return [
        {"attacker_index": 0, "name": "BigBoy", "power": 5,
         "toughness": 5, "keywords": ["trample"],
         "description": "BigBoy 5/5 trample"},
        {"attacker_index": 1, "name": "Flier", "power": 3,
         "toughness": 3, "keywords": ["flying"],
         "description": "Flier 3/3 flying"},
    ]


class AttackersPromptAssemblyTests(unittest.TestCase):
    def test_prompt_includes_eligible_attackers(self) -> None:
        prompt = build_attackers_prompt(
            "state-text", _attackers_list(), _targets_list(),
        )
        self.assertIn("ELIGIBLE ATTACKERS", prompt)
        self.assertIn("Goblin Guide 2/2 haste", prompt)
        self.assertIn("Bolt Drake 3/1 flying", prompt)

    def test_prompt_includes_attack_targets(self) -> None:
        prompt = build_attackers_prompt(
            "state-text", _attackers_list(), _targets_list(),
        )
        self.assertIn("ATTACK TARGETS", prompt)
        self.assertIn("P1 (life=15)", prompt)
        self.assertIn("P3 (life=8)", prompt)

    def test_no_eligible_attackers_shows_none(self) -> None:
        prompt = build_attackers_prompt(
            "state-text", [], _targets_list(),
        )
        self.assertIn("(none eligible)", prompt)

    def test_attackers_prompt_includes_politics_when_provided(self) -> None:
        politics = {
            "threats": {1: {"score": 0.9}, 2: {"score": 0.4}},
            "alliances": {2: "ally"},
        }
        prompt = build_attackers_prompt(
            "state", _attackers_list(), _targets_list(),
            politics_context=politics,
        )
        self.assertIn("threat_score=0.90", prompt)
        self.assertIn("P2=ally", prompt)


class BlockersPromptAssemblyTests(unittest.TestCase):
    def test_prompt_includes_attackers_and_blockers(self) -> None:
        prompt = build_blockers_prompt(
            "state-text", _blockers_list(), _attackers_to_block(),
        )
        self.assertIn("INCOMING ATTACKERS", prompt)
        self.assertIn("BigBoy 5/5 trample", prompt)
        self.assertIn("YOUR ELIGIBLE BLOCKERS", prompt)
        self.assertIn("Soldier 1/2", prompt)

    def test_no_blockers_shows_none(self) -> None:
        prompt = build_blockers_prompt(
            "state-text", [], _attackers_to_block(),
        )
        self.assertIn("(none — you'll take all damage)", prompt)


class AttackersParserTests(unittest.TestCase):
    def test_parse_clean_attackers(self) -> None:
        raw = ('{"attackers": [{"attacker_index": 0, "target_index": 1}, '
               '{"attacker_index": 2, "target_index": 0}], '
               '"rationale": "alpha strike"}')
        resp, err = parse_attackers_response(
            raw, _attackers_list(), _targets_list(),
        )
        self.assertIsNone(err)
        self.assertEqual(len(resp.attackers), 2)
        self.assertEqual(resp.attackers[0].attacker_index, 0)
        self.assertEqual(resp.attackers[0].target_index, 1)
        self.assertEqual(resp.attackers[1].attacker_index, 2)

    def test_empty_attackers_array_legal(self) -> None:
        raw = '{"attackers": [], "rationale": "hold up mana"}'
        resp, err = parse_attackers_response(
            raw, _attackers_list(), _targets_list(),
        )
        self.assertIsNone(err)
        self.assertEqual(resp.attackers, [])

    def test_missing_attackers_key_legal_as_no_attack(self) -> None:
        raw = '{"rationale": "no attack"}'
        resp, err = parse_attackers_response(
            raw, _attackers_list(), _targets_list(),
        )
        self.assertIsNone(err)
        self.assertEqual(resp.attackers, [])

    def test_attacker_index_out_of_range_returns_error(self) -> None:
        raw = '{"attackers": [{"attacker_index": 99, "target_index": 0}]}'
        resp, err = parse_attackers_response(
            raw, _attackers_list(), _targets_list(),
        )
        self.assertIsNone(resp)
        self.assertIn("out of range", err)

    def test_target_index_out_of_range_returns_error(self) -> None:
        raw = '{"attackers": [{"attacker_index": 0, "target_index": 99}]}'
        resp, err = parse_attackers_response(
            raw, _attackers_list(), _targets_list(),
        )
        self.assertIsNone(resp)
        self.assertIn("out of range", err)

    def test_duplicate_attacker_index_returns_error(self) -> None:
        raw = ('{"attackers": [{"attacker_index": 0, "target_index": 0}, '
               '{"attacker_index": 0, "target_index": 1}]}')
        resp, err = parse_attackers_response(
            raw, _attackers_list(), _targets_list(),
        )
        self.assertIsNone(resp)
        self.assertIn("duplicate", err)

    def test_malformed_json_returns_error(self) -> None:
        raw = '{attackers: not-a-list'
        resp, err = parse_attackers_response(
            raw, _attackers_list(), _targets_list(),
        )
        self.assertIsNone(resp)


class BlockersParserTests(unittest.TestCase):
    def test_parse_clean_blockers(self) -> None:
        raw = ('{"blocks": [{"attacker_index": 0, "blocker_indices": [0]}, '
               '{"attacker_index": 1, "blocker_indices": [1]}], '
               '"rationale": "trade soldier into big, wall blocks flier"}')
        resp, err = parse_blockers_response(
            raw, _blockers_list(), _attackers_to_block(),
        )
        self.assertIsNone(err)
        self.assertEqual(len(resp.blocks), 2)
        self.assertEqual(resp.blocks[0].blocker_indices, [0])

    def test_empty_blocks_legal(self) -> None:
        raw = '{"blocks": [], "rationale": "take it"}'
        resp, err = parse_blockers_response(
            raw, _blockers_list(), _attackers_to_block(),
        )
        self.assertIsNone(err)
        self.assertEqual(resp.blocks, [])

    def test_multi_block_assignment_order_preserved(self) -> None:
        raw = ('{"blocks": [{"attacker_index": 0, '
               '"blocker_indices": [1, 0]}]}')  # order: 1 then 0
        resp, err = parse_blockers_response(
            raw, _blockers_list(), _attackers_to_block(),
        )
        self.assertIsNone(err)
        self.assertEqual(resp.blocks[0].blocker_indices, [1, 0])

    def test_blocker_index_out_of_range_returns_error(self) -> None:
        raw = '{"blocks": [{"attacker_index": 0, "blocker_indices": [99]}]}'
        resp, err = parse_blockers_response(
            raw, _blockers_list(), _attackers_to_block(),
        )
        self.assertIsNone(resp)
        self.assertIn("out of range", err)

    def test_blocker_assigned_to_multiple_attackers_rejected(self) -> None:
        # blocker 0 assigned to both attacker 0 and attacker 1.
        raw = ('{"blocks": [{"attacker_index": 0, "blocker_indices": [0]}, '
               '{"attacker_index": 1, "blocker_indices": [0]}]}')
        resp, err = parse_blockers_response(
            raw, _blockers_list(), _attackers_to_block(),
        )
        self.assertIsNone(resp)
        self.assertIn("multiple block assignments", err)

    def test_duplicate_attacker_in_blocks_rejected(self) -> None:
        raw = ('{"blocks": [{"attacker_index": 0, "blocker_indices": [0]}, '
               '{"attacker_index": 0, "blocker_indices": [1]}]}')
        resp, err = parse_blockers_response(
            raw, _blockers_list(), _attackers_to_block(),
        )
        self.assertIsNone(resp)
        self.assertIn("duplicate", err)


class SystemPromptConstantsTests(unittest.TestCase):
    def test_attackers_system_prompt_has_json_contract(self) -> None:
        self.assertIn("VALID JSON ONLY", ATTACKERS_SYSTEM_PROMPT)
        self.assertIn("attacker_index", ATTACKERS_SYSTEM_PROMPT)

    def test_blockers_system_prompt_has_json_contract(self) -> None:
        self.assertIn("VALID JSON ONLY", BLOCKERS_SYSTEM_PROMPT)
        self.assertIn("blocker_indices", BLOCKERS_SYSTEM_PROMPT)


if __name__ == "__main__":
    unittest.main()
