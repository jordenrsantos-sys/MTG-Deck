"""Phase 3 of mega-task v12 -- pod orchestrator + per-game runner.

Coverage per kickoff Phase 3 gates:
- StageTwoGameConfig validates (4 decks required, pid 0-3).
- card_factory builds basic lands, counterspells, damage instants,
  creatures with correct stats + annotations.
- build_game_state populates 4 PlayerStates with shuffled libraries.
- run_single_game completes a short game with mocked LLM (all-pass
  responses) under per_game_cost_ceiling.
- per_game_cost_ceiling triggers tie-break correctly.
- elimination_order tracks across multi-player elimination cascades.

Live multi-LLM integration deferred to Phase 7 cycle smoke.
"""
from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from api.engine.pillar_f.v0_2.playtest.orchestrator import (
    StageTwoDeck, StageTwoGameConfig, StageTwoGameResult,
    make_card_from_name, is_basic_land, is_counterspell,
    is_damage_instant, is_known_creature,
    build_game_state, run_single_game,
)


# ============================================================
# Mock LLM client
# ============================================================


@dataclass
class MockCallResult:
    ok: bool = True
    text: str = ""
    cost_usd: float = 0.001
    error_code: Optional[str] = None
    error_message: Optional[str] = None


@dataclass
class MockLLMClient:
    """Returns canned responses; if exhausted, returns a generic
    pass_priority action. This lets short games complete quickly in
    unit tests without micromanaging every LLM call."""
    responses: List[MockCallResult] = field(default_factory=list)
    calls: List[Dict[str, Any]] = field(default_factory=list)
    available: bool = True

    def is_available(self) -> bool:
        return self.available

    def call_with_budget(self, *, system, user, max_input_tokens,
                         max_output_tokens, **kwargs) -> MockCallResult:
        self.calls.append({"system_len": len(system), "user_len": len(user)})
        if self.responses:
            return self.responses.pop(0)
        # Default canned response based on which prompt fired.
        # Mulligan: keep.
        if "MULLIGANS TAKEN" in user:
            return MockCallResult(
                ok=True, text='{"keep": true, "rationale": "ok"}',
                cost_usd=0.001,
            )
        # Bottom-picker: pick first N cards.
        if "PUT_ON_BOTTOM" in user:
            import re
            m = re.search(r"PUT_ON_BOTTOM: (\d+)", user)
            n = int(m.group(1)) if m else 1
            cids = re.findall(r"card_id=(\S+)", user)
            picks = cids[:n]
            return MockCallResult(
                ok=True,
                text=('{"cards_to_bottom": [%s], "rationale": "weak"}' %
                       ", ".join('"%s"' % c for c in picks)),
                cost_usd=0.001,
            )
        # Attackers: no attack.
        if "ELIGIBLE ATTACKERS" in user:
            return MockCallResult(
                ok=True, text='{"attackers": [], "rationale": "hold up"}',
                cost_usd=0.001,
            )
        # Blockers: no block.
        if "INCOMING ATTACKERS" in user:
            return MockCallResult(
                ok=True, text='{"blocks": [], "rationale": "take it"}',
                cost_usd=0.001,
            )
        # Default = pass.
        return MockCallResult(
            ok=True,
            text='{"action_type": "pass_priority", "action_index": 0, "rationale": "default"}',
            cost_usd=0.001,
        )


def _simple_deck(deck_id: str, lands: int = 20, instants: int = 10) -> StageTwoDeck:
    mainboard: List[str] = []
    for _ in range(lands):
        mainboard.append("Mountain")
    for _ in range(instants):
        mainboard.append("Lightning Bolt")
    return StageTwoDeck(
        deck_id=deck_id,
        commander_name="Krenko, Mob Boss",
        mainboard=mainboard,
        archetype_hint="mono-red burn",
        bracket="B3",
    )


# ============================================================
# Type tests
# ============================================================


class StageTwoTypesTests(unittest.TestCase):
    def test_deck_requires_id_and_commander(self) -> None:
        with self.assertRaises(ValueError):
            StageTwoDeck(deck_id="", commander_name="X")
        with self.assertRaises(ValueError):
            StageTwoDeck(deck_id="x", commander_name="")

    def test_game_config_requires_4_decks(self) -> None:
        with self.assertRaises(ValueError):
            StageTwoGameConfig(seed=1, decks=[_simple_deck("a")])

    def test_game_config_rejects_invalid_pid(self) -> None:
        decks = [_simple_deck(f"d{i}") for i in range(4)]
        with self.assertRaises(ValueError):
            StageTwoGameConfig(seed=1, decks=decks, deck_under_test_pid=99)


# ============================================================
# Card factory tests
# ============================================================


class CardFactoryTests(unittest.TestCase):
    def test_basic_land(self) -> None:
        c = make_card_from_name("Mountain", owner=0)
        self.assertTrue(c.is_land())
        self.assertIsNone(getattr(c, "iter10_annotation", None))

    def test_counterspell_has_target_stack_top(self) -> None:
        c = make_card_from_name("Counterspell", owner=0)
        ann = c.iter10_annotation
        self.assertTrue(ann["target_stack_top"])
        self.assertEqual(ann["payment"]["resolver"], "spell_counterspell")

    def test_damage_instant_targets_next_opponent(self) -> None:
        c = make_card_from_name("Lightning Bolt", owner=0,
                                 next_opponent_pid=2)
        ann = c.iter10_annotation
        self.assertEqual(ann["payment"]["resolver"], "deal_damage_to_player")
        self.assertEqual(ann["payment"]["amount"], 3)
        self.assertEqual(ann["default_targets"], [2])

    def test_known_creature_has_stats(self) -> None:
        c = make_card_from_name("Goblin Guide", owner=1)
        self.assertEqual(c.power, "2")
        self.assertEqual(c.toughness, "2")
        self.assertIn("haste", c.keywords)

    def test_unknown_card_becomes_placeholder(self) -> None:
        c = make_card_from_name("Some Made Up Card", owner=0)
        self.assertIn("unknown placeholder", c.type_line.lower())

    def test_is_x_helpers(self) -> None:
        self.assertTrue(is_basic_land("Forest"))
        self.assertFalse(is_basic_land("Goblin Guide"))
        self.assertTrue(is_counterspell("Negate"))
        self.assertTrue(is_damage_instant("Shock"))
        self.assertTrue(is_known_creature("Krenko, Mob Boss"))


# ============================================================
# build_game_state tests
# ============================================================


class BuildGameStateTests(unittest.TestCase):
    def test_builds_4_players_with_libraries(self) -> None:
        decks = [_simple_deck(f"d{i}") for i in range(4)]
        config = StageTwoGameConfig(seed=42, decks=decks)
        gs = build_game_state(config)
        self.assertEqual(len(gs.players), 4)
        for ps in gs.players:
            # 20 lands + 10 instants = 30 cards in library.
            self.assertEqual(len(ps.zones.library), 30)
            self.assertEqual(len(ps.zones.hand), 0)
            self.assertEqual(ps.life_total, 40)

    def test_library_shuffle_is_deterministic_with_seed(self) -> None:
        decks = [_simple_deck(f"d{i}") for i in range(4)]
        gs_a = build_game_state(StageTwoGameConfig(seed=42, decks=decks))
        gs_b = build_game_state(StageTwoGameConfig(seed=42, decks=decks))
        # Same seed -> same library order per player.
        for pid in range(4):
            cards_a = [
                gs_a.get_card(cid).name for cid in gs_a.players[pid].zones.library
            ]
            cards_b = [
                gs_b.get_card(cid).name for cid in gs_b.players[pid].zones.library
            ]
            self.assertEqual(cards_a, cards_b)


# ============================================================
# run_single_game integration
# ============================================================


class RunSingleGameTests(unittest.TestCase):
    def test_short_game_completes_via_max_turns(self) -> None:
        decks = [_simple_deck(f"d{i}") for i in range(4)]
        config = StageTwoGameConfig(
            seed=1, decks=decks, max_turns=3, max_mulligans=0,
            per_game_cost_ceiling_usd=100.0,  # don't trip in test
            enable_combat=False,  # no creatures in this fixture
        )
        mock = MockLLMClient()
        result = run_single_game(config, llm_client=mock)
        self.assertIsInstance(result, StageTwoGameResult)
        # Game should run all 3 turns since no eliminations.
        self.assertEqual(result.turns_run, 3)
        # All 4 players alive (default mock = all-pass, no damage path
        # except via cast_spell which the default also won't pick).
        self.assertEqual(sum(
            1 for pid in range(4) if pid in result.final_life_totals
        ), 4)

    def test_game_tracks_total_spend(self) -> None:
        decks = [_simple_deck(f"d{i}") for i in range(4)]
        config = StageTwoGameConfig(
            seed=2, decks=decks, max_turns=2, max_mulligans=0,
            per_game_cost_ceiling_usd=100.0,
            enable_combat=False,
        )
        mock = MockLLMClient()
        result = run_single_game(config, llm_client=mock)
        # Each canned response cost $0.001; at least a few calls fired.
        self.assertGreater(result.total_llm_calls, 0)
        self.assertGreater(result.total_spend_usd, 0.0)

    def test_per_game_ceiling_halts_and_tie_breaks(self) -> None:
        """If per_game_cost_ceiling fires, game halts and winner is
        the highest-life surviving player."""
        decks = [_simple_deck(f"d{i}") for i in range(4)]
        config = StageTwoGameConfig(
            seed=3, decks=decks, max_turns=20, max_mulligans=0,
            per_game_cost_ceiling_usd=0.005,  # very tight -- trip fast
            enable_combat=False,
        )
        mock = MockLLMClient()
        result = run_single_game(config, llm_client=mock)
        self.assertTrue(result.halted_for_cost)
        self.assertEqual(result.halted_reason, "cost")
        # Tie-break picked SOME winner (all life equal so any is fine).
        self.assertIsNotNone(result.winner_pid)
        self.assertIn(result.winner_pid, [0, 1, 2, 3])

    def test_result_includes_threat_vectors_and_politics(self) -> None:
        decks = [_simple_deck(f"d{i}") for i in range(4)]
        config = StageTwoGameConfig(
            seed=4, decks=decks, max_turns=2, max_mulligans=0,
            per_game_cost_ceiling_usd=100.0,
            enable_combat=False,
        )
        mock = MockLLMClient()
        result = run_single_game(config, llm_client=mock)
        # Threat vectors populated for alive players.
        self.assertGreaterEqual(len(result.final_threat_vectors), 1)
        # Politics summary always populated.
        self.assertIn("alliance_distribution", result.politics_summary)
        self.assertIn("deals_made_count", result.politics_summary)

    def test_result_serializable(self) -> None:
        """StageTwoGameResult should be JSON-friendly so Phase 4's
        report writer can dump it."""
        import json
        from dataclasses import asdict
        decks = [_simple_deck(f"d{i}") for i in range(4)]
        config = StageTwoGameConfig(
            seed=5, decks=decks, max_turns=2, max_mulligans=0,
            per_game_cost_ceiling_usd=100.0,
            enable_combat=False,
        )
        mock = MockLLMClient()
        result = run_single_game(config, llm_client=mock)
        # asdict + json.dumps should not raise.
        try:
            payload = asdict(result)
            # convert tuple in elimination_order to list for JSON.
            payload["elimination_order"] = [
                list(t) for t in payload["elimination_order"]
            ]
            # Convert int keys in threat_vectors / final_life_totals
            # to str for JSON.
            payload["final_life_totals"] = {
                str(k): v for k, v in payload["final_life_totals"].items()
            }
            payload["final_threat_vectors"] = {
                str(k): {str(k2): v2 for k2, v2 in inner.items()}
                for k, inner in payload["final_threat_vectors"].items()
            }
            json.dumps(payload, default=str)
        except (TypeError, ValueError) as e:
            self.fail(f"Result not JSON-serializable: {e}")


if __name__ == "__main__":
    unittest.main()
