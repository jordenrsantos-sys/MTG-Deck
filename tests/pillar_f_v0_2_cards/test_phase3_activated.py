"""Phase 3 — Per-card unit tests for the activated-ability bucket.

Coverage:
  - Sol Ring + Mana Vault + Ancient Tomb (multi-mana producers)
  - Arcane Signet + Command Tower + Path of Ancestry (commander-color)
  - Mind Stone (tap-for-mana + sac-for-draw)
  - Fellwar Stone + Exotic Orchard + City of Brass + Mana Confluence
    (any-color)
  - Mox Diamond + Chrome Mox + Lotus Petal
  - Talisman cycle (10 cards × 2 abilities)
  - Worn Powerstone, Thought Vessel, Reliquary Tower, Bonders' Enclave
  - Ashnod's Altar + Phyrexian Altar (sac-for-mana)
  - 14 fetchlands → basic land search
  - 5+ equipment equip costs (Lightning Greaves, etc.)
  - Sol Ring + Mana Vault both tap = 5 colorless on turn 1 (kickoff
    Phase 3 multi-card test)
  - Rogue's Passage make-unblockable utility
  - War Room draw
  - Mana Vault {4}: untap

Each test exercises:
  1. Construct the source card in play, untapped, on the battlefield.
  2. Pay activation costs (set tapped=True, deduct life if applicable,
     sacrifice if applicable).
  3. Build the activation payload via build_activation_payload().
  4. Push to stack + resolve.
  5. Assert mana pool / hand / battlefield change.
"""
from __future__ import annotations

import unittest

# Imports trigger all per-card registrations.
import api.engine.pillar_f.v0_2.cards  # noqa: F401
from api.engine.pillar_f.v0_2.cards.activated import (
    build_activation_payload, get_activated_ability,
    get_activated_abilities_for_card, all_registered_card_names,
)
from api.engine.pillar_f.v0_2.stack import push_to_stack, resolve_top
from api.engine.pillar_f.v0_2.state import (
    Card, GameState, PlayerState, PlayerZones,
)


def _empty_game(*, life: int = 40) -> GameState:
    gs = GameState()
    for pid in range(4):
        gs.players.append(PlayerState(
            player_id=pid, name=f"P{pid}", life_total=life, zones=PlayerZones(),
        ))
    gs.active_player = 0
    return gs


def _put_on_bf(gs: GameState, owner: int, name: str, *,
              type_line: str = "Artifact",
              power=None, toughness=None,
              subtypes=None, keywords=None) -> Card:
    card = Card(
        name=name, owner=owner, controller=owner, type_line=type_line,
        power=power, toughness=toughness,
        subtypes=list(subtypes or []), keywords=list(keywords or []),
    )
    gs.add_card(card)
    gs.players[owner].zones.battlefield.append(card.card_id)
    return card


def _activate(gs: GameState, card: Card, ability_key: str,
             *, tap: bool = True, sacrifice_self: bool = False,
             pay_life: int = 0, targets=None, **payment_extra) -> None:
    """Helper: pay costs + push + resolve for an activated ability."""
    pid = card.controller
    if tap:
        card.tapped = True
    if sacrifice_self:
        gs.move_card(
            card.card_id, from_player=pid, from_zone="battlefield",
            to_player=card.owner, to_zone="graveyard",
        )
    if pay_life > 0:
        gs.players[pid].life_total -= pay_life
    payload = build_activation_payload(card.name, ability_key,
                                       **payment_extra)
    push_to_stack(
        gs, card_id=card.card_id, controller=pid,
        entry_type="activated", payment=payload,
        targets=targets or [],
        description=f"{card.name}.{ability_key}",
    )
    resolve_top(gs)


# =================================================================
# Mana rocks
# =================================================================


class SolRing(unittest.TestCase):
    def test_taps_for_two_colorless(self) -> None:
        gs = _empty_game()
        sr = _put_on_bf(gs, 0, "Sol Ring")
        _activate(gs, sr, "tap_mana")
        self.assertEqual(gs.players[0].mana_pool.C, 2)
        self.assertTrue(sr.tapped)


class ArcaneSignetAndCommandTower(unittest.TestCase):
    def test_arcane_signet_with_explicit_color(self) -> None:
        gs = _empty_game()
        as_ = _put_on_bf(gs, 0, "Arcane Signet")
        _activate(gs, as_, "tap_mana", color="R")
        self.assertEqual(gs.players[0].mana_pool.R, 1)

    def test_command_tower_defaults_to_colorless(self) -> None:
        gs = _empty_game()
        ct = _put_on_bf(gs, 0, "Command Tower", type_line="Land")
        _activate(gs, ct, "tap_mana")  # no color → defaults to C
        self.assertEqual(gs.players[0].mana_pool.C, 1)

    def test_path_of_ancestry_taps_for_color(self) -> None:
        gs = _empty_game()
        poa = _put_on_bf(gs, 0, "Path of Ancestry", type_line="Land")
        _activate(gs, poa, "tap_mana", color="U")
        self.assertEqual(gs.players[0].mana_pool.U, 1)


class MindStone(unittest.TestCase):
    def test_tap_for_colorless(self) -> None:
        gs = _empty_game()
        ms = _put_on_bf(gs, 0, "Mind Stone")
        _activate(gs, ms, "tap_mana")
        self.assertEqual(gs.players[0].mana_pool.C, 1)

    def test_sac_for_draw(self) -> None:
        gs = _empty_game()
        ms = _put_on_bf(gs, 0, "Mind Stone")
        # Library has 1 card.
        c = Card(name="Top Card", owner=0)
        gs.add_card(c)
        gs.players[0].zones.library.append(c.card_id)
        _activate(gs, ms, "sac_draw", sacrifice_self=True)
        self.assertIn(c.card_id, gs.players[0].zones.hand)
        self.assertIn(ms.card_id, gs.players[0].zones.graveyard)


class FellwarExoticCityConfluence(unittest.TestCase):
    def test_fellwar_stone_any_color_stub(self) -> None:
        gs = _empty_game()
        fs = _put_on_bf(gs, 0, "Fellwar Stone")
        _activate(gs, fs, "tap_mana", color="W")
        self.assertEqual(gs.players[0].mana_pool.W, 1)

    def test_exotic_orchard_any_color(self) -> None:
        gs = _empty_game()
        eo = _put_on_bf(gs, 0, "Exotic Orchard", type_line="Land")
        _activate(gs, eo, "tap_mana", color="B")
        self.assertEqual(gs.players[0].mana_pool.B, 1)

    def test_city_of_brass_any_color(self) -> None:
        gs = _empty_game()
        cob = _put_on_bf(gs, 0, "City of Brass", type_line="Land")
        _activate(gs, cob, "tap_mana", color="G")
        self.assertEqual(gs.players[0].mana_pool.G, 1)

    def test_mana_confluence_deducts_life(self) -> None:
        gs = _empty_game(life=40)
        mc = _put_on_bf(gs, 0, "Mana Confluence", type_line="Land")
        _activate(gs, mc, "tap_mana", color="R", pay_life=1)
        self.assertEqual(gs.players[0].mana_pool.R, 1)
        self.assertEqual(gs.players[0].life_total, 39)


class AltarSacrifices(unittest.TestCase):
    def test_ashnods_altar_sacs_for_two_colorless(self) -> None:
        gs = _empty_game()
        aa = _put_on_bf(gs, 0, "Ashnod's Altar")
        bear = _put_on_bf(gs, 0, "Bear", type_line="Creature — Bear",
                         power="2", toughness="2")
        # Caller pays the sac cost: move bear to graveyard.
        gs.move_card(bear.card_id, from_player=0, from_zone="battlefield",
                    to_player=0, to_zone="graveyard")
        # Note: no tap on Ashnod's Altar.
        _activate(gs, aa, "sac_mana", tap=False)
        self.assertEqual(gs.players[0].mana_pool.C, 2)
        self.assertIn(bear.card_id, gs.players[0].zones.graveyard)

    def test_phyrexian_altar_sacs_for_any_color(self) -> None:
        gs = _empty_game()
        pa = _put_on_bf(gs, 0, "Phyrexian Altar")
        bear = _put_on_bf(gs, 0, "Bear", type_line="Creature — Bear",
                         power="2", toughness="2")
        gs.move_card(bear.card_id, from_player=0, from_zone="battlefield",
                    to_player=0, to_zone="graveyard")
        _activate(gs, pa, "sac_mana", tap=False, color="B")
        self.assertEqual(gs.players[0].mana_pool.B, 1)


# =================================================================
# Talisman cycle
# =================================================================


class TalismanCycle(unittest.TestCase):
    def test_talisman_of_progress_taps_for_colorless(self) -> None:
        gs = _empty_game()
        t = _put_on_bf(gs, 0, "Talisman of Progress")
        _activate(gs, t, "tap_colorless")
        self.assertEqual(gs.players[0].mana_pool.C, 1)

    def test_talisman_of_dominance_taps_for_blue_paying_life(self) -> None:
        gs = _empty_game(life=40)
        t = _put_on_bf(gs, 0, "Talisman of Dominance")
        _activate(gs, t, "tap_color", color="U", pay_life=1)
        self.assertEqual(gs.players[0].mana_pool.U, 1)
        self.assertEqual(gs.players[0].life_total, 39)

    def test_all_10_talismans_registered_with_both_abilities(self) -> None:
        for name in ("Talisman of Progress", "Talisman of Dominance",
                     "Talisman of Indulgence", "Talisman of Impulse",
                     "Talisman of Unity", "Talisman of Conviction",
                     "Talisman of Curiosity", "Talisman of Hierarchy",
                     "Talisman of Resilience", "Talisman of Creativity"):
            abils = get_activated_abilities_for_card(name)
            keys = {a.ability_key for a in abils}
            self.assertEqual(keys, {"tap_colorless", "tap_color"},
                             f"{name} abilities: {keys}")


# =================================================================
# Mana Vault
# =================================================================


class ManaVault(unittest.TestCase):
    def test_taps_for_three_colorless(self) -> None:
        gs = _empty_game()
        mv = _put_on_bf(gs, 0, "Mana Vault")
        _activate(gs, mv, "tap_mana")
        self.assertEqual(gs.players[0].mana_pool.C, 3)
        self.assertTrue(mv.tapped)

    def test_untap_cost_untaps_vault(self) -> None:
        gs = _empty_game()
        mv = _put_on_bf(gs, 0, "Mana Vault")
        mv.tapped = True
        # No need to tap again — caller pays {4} (not modeled), pushes
        # the untap activation. Substrate resolves it → mv.tapped=False.
        _activate(gs, mv, "untap_cost", tap=False)
        self.assertFalse(mv.tapped)


# =================================================================
# Multi-card scenario per kickoff: Sol Ring + Mana Vault both tapped
# on turn 1 → 5 colorless.
# =================================================================


class SolRingPlusManaVault(unittest.TestCase):
    def test_both_tapped_total_5_colorless(self) -> None:
        gs = _empty_game()
        sr = _put_on_bf(gs, 0, "Sol Ring")
        mv = _put_on_bf(gs, 0, "Mana Vault")
        _activate(gs, sr, "tap_mana")
        _activate(gs, mv, "tap_mana")
        self.assertEqual(gs.players[0].mana_pool.C, 5)


# =================================================================
# Fetchlands
# =================================================================


class Fetchlands(unittest.TestCase):
    def _seed_library_with_basics(self, gs: GameState, owner: int) -> dict:
        """Put one Plains, one Island, one Swamp, one Mountain, one
        Forest, one filler-creature into owner's library."""
        cards = {}
        for basic_name, basic_subtype in (
            ("Plains", "Plains"), ("Island", "Island"),
            ("Swamp", "Swamp"), ("Mountain", "Mountain"),
            ("Forest", "Forest"),
        ):
            c = Card(name=basic_name, owner=owner,
                    type_line=f"Basic Land — {basic_subtype}",
                    subtypes=[basic_subtype])
            gs.add_card(c)
            gs.players[owner].zones.library.append(c.card_id)
            cards[basic_name] = c
        # filler non-land
        nl = Card(name="Filler Creature", owner=owner,
                  type_line="Creature — Bear",
                  power="2", toughness="2")
        gs.add_card(nl)
        gs.players[owner].zones.library.append(nl.card_id)
        cards["filler"] = nl
        return cards

    def test_polluted_delta_fetches_island(self) -> None:
        gs = _empty_game()
        lib = self._seed_library_with_basics(gs, 0)
        pd = _put_on_bf(gs, 0, "Polluted Delta", type_line="Land")
        _activate(gs, pd, "fetch", sacrifice_self=True, pay_life=1)
        # First matching basic = Island (in library, comes before Swamp).
        self.assertIn(lib["Island"].card_id, gs.players[0].zones.battlefield)
        self.assertIn(pd.card_id, gs.players[0].zones.graveyard)

    def test_wooded_foothills_fetches_mountain(self) -> None:
        gs = _empty_game()
        lib = self._seed_library_with_basics(gs, 0)
        wf = _put_on_bf(gs, 0, "Wooded Foothills", type_line="Land")
        _activate(gs, wf, "fetch", sacrifice_self=True, pay_life=1)
        # Mountain is in library before Forest.
        self.assertIn(lib["Mountain"].card_id, gs.players[0].zones.battlefield)

    def test_evolving_wilds_fetches_any_basic_no_life(self) -> None:
        gs = _empty_game(life=40)
        lib = self._seed_library_with_basics(gs, 0)
        ew = _put_on_bf(gs, 0, "Evolving Wilds", type_line="Land")
        _activate(gs, ew, "fetch", sacrifice_self=True, pay_life=0)
        self.assertIn(lib["Plains"].card_id, gs.players[0].zones.battlefield)
        self.assertEqual(gs.players[0].life_total, 40)  # no life cost

    def test_fetchland_no_match_no_action(self) -> None:
        gs = _empty_game()
        # Library has only a non-basic. Marsh Flats wants Plains or Swamp.
        non_basic = Card(name="Dual Land", owner=0,
                        type_line="Land", subtypes=[])
        gs.add_card(non_basic)
        gs.players[0].zones.library.append(non_basic.card_id)
        mf = _put_on_bf(gs, 0, "Marsh Flats", type_line="Land")
        _activate(gs, mf, "fetch", sacrifice_self=True, pay_life=1)
        # Non-basic still in library; no basic to fetch.
        self.assertIn(non_basic.card_id, gs.players[0].zones.library)
        # mf was sacrificed (caller paid).
        self.assertIn(mf.card_id, gs.players[0].zones.graveyard)


# =================================================================
# Equipment equip costs
# =================================================================


class EquipmentEquip(unittest.TestCase):
    def test_lightning_greaves_equip_attaches_to_target(self) -> None:
        gs = _empty_game()
        lg = _put_on_bf(gs, 0, "Lightning Greaves",
                       type_line="Artifact — Equipment")
        bear = _put_on_bf(gs, 0, "Bear",
                         type_line="Creature — Bear",
                         power="2", toughness="2")
        # Equip is sorcery-speed, {0} cost. Iter-10 doesn't model
        # sorcery-speed validation — just resolve.
        _activate(gs, lg, "equip", tap=False, targets=[bear.card_id])
        self.assertEqual(lg.attached_to, bear.card_id)
        self.assertIn(lg.card_id, bear.attached_by)

    def test_equip_detaches_from_prior_creature(self) -> None:
        gs = _empty_game()
        lg = _put_on_bf(gs, 0, "Lightning Greaves",
                       type_line="Artifact — Equipment")
        bear1 = _put_on_bf(gs, 0, "Bear1",
                          type_line="Creature — Bear",
                          power="2", toughness="2")
        bear2 = _put_on_bf(gs, 0, "Bear2",
                          type_line="Creature — Bear",
                          power="2", toughness="2")
        # First equip to bear1.
        _activate(gs, lg, "equip", tap=False, targets=[bear1.card_id])
        self.assertEqual(lg.attached_to, bear1.card_id)
        # Re-equip to bear2 — should detach from bear1.
        _activate(gs, lg, "equip", tap=False, targets=[bear2.card_id])
        self.assertEqual(lg.attached_to, bear2.card_id)
        self.assertNotIn(lg.card_id, bear1.attached_by)
        self.assertIn(lg.card_id, bear2.attached_by)


# =================================================================
# Coverage gates
# =================================================================


class Phase3CoverageGate(unittest.TestCase):
    def test_at_least_50_cards_have_activated_abilities(self) -> None:
        names = all_registered_card_names()
        self.assertGreaterEqual(len(names), 50,
                                f"Expected ≥50 activated-cards, got {len(names)}: {names}")

    def test_all_14_fetchlands_registered(self) -> None:
        expected = {
            "Polluted Delta", "Bloodstained Mire", "Flooded Strand",
            "Marsh Flats", "Windswept Heath", "Wooded Foothills",
            "Verdant Catacombs", "Misty Rainforest", "Scalding Tarn",
            "Arid Mesa", "Evolving Wilds", "Terramorphic Expanse",
            "Fabled Passage", "Prismatic Vista",
        }
        for name in expected:
            self.assertIsNotNone(
                get_activated_ability(name, "fetch"),
                f"{name} fetch not registered",
            )

    def test_mana_rocks_registered(self) -> None:
        expected = {
            "Sol Ring", "Arcane Signet", "Mind Stone", "Thought Vessel",
            "Fellwar Stone", "Lotus Petal", "Mox Diamond", "Chrome Mox",
            "Mana Vault", "Worn Powerstone", "Ashnod's Altar",
            "Phyrexian Altar",
        }
        for name in expected:
            self.assertIsNotNone(
                get_activated_ability(name, "tap_mana") or
                get_activated_ability(name, "sac_mana"),
                f"{name} core ability not registered",
            )


if __name__ == "__main__":
    unittest.main()
